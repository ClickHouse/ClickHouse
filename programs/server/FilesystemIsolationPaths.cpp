#include <FilesystemIsolationPaths.h>

#include <Core/Defines.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Jemalloc.h>
#include <Common/filesystemHelpers.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <fmt/format.h>

#include <array>
#include <cstdlib>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace
{

using Access = FilesystemIsolationPath::Access;

/// The system paths every server needs, read-only. A missing one is skipped.
constexpr std::array system_paths = {
    /// Name resolution and the network services.
    "/etc/hosts",
    "/etc/resolv.conf",
    "/etc/nsswitch.conf",
    "/etc/host.conf",
    "/etc/gai.conf",
    "/etc/services",
    "/etc/protocols",
    /// The names of users and groups.
    "/etc/passwd",
    "/etc/group",
    /// The time zone of the system and the time zone database.
    "/etc/localtime",
    "/usr/share/zoneinfo",
    /// A link to `/proc/self/mounts`, which is recreated inside.
    "/etc/mtab",
    /// The CA certificates of the system, for TLS connections, and the targets of the links in them.
    "/etc/ssl",
    "/etc/pki",
    "/etc/ca-certificates",
    "/usr/share/ca-certificates",
    "/usr/local/share/ca-certificates",
    /// The separate debug info of the executable, for the symbols in stack traces.
    "/usr/lib/debug",
};

/// A path with a wildcard in it means every file matching it, so the directory up to the first wildcard is needed.
bool hasWildcard(const std::string & component)
{
    return component.find_first_of("*?[{") != std::string::npos;
}

class Collector
{
public:
    Collector(const Poco::Util::AbstractConfiguration & config_, const std::string & config_path, const std::string & working_directory_)
        : config(config_)
        , working_directory(working_directory_)
        , config_file(absolute(config_path, working_directory_))
        , config_directory(config_file.parent_path())
        , data_path(absolute(config.getString("path", DBMS_DEFAULT_PATH), working_directory))
    {
    }

    std::vector<FilesystemIsolationPath> collect() &&
    {
        for (const auto * path : system_paths)
            add(path, Access::ReadOnly, "system");

        /// The socket of the service manager, which the server notifies when it is ready (`systemdNotify`). Only the
        /// socket is visible, not its directory. A socket on a read-only mount can still be sent to. A name starting
        /// with `@` is an abstract socket, which is not in the filesystem.
        if (const char * notify_socket = std::getenv("NOTIFY_SOCKET"); notify_socket && *notify_socket == '/') // NOLINT(concurrency-mt-unsafe)
            add(notify_socket, Access::ReadOnly, "the environment variable NOTIFY_SOCKET");

        if (config.getBool("logger.use_syslog", false))
            add("/dev/log", Access::ReadOnly, "`logger.use_syslog`");

        addConfigFiles();
        addDataPaths();
        addDisks();
        addKeeper();
        addLogs();
        addCertificates();
        addBridge();
        addJemallocProfiles();
        addExtraPaths();

        return std::move(paths);
    }

private:
    const Poco::Util::AbstractConfiguration & config;
    const fs::path working_directory;
    const fs::path config_file;
    const fs::path config_directory;
    const fs::path data_path;
    std::vector<FilesystemIsolationPath> paths;

    static fs::path absolute(const std::string & path, const fs::path & base)
    {
        fs::path result(path);
        if (result.is_relative())
            result = base / result;
        result = result.lexically_normal();
        /// No trailing slash, so that equal directories compare equal.
        if (result.has_parent_path() && !result.has_filename())
            result = result.parent_path();
        return result;
    }

    void add(const fs::path & path, Access access, const std::string & source)
    {
        if (path.empty())
            return;
        paths.push_back({path.string(), access, source});
    }

    /// For a file, its directory, so that a file replaced by a new one, as a renewed certificate is, is visible.
    /// If the file is a symbolic link, the directories of the targets of the links in the chain as well: a
    /// certificate of Let's Encrypt in `live/<domain>` is a link to `../../archive/<domain>/fullchain<N>.pem`, and a
    /// renewal adds a new file next to the target and points the link at it.
    void addLocation(const fs::path & path, Access access, const std::string & source)
    {
        std::error_code error;
        if (fs::is_directory(path, error))
        {
            add(path, access, source);
            return;
        }
        add(path.parent_path(), access, source);

        /// The same limit on the number of links as in the kernel, against a loop.
        constexpr size_t max_links = 40;
        fs::path current = path;
        for (size_t i = 0; i < max_links && FS::isSymlinkNoThrow(current); ++i)
        {
            fs::path target = FS::readSymlink(current);
            if (target.is_relative())
                target = current.parent_path() / target;
            current = target.lexically_normal();
            add(current.parent_path(), access, source);
        }
    }

    /// For a path with wildcards, the directory up to the first wildcard; otherwise the location of the path.
    void addPattern(const fs::path & pattern, Access access, const std::string & source)
    {
        fs::path prefix;
        for (const auto & component : pattern)
        {
            if (hasWildcard(component.string()))
            {
                add(prefix, access, source);
                return;
            }
            prefix /= component;
        }
        addLocation(pattern, access, source);
    }

    /// Every key `name` and `name[N]` under `prefix`.
    std::vector<std::string> getMultipleKeys(const std::string & prefix, const std::string & name) const
    {
        std::vector<std::string> result;
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(prefix, keys);
        for (const auto & key : keys)
            if (key == name || key.starts_with(name + "["))
                result.push_back(prefix.empty() ? key : prefix + "." + key);
        return result;
    }

    void addKey(const std::string & key, Access access, const fs::path & base)
    {
        if (config.has(key))
            add(absolute(config.getString(key), base), access, fmt::format("`{}`", key));
    }

    void addKeyLocation(const std::string & key, Access access, const fs::path & base)
    {
        if (config.has(key))
            if (const auto value = config.getString(key); !value.empty())
                addLocation(absolute(value, base), access, fmt::format("`{}`", key));
    }

    /// A configuration file and the files merged into it from `<name>.d` and `conf.d`, with the targets of the
    /// symbolic links among them, which can be outside the directory of the configuration file. They are read again
    /// when the configuration is reloaded.
    void addConfigFileWithMergeFiles(const fs::path & file, const std::string & source)
    {
        addLocation(file, Access::ReadOnly, source);
        for (const auto & merge_file : ConfigProcessor::getConfigMergeFiles(file.string()))
            addLocation(merge_file, Access::ReadOnly, fmt::format("merged into {}", source));
    }

    /// A configuration that is processed on its own, such as a users configuration in a separate file, which
    /// `UsersConfigAccessStorage` loads with its own `ConfigProcessor`: its files, and its own substitutions file
    /// from `include_from`, which does not have to be the one of the main configuration.
    void addIndependentConfigFile(const fs::path & file, const std::string & source)
    {
        addConfigFileWithMergeFiles(file, source);
        if (file == config_file)
            return;
        std::error_code error;
        if (!fs::exists(file, error))
            return;

        /// The ZooKeeper includes are left unprocessed: there is no ZooKeeper yet, and they do not name local files.
        auto loaded = ConfigProcessor(file.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false).loadConfig(/* allow_zk_includes = */ true);
        if (loaded.configuration->has("include_from"))
            if (const auto value = loaded.configuration->getString("include_from"); !value.empty())
                addLocation(absolute(value, working_directory), Access::ReadOnly, fmt::format("`include_from` of {}", source));
    }

    void addConfigFiles()
    {
        /// The main configuration file, `config.d` and the other files next to it, watched for changes.
        add(config_directory, Access::ReadOnly, "the directory of the configuration file");
        addConfigFileWithMergeFiles(config_file, "the configuration file");
        addKeyLocation("include_from", Access::ReadOnly, working_directory);

        /// A relative path is looked up next to the configuration file first.
        auto add_users_config = [&](const std::string & key)
        {
            if (!config.has(key))
                return;
            const std::string value = config.getString(key);
            if (value.empty())
                return;
            const fs::path next_to_config = absolute(value, config_directory);
            std::error_code error;
            addIndependentConfigFile(fs::exists(next_to_config, error) ? next_to_config : absolute(value, working_directory), fmt::format("`{}`", key));
        };
        add_users_config("users_config");

        /// Every entry of `user_directories`, with the same names of the types as in
        /// `AccessControl::addStoragesFromUserDirectoriesConfig`.
        Poco::Util::AbstractConfiguration::Keys user_directories;
        config.keys("user_directories", user_directories);
        for (const auto & entry : user_directories)
        {
            std::string type = entry.substr(0, entry.find('['));
            const std::string key = fmt::format("user_directories.{}.path", entry);
            if (type == "users_xml" || type == "users.xml" || type == "users_config")
                add_users_config(key);
            else if (type == "local_directory" || type == "local")
                addKey(key, Access::ReadWrite, working_directory);
        }

        for (const auto * name : {"dictionaries_config", "user_defined_executable_functions_config", "user_defined_executable_function_drivers_config"})
            for (const auto & key : getMultipleKeys("", name))
                if (const auto pattern = config.getString(key); !pattern.empty())
                    addPattern(absolute(pattern, config_directory), Access::ReadOnly, fmt::format("`{}`", key));
    }

    void addDataPaths()
    {
        add(data_path, Access::ReadWrite, "`path`");
        addKey("tmp_path", Access::ReadWrite, data_path);
        addKey("user_files_path", Access::ReadWrite, data_path);
        addKey("format_schema_path", Access::ReadWrite, data_path);
        addKey("dynamic_user_defined_executable_functions_path", Access::ReadWrite, data_path);
        addKey("filesystem_caches_path", Access::ReadWrite, data_path);
        addKey("custom_cached_disks_base_directory", Access::ReadWrite, working_directory);
        addKey("custom_local_disks_base_directory", Access::ReadWrite, working_directory);
        addKey("access_control_path", Access::ReadWrite, working_directory);
        addKey("user_defined_path", Access::ReadWrite, working_directory);
        addKey("workload_path", Access::ReadWrite, working_directory);
        addKey("core_path", Access::ReadWrite, working_directory);

        /// They are inside `path` by default, which is not executable, and are mounted separately to be read-only and,
        /// for the scripts, executable.
        const fs::path user_scripts_path = absolute(config.getString("user_scripts_path", "user_scripts"), data_path);
        add(user_scripts_path, Access::ReadOnly, "`user_scripts_path`");
        /// Except the WebAssembly modules, which `WasmModuleManager` keeps in the subdirectory `wasm` and writes when
        /// they are inserted into or deleted from `system.webassembly_modules`. It is created before the mount.
        if (config.getBool("allow_experimental_webassembly_udf", false))
            add(user_scripts_path / "wasm", Access::ReadWrite, "the WebAssembly modules in `user_scripts_path`");
        add(absolute(config.getString("top_level_domains_path", "top_level_domains"), data_path), Access::ReadOnly, "`top_level_domains_path`");
        add(absolute(config.getString("google_protos_path", "/usr/share/clickhouse/protos/"), working_directory), Access::ReadOnly, "`google_protos_path`");

        addKeyLocation("path_to_regions_hierarchy_file", Access::ReadOnly, working_directory);
        addKeyLocation("path_to_regions_names_files", Access::ReadOnly, working_directory);
        addKeyLocation("hdfs.libhdfs3_conf", Access::ReadOnly, config_directory);
        addKeyLocation("nats.credential_file", Access::ReadOnly, working_directory);

        for (const auto & key : getMultipleKeys("backups", "allowed_path"))
            add(absolute(config.getString(key), data_path), Access::ReadWrite, fmt::format("`{}`", key));

        /// The same default as in `NamedCollectionsMetadataStorage::create`; both `local` and `local_encrypted` keep the
        /// collections in this directory.
        if (config.getString("named_collections_storage.type", "local").starts_with("local"))
            addKey("named_collections_storage.path", Access::ReadWrite, working_directory);

        Poco::Util::AbstractConfiguration::Keys caches;
        config.keys("filesystem_caches", caches);
        const fs::path caches_base = config.has("filesystem_caches_path") ? absolute(config.getString("filesystem_caches_path"), data_path) : data_path / "caches";
        for (const auto & cache : caches)
            addKey(fmt::format("filesystem_caches.{}.path", cache), Access::ReadWrite, caches_base);
    }

    void addDisks()
    {
        Poco::Util::AbstractConfiguration::Keys disks;
        config.keys("storage_configuration.disks", disks);
        const fs::path caches_base = config.has("filesystem_caches_path") ? absolute(config.getString("filesystem_caches_path"), data_path) : data_path / "caches";
        for (const auto & disk : disks)
        {
            const std::string prefix = fmt::format("storage_configuration.disks.{}", disk);
            const std::string type = config.getString(prefix + ".type", "local");
            /// The path of an encrypted disk is inside its underlying disk.
            if (type == "encrypted")
                continue;
            addKey(prefix + ".path", Access::ReadWrite, type == "cache" ? caches_base : working_directory);
            addKey(prefix + ".metadata_path", Access::ReadWrite, working_directory);

            /// An object storage disk with several locations, each of them a separate object storage, as in
            /// `registerDiskObjectStorage`. A local one keeps its objects in its own `path`.
            Poco::Util::AbstractConfiguration::Keys locations;
            config.keys(prefix + ".locations", locations);
            for (const auto & location : locations)
            {
                const std::string location_prefix = fmt::format("{}.locations.{}", prefix, location);
                /// The same keys for the type as in `ObjectStorageFactory::create`.
                const std::string location_type = config.getString(location_prefix + ".object_storage_type", config.getString(location_prefix + ".type", ""));
                if (location_type.starts_with("local"))
                    addKey(location_prefix + ".path", Access::ReadWrite, working_directory);
            }
        }
    }

    void addKeeper()
    {
        for (const auto * name : {"storage_path", "log_storage_path", "snapshot_storage_path", "data_storage_path"})
            addKey(fmt::format("keeper_server.{}", name), Access::ReadWrite, working_directory);

        /// The directory of the state file, with the same precedence as in `KeeperContext::getStatePathFromConfig`:
        /// without `storage_path` and `state_storage_disk`, it is the parent of `snapshot_storage_path` or, if that
        /// is not set, of `log_storage_path`.
        if (config.has("keeper_server.state_storage_disk") || config.has("keeper_server.storage_path"))
            return;
        for (const auto * name : {"snapshot_storage_path", "log_storage_path"})
        {
            const std::string key = fmt::format("keeper_server.{}", name);
            if (config.has(key))
            {
                add(absolute(fs::path(config.getString(key)).parent_path().string(), working_directory), Access::ReadWrite,
                    fmt::format("the state file of Keeper, next to `{}`", key));
                return;
            }
        }
    }

    void addLogs()
    {
        for (const auto * key : {"logger.log", "logger.errorlog", "logger.stderr", "logger.stdout", "pid"})
            if (config.has(key))
                if (const auto value = config.getString(key); !value.empty())
                    add(absolute(value, working_directory).parent_path(), Access::ReadWrite, fmt::format("`{}`", key));
    }

    void addCertificates()
    {
        for (const auto * side : {"server", "client"})
            for (const auto * name : {"privateKeyFile", "certificateFile", "caConfig", "dhParamsFile"})
                addKeyLocation(fmt::format("openSSL.{}.{}", side, name), Access::ReadOnly, working_directory);

        Poco::Util::AbstractConfiguration::Keys protocols;
        config.keys("protocols", protocols);
        for (const auto & protocol : protocols)
            for (const auto * name : {"privateKeyFile", "certificateFile", "caConfig", "dhParamsFile"})
                addKeyLocation(fmt::format("protocols.{}.{}", protocol, name), Access::ReadOnly, working_directory);

        for (const auto * key : {"grpc.ssl_key_file", "grpc.ssl_cert_file", "grpc.ssl_ca_cert_file", "arrowflight.ssl_cert_file", "arrowflight.ssl_key_file", "kerberos.keytab",
                                 "ssh_server.host_rsa_key", "ssh_server.host_ecdsa_key", "ssh_server.host_ed25519_key"})
            addKeyLocation(key, Access::ReadOnly, working_directory);

        Poco::Util::AbstractConfiguration::Keys ldap_servers;
        config.keys("ldap_servers", ldap_servers);
        for (const auto & server : ldap_servers)
            for (const auto * name : {"tls_cert_file", "tls_key_file", "tls_ca_cert_file", "tls_ca_cert_dir"})
                addKeyLocation(fmt::format("ldap_servers.{}.{}", server, name), Access::ReadOnly, working_directory);
    }

    /// The server starts `clickhouse-odbc-bridge` from the directory of its own executable. Only the program is
    /// visible, not the directory, which is usually `/usr/bin`.
    void addBridge()
    {
        add(fs::path(config.getString("application.dir", "/usr/bin")) / "clickhouse-odbc-bridge", Access::ReadOnly, "the ODBC bridge");
    }

    /// The directory of the heap profiles that jemalloc writes, if its option `prof_prefix` names one, for example
    /// with `MALLOC_CONF=prof_prefix:/var/log/clickhouse-server/jemalloc/clickhouse`. Without it, the profiles go to
    /// `/tmp`, which is private and writable.
    void addJemallocProfiles()
    {
#if USE_JEMALLOC
        const char * prefix = nullptr;
        if (!Jemalloc::tryGetValue("opt.prof_prefix", prefix) || !prefix)
            return;
        const fs::path prefix_path(prefix);
        if (prefix_path.has_parent_path())
            add(absolute(prefix_path.parent_path().string(), working_directory), Access::ReadWrite, "the option `prof_prefix` of jemalloc");
#endif
    }

    void addExtraPaths()
    {
        for (const auto & key : getMultipleKeys("filesystem_isolation_paths", "read_only"))
            add(absolute(config.getString(key), working_directory), Access::ReadOnly, fmt::format("`{}`", key));
        for (const auto & key : getMultipleKeys("filesystem_isolation_paths", "read_write"))
            add(absolute(config.getString(key), working_directory), Access::ReadWrite, fmt::format("`{}`", key));
    }
};

}

std::vector<FilesystemIsolationPath> collectFilesystemIsolationPaths(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_path, const std::string & working_directory)
{
    return Collector(config, config_path, working_directory).collect();
}

}
