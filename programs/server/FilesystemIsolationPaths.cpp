#include <FilesystemIsolationPaths.h>

#include <Core/Defines.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Jemalloc.h>
#include <Common/filesystemHelpers.h>

#include <Poco/Exception.h>
#include <Poco/Glob.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <set>
#include <string_view>

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
    /// The directories of the sockets of the services that the modules of the Name Service Switch connect to:
    /// `systemd-resolved` for `nss-resolve`, and the caching daemon `nscd`. A socket on a read-only mount can still
    /// be connected to. The directories rather than the sockets, because a service that restarts binds a new socket,
    /// and a bind mount of the old one would keep pointing at the old, closed one. The same for the files that
    /// `systemd-resolved` replaces, such as `stub-resolv.conf`, which `/etc/resolv.conf` is often a link to. Their
    /// parents are not visible, because they contain other sockets, such as the one of the service manager in
    /// `/run/systemd`; the directories themselves are kept when the services restart.
    "/run/systemd/resolve",
    "/run/nscd",
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

/// Whether `path` is `ancestor` or is inside it.
bool isSameOrInside(const fs::path & path, const fs::path & ancestor)
{
    return std::mismatch(path.begin(), path.end(), ancestor.begin(), ancestor.end()).second == ancestor.end();
}

/// A path with a wildcard in it means every file matching it, so the directory up to the first wildcard is needed.
bool hasWildcard(const std::string & component)
{
    return component.find_first_of("*?[{") != std::string::npos;
}

class Collector
{
public:
    Collector(
        const Poco::Util::AbstractConfiguration & config_,
        const std::string & config_path,
        const std::string & working_directory_,
        const std::string & startup_directory_)
        : config(config_)
        , working_directory(working_directory_)
        , startup_directory(startup_directory_)
        , is_daemon(config.getBool("application.runAsDaemon", false))
        , config_file(absolute(config_path, startup_directory))
        , config_directory(config_file.parent_path())
        , data_path(absolute(config.getString("path", DBMS_DEFAULT_PATH), working_directory))
        , runtime_directory(is_daemon ? coreDirectory() : working_directory)
    {
    }

    std::vector<FilesystemIsolationPath> collect() &&
    {
        for (const auto * path : system_paths)
        {
            add(path, Access::ReadOnly, "system");
            addLinkTargetDirectories(path, Access::ReadOnly, "system");
        }

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
        addKerberosProfiles();
        addBridge();
        addJemallocProfiles();
        addExtraPaths();

        return std::move(paths);
    }

private:
    const Poco::Util::AbstractConfiguration & config;
    const fs::path working_directory;
    /// The working directory when the filesystem is isolated: the directory of the executable with `--daemon`, where
    /// `BaseDaemon::initialize` changes to before loading the configuration, and `working_directory` otherwise.
    const fs::path startup_directory;
    const bool is_daemon;
    const fs::path config_file;
    const fs::path config_directory;
    const fs::path data_path;
    /// The working directory of `Server::main`, against which the paths that are used after the startup are resolved:
    /// with `--daemon`, the directory of the core dumps, where `BaseDaemon::initialize` changes to at its end. Only
    /// `path` is resolved against `working_directory`, the original working directory, in any case.
    const fs::path runtime_directory;
    std::vector<FilesystemIsolationPath> paths;

    /// Without `.` and empty components, so that equal paths compare equal, but with `..`: after a symbolic link it
    /// means the parent of the target of the link, as for `getCanonicalPath`, rather than the directory of the link.
    /// `resolvePath` follows the links and `..` as the kernel does when the paths are mounted.
    static fs::path absolute(const std::string & path, const fs::path & base)
    {
        fs::path joined(path);
        if (joined.is_relative())
            joined = base / joined;
        fs::path result = joined.root_path();
        for (const auto & component : joined.relative_path())
            if (!component.empty() && component != ".")
                result /= component;
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
        /// The same limit on the number of links as in the kernel, against a loop.
        constexpr size_t max_links = 40;
        fs::path current = path;
        for (size_t i = 0;; ++i)
        {
            /// A directory, also as the target of a link, on its own.
            if (fs::is_directory(current, error))
            {
                add(current, access, source);
                return;
            }
            /// A device or a standard stream, such as `/dev/null` or `/dev/stderr` for a log, on its own, also when it
            /// is the target of a link, as for a log file that is a link to `/dev/null`: the isolated filesystem has
            /// its own `/dev` with only a few devices, and the one of the host must stay invisible. A file that does
            /// not exist is not created there, and it cannot be created inside either. The links a device can be
            /// itself, such as `/dev/stdout`, are resolved when it is mounted.
            if (isSameOrInside(fs::weakly_canonical(current.parent_path(), error) / current.filename(), "/dev"))
            {
                if (fs::exists(current, error))
                    add(current, access, source);
                return;
            }
            add(current.parent_path(), access, source);

            if (i == max_links || !FS::isSymlinkNoThrow(current))
                return;
            fs::path target = FS::readSymlink(current);
            /// The `..` in a relative target is relative to where the link really is: its directory can be a link
            /// itself, or be inside one, as in `certs -> /etc/letsencrypt/live/<domain>`, so it is resolved first.
            if (target.is_relative())
                target = fs::canonical(current.parent_path()) / target;
            /// The `..` in the target is kept: after a link in the target, as in `versions/current/../fullchain.pem`,
            /// it means the parent of the target of that link, which `resolvePath` follows as the kernel does.
            current = absolute(target.string(), "/");
        }
    }

    /// For a system file that is a symbolic link, the directories of the targets of the links in the chain outside
    /// of `/etc`. A program managing such a file outside of `/etc`, as NetworkManager does with
    /// `/etc/resolv.conf -> /run/NetworkManager/resolv.conf`, replaces it with a new one, and a mount of only the old
    /// file would keep the old content forever. A regular file in `/etc` that is replaced stays the old one: `/etc`
    /// itself is not visible.
    void addLinkTargetDirectories(const fs::path & path, Access access, const std::string & source)
    {
        constexpr size_t max_links = 40;
        fs::path current = path;
        for (size_t i = 0; i < max_links && FS::isSymlinkNoThrow(current); ++i)
        {
            fs::path target = FS::readSymlink(current);
            std::error_code error;
            if (target.is_relative())
            {
                const fs::path directory = fs::canonical(current.parent_path(), error);
                if (error)
                    return;
                target = directory / target;
            }
            /// With `..`, as in `addLocation`.
            current = absolute(target.string(), "/");
            if (current.filename() == "..")
                return;
            /// Nor a top-level directory, such as `/run`, with the sockets of every service in it, or `/dev`.
            const fs::path directory = current.parent_path();
            const fs::path real_directory = fs::canonical(directory, error);
            if (error)
                return;
            const fs::path relative = real_directory.relative_path();
            const auto depth = std::distance(relative.begin(), relative.end());
            if (depth >= 2 && !isSameOrInside(real_directory, "/etc") && !isSameOrInside(real_directory, "/dev"))
                add(directory, access, fmt::format("the target of a link in {}", source));
        }
    }

    /// The configurations matching a pattern, each processed on its own, as `ExternalLoaderXMLConfigRepository`
    /// does for the definitions of dictionaries and executable functions. For a pattern with wildcards, the directory
    /// up to the first wildcard as well, for the files added later.
    void addIndependentConfigPattern(const fs::path & pattern, const std::string & source)
    {
        fs::path prefix;
        for (const auto & component : pattern)
        {
            if (hasWildcard(component.string()))
            {
                add(prefix, Access::ReadOnly, source);
                std::set<std::string> files;
                Poco::Glob::glob(pattern.string(), files, 0);
                for (const auto & file : files)
                    if (!ConfigProcessor::isPreprocessedFile(file))
                        addDefinitionConfigFile(file, fmt::format("{} matching {}", file, source));
                return;
            }
            prefix /= component;
        }
        addDefinitionConfigFile(pattern, source);
    }

    /// A file with definitions of dictionaries or executable functions. `ExternalLoader` logs a file it cannot load
    /// and goes on with the others, so such a file does not prevent the server from starting here either: its own
    /// substitutions file is not known then, and the loader reports the error later, as it does without the isolation.
    void addDefinitionConfigFile(const fs::path & file, const std::string & source)
    {
        addConfigFileWithMergeFiles(file, source);
        try
        {
            addIncludeFrom(file, source);
        }
        catch (const Poco::Exception &) // NOLINT(bugprone-empty-catch)
        {
        }
        catch (const std::exception &) // NOLINT(bugprone-empty-catch)
        {
        }
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

    /// An empty value means that the path is not used, as for `access_control_path`, rather than the base directory.
    void addKey(const std::string & key, Access access, const fs::path & base)
    {
        if (config.has(key))
            if (const auto value = config.getString(key); !value.empty())
                add(absolute(value, base), access, fmt::format("`{}`", key));
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
        addIncludeFrom(file, source);
    }

    void addIncludeFrom(const fs::path & file, const std::string & source)
    {
        if (file == config_file)
            return;
        std::error_code error;
        if (!fs::exists(file, error))
            return;

        /// These files are loaded by `Server::main` and later, in `runtime_directory`, and `ConfigProcessor` opens
        /// `include_from` relative to the current directory, so they are loaded there here as well. With `--daemon`,
        /// the process is not there yet, and the directory may not exist yet: `BaseDaemon::initialize` creates it.
        const fs::path previous_directory = fs::current_path();
        const bool change_directory = runtime_directory != previous_directory;
        if (change_directory)
        {
            fs::create_directories(runtime_directory);
            fs::current_path(runtime_directory);
        }
        DB::ConfigProcessor::LoadedConfig loaded;
        try
        {
            /// The ZooKeeper includes are left unprocessed: there is no ZooKeeper yet, and they do not name local files.
            loaded = ConfigProcessor(file.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false).loadConfig(/* allow_zk_includes = */ true);
        }
        catch (...)
        {
            if (change_directory)
                fs::current_path(previous_directory);
            throw;
        }
        if (change_directory)
            fs::current_path(previous_directory);

        if (loaded.configuration->has("include_from"))
            if (const auto value = loaded.configuration->getString("include_from"); !value.empty())
                addLocation(absolute(value, runtime_directory), Access::ReadOnly, fmt::format("`include_from` of {}", source));
    }

    void addConfigFiles()
    {
        /// The main configuration file, `config.d` and the other files next to it, watched for changes.
        add(config_directory, Access::ReadOnly, "the directory of the configuration file");
        addConfigFileWithMergeFiles(config_file, "the configuration file");
        for (const auto & base : configLoadDirectories())
            addKeyLocation("include_from", Access::ReadOnly, base);

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
            addIndependentConfigFile(fs::exists(next_to_config, error) ? next_to_config : absolute(value, runtime_directory), fmt::format("`{}`", key));
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
                addKey(key, Access::ReadWrite, runtime_directory);
        }

        std::vector<std::string> definitions = {"dictionaries_config", "user_defined_executable_functions_config"};
        /// The drivers are loaded only with this setting, as in `Context::loadUserDefinedExecutableFunctionDrivers`.
        if (config.getBool("allow_experimental_executable_udf_drivers", false))
            definitions.emplace_back("user_defined_executable_function_drivers_config");
        for (const auto & name : definitions)
            for (const auto & key : getMultipleKeys("", name))
                if (const auto pattern = config.getString(key); !pattern.empty())
                    addDefinitionConfigPattern(pattern, fmt::format("`{}`", key));
    }

    /// The same lookup as in `ExternalLoaderXMLConfigRepository::getAllLoadablesDefinitionNames`: a relative pattern
    /// is looked up next to the configuration file and, only if nothing matches there, in the working directory.
    void addDefinitionConfigPattern(const std::string & pattern, const std::string & source)
    {
        const fs::path next_to_config = absolute(pattern, config_directory);
        addIndependentConfigPattern(next_to_config, source);
        if (fs::path(pattern).is_absolute())
            return;
        std::set<std::string> files;
        Poco::Glob::glob(next_to_config.string(), files, 0);
        if (files.empty())
            addIndependentConfigPattern(absolute(pattern, runtime_directory), source);
    }

    void addDataPaths()
    {
        add(data_path, Access::ReadWrite, "`path`");
        addKey("tmp_path", Access::ReadWrite, data_path);
        addKey("user_files_path", Access::ReadWrite, data_path);
        addKey("format_schema_path", Access::ReadWrite, data_path);
        addKey("dynamic_user_defined_executable_functions_path", Access::ReadWrite, data_path);
        addKey("filesystem_caches_path", Access::ReadWrite, data_path);
        addKey("custom_cached_disks_base_directory", Access::ReadWrite, runtime_directory);
        addKey("custom_local_disks_base_directory", Access::ReadWrite, runtime_directory);
        addKey("access_control_path", Access::ReadWrite, runtime_directory);
        addKey("user_defined_path", Access::ReadWrite, runtime_directory);
        addKey("workload_path", Access::ReadWrite, runtime_directory);
        /// With `--daemon`, the directory of the core dumps is where `BaseDaemon::initialize` changes to at its end.
        if (is_daemon)
            add(coreDirectory(), Access::ReadWrite, "`core_path`");
        else
            addKey("core_path", Access::ReadWrite, runtime_directory);

        /// They are inside `path` by default, which is not executable, and are mounted separately to be read-only and,
        /// for the scripts, executable.
        const fs::path user_scripts_path = absolute(config.getString("user_scripts_path", "user_scripts"), data_path);
        add(user_scripts_path, Access::ReadOnly, "`user_scripts_path`");
        /// Except the WebAssembly modules, which `WasmModuleManager` keeps in the subdirectory `wasm` and writes when
        /// they are inserted into or deleted from `system.webassembly_modules`. It is created before the mount.
        if (config.getBool("allow_experimental_webassembly_udf", false))
            add(user_scripts_path / "wasm", Access::ReadWrite, "the WebAssembly modules in `user_scripts_path`");
        add(absolute(config.getString("top_level_domains_path", "top_level_domains"), data_path), Access::ReadOnly, "`top_level_domains_path`");
        add(absolute(config.getString("google_protos_path", "/usr/share/clickhouse/protos/"), runtime_directory), Access::ReadOnly, "`google_protos_path`");

        addKeyLocation("path_to_regions_hierarchy_file", Access::ReadOnly, runtime_directory);
        addKeyLocation("path_to_regions_names_files", Access::ReadOnly, runtime_directory);
        addHdfsConfig();
        addKeyLocation("nats.credential_file", Access::ReadOnly, runtime_directory);

        for (const auto & key : getMultipleKeys("backups", "allowed_path"))
            addKey(key, Access::ReadWrite, data_path);

        /// The same default as in `NamedCollectionsMetadataStorage::create`; both `local` and `local_encrypted` keep the
        /// collections in this directory.
        if (config.getString("named_collections_storage.type", "local").starts_with("local"))
            addKey("named_collections_storage.path", Access::ReadWrite, runtime_directory);

        Poco::Util::AbstractConfiguration::Keys caches;
        config.keys("filesystem_caches", caches);
        const fs::path caches_base = getCachesBase();
        for (const auto & cache : caches)
            addKey(fmt::format("filesystem_caches.{}.path", cache), Access::ReadWrite, caches_base);
    }

    /// The directory of the filesystem caches with relative paths, as in `getCache` in `RegisterDiskCache.cpp`. An
    /// empty `filesystem_caches_path` is the same as none.
    fs::path getCachesBase() const
    {
        const std::string value = config.getString("filesystem_caches_path", "");
        return value.empty() ? data_path / "caches" : absolute(value, data_path);
    }

    void addDisks()
    {
        Poco::Util::AbstractConfiguration::Keys disks;
        config.keys("storage_configuration.disks", disks);
        const fs::path caches_base = getCachesBase();
        for (const auto & disk : disks)
        {
            const std::string prefix = fmt::format("storage_configuration.disks.{}", disk);
            const std::string type = config.getString(prefix + ".type", "local");
            /// The path of an encrypted disk is inside its underlying disk.
            if (type == "encrypted")
                continue;
            addKey(prefix + ".path", Access::ReadWrite, type == "cache" ? caches_base : runtime_directory);
            /// A cache disk can take its settings, with the path, from a named collection, as in `getCache` in
            /// `RegisterDiskCache.cpp`. The collections of the configuration are in `named_collections`; the ones
            /// created with a query are not known before the server starts.
            if (type == "cache" && config.has(prefix + ".cache_name"))
                addKey(fmt::format("named_collections.{}.path", config.getString(prefix + ".cache_name")), Access::ReadWrite, caches_base);
            addKey(prefix + ".metadata_path", Access::ReadWrite, runtime_directory);

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
                    addKey(location_prefix + ".path", Access::ReadWrite, runtime_directory);
            }
        }
    }

    void addKeeper()
    {
        for (const auto * name : {"storage_path", "log_storage_path", "snapshot_storage_path", "data_storage_path"})
            addKey(fmt::format("keeper_server.{}", name), Access::ReadWrite, runtime_directory);

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
                add(absolute(fs::path(config.getString(key)).parent_path().string(), runtime_directory), Access::ReadWrite,
                    fmt::format("the state file of Keeper, next to `{}`", key));
                return;
            }
        }
    }

    /// With the targets of the links, as a log file can be a link to another directory.
    /// The paths of the logs are relative to the directories that `BaseDaemon::initialize` opens them in: `stderr` and
    /// `stdout` in `startup_directory`, and the logs and the pid file after it has changed to `logDirectory`.
    void addLogs()
    {
        for (const auto * key : {"logger.stderr", "logger.stdout"})
            addKeyLocation(key, Access::ReadWrite, startup_directory);
        for (const auto * key : {"logger.log", "logger.errorlog", "pid"})
            addKeyLocation(key, Access::ReadWrite, logDirectory());
        /// With `--daemon`, `stderr.log` and `stdout.log` are created in the directory of the log by default.
        if (is_daemon && !config.getString("logger.log", "").empty())
            add(logDirectory(), Access::ReadWrite, "the directory of `logger.log`");
    }

    /// The directories a configuration file is loaded in: `startup_directory` at the startup, and `runtime_directory`
    /// when it is reloaded.
    std::vector<fs::path> configLoadDirectories() const
    {
        if (startup_directory == runtime_directory)
            return {startup_directory};
        return {startup_directory, runtime_directory};
    }

    /// The working directory after `BaseDaemon::initialize` has created the directory of the log: with `--daemon`, it
    /// changes to that directory, or to `/tmp` without a log.
    fs::path logDirectory() const
    {
        if (!is_daemon)
            return startup_directory;
        const std::string log = config.getString("logger.log", "");
        return log.empty() ? fs::path("/tmp") : absolute(log, startup_directory).parent_path();
    }

    /// The directory of the core dumps that `BaseDaemon::initialize` changes to at its end with `--daemon`: `core_path`
    /// relative to `logDirectory`, or `cores` in `path`. If it cannot create it, it falls back to the directory of the
    /// log, which is visible anyway.
    fs::path coreDirectory() const
    {
        const std::string core_path = config.getString("core_path", "");
        return core_path.empty() ? data_path / "cores" : absolute(core_path, logDirectory());
    }

    /// The same lookup as in `Server::main`: a relative path is relative to the working directory, or, if there is no
    /// such file, to the directory of the main configuration, if the file is there.
    void addHdfsConfig()
    {
        const std::string key = "hdfs.libhdfs3_conf";
        if (const std::string value = config.getString(key, ""); !value.empty())
        {
            fs::path file = absolute(value, runtime_directory);
            std::error_code error;
            if (fs::path(value).is_relative() && !fs::exists(file, error))
                if (const fs::path next_to_config = absolute(value, config_directory); fs::exists(next_to_config, error))
                    file = next_to_config;
            addLocation(file, Access::ReadOnly, fmt::format("`{}`", key));
        }

        /// The credentials of Kerberos, in `hdfs` and in the sections `hdfs_<user>` for the users of HDFS, as
        /// `HDFSBuilderWrapper::loadFromConfig` reads them. The cache of the tickets is written to, also as a new
        /// file in its directory; its name can have the type of the cache in front, as in `FILE:/tmp/krb5cc`.
        std::vector<std::string> sections = {"hdfs"};
        Poco::Util::AbstractConfiguration::Keys top_level_keys;
        config.keys(top_level_keys);
        for (const auto & top_level_key : top_level_keys)
            if (top_level_key.starts_with("hdfs_"))
                sections.push_back(top_level_key);
        for (const auto & section : sections)
        {
            addKeyLocation(section + ".hadoop_kerberos_keytab", Access::ReadOnly, runtime_directory);
            std::string cache = config.getString(section + ".hadoop_security_kerberos_ticket_cache_path", "");
            for (const std::string_view type : {"FILE:", "DIR:"})
                if (cache.starts_with(type))
                    cache = cache.substr(type.size());
            if (!cache.empty() && !cache.contains(':'))
                addLocation(absolute(cache, runtime_directory), Access::ReadWrite, fmt::format("`{}.hadoop_security_kerberos_ticket_cache_path`", section));
        }
    }

    void addCertificates()
    {
        for (const auto * side : {"server", "client"})
            for (const auto * name : {"privateKeyFile", "certificateFile", "caConfig", "dhParamsFile"})
                addKeyLocation(fmt::format("openSSL.{}.{}", side, name), Access::ReadOnly, runtime_directory);

        Poco::Util::AbstractConfiguration::Keys protocols;
        config.keys("protocols", protocols);
        for (const auto & protocol : protocols)
            for (const auto * name : {"privateKeyFile", "certificateFile", "caConfig", "dhParamsFile"})
                addKeyLocation(fmt::format("protocols.{}.{}", protocol, name), Access::ReadOnly, runtime_directory);

        for (const auto * key : {"grpc.ssl_key_file", "grpc.ssl_cert_file", "grpc.ssl_ca_cert_file", "arrowflight.ssl_cert_file", "arrowflight.ssl_key_file", "kerberos.keytab",
                                 "ssh_server.host_rsa_key", "ssh_server.host_ecdsa_key", "ssh_server.host_ed25519_key"})
            addKeyLocation(key, Access::ReadOnly, runtime_directory);

        Poco::Util::AbstractConfiguration::Keys ldap_servers;
        config.keys("ldap_servers", ldap_servers);
        for (const auto & server : ldap_servers)
            for (const auto * name : {"tls_cert_file", "tls_key_file", "tls_ca_cert_file", "tls_ca_cert_dir"})
                addKeyLocation(fmt::format("ldap_servers.{}.{}", server, name), Access::ReadOnly, runtime_directory);
    }

    /// The profiles of Kerberos at the default paths of the bundled MIT Kerberos (`DEFAULT_PROFILE_PATH`), with the
    /// files and directories they name in `include` and `includedir`. They hold the default realm and the locations of
    /// the KDCs, which the clients of HDFS and Kafka with a keytab need, and these can be created at runtime by tables.
    void addKerberosProfiles()
    {
        std::vector<fs::path> pending = {"/etc/krb5.conf", "/usr/local/etc/krb5.conf"};
        std::set<fs::path> seen;
        /// Against a loop of includes and an unbounded number of files.
        constexpr size_t max_files = 100;
        while (!pending.empty() && seen.size() < max_files)
        {
            const fs::path file = std::move(pending.back());
            pending.pop_back();
            std::error_code error;
            if (!seen.insert(file).second || !fs::is_regular_file(file, error))
                continue;
            add(file, Access::ReadOnly, "a profile of Kerberos");
            addLinkTargetDirectories(file, Access::ReadOnly, "a profile of Kerberos");

            std::ifstream in(file);
            std::string line;
            while (std::getline(in, line))
            {
                /// As `profile_parse_file` does: the directive at the start of a line, followed by whitespace and an
                /// absolute path, which is the rest of the line.
                for (const std::string_view directive : {"include", "includedir"})
                {
                    if (!line.starts_with(directive) || line.size() <= directive.size() || !std::isspace(static_cast<unsigned char>(line[directive.size()])))
                        continue;
                    const size_t begin = line.find_first_not_of(" \t", directive.size());
                    const size_t end = line.find_last_not_of(" \t\r");
                    if (begin == std::string::npos || line[begin] != '/')
                        continue;
                    const fs::path included = absolute(line.substr(begin, end + 1 - begin), "/");
                    if (directive == "include")
                    {
                        pending.push_back(included);
                    }
                    else if (fs::is_directory(included, error))
                    {
                        /// The directory itself, for the files added to it later, and the files in it, which may be
                        /// links to other places.
                        add(included, Access::ReadOnly, "a directory of profiles of Kerberos");
                        for (const auto & entry : fs::directory_iterator(included, error))
                            pending.push_back(entry.path());
                    }
                }
            }
        }
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
            add(absolute(prefix_path.parent_path().string(), runtime_directory), Access::ReadWrite, "the option `prof_prefix` of jemalloc");
#endif
    }

    void addExtraPaths()
    {
        for (const auto & key : getMultipleKeys("filesystem_isolation_paths", "read_only"))
            addKey(key, Access::ReadOnly, runtime_directory);
        for (const auto & key : getMultipleKeys("filesystem_isolation_paths", "read_write"))
            addKey(key, Access::ReadWrite, runtime_directory);
    }
};

}

std::vector<FilesystemIsolationPath> collectFilesystemIsolationPaths(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_path,
    const std::string & working_directory,
    const std::string & startup_directory)
{
    return Collector(config, config_path, working_directory, startup_directory).collect();
}

}
