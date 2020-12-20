#include <iostream>
#include <filesystem>
#include <boost/program_options.hpp>

#include <sys/stat.h>
#include <pwd.h>

#if defined(__linux__)
    #include <syscall.h>
    #include <linux/capability.h>
#endif

#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/formatReadable.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/hex.h>
#include <common/getResource.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/MMapReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/copyData.h>
#include <IO/Operators.h>
#include <readpassphrase.h>

#include <Poco/Util/XMLConfiguration.h>


/** This tool can be used to install ClickHouse without a deb/rpm/tgz package, having only "clickhouse" binary.
  * It also allows to avoid dependency on systemd, upstart, SysV init.
  *
  * The following steps are performed:
  *
  * - copying the binary to binary directory (/usr/bin).
  * - creation of symlinks for tools.
  * - creation of clickhouse user and group.
  * - creation of config directory (/etc/clickhouse-server).
  * - creation of default configuration files.
  * - creation of a directory for logs (/var/log/clickhouse-server).
  * - creation of a data directory if not exists.
  * - setting a password for default user.
  * - choose an option to listen connections.
  * - changing the ownership and mode of the directories.
  * - setting capabilities for binary.
  * - setting ulimits for the user.
  * - (todo) put service in cron.
  *
  * It does not install clickhouse-odbc-bridge.
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int SYSTEM_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}

}


using namespace DB;
namespace po = boost::program_options;
namespace fs = std::filesystem;


static auto executeScript(const std::string & command, bool throw_on_error = false)
{
    auto sh = ShellCommand::execute(command);
    WriteBufferFromFileDescriptor wb_stdout(STDOUT_FILENO);
    WriteBufferFromFileDescriptor wb_stderr(STDERR_FILENO);
    copyData(sh->out, wb_stdout);
    copyData(sh->err, wb_stderr);

    if (throw_on_error)
    {
        sh->wait();
        return 0;
    }
    else
        return sh->tryWait();
}

static bool ask(std::string question)
{
    while (true)
    {
        std::string answer;
        std::cout << question;
        std::getline(std::cin, answer);
        if (!std::cin.good())
            return false;

        if (answer.empty() || answer == "n" || answer == "N")
            return false;
        if (answer == "y" || answer == "Y")
            return true;
    }
}

static bool filesEqual(std::string path1, std::string path2)
{
    MMapReadBufferFromFile in1(path1, 0);
    MMapReadBufferFromFile in2(path2, 0);

    /// memcmp is faster than hashing and comparing hashes
    return in1.buffer().size() == in2.buffer().size()
        && 0 == memcmp(in1.buffer().begin(), in2.buffer().begin(), in1.buffer().size());
}


int mainEntryClickHouseInstall(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("prefix", po::value<std::string>()->default_value(""), "prefix for all paths")
        ("binary-path", po::value<std::string>()->default_value("/usr/bin"), "where to install binaries")
        ("config-path", po::value<std::string>()->default_value("/etc/clickhouse-server"), "where to install configs")
        ("log-path", po::value<std::string>()->default_value("/var/log/clickhouse-server"), "where to create log directory")
        ("data-path", po::value<std::string>()->default_value("/var/lib/clickhouse"), "directory for data")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-server"), "directory for pid file")
        ("user", po::value<std::string>()->default_value("clickhouse"), "clickhouse user to create")
        ("group", po::value<std::string>()->default_value("clickhouse"), "clickhouse group to create")
    ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: "
            << (getuid() == 0 ? "" : "sudo ")
            << argv[0]
            << " install [options]\n";
        std::cout << desc << '\n';
        return 1;
    }

    try
    {
        /// We need to copy binary to the binary directory.
        /// The binary is currently run. We need to obtain its path from procfs.

        fs::path binary_self_path = "/proc/self/exe";
        if (!fs::exists(binary_self_path))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot obtain path to the binary from {}, file doesn't exist",
                            binary_self_path.string());

        fs::path binary_self_canonical_path = fs::canonical(binary_self_path);

        /// Copy binary to the destination directory.

        /// TODO An option to link instead of copy - useful for developers.

        fs::path prefix = fs::path(options["prefix"].as<std::string>());
        fs::path bin_dir = prefix / fs::path(options["binary-path"].as<std::string>());

        fs::path main_bin_path = bin_dir / "clickhouse";
        fs::path main_bin_tmp_path = bin_dir / "clickhouse.new";
        fs::path main_bin_old_path = bin_dir / "clickhouse.old";

        size_t binary_size = fs::file_size(binary_self_path);

        bool old_binary_exists = fs::exists(main_bin_path);
        bool already_installed = false;

        /// Check if the binary is the same file (already installed).
        if (old_binary_exists && binary_self_canonical_path == fs::canonical(main_bin_path))
        {
            already_installed = true;
            fmt::print("ClickHouse binary is already located at {}\n", main_bin_path.string());
        }
        /// Check if binary has the same content.
        else if (old_binary_exists && binary_size == fs::file_size(main_bin_path))
        {
            fmt::print("Found already existing ClickHouse binary at {} having the same size. Will check its contents.\n",
                main_bin_path.string());

            if (filesEqual(binary_self_path.string(), main_bin_path.string()))
            {
                already_installed = true;
                fmt::print("ClickHouse binary is already located at {} and it has the same content as {}\n",
                    main_bin_path.string(), binary_self_canonical_path.string());
            }
        }

        if (already_installed)
        {
            if (0 != chmod(main_bin_path.string().c_str(), S_IRUSR | S_IRGRP | S_IROTH | S_IXUSR | S_IXGRP | S_IXOTH))
                throwFromErrno(fmt::format("Cannot chmod {}", main_bin_path.string()), ErrorCodes::SYSTEM_ERROR);
        }
        else
        {
            size_t available_space = fs::space(bin_dir).available;
            if (available_space < binary_size)
                throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Not enough space for clickhouse binary in {}, required {}, available {}.",
                    bin_dir.string(), ReadableSize(binary_size), ReadableSize(available_space));

            fmt::print("Copying ClickHouse binary to {}\n", main_bin_tmp_path.string());

            try
            {
                ReadBufferFromFile in(binary_self_path.string());
                WriteBufferFromFile out(main_bin_tmp_path.string());
                copyData(in, out);
                out.sync();

                if (0 != fchmod(out.getFD(), S_IRUSR | S_IRGRP | S_IROTH | S_IXUSR | S_IXGRP | S_IXOTH))
                    throwFromErrno(fmt::format("Cannot chmod {}", main_bin_tmp_path.string()), ErrorCodes::SYSTEM_ERROR);

                out.finalize();
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::CANNOT_OPEN_FILE && geteuid() != 0)
                    std::cerr << "Install must be run as root: sudo ./clickhouse install\n";
                throw;
            }

            if (old_binary_exists)
            {
                fmt::print("{} already exists, will rename existing binary to {} and put the new binary in place\n",
                        main_bin_path.string(), main_bin_old_path.string());

                /// There is file exchange operation in Linux but it's not portable.
                fs::rename(main_bin_path, main_bin_old_path);
            }

            fmt::print("Renaming {} to {}.\n", main_bin_tmp_path.string(), main_bin_path.string());
            fs::rename(main_bin_tmp_path, main_bin_path);
        }

        /// Create symlinks.

        std::initializer_list<const char *> tools
        {
            "clickhouse-server",
            "clickhouse-client",
            "clickhouse-local",
            "clickhouse-benchmark",
            "clickhouse-copier",
            "clickhouse-obfuscator",
            "clickhouse-git-import",
            "clickhouse-compressor",
            "clickhouse-format",
            "clickhouse-extract-from-config"
        };

        for (const auto & tool : tools)
        {
            bool need_to_create = true;
            fs::path symlink_path = bin_dir / tool;

            if (fs::exists(symlink_path))
            {
                bool is_symlink = fs::is_symlink(symlink_path);
                fs::path points_to;
                if (is_symlink)
                    points_to = fs::absolute(fs::read_symlink(symlink_path));

                if (is_symlink && points_to == main_bin_path)
                {
                    need_to_create = false;
                }
                else
                {
                    if (!is_symlink)
                    {
                        fs::path rename_path = symlink_path.replace_extension(".old");
                        fmt::print("File {} already exists but it's not a symlink. Will rename to {}.\n",
                                   symlink_path.string(), rename_path.string());
                        fs::rename(symlink_path, rename_path);
                    }
                    else if (points_to != main_bin_path)
                    {
                        fmt::print("Symlink {} already exists but it points to {}. Will replace the old symlink to {}.\n",
                                   symlink_path.string(), points_to.string(), main_bin_path.string());
                        fs::remove(symlink_path);
                    }
                }
            }

            if (need_to_create)
            {
                fmt::print("Creating symlink {} to {}.\n", symlink_path.string(), main_bin_path.string());
                fs::create_symlink(main_bin_path, symlink_path);
            }
        }

        /// Creation of clickhouse user and group.

        std::string user = options["user"].as<std::string>();
        std::string group = options["group"].as<std::string>();

        if (!group.empty())
        {
            {
                fmt::print("Creating clickhouse group if it does not exist.\n");
                std::string command = fmt::format("groupadd -r {}", group);
                fmt::print(" {}\n", command);
                executeScript(command);
            }
        }
        else
            fmt::print("Will not create clickhouse group");

        if (!user.empty())
        {
            fmt::print("Creating clickhouse user if it does not exist.\n");
            std::string command = group.empty()
                ? fmt::format("useradd -r --shell /bin/false --home-dir /nonexistent --user-group {}", user)
                : fmt::format("useradd -r --shell /bin/false --home-dir /nonexistent -g {} {}", group, user);
            fmt::print(" {}\n", command);
            executeScript(command);

            if (group.empty())
                group = user;

            /// Setting ulimits.
            try
            {
                fs::path ulimits_dir = "/etc/security/limits.d";
                fs::path ulimits_file = ulimits_dir / fmt::format("{}.conf", user);
                fmt::print("Will set ulimits for {} user in {}.\n", user, ulimits_file.string());
                std::string ulimits_content = fmt::format(
                    "{0}\tsoft\tnofile\t262144\n"
                    "{0}\thard\tnofile\t262144\n", user);

                fs::create_directories(ulimits_dir);

                WriteBufferFromFile out(ulimits_file.string());
                out.write(ulimits_content.data(), ulimits_content.size());
                out.sync();
                out.finalize();
            }
            catch (...)
            {
                std::cerr << "Cannot set ulimits: " << getCurrentExceptionMessage(false) << "\n";
            }

            /// TODO Set ulimits on Mac OS X
        }
        else
            fmt::print("Will not create clickhouse user.\n");

        /// Creating configuration files and directories.

        fs::path config_dir = prefix / options["config-path"].as<std::string>();

        if (!fs::exists(config_dir))
        {
            fmt::print("Creating config directory {}.\n", config_dir.string());
            fs::create_directories(config_dir);
        }

        fs::path main_config_file = config_dir / "config.xml";
        fs::path users_config_file = config_dir / "users.xml";
        fs::path config_d = config_dir / "config.d";
        fs::path users_d = config_dir / "users.d";

        std::string log_path = prefix / options["log-path"].as<std::string>();
        std::string data_path = prefix / options["data-path"].as<std::string>();
        std::string pid_path = prefix / options["pid-path"].as<std::string>();

        bool has_password_for_default_user = false;

        if (!fs::exists(config_d))
        {
            fmt::print("Creating config directory {} that is used for tweaks of main server configuration.\n", config_d.string());
            fs::create_directory(config_d);
        }

        if (!fs::exists(users_d))
        {
            fmt::print("Creating config directory {} that is used for tweaks of users configuration.\n", users_d.string());
            fs::create_directory(users_d);
        }

        if (!fs::exists(main_config_file))
        {
            std::string_view main_config_content = getResource("config.xml");
            if (main_config_content.empty())
            {
                fmt::print("There is no default config.xml, you have to download it and place to {}.\n", main_config_file.string());
            }
            else
            {
                WriteBufferFromFile out(main_config_file.string());
                out.write(main_config_content.data(), main_config_content.size());
                out.sync();
                out.finalize();
            }
        }
        else
        {
            fmt::print("Config file {} already exists, will keep it and extract path info from it.\n", main_config_file.string());

            ConfigProcessor processor(main_config_file.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
            ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(processor.processConfig()));

            if (configuration->has("path"))
            {
                data_path = configuration->getString("path");
                fmt::print("{} has {} as data path.\n", main_config_file.string(), data_path);
            }

            if (configuration->has("logger.log"))
            {
                log_path = fs::path(configuration->getString("logger.log")).remove_filename();
                fmt::print("{} has {} as log path.\n", main_config_file.string(), log_path);
            }
        }


        if (!fs::exists(users_config_file))
        {
            std::string_view users_config_content = getResource("users.xml");
            if (users_config_content.empty())
            {
                fmt::print("There is no default users.xml, you have to download it and place to {}.\n", users_config_file.string());
            }
            else
            {
                WriteBufferFromFile out(users_config_file.string());
                out.write(users_config_content.data(), users_config_content.size());
                out.sync();
                out.finalize();
            }
        }
        else
        {
            fmt::print("Users config file {} already exists, will keep it and extract users info from it.\n", users_config_file.string());

            /// Check if password for default user already specified.
            ConfigProcessor processor(users_config_file.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
            ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(processor.processConfig()));

            if (!configuration->getString("users.default.password", "").empty()
                || !configuration->getString("users.default.password_sha256_hex", "").empty()
                || !configuration->getString("users.default.password_double_sha1_hex", "").empty())
            {
                has_password_for_default_user = true;
            }
        }

        /// Chmod and chown configs
        {
            std::string command = fmt::format("chown --recursive {}:{} '{}'", user, group, config_dir.string());
            fmt::print(" {}\n", command);
            executeScript(command);
        }

        /// Symlink "preprocessed_configs" is created by the server, so "write" is needed.
        fs::permissions(config_dir, fs::perms::owner_all, fs::perm_options::replace);

        /// Subdirectories, so "execute" is needed.
        if (fs::exists(config_d))
            fs::permissions(config_d, fs::perms::owner_read | fs::perms::owner_exec, fs::perm_options::replace);
        if (fs::exists(users_d))
            fs::permissions(users_d, fs::perms::owner_read | fs::perms::owner_exec, fs::perm_options::replace);

        /// Readonly.
        if (fs::exists(main_config_file))
            fs::permissions(main_config_file, fs::perms::owner_read, fs::perm_options::replace);
        if (fs::exists(users_config_file))
            fs::permissions(users_config_file, fs::perms::owner_read, fs::perm_options::replace);

        /// Create directories for data and log.

        if (fs::exists(log_path))
        {
            fmt::print("Log directory {} already exists.\n", log_path);
        }
        else
        {
            fmt::print("Creating log directory {}.\n", log_path);
            fs::create_directories(log_path);
        }

        if (fs::exists(data_path))
        {
            fmt::print("Data directory {} already exists.\n", data_path);
        }
        else
        {
            fmt::print("Creating data directory {}.\n", data_path);
            fs::create_directories(data_path);
        }

        if (fs::exists(pid_path))
        {
            fmt::print("Pid directory {} already exists.\n", pid_path);
        }
        else
        {
            fmt::print("Creating pid directory {}.\n", pid_path);
            fs::create_directories(pid_path);
        }

        /// Chmod and chown data and log directories
        {
            std::string command = fmt::format("chown --recursive {}:{} '{}'", user, group, log_path);
            fmt::print(" {}\n", command);
            executeScript(command);
        }

        {
            std::string command = fmt::format("chown --recursive {}:{} '{}'", user, group, pid_path);
            fmt::print(" {}\n", command);
            executeScript(command);
        }

        {
            /// Not recursive, because there can be a huge number of files and it will be slow.
            std::string command = fmt::format("chown {}:{} '{}'", user, group, data_path);
            fmt::print(" {}\n", command);
            executeScript(command);
        }

        /// All users are allowed to read pid file (for clickhouse status command).
        fs::permissions(pid_path, fs::perms::owner_all | fs::perms::group_read | fs::perms::others_read, fs::perm_options::replace);

        /// Other users in clickhouse group are allowed to read and even delete logs.
        fs::permissions(log_path, fs::perms::owner_all | fs::perms::group_all, fs::perm_options::replace);

        /// Data directory is not accessible to anyone except clickhouse.
        fs::permissions(data_path, fs::perms::owner_all, fs::perm_options::replace);

        /// Set up password for default user.

        bool stdin_is_a_tty = isatty(STDIN_FILENO);
        bool stdout_is_a_tty = isatty(STDOUT_FILENO);
        bool is_interactive = stdin_is_a_tty && stdout_is_a_tty;

        if (has_password_for_default_user)
        {
            fmt::print("Password for default user is already specified. To remind or reset, see {} and {}.\n",
                       users_config_file.string(), users_d.string());
        }
        else if (!is_interactive)
        {
            fmt::print("Password for default user is empty string. See {} and {} to change it.\n",
                       users_config_file.string(), users_d.string());
        }
        else
        {
            char buf[1000] = {};
            std::string password;
            if (auto * result = readpassphrase("Enter password for default user: ", buf, sizeof(buf), 0))
                password = result;

            if (!password.empty())
            {
                std::string password_file = users_d / "default-password.xml";
                WriteBufferFromFile out(password_file);
#if USE_SSL
                std::vector<uint8_t> hash;
                hash.resize(32);
                encodeSHA256(password, hash.data());
                std::string hash_hex;
                hash_hex.resize(64);
                for (size_t i = 0; i < 32; ++i)
                    writeHexByteLowercase(hash[i], &hash_hex[2 * i]);
                out << "<yandex>\n"
                    "    <users>\n"
                    "        <default>\n"
                    "            <password remove='1' />\n"
                    "            <password_sha256_hex>" << hash_hex << "</password_sha256_hex>\n"
                    "        </default>\n"
                    "    </users>\n"
                    "</yandex>\n";
                out.sync();
                out.finalize();
                fmt::print("Password for default user is saved in file {}.\n", password_file);
#else
                out << "<yandex>\n"
                    "    <users>\n"
                    "        <default>\n"
                    "            <password><![CDATA[" << password << "]]></password>\n"
                    "        </default>\n"
                    "    </users>\n"
                    "</yandex>\n";
                out.sync();
                out.finalize();
                fmt::print("Password for default user is saved in plaintext in file {}.\n", password_file);
#endif
                has_password_for_default_user = true;
            }
            else
                fmt::print("Password for default user is empty string. See {} and {} to change it.\n",
                           users_config_file.string(), users_d.string());
        }

        /** Set capabilities for the binary.
          *
          * 1. Check that "setcap" tool exists.
          * 2. Check that an arbitrary program with installed capabilities can run.
          * 3. Set the capabilities.
          *
          * The second is important for Docker and systemd-nspawn.
          * When the container has no capabilities,
          * but the executable file inside the container has capabilities,
          *  then attempt to run this file will end up with a cryptic "Operation not permitted" message.
          */

#if defined(__linux__)
        fmt::print("Setting capabilities for clickhouse binary. This is optional.\n");
        std::string command = fmt::format("command -v setcap >/dev/null"
            " && echo > {0} && chmod a+x {0} && {0} && setcap 'cap_net_admin,cap_ipc_lock,cap_sys_nice+ep' {0} && {0} && rm {0}"
            " && setcap 'cap_net_admin,cap_ipc_lock,cap_sys_nice+ep' {1}"
            " || echo \"Cannot set 'net_admin' or 'ipc_lock' or 'sys_nice' capability for clickhouse binary."
                " This is optional. Taskstats accounting will be disabled."
                " To enable taskstats accounting you may add the required capability later manually.\"",
            "/tmp/test_setcap.sh", fs::canonical(main_bin_path).string());
        fmt::print(" {}\n", command);
        executeScript(command);
#endif

        /// If password was set, ask for open for connections.
        if (is_interactive && has_password_for_default_user)
        {
            if (ask("Allow server to accept connections from the network (default is localhost only), [y/N]: "))
            {
                std::string listen_file = config_d / "listen.xml";
                WriteBufferFromFile out(listen_file);
                out << "<yandex>\n"
                    "    <listen_host>::</listen_host>\n"
                    "</yandex>\n";
                out.sync();
                out.finalize();
                fmt::print("The choice is saved in file {}.\n", listen_file);
            }
        }

        std::string maybe_password;
        if (has_password_for_default_user)
            maybe_password = " --password";

        fmt::print(
            "\nClickHouse has been successfully installed.\n"
            "\nStart clickhouse-server with:\n"
            " sudo clickhouse start\n"
            "\nStart clickhouse-client with:\n"
            " clickhouse-client{}\n\n",
            maybe_password);
    }
    catch (const fs::filesystem_error &)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';

        if (getuid() != 0)
            std::cerr << "\nRun with sudo.\n";

        return getCurrentExceptionCode();
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}


namespace
{
    int start(const std::string & user, const fs::path & executable, const fs::path & config, const fs::path & pid_file)
    {
        if (fs::exists(pid_file))
        {
            ReadBufferFromFile in(pid_file.string());
            UInt64 pid;
            if (tryReadIntText(pid, in))
            {
                fmt::print("{} file exists and contains pid = {}.\n", pid_file.string(), pid);

                if (0 == kill(pid, 0))
                {
                    fmt::print("The process with pid = {} is already running.\n", pid);
                    return 2;
                }
            }
            else
            {
                fmt::print("{} file exists but damaged, ignoring.\n", pid_file.string());
                fs::remove(pid_file);
            }
        }
        else
        {
            /// Create a directory for pid file.
            /// It's created by "install" but we also support cases when ClickHouse is already installed different way.
            fs::path pid_path = pid_file;
            pid_path.remove_filename();
            fs::create_directories(pid_path);
            /// All users are allowed to read pid file (for clickhouse status command).
            fs::permissions(pid_path, fs::perms::owner_all | fs::perms::group_read | fs::perms::others_read, fs::perm_options::replace);

            {
                std::string command = fmt::format("chown --recursive {} '{}'", user, pid_path.string());
                fmt::print(" {}\n", command);
                executeScript(command);
            }
        }

        std::string command = fmt::format("{} --config-file {} --pid-file {} --daemon",
            executable.string(), config.string(), pid_file.string());

        if (!user.empty())
        {
            bool may_need_sudo = geteuid() != 0;
            if (may_need_sudo)
            {
                struct passwd *p = getpwuid(geteuid());
                // Only use sudo when we are not the given user
                if (p == nullptr || std::string(p->pw_name) != user)
                    command = fmt::format("sudo -u '{}' {}", user, command);
            }
            else
                command = fmt::format("su -s /bin/sh '{}' -c '{}'", user, command);
        }

        fmt::print("Will run {}\n", command);
        executeScript(command, true);

        /// Wait to start.

        size_t try_num = 0;
        constexpr size_t num_tries = 60;
        for (; try_num < num_tries; ++try_num)
        {
            fmt::print("Waiting for server to start\n");
            if (fs::exists(pid_file))
            {
                fmt::print("Server started\n");
                break;
            }
            ::sleep(1);
        }

        if (try_num == num_tries)
        {
            fmt::print("Cannot start server. You can execute {} without --daemon option to run manually.\n", command);

            fs::path log_path;

            {
                ConfigProcessor processor(config.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
                ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(processor.processConfig()));

                if (configuration->has("logger.log"))
                    log_path = fs::path(configuration->getString("logger.log")).remove_filename();
            }

            if (log_path.empty())
            {
                fmt::print("Cannot obtain path to logs (logger.log) from config file {}.\n", config.string());
            }
            else
            {
                fs::path stderr_path = log_path;
                stderr_path.replace_filename("stderr.log");
                fmt::print("Look for logs at {} and for {}.\n", log_path.string(), stderr_path.string());
            }

            return 3;
        }

        return 0;
    }

    UInt64 isRunning(const fs::path & pid_file)
    {
        UInt64 pid = 0;

        if (fs::exists(pid_file))
        {
            ReadBufferFromFile in(pid_file.string());
            if (tryReadIntText(pid, in))
            {
                fmt::print("{} file exists and contains pid = {}.\n", pid_file.string(), pid);
            }
            else
            {
                fmt::print("{} file exists but damaged, ignoring.\n", pid_file.string());
                fs::remove(pid_file);
            }
        }

        if (!pid)
        {
            auto sh = ShellCommand::execute("pidof clickhouse-server");

            if (tryReadIntText(pid, sh->out))
            {
                fmt::print("Found pid = {} in the list of running processes.\n", pid);
            }
            else if (!sh->out.eof())
            {
                fmt::print("The pidof command returned unusual output.\n");
            }

            WriteBufferFromFileDescriptor stderr(STDERR_FILENO);
            copyData(sh->err, stderr);

            sh->tryWait();
        }

        if (pid)
        {
            if (0 == kill(pid, 0))
            {
                fmt::print("The process with pid = {} is running.\n", pid);
            }
        }

        if (!pid)
        {
            fmt::print("Now there is no clickhouse-server process.\n");
        }

        return pid;
    }

    int stop(const fs::path & pid_file, bool force)
    {
        UInt64 pid = isRunning(pid_file);

        if (!pid)
            return 0;

        int signal = force ? SIGKILL : SIGTERM;
        const char * signal_name = force ? "kill" : "terminate";

        if (0 == kill(pid, signal))
            fmt::print("Sent {} signal to process with pid {}.\n", signal_name, pid);
        else
            throwFromErrno(fmt::format("Cannot send {} signal", signal_name), ErrorCodes::SYSTEM_ERROR);

        size_t try_num = 0;
        constexpr size_t num_tries = 60;
        for (; try_num < num_tries; ++try_num)
        {
            fmt::print("Waiting for server to stop\n");
            if (!isRunning(pid_file))
            {
                fmt::print("Server stopped\n");
                break;
            }
            ::sleep(1);
        }

        if (try_num == num_tries)
        {
            fmt::print("Will terminate forcefully.\n", pid);
            if (0 == kill(pid, 9))
                fmt::print("Sent kill signal.\n", pid);
            else
                throwFromErrno("Cannot send kill signal", ErrorCodes::SYSTEM_ERROR);
        }

        return 0;
    }
}


int mainEntryClickHouseStart(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("binary-path", po::value<std::string>()->default_value("/usr/bin"), "directory with binary")
        ("config-path", po::value<std::string>()->default_value("/etc/clickhouse-server"), "directory with configs")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-server"), "directory for pid file")
        ("user", po::value<std::string>()->default_value("clickhouse"), "clickhouse user")
    ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: "
            << (getuid() == 0 ? "" : "sudo ")
            << argv[0]
            << " start\n";
        return 1;
    }

    try
    {
        std::string user = options["user"].as<std::string>();

        fs::path executable = fs::path(options["binary-path"].as<std::string>()) / "clickhouse-server";
        fs::path config = fs::path(options["config-path"].as<std::string>()) / "config.xml";
        fs::path pid_file = fs::path(options["pid-path"].as<std::string>()) / "clickhouse-server.pid";

        return start(user, executable, config, pid_file);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}


int mainEntryClickHouseStop(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-server"), "directory for pid file")
        ("force", po::value<bool>()->default_value(false), "Stop with KILL signal instead of TERM")
    ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: "
            << (getuid() == 0 ? "" : "sudo ")
            << argv[0]
            << " stop\n";
        return 1;
    }

    try
    {
        fs::path pid_file = fs::path(options["pid-path"].as<std::string>()) / "clickhouse-server.pid";

        return stop(pid_file, options["force"].as<bool>());
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}


int mainEntryClickHouseStatus(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-server"), "directory for pid file")
    ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: "
            << (getuid() == 0 ? "" : "sudo ")
            << argv[0]
            << " status\n";
        return 1;
    }

    try
    {
        fs::path pid_file = fs::path(options["pid-path"].as<std::string>()) / "clickhouse-server.pid";
        isRunning(pid_file);
        return 0;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}


int mainEntryClickHouseRestart(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("binary-path", po::value<std::string>()->default_value("/usr/bin"), "directory with binary")
        ("config-path", po::value<std::string>()->default_value("/etc/clickhouse-server"), "directory with configs")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-server"), "directory for pid file")
        ("user", po::value<std::string>()->default_value("clickhouse"), "clickhouse user")
        ("force", po::value<bool>()->default_value(false), "Stop with KILL signal instead of TERM")
    ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: "
            << (getuid() == 0 ? "" : "sudo ")
            << argv[0]
            << " restart\n";
        return 1;
    }

    try
    {
        std::string user = options["user"].as<std::string>();

        fs::path executable = fs::path(options["binary-path"].as<std::string>()) / "clickhouse-server";
        fs::path config = fs::path(options["config-path"].as<std::string>()) / "config.xml";
        fs::path pid_file = fs::path(options["pid-path"].as<std::string>()) / "clickhouse-server.pid";

        if (int res = stop(pid_file, options["force"].as<bool>()))
            return res;
        return start(user, executable, config, pid_file);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}
