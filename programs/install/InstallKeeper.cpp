#include <iostream>
#include <filesystem>
#include <boost/program_options.hpp>
#include <Common/Exception.h>
#include <common/getResource.h>
#include <Common/Config/ConfigProcessor.h>
#include <Poco/Util/XMLConfiguration.h>
#include <IO/WriteBufferFromFile.h>

#include "InstallCommon.h"

using namespace DB;
namespace po = boost::program_options;
namespace fs = std::filesystem;


namespace
{

/// Creating configuration files and directories.
void setupKeeperConfigsAndDirectories(po::variables_map & options)
{
    fs::path prefix = fs::path(options["prefix"].as<std::string>());

    fs::path config_dir = prefix / options["config-path"].as<std::string>();

    if (!fs::exists(config_dir))
    {
        fmt::print("Creating config directory {}.\n", config_dir.string());
        fs::create_directories(config_dir);
    }

    fs::path main_config_file = config_dir / "keeper_config.xml";
    fs::path config_d = config_dir / "config.d";

    std::string log_path = prefix / options["log-path"].as<std::string>();
    std::string data_path = prefix / options["data-path"].as<std::string>();
    std::string pid_path = prefix / options["pid-path"].as<std::string>();

    if (!fs::exists(config_d))
    {
        fmt::print("Creating config directory {} that is used for tweaks of main server configuration.\n", config_d.string());
        fs::create_directory(config_d);
    }

    if (!fs::exists(main_config_file))
    {
        std::string_view main_config_content = getResource("keeper_config.xml");
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

    std::string user = options["user"].as<std::string>();
    std::string group = options["group"].as<std::string>();

    /// Chmod and chown configs
    {
        std::string command = fmt::format("chown --recursive {}:{} '{}'", user, group, config_dir.string());
        fmt::print(" {}\n", command);
        executeScript(command);
    }

    /// Symlink "preprocessed_configs" is created by the server, so "write" is needed.
    fs::permissions(config_dir, fs::perms::owner_all, fs::perm_options::replace);

    /// Readonly.
    if (fs::exists(main_config_file))
        fs::permissions(main_config_file, fs::perms::owner_read, fs::perm_options::replace);

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
}

}

int mainEntryClickHouseKeeperInstall(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("prefix", po::value<std::string>()->default_value(""), "prefix for all paths")
        ("binary-path", po::value<std::string>()->default_value("/usr/bin"), "where to install binaries")
        ("config-path", po::value<std::string>()->default_value("/etc/clickhouse-keeper"), "where to install configs")
        ("log-path", po::value<std::string>()->default_value("/var/log/clickhouse-keeper"), "where to create log directory")
        ("data-path", po::value<std::string>()->default_value("/var/lib/clickhouse"), "directory for data")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-keeper"), "directory for pid file")
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
            << " install-keeper [options]\n";
        std::cout << desc << '\n';
        return 1;
    }

    try
    {
        /// Create symlinks.
        std::vector<std::string> tools
        {
            "clickhouse-keeper",
        };

        installBinaries(options, tools);
        createUsers(options);
        setupKeeperConfigsAndDirectories(options);

        fmt::print(
            "\nClickHouse-Keeper has been successfully installed.\n"
            "\nStart clickhouse-keeper with:\n"
            " sudo clickhouse start-keeper\n");
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

int mainEntryClickHouseKeeperStart(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("binary-path", po::value<std::string>()->default_value("/usr/bin"), "directory with binary")
        ("config-path", po::value<std::string>()->default_value("/etc/clickhouse-keeper"), "directory with configs")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-keeper"), "directory for pid file")
        ("user", po::value<std::string>()->default_value("clickhouse"), "clickhouse user")
    ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: "
            << (getuid() == 0 ? "" : "sudo ")
            << argv[0]
            << " start-keeper\n";
        return 1;
    }

    try
    {
        std::string user = options["user"].as<std::string>();

        fs::path executable = fs::path(options["binary-path"].as<std::string>()) / "clickhouse-keeper";
        fs::path config = fs::path(options["config-path"].as<std::string>()) / "keeper_config.xml";
        fs::path pid_file = fs::path(options["pid-path"].as<std::string>()) / "clickhouse-keeper.pid";

        return start(user, executable, config, pid_file);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}


int mainEntryClickHouseKeeperStop(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-keeper"), "directory for pid file")
        ("force", po::value<bool>()->default_value(false), "Stop with KILL signal instead of TERM")
    ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: "
            << (getuid() == 0 ? "" : "sudo ")
            << argv[0]
            << " stop-keeper\n";
        return 1;
    }

    try
    {
        fs::path pid_file = fs::path(options["pid-path"].as<std::string>()) / "clickhouse-keeper.pid";

        return stop(pid_file, options["force"].as<bool>(), "clickhouse-keeper");
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}

int mainEntryClickHouseKeeperStatus(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-keeper"), "directory for pid file")
    ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: "
            << (getuid() == 0 ? "" : "sudo ")
            << argv[0]
            << " status-keeper\n";
        return 1;
    }

    try
    {
        fs::path pid_file = fs::path(options["pid-path"].as<std::string>()) / "clickhouse-keeper.pid";
        isRunning(pid_file, "clickhouse-keeper");
        return 0;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}

int mainEntryClickHouseKeeperRestart(int argc, char ** argv)
{
    po::options_description desc;
    desc.add_options()
        ("help,h", "produce help message")
        ("binary-path", po::value<std::string>()->default_value("/usr/bin"), "directory with binary")
        ("config-path", po::value<std::string>()->default_value("/etc/clickhouse-keeper"), "directory with configs")
        ("pid-path", po::value<std::string>()->default_value("/var/run/clickhouse-keeper"), "directory for pid file")
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
            << " restart-keeper\n";
        return 1;
    }

    try
    {
        std::string user = options["user"].as<std::string>();

        fs::path executable = fs::path(options["binary-path"].as<std::string>()) / "clickhouse-keeper";
        fs::path config = fs::path(options["config-path"].as<std::string>()) / "keeper_config.xml";
        fs::path pid_file = fs::path(options["pid-path"].as<std::string>()) / "clickhouse-keeper.pid";

        if (int res = stop(pid_file, options["force"].as<bool>(), "clickhouse-keeper"))
            return res;
        return start(user, executable, config, pid_file);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}
