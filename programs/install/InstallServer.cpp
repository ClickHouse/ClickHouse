#include <iostream>
#include <filesystem>
#include <boost/program_options.hpp>
#include <Common/Exception.h>

#include "InstallCommon.h"

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

using namespace DB;
namespace po = boost::program_options;
namespace fs = std::filesystem;

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
        /// Create symlinks.
        std::vector<std::string> tools
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
            "clickhouse-extract-from-config",
        };

        installBinaries(options, tools);
        createUsers(options);
        bool has_password_for_default_user = setupConfigsAndDirectories(options);
        createPasswordAndFinish(options, has_password_for_default_user);
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

        return stop(pid_file, options["force"].as<bool>(), "clickhouse-server");
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
        isRunning(pid_file, "clickhouse-server");
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

        if (int res = stop(pid_file, options["force"].as<bool>(), "clickhouse-server"))
            return res;
        return start(user, executable, config, pid_file);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << '\n';
        return getCurrentExceptionCode();
    }
}
