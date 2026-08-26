#include <BridgeHelper/IBridgeHelper.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ReadHelpers.h>
#include <filesystem>
#include <thread>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_SERVER_IS_NOT_RESPONDING;
    extern const int BAD_ARGUMENTS;
}


void IBridgeHelper::startBridgeSync()
{
    if (!bridgeHandShake())
    {
        LOG_TRACE(getLog(), "{} is not running, will try to start it", serviceAlias());
        startBridge(startBridgeCommand());
        bool started = false;

        uint64_t milliseconds_to_wait = 10; /// Exponential backoff
        uint64_t counter = 0;

        while (milliseconds_to_wait < 10000)
        {
            ++counter;
            LOG_TRACE(getLog(), "Checking {} is running, try {}", serviceAlias(), counter);

            if (bridgeHandShake())
            {
                started = true;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds_to_wait));
            milliseconds_to_wait *= 2;
        }

        if (!started)
            throw Exception(ErrorCodes::EXTERNAL_SERVER_IS_NOT_RESPONDING, "BridgeHelper: {} is not responding", serviceAlias());
    }
}


std::unique_ptr<ShellCommand> IBridgeHelper::startBridgeCommand()
{
    if (startBridgeManually())
        throw Exception(ErrorCodes::EXTERNAL_SERVER_IS_NOT_RESPONDING, "{} is not running. Please, start it manually", serviceAlias());

    const auto & config = getConfig();
    /// Path to executable folder
    fs::path path(config.getString("application.dir", "/usr/bin"));

    std::vector<std::string> cmd_args;
    path /= serviceFileName();

    cmd_args.push_back("--http-port");
    cmd_args.push_back(std::to_string(config.getUInt(configPrefix() + ".port", getDefaultPort())));
    cmd_args.push_back("--listen-host");
    cmd_args.push_back(config.getString(configPrefix() + ".listen_host", DEFAULT_HOST));
    cmd_args.push_back("--http-timeout");
    cmd_args.push_back(std::to_string(getHTTPTimeout().totalMicroseconds()));
    cmd_args.push_back("--http-max-field-value-size");
    cmd_args.push_back("99999999999999999"); // something "big" to accept large datasets (issue 47616)
    if (config.has("logger." + configPrefix() + "_log"))
    {
        cmd_args.push_back("--log-path");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_log"));
    }
    if (config.has("logger." + configPrefix() + "_errlog"))
    {
        cmd_args.push_back("--err-log-path");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_errlog"));
    }
    if (config.has("logger." + configPrefix() + "_stdout"))
    {
        cmd_args.push_back("--stdout-path");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_stdout"));
    }
    if (config.has("logger." + configPrefix() + "_stderr"))
    {
        cmd_args.push_back("--stderr-path");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_stderr"));
    }
    if (config.has("logger." + configPrefix() + "_level"))
    {
        cmd_args.push_back("--log-level");
        cmd_args.push_back(config.getString("logger." + configPrefix() + "_level"));
    }

    std::string allowed_paths;
    for (const auto * allowed_path_config : {"dictionaries_lib_path", "catboost_lib_path"})
    {
        if (config.has(allowed_path_config))
        {
            std::string allowed_path = config.getString(allowed_path_config);
            if (allowed_path.contains(':'))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`{}` cannot contain the colon (:) symbol: {}", allowed_path_config, allowed_path);

            if (!allowed_paths.empty())
                allowed_paths += ":";
            allowed_paths += allowed_path;
        }
    }

    if (!allowed_paths.empty())
    {
        cmd_args.push_back("--libraries-path");
        cmd_args.push_back(allowed_paths);
    }

    LOG_TRACE(getLog(), "Starting {}", serviceAlias());

    /// We will terminate it with the KILL signal instead of the TERM signal,
    /// because it's more reliable for arbitrary third-party ODBC drivers.
    /// The drivers can spawn threads, install their own signal handlers... we don't care.

    ShellCommand::Config command_config(path.string());
    command_config.arguments = cmd_args;
    command_config.terminate_in_destructor_strategy = ShellCommand::DestructorStrategy(true, SIGKILL);

    return ShellCommand::executeDirect(command_config);
}

}
