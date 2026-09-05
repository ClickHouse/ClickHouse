#include <Proxy.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/Environment.h>

#include <iostream>

#if USE_SILK
#include <ProxyServer.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

void ProxyApplication::initialize(Poco::Util::Application & self)
{
    BaseDaemon::initialize(self);
    logger().information("Starting ClickHouse proxy");
    LOG_INFO(&logger(), "OS Name = {}, OS Version = {}, OS Architecture = {}",
        Poco::Environment::osName(), Poco::Environment::osVersion(), Poco::Environment::osArchitecture());
}

void ProxyApplication::uninitialize()
{
    BaseDaemon::uninitialize();
}

std::string ProxyApplication::getDefaultConfigFileName() const
{
    return "proxy_config.xml";
}

bool ProxyApplication::allowTextLog() const
{
    return false;
}

int ProxyApplication::main(const std::vector<std::string> & /*args*/)
try
{
#if USE_SILK
    auto log = getLogger("Proxy");

    server = std::make_unique<Proxy::ProxyServer>(config(), log);
    server->start(config());

    LOG_INFO(log, "Ready for connections.");
    waitForTerminationRequest();

    server->stop();
    return Application::EXIT_OK;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "clickhouse-proxy is built on the silk fiber framework, which is only available on Linux "
        "for x86-64 (v2+) and AArch64 with io_uring. This build does not include it.");
#endif
}
catch (...)
{
    LOG_ERROR(&logger(), "{}", getCurrentExceptionMessage(true));
    auto code = getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : 1;
}

}

int mainEntryClickHouseProxy(int argc, char ** argv);
int mainEntryClickHouseProxy(int argc, char ** argv)
{
    DB::ProxyApplication app;
    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return static_cast<UInt8>(code) ? code : 1;
    }
}
