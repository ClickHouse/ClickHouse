#include <Functions/FunctionConstantBase.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Core/ServerUUID.h>
#include <Common/SymbolIndex.h>
#include <Common/DNSResolver.h>
#include <Common/DateLUT.h>
#include <Common/ClickHouseRevision.h>

#if defined(OS_LINUX)
#    include <Poco/Environment.h>
#endif

#include <Common/config_version.h>


namespace DB
{
namespace
{

#if defined(__ELF__) && !defined(OS_FREEBSD)
    /// buildId() - returns the compiler build id of the running binary.
    class FunctionBuildId : public FunctionConstantBase<FunctionBuildId, String, DataTypeString>
    {
    public:
        static constexpr auto name = "buildId";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionBuildId>(context); }
        explicit FunctionBuildId(ContextPtr context) : FunctionConstantBase(SymbolIndex::instance()->getBuildIDHex(), context->isDistributed()) {}
    };
#endif


    /// Get the host name. Is is constant on single server, but is not constant in distributed queries.
    class FunctionHostName : public FunctionConstantBase<FunctionHostName, String, DataTypeString>
    {
    public:
        static constexpr auto name = "hostName";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionHostName>(context); }
        explicit FunctionHostName(ContextPtr context) : FunctionConstantBase(DNSResolver::instance().getHostName(), context->isDistributed()) {}
    };


    class FunctionServerUUID : public FunctionConstantBase<FunctionServerUUID, UUID, DataTypeUUID>
    {
    public:
        static constexpr auto name = "serverUUID";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionServerUUID>(context); }
        explicit FunctionServerUUID(ContextPtr context) : FunctionConstantBase(ServerUUID::get(), context->isDistributed()) {}
    };


    class FunctionTcpPort : public FunctionConstantBase<FunctionTcpPort, UInt16, DataTypeUInt16>
    {
    public:
        static constexpr auto name = "tcpPort";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTcpPort>(context); }
        explicit FunctionTcpPort(ContextPtr context) : FunctionConstantBase(context->getTCPPort(), context->isDistributed()) {}
    };


    /// Returns the server time zone.
    class FunctionTimezone : public FunctionConstantBase<FunctionTimezone, String, DataTypeString>
    {
    public:
        static constexpr auto name = "timezone";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimezone>(context); }
        explicit FunctionTimezone(ContextPtr context) : FunctionConstantBase(String{DateLUT::instance().getTimeZone()}, context->isDistributed()) {}
    };


    /// Returns server uptime in seconds.
    class FunctionUptime : public FunctionConstantBase<FunctionUptime, UInt32, DataTypeUInt32>
    {
    public:
        static constexpr auto name = "uptime";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionUptime>(context); }
        explicit FunctionUptime(ContextPtr context) : FunctionConstantBase(context->getUptimeSeconds(), context->isDistributed()) {}
    };


    /// version() - returns the current version as a string.
    class FunctionVersion : public FunctionConstantBase<FunctionVersion, String, DataTypeString>
    {
    public:
        static constexpr auto name = "version";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionVersion>(context); }
        explicit FunctionVersion(ContextPtr context) : FunctionConstantBase(VERSION_STRING, context->isDistributed()) {}
    };

    /// revision() - returns the current revision.
    class FunctionRevision : public FunctionConstantBase<FunctionRevision, UInt32, DataTypeUInt32>
    {
    public:
        static constexpr auto name = "revision";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionRevision>(context); }
        explicit FunctionRevision(ContextPtr context) : FunctionConstantBase(ClickHouseRevision::getVersionRevision(), context->isDistributed()) {}
    };

    class FunctionZooKeeperSessionUptime : public FunctionConstantBase<FunctionZooKeeperSessionUptime, UInt32, DataTypeUInt32>
    {
    public:
        static constexpr auto name = "zookeeperSessionUptime";
        explicit FunctionZooKeeperSessionUptime(ContextPtr context)
            : FunctionConstantBase(context->getZooKeeperSessionUptime(), context->isDistributed())
        {
        }
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionZooKeeperSessionUptime>(context); }
    };

#if defined(OS_LINUX)
    class FunctionGetOSKernelVersion : public FunctionConstantBase<FunctionGetOSKernelVersion, String, DataTypeString>
    {
    public:
        static constexpr auto name = "getOSKernelVersion";
        explicit FunctionGetOSKernelVersion(ContextPtr context) : FunctionConstantBase(Poco::Environment::osName() + " " + Poco::Environment::osVersion(), context->isDistributed()) {}
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGetOSKernelVersion>(context); }
    };
#endif

}


void registerFunctionBuildId([[maybe_unused]] FunctionFactory & factory)
{
#if defined(__ELF__) && !defined(OS_FREEBSD)
    factory.registerFunction<FunctionBuildId>();
#endif
}

void registerFunctionHostName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHostName>();
    factory.registerAlias("hostname", "hostName");
}

void registerFunctionServerUUID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionServerUUID>();
}

void registerFunctionTcpPort(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTcpPort>();
}

void registerFunctionTimezone(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimezone>();
    factory.registerAlias("timeZone", "timezone");
}

void registerFunctionUptime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUptime>();
}

void registerFunctionVersion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVersion>(FunctionFactory::CaseInsensitive);
}

void registerFunctionRevision(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRevision>(FunctionFactory::CaseInsensitive);
}

void registerFunctionZooKeeperSessionUptime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionZooKeeperSessionUptime>();
}


void registerFunctionGetOSKernelVersion([[maybe_unused]] FunctionFactory & factory)
{
#if defined(OS_LINUX)
    factory.registerFunction<FunctionGetOSKernelVersion>();
#endif
}


}

