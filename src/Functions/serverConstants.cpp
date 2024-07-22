#include <Functions/FunctionConstantBase.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Core/ServerUUID.h>
#include <Common/SymbolIndex.h>
#include <Common/DNSResolver.h>
#include <Common/DateLUT.h>
#include <Common/ClickHouseRevision.h>

#include <Poco/Environment.h>

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
        explicit FunctionBuildId(ContextPtr context) : FunctionConstantBase(SymbolIndex::instance().getBuildIDHex(), context->isDistributed()) {}
    };
#endif


    /// Get the host name. It is constant on single server, but is not constant in distributed queries.
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


    class FunctionTCPPort : public FunctionConstantBase<FunctionTCPPort, UInt16, DataTypeUInt16>
    {
    public:
        static constexpr auto name = "tcpPort";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTCPPort>(context); }
        explicit FunctionTCPPort(ContextPtr context) : FunctionConstantBase(context->getTCPPort(), context->isDistributed()) {}
    };


    /// Returns timezone for current session.
    class FunctionTimezone : public FunctionConstantBase<FunctionTimezone, String, DataTypeString>
    {
    public:
        static constexpr auto name = "timezone";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimezone>(context); }
        explicit FunctionTimezone(ContextPtr context) : FunctionConstantBase(DateLUT::instance().getTimeZone(), context->isDistributed()) {}
    };

    /// Returns the server time zone (timezone in which server runs).
    class FunctionServerTimezone : public FunctionConstantBase<FunctionServerTimezone, String, DataTypeString>
    {
    public:
        static constexpr auto name = "serverTimezone";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionServerTimezone>(context); }
        explicit FunctionServerTimezone(ContextPtr context) : FunctionConstantBase(DateLUT::serverTimezoneInstance().getTimeZone(), context->isDistributed()) {}
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

    class FunctionGetOSKernelVersion : public FunctionConstantBase<FunctionGetOSKernelVersion, String, DataTypeString>
    {
    public:
        static constexpr auto name = "getOSKernelVersion";
        explicit FunctionGetOSKernelVersion(ContextPtr context) : FunctionConstantBase(Poco::Environment::osName() + " " + Poco::Environment::osVersion(), context->isDistributed()) {}
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGetOSKernelVersion>(context); }
    };

    class FunctionDisplayName : public FunctionConstantBase<FunctionDisplayName, String, DataTypeString>
    {
    public:
        static constexpr auto name = "displayName";
        explicit FunctionDisplayName(ContextPtr context) : FunctionConstantBase(context->getConfigRef().getString("display_name", getFQDNOrHostName()), context->isDistributed()) {}
        static FunctionPtr create(ContextPtr context) {return std::make_shared<FunctionDisplayName>(context); }
    };
}

#if defined(__ELF__) && !defined(OS_FREEBSD)
REGISTER_FUNCTION(BuildId)
{
    factory.registerFunction<FunctionBuildId>();
}
#endif

REGISTER_FUNCTION(HostName)
{
    factory.registerFunction<FunctionHostName>();
    factory.registerAlias("hostname", "hostName");
}

REGISTER_FUNCTION(ServerUUID)
{
    factory.registerFunction<FunctionServerUUID>();
}

REGISTER_FUNCTION(TCPPort)
{
    factory.registerFunction<FunctionTCPPort>();
}

REGISTER_FUNCTION(Timezone)
{
    factory.registerFunction<FunctionTimezone>(
        FunctionDocumentation{
        .description=R"(
Returns the default timezone for current session.
Used as default timezone for parsing DateTime|DateTime64 without explicitly specified timezone.
Can be changed with SET timezone = 'New/Tz'

[example:timezone]
    )",
    .examples{{"timezone", "SELECT timezone();", ""}},
    .categories{"Constant", "Miscellaneous"}
});
factory.registerAlias("timeZone", "timezone");
}

REGISTER_FUNCTION(ServerTimezone)
{
    factory.registerFunction<FunctionServerTimezone>(
    FunctionDocumentation{
        .description=R"(
Returns the timezone name in which server operates.

[example:serverTimezone]
    )",
     .examples{{"serverTimezone", "SELECT serverTimezone();", ""}},
     .categories{"Constant", "Miscellaneous"}
});
    factory.registerAlias("serverTimeZone", "serverTimezone");
}

REGISTER_FUNCTION(Uptime)
{
    factory.registerFunction<FunctionUptime>();
}

REGISTER_FUNCTION(Version)
{
    factory.registerFunction<FunctionVersion>({}, FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(Revision)
{
    factory.registerFunction<FunctionRevision>({}, FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(ZooKeeperSessionUptime)
{
    factory.registerFunction<FunctionZooKeeperSessionUptime>();
}


REGISTER_FUNCTION(GetOSKernelVersion)
{
    factory.registerFunction<FunctionGetOSKernelVersion>();
}


REGISTER_FUNCTION(DisplayName)
{
    factory.registerFunction<FunctionDisplayName>(FunctionDocumentation
        {
            .description=R"(
Returns the value of `display_name` from config or server FQDN if not set.

[example:displayName]
)",
            .examples{{"displayName", "SELECT displayName();", ""}},
            .categories{"Constant", "Miscellaneous"}
        },
        FunctionFactory::CaseSensitive);
}


}
