#include <Functions/FunctionConstantBase.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Core/ServerUUID.h>
#include <Core/Settings.h>
#include <Common/SymbolIndex.h>
#include <Common/DNSResolver.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/ClickHouseRevision.h>
#include <Interpreters/Context.h>

#include <Poco/Environment.h>

#include <Common/config_version.h>


namespace DB
{
namespace
{

    template<typename Derived, typename T, typename ColumnT>
    class FunctionServerConstantBase : public FunctionConstantBase<Derived, T, ColumnT>
    {
    public:
        using FunctionConstantBase<Derived, T, ColumnT>::FunctionConstantBase;
        bool isServerConstant() const override { return true; }
    };

#if defined(__ELF__) && !defined(OS_FREEBSD)
    /// buildId() - returns the compiler build id of the running binary.
    class FunctionBuildId : public FunctionServerConstantBase<FunctionBuildId, String, DataTypeString>
    {
    public:
        static constexpr auto name = "buildId";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionBuildId>(context); }
        explicit FunctionBuildId(ContextPtr context) : FunctionServerConstantBase(SymbolIndex::instance().getBuildIDHex(), context->isDistributed()) {}
    };
#endif


    /// Get the host name. It is constant on single server, but is not constant in distributed queries.
    class FunctionHostName : public FunctionServerConstantBase<FunctionHostName, String, DataTypeString>
    {
    public:
        static constexpr auto name = "hostName";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionHostName>(context); }
        explicit FunctionHostName(ContextPtr context) : FunctionServerConstantBase(DNSResolver::instance().getHostName(), context->isDistributed()) {}
    };


    class FunctionServerUUID : public FunctionServerConstantBase<FunctionServerUUID, UUID, DataTypeUUID>
    {
    public:
        static constexpr auto name = "serverUUID";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionServerUUID>(context); }
        explicit FunctionServerUUID(ContextPtr context) : FunctionServerConstantBase(ServerUUID::get(), context->isDistributed()) {}
    };


    class FunctionTCPPort : public FunctionServerConstantBase<FunctionTCPPort, UInt16, DataTypeUInt16>
    {
    public:
        static constexpr auto name = "tcpPort";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTCPPort>(context); }
        explicit FunctionTCPPort(ContextPtr context) : FunctionServerConstantBase(context->getTCPPort(), context->isDistributed()) {}
    };


    /// Returns timezone for current session.
    class FunctionTimezone : public FunctionServerConstantBase<FunctionTimezone, String, DataTypeString>
    {
    public:
        static constexpr auto name = "timezone";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimezone>(context); }
        explicit FunctionTimezone(ContextPtr context) : FunctionServerConstantBase(DateLUT::instance().getTimeZone(), context->isDistributed()) {}
    };

    /// Returns the server time zone (timezone in which server runs).
    class FunctionServerTimezone : public FunctionServerConstantBase<FunctionServerTimezone, String, DataTypeString>
    {
    public:
        static constexpr auto name = "serverTimezone";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionServerTimezone>(context); }
        explicit FunctionServerTimezone(ContextPtr context) : FunctionServerConstantBase(DateLUT::serverTimezoneInstance().getTimeZone(), context->isDistributed()) {}
    };


    /// Returns server uptime in seconds.
    class FunctionUptime : public FunctionServerConstantBase<FunctionUptime, UInt32, DataTypeUInt32>
    {
    public:
        static constexpr auto name = "uptime";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionUptime>(context); }
        explicit FunctionUptime(ContextPtr context) : FunctionServerConstantBase(context->getUptimeSeconds(), context->isDistributed()) {}
    };


    /// version() - returns the current version as a string.
    class FunctionVersion : public FunctionServerConstantBase<FunctionVersion, String, DataTypeString>
    {
    public:
        static constexpr auto name = "version";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionVersion>(context); }
        explicit FunctionVersion(ContextPtr context) : FunctionServerConstantBase(VERSION_STRING, context->isDistributed()) {}
    };

    /// revision() - returns the current revision.
    class FunctionRevision : public FunctionServerConstantBase<FunctionRevision, UInt32, DataTypeUInt32>
    {
    public:
        static constexpr auto name = "revision";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionRevision>(context); }
        explicit FunctionRevision(ContextPtr context) : FunctionServerConstantBase(ClickHouseRevision::getVersionRevision(), context->isDistributed()) {}
    };

    class FunctionZooKeeperSessionUptime : public FunctionServerConstantBase<FunctionZooKeeperSessionUptime, UInt32, DataTypeUInt32>
    {
    public:
        static constexpr auto name = "zookeeperSessionUptime";
        explicit FunctionZooKeeperSessionUptime(ContextPtr context)
            : FunctionServerConstantBase(context->getZooKeeperSessionUptime(), context->isDistributed())
        {
        }
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionZooKeeperSessionUptime>(context); }
    };

    class FunctionGetOSKernelVersion : public FunctionServerConstantBase<FunctionGetOSKernelVersion, String, DataTypeString>
    {
    public:
        static constexpr auto name = "getOSKernelVersion";
        explicit FunctionGetOSKernelVersion(ContextPtr context) : FunctionServerConstantBase(Poco::Environment::osName() + " " + Poco::Environment::osVersion(), context->isDistributed()) {}
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGetOSKernelVersion>(context); }
    };

    class FunctionDisplayName : public FunctionServerConstantBase<FunctionDisplayName, String, DataTypeString>
    {
    public:
        static constexpr auto name = "displayName";
        explicit FunctionDisplayName(ContextPtr context) : FunctionServerConstantBase(context->getConfigRef().getString("display_name", getFQDNOrHostName()), context->isDistributed()) {}
        static FunctionPtr create(ContextPtr context) {return std::make_shared<FunctionDisplayName>(context); }
    };
}

#if defined(__ELF__) && !defined(OS_FREEBSD)
REGISTER_FUNCTION(BuildId)
{
    FunctionDocumentation::Description description = R"(
Returns the build ID generated by a compiler for the running ClickHouse server binary.
If executed in the context of a distributed table, this function generates a normal column with values relevant to each shard.
Otherwise it produces a constant value.
    )";
    FunctionDocumentation::Syntax syntax = "buildId()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the build ID.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example", R"(
SELECT buildId()
        )",
        R"(
┌─buildId()────────────────────────────────┐
│ AB668BEF095FAA6BD26537F197AC2AF48A927FB4 │
└──────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBuildId>(documentation);
}
#endif

REGISTER_FUNCTION(HostName)
{
    FunctionDocumentation::Description description = R"(
Returns the name of the host on which this function was executed.
If the function executes on a remote server (distributed processing), the remote server name is returned.
If the function executes in the context of a distributed table, it generates a normal column with values relevant to each shard.
Otherwise it produces a constant value.
    )";
    FunctionDocumentation::Syntax syntax = "hostName()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the host name.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT hostName()
        )",
        R"(
┌─hostName()─┐
│ clickhouse │
└────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHostName>(documentation);
    factory.registerAlias("hostname", "hostName");
}

REGISTER_FUNCTION(ServerUUID)
{
    FunctionDocumentation::Description description_serverUUID = R"(
Returns the random and unique UUID (v4) generated when the server is first started.
The UUID is persisted, i.e. the second, third, etc. server start return the same UUID.
    )";
    FunctionDocumentation::Syntax syntax_serverUUID = "serverUUID()";
    FunctionDocumentation::Arguments arguments_serverUUID = {};
    FunctionDocumentation::ReturnedValue returned_value_serverUUID = {"Returns the random UUID of the server.", {"UUID"}};
    FunctionDocumentation::Examples examples_serverUUID = {
    {
        "Usage example",
        R"(
SELECT serverUUID();
        )",
        R"(
┌─serverUUID()─────────────────────────────┐
│ 7ccc9260-000d-4d5c-a843-5459abaabb5f     │
└──────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_serverUUID = {20, 1};
    FunctionDocumentation::Category category_serverUUID = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_serverUUID = {description_serverUUID, syntax_serverUUID, arguments_serverUUID, {}, returned_value_serverUUID, examples_serverUUID, introduced_in_serverUUID, category_serverUUID};

    factory.registerFunction<FunctionServerUUID>(documentation_serverUUID);
}

REGISTER_FUNCTION(TCPPort)
{
    FunctionDocumentation::Description description = R"(
Returns the [native interface](/interfaces/tcp) TCP port number listened to by the server.
If executed in the context of a distributed table, this function generates a normal column with values relevant to each shard.
Otherwise it produces a constant value.
    )";
    FunctionDocumentation::Syntax syntax = "tcpPort()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the TCP port number.", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT tcpPort()
        )",
        R"(
┌─tcpPort()─┐
│      9000 │
└───────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTCPPort>(documentation);
}

REGISTER_FUNCTION(Timezone)
{
    FunctionDocumentation::Description description = R"(
Returns the time zone name of the current session or converts a time zone
offset or name to a canonical time zone name.
    )";
    FunctionDocumentation::Syntax syntax = R"(
timezone()
    )";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the canonical time zone name as a", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT timezone()
        )",
        R"(
┌─timezone()───────┐
│ Europe/Amsterdam │
└──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimezone>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("timeZone", "timezone");
}

REGISTER_FUNCTION(ServerTimezone)
{
    FunctionDocumentation::Description description = R"(
Returns the timezone of the server, i.e. the value of the [`timezone`](/operations/server-configuration-parameters/settings#timezone) setting.
If the function is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard. Otherwise, it produces a constant value.
    )";
    FunctionDocumentation::Syntax syntax = "serverTimezone()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the server timezone as a", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example", R"(
SELECT serverTimeZone()
        )",
        R"(
┌─serverTimeZone()─┐
│ UTC              │
└──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionServerTimezone>(documentation);
    factory.registerAlias("serverTimeZone", "serverTimezone");
}

REGISTER_FUNCTION(Uptime)
{
    FunctionDocumentation::Description description = R"(
Returns the server's uptime in seconds.
If executed in the context of a distributed table, this function generates a normal column with values relevant to each shard.
Otherwise it produces a constant value.
    )";
    FunctionDocumentation::Syntax syntax = "uptime()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the server uptime in seconds.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT uptime() AS Uptime
        )",
        R"(
┌─Uptime─┐
│  55867 │
└────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionUptime>(documentation);
}

REGISTER_FUNCTION(Version)
{
    FunctionDocumentation::Description description = R"(
Returns the current version of ClickHouse as a string in the form: `major_version.minor_version.patch_version.number_of_commits_since_the_previous_stable_release`.
If executed in the context of a distributed table, this function generates a normal column with values relevant to each shard.
Otherwise, it produces a constant value.
    )";
    FunctionDocumentation::Syntax syntax = "version()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the current version of ClickHouse.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT version()
        )",
        R"(
┌─version()─┐
│ 24.2.1.1  │
└───────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionVersion>(documentation, FunctionFactory::Case::Insensitive);
}

REGISTER_FUNCTION(Revision)
{
    FunctionDocumentation::Description description = R"(
Returns the current ClickHouse server revision.
    )";
    FunctionDocumentation::Syntax syntax = "revision()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the current ClickHouse server revision.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT revision()
        )",
        R"(
┌─revision()─┐
│      54485 │
└────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRevision>(documentation, FunctionFactory::Case::Insensitive);
}

REGISTER_FUNCTION(ZooKeeperSessionUptime)
{
    FunctionDocumentation::Description description_zookeeperSessionUptime = R"(
Returns the uptime of the current ZooKeeper session in seconds.
)";
    FunctionDocumentation::Syntax syntax_zookeeperSessionUptime = "zookeeperSessionUptime()";
    FunctionDocumentation::Arguments arguments_zookeeperSessionUptime = {};
    FunctionDocumentation::ReturnedValue returned_value_zookeeperSessionUptime = {"Returns the uptime of the current ZooKeeper session in seconds.", {"UInt32"}};
    FunctionDocumentation::Examples examples_zookeeperSessionUptime = {
    {
        "Usage example",
        R"(
SELECT zookeeperSessionUptime();
        )",
        R"(
┌─zookeeperSessionUptime()─┐
│                      286 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_zookeeperSessionUptime = {21, 11};
    FunctionDocumentation::Category category_zookeeperSessionUptime = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_zookeeperSessionUptime = {description_zookeeperSessionUptime, syntax_zookeeperSessionUptime, arguments_zookeeperSessionUptime, {}, returned_value_zookeeperSessionUptime, examples_zookeeperSessionUptime, introduced_in_zookeeperSessionUptime, category_zookeeperSessionUptime};

    factory.registerFunction<FunctionZooKeeperSessionUptime>(documentation_zookeeperSessionUptime);
}


REGISTER_FUNCTION(GetOSKernelVersion)
{
    FunctionDocumentation::Description description_getOSKernelVersion = R"(
Returns a string with the OS kernel version.
)";
    FunctionDocumentation::Syntax syntax_getOSKernelVersion = "getOSKernelVersion()";
    FunctionDocumentation::Arguments arguments_getOSKernelVersion = {};
    FunctionDocumentation::ReturnedValue returned_value_getOSKernelVersion = {"Returns the current OS kernel version.", {"String"}};
    FunctionDocumentation::Examples examples_getOSKernelVersion = {
    {
        "Usage example",
        R"(
SELECT getOSKernelVersion();
        )",
        R"(
┌─getOSKernelVersion()────┐
│ Linux 4.15.0-55-generic │
└─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_getOSKernelVersion = {21, 11};
    FunctionDocumentation::Category category_getOSKernelVersion = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_getOSKernelVersion = {description_getOSKernelVersion, syntax_getOSKernelVersion, arguments_getOSKernelVersion, {}, returned_value_getOSKernelVersion, examples_getOSKernelVersion, introduced_in_getOSKernelVersion, category_getOSKernelVersion};

    factory.registerFunction<FunctionGetOSKernelVersion>(documentation_getOSKernelVersion);
}


REGISTER_FUNCTION(DisplayName)
{
    FunctionDocumentation::Description description = R"(
Returns the value of `display_name` from [config](/operations/configuration-files) or the server's Fully Qualified Domain Name (FQDN) if not set.
)";
    FunctionDocumentation::Syntax syntax = "displayName()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the value of `display_name` from config or server FQDN if not set.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT displayName();
        )",
        R"(
┌─displayName()─┐
│ production    │
└───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDisplayName>(documentation);
}


}
