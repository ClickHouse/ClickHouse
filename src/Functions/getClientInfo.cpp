#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/convertFieldToType.h>
#include <Core/Field.h>
#include <Common/Exception.h>

#include <Poco/Net/SocketAddress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

String addressToString(const std::shared_ptr<Poco::Net::SocketAddress> & address)
{
    if (!address)
        return {};
    return address->toString();
}

/// Fixed return type for each ClientInfo attribute (does not depend on the current value).
DataTypePtr getClientInfoAttributeType(std::string_view name)
{
    if (name == "query_kind"
        || name == "current_user"
        || name == "current_query_id"
        || name == "current_address"
        || name == "authenticated_user"
        || name == "initial_user"
        || name == "initial_query_id"
        || name == "initial_address"
        || name == "connection_address"
        || name == "interface"
        || name == "certificate"
        || name == "os_user"
        || name == "client_hostname"
        || name == "client_name"
        || name == "client_agent"
        || name == "http_method"
        || name == "http_user_agent"
        || name == "http_referer"
        || name == "forwarded_for"
        || name == "quota_key")
        return std::make_shared<DataTypeString>();

    if (name == "is_secure"
        || name == "is_replicated_database_internal"
        || name == "is_shared_catalog_internal"
        || name == "is_internal"
        || name == "collaborate_with_initiator")
        return std::make_shared<DataTypeUInt8>();

    if (name == "client_version_major"
        || name == "client_version_minor"
        || name == "client_version_patch"
        || name == "connection_client_version_major"
        || name == "connection_client_version_minor"
        || name == "connection_client_version_patch"
        || name == "connection_id"
        || name == "distributed_depth"
        || name == "number_of_current_replica")
        return std::make_shared<DataTypeUInt64>();

    if (name == "client_tcp_protocol_version"
        || name == "connection_tcp_protocol_version"
        || name == "connection_parallel_replicas_protocol_version"
        || name == "script_query_number"
        || name == "script_line_number")
        return std::make_shared<DataTypeUInt32>();

    if (name == "initial_query_start_time")
        return std::make_shared<DataTypeDateTime>();

    if (name == "initial_query_start_time_microseconds")
        return std::make_shared<DataTypeDateTime64>(6);

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown ClientInfo attribute name: '{}'", name);
}

String queryKindToString(ClientInfo::QueryKind kind)
{
    switch (kind)
    {
        case ClientInfo::QueryKind::NO_QUERY:
            return "NO_QUERY";
        case ClientInfo::QueryKind::INITIAL_QUERY:
            return "INITIAL_QUERY";
        case ClientInfo::QueryKind::SECONDARY_QUERY:
            return "SECONDARY_QUERY";
    }
    return "NO_QUERY";
}

Field getClientInfoAttributeValue(const ClientInfo & client_info, std::string_view name)
{
    if (name == "query_kind")
        return queryKindToString(client_info.query_kind);
    if (name == "current_user")
        return client_info.current_user;
    if (name == "current_query_id")
        return client_info.current_query_id;
    if (name == "current_address")
        return addressToString(client_info.current_address);
    if (name == "authenticated_user")
        return client_info.authenticated_user;
    if (name == "initial_user")
        return client_info.initial_user;
    if (name == "initial_query_id")
        return client_info.initial_query_id;
    if (name == "initial_address")
        return addressToString(client_info.initial_address);
    if (name == "connection_address")
        return addressToString(client_info.connection_address);
    if (name == "initial_query_start_time")
        return static_cast<UInt64>(client_info.initial_query_start_time);
    if (name == "initial_query_start_time_microseconds")
        return DecimalField<Decimal64>(client_info.initial_query_start_time_microseconds, 6);
    if (name == "interface")
        return toString(client_info.interface);
    if (name == "is_secure")
        return static_cast<UInt64>(client_info.is_secure);
    if (name == "certificate")
        return client_info.certificate;
    if (name == "os_user")
        return client_info.os_user;
    if (name == "client_hostname")
        return client_info.client_hostname;
    if (name == "client_name")
        return client_info.client_name;
    if (name == "client_agent")
        return client_info.client_agent;
    if (name == "client_version_major")
        return client_info.client_version_major;
    if (name == "client_version_minor")
        return client_info.client_version_minor;
    if (name == "client_version_patch")
        return client_info.client_version_patch;
    if (name == "client_tcp_protocol_version")
        return static_cast<UInt64>(client_info.client_tcp_protocol_version);
    if (name == "script_query_number")
        return static_cast<UInt64>(client_info.script_query_number);
    if (name == "script_line_number")
        return static_cast<UInt64>(client_info.script_line_number);
    if (name == "connection_client_version_major")
        return client_info.connection_client_version_major;
    if (name == "connection_client_version_minor")
        return client_info.connection_client_version_minor;
    if (name == "connection_client_version_patch")
        return client_info.connection_client_version_patch;
    if (name == "connection_tcp_protocol_version")
        return static_cast<UInt64>(client_info.connection_tcp_protocol_version);
    if (name == "connection_parallel_replicas_protocol_version")
        return static_cast<UInt64>(client_info.connection_parallel_replicas_protocol_version);
    if (name == "http_method")
        return toString(client_info.http_method);
    if (name == "http_user_agent")
        return client_info.http_user_agent;
    if (name == "http_referer")
        return client_info.http_referer;
    if (name == "connection_id")
        return client_info.connection_id;
    if (name == "forwarded_for")
        return client_info.forwarded_for;
    if (name == "quota_key")
        return client_info.quota_key;
    if (name == "distributed_depth")
        return client_info.distributed_depth;
    if (name == "is_replicated_database_internal")
        return static_cast<UInt64>(client_info.is_replicated_database_internal);
    if (name == "is_shared_catalog_internal")
        return static_cast<UInt64>(client_info.is_shared_catalog_internal);
    if (name == "is_internal")
        return static_cast<UInt64>(client_info.is_internal);
    if (name == "collaborate_with_initiator")
        return static_cast<UInt64>(client_info.collaborate_with_initiator);
    if (name == "number_of_current_replica")
        return client_info.number_of_current_replica;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown ClientInfo attribute name: '{}'", name);
}

/// Get a ClientInfo attribute value by name.
/// Mirrors getSetting: constant string argument, return type depends on the attribute.
class FunctionGetClientInfo final : public IFunction, WithContext
{
public:
    static constexpr auto name = "getClientInfo";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetClientInfo>(context_); }

    explicit FunctionGetClientInfo(ContextPtr context_)
        : WithContext(context_)
    {
    }

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return getClientInfoAttributeType(getAttributeName(arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto attr_name = getAttributeName(arguments);
        const Field value = getClientInfoAttributeValue(getContext()->getClientInfo(), attr_name);
        return result_type->createColumnConst(input_rows_count, convertFieldToType(value, *result_type));
    }

private:
    std::string_view getAttributeName(const ColumnsWithTypeAndName & arguments) const
    {
        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} should be a constant string with the name of a ClientInfo attribute",
                getName());

        const auto * column = arguments[0].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The argument of function {} should be a constant string with the name of a ClientInfo attribute",
                getName());

        return column->getDataAt(0);
    }
};

}

REGISTER_FUNCTION(GetClientInfo)
{
    FunctionDocumentation::Description description = R"(
Returns a field from the client info of the current query.
Useful for DEFAULT / MATERIALIZED columns that should record where an INSERT came from
(for example, the client hostname).

The set of supported attribute names matches simple scalar fields of `ClientInfo`
(the same information that appears in `system.query_log` / `system.processes`).
Complex fields such as HTTP headers and OpenTelemetry trace context are not exposed;
use `getClientHTTPHeader` for headers.

Supported string attributes:
`query_kind`, `current_user`, `current_query_id`, `current_address`, `authenticated_user`,
`initial_user`, `initial_query_id`, `initial_address`, `connection_address`, `interface`,
`certificate`, `os_user`, `client_hostname`, `client_name`, `client_agent`, `http_method`,
`http_user_agent`, `http_referer`, `forwarded_for`, `quota_key`.

Supported numeric / temporal attributes:
`is_secure`, `is_replicated_database_internal`, `is_shared_catalog_internal`, `is_internal`,
`collaborate_with_initiator`, `client_version_major`, `client_version_minor`,
`client_version_patch`, `client_tcp_protocol_version`, `script_query_number`,
`script_line_number`, `connection_client_version_major`, `connection_client_version_minor`,
`connection_client_version_patch`, `connection_tcp_protocol_version`,
`connection_parallel_replicas_protocol_version`, `connection_id`, `distributed_depth`,
`number_of_current_replica`, `initial_query_start_time`, `initial_query_start_time_microseconds`.

`interface` and `http_method` are returned as human-readable strings (for example `TCP`, `HTTP`, `GET`).
`query_kind` is returned as `NO_QUERY`, `INITIAL_QUERY`, or `SECONDARY_QUERY`.
)";
    FunctionDocumentation::Syntax syntax = "getClientInfo(attribute_name)";
    FunctionDocumentation::Arguments arguments = {
        {"attribute_name", "Name of the ClientInfo field to read. Must be a constant string.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Value of the requested ClientInfo attribute. The data type depends on the attribute.",
        {"String", "UInt8", "UInt32", "UInt64", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Client hostname",
            R"(
SELECT getClientInfo('client_hostname');
            )",
            R"(
┌─getClientInfo('client_hostname')─┐
│ clickhouse.dev.local             │
└──────────────────────────────────┘
            )"
        },
        {
            "Interface",
            R"(
SELECT getClientInfo('interface');
            )",
            R"(
┌─getClientInfo('interface')─┐
│ TCP                        │
└────────────────────────────┘
            )"
        },
        {
            "Use in DEFAULT column",
            R"(
CREATE TABLE example
(
    x UInt32,
    host String DEFAULT getClientInfo('client_hostname')
)
ENGINE = Memory;
            )",
            R"(
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("getClientInfo", [](ContextPtr context) { return FunctionGetClientInfo::create(context); }, documentation);
}

}
