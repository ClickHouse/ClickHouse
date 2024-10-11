#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Core/Field.h>
#include <Core/Settings.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_get_client_http_header;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int FUNCTION_NOT_ALLOWED;
}

namespace
{

class FunctionGetClientHTTPHeader : public IFunction, WithContext
{
public:
    explicit FunctionGetClientHTTPHeader(ContextPtr context_)
        : WithContext(context_)
    {
        if (!getContext()->getSettingsRef()[Setting::allow_get_client_http_header])
            throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "The function getClientHTTPHeader requires setting `allow_get_client_http_header` to be enabled.");
    }

    String getName() const override { return "getClientHTTPHeader"; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must be String", getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ClientInfo & client_info = getContext()->getClientInfo();

        const auto & source = arguments[0].column;
        auto result = result_type->createColumn();
        result->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            Field header;
            source->get(row, header);
            if (auto it = client_info.http_headers.find(header.safeGet<String>()); it != client_info.http_headers.end())
                result->insert(it->second);
            else
                result->insertDefault();
        }

        return result;
    }
};

}

REGISTER_FUNCTION(GetClientHTTPHeader)
{
    factory.registerFunction("getClientHTTPHeader",
        [](ContextPtr context) { return std::make_shared<FunctionGetClientHTTPHeader>(context); },
        FunctionDocumentation{
            .description = R"(
Get the value of an HTTP header.

If there is no such header or the current request is not performed via the HTTP interface, the function returns an empty string.
Certain HTTP headers (e.g., `Authentication` and `X-ClickHouse-*`) are restricted.

The function requires the setting `allow_get_client_http_header` to be enabled.
The setting is not enabled by default for security reasons, because some headers, such as `Cookie`, could contain sensitive info.

HTTP headers are case sensitive for this function.

If the function is used in the context of a distributed query, it returns non-empty result only on the initiator node.
",
            .syntax = "getClientHTTPHeader(name)",
            .arguments = {{"name", "The HTTP header name (String)"}},
            .returned_value = "The value of the header (String).",
            .categories{"Miscellaneous"}});
}

}
