#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include "Disks/DiskType.h"
#include "Interpreters/Context_fwd.h"
#include <Core/Field.h>
#include <Poco/Net/NameValueCollection.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int FUNCTION_NOT_ALLOWED;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** Get the value of parameter in http headers.
  * If there no such parameter or the method of request is not
  * http, the function will throw an exception.
  */
class FunctionGetClientHTTPHeader : public IFunction, WithContext
{
private:

public:
    explicit FunctionGetClientHTTPHeader(ContextPtr context_): WithContext(context_) {}

    static constexpr auto name = "getClientHTTPHeader";

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetClientHTTPHeader>(context_);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }


    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!getContext()->allowGetHTTPHeaderFunction())
            throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "The function {} is not enabled, you can set allow_get_client_http_header in config file.", getName());

        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must have String type", getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & client_info = getContext()->getClientInfo();
        const auto & method = client_info.http_method;
        const auto & headers = client_info.headers;
        const IColumn * arg_column = arguments[0].column.get();
        const ColumnString * arg_string = checkAndGetColumn<ColumnString>(arg_column);

        if (!arg_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must be constant String", getName());

        if (method != ClientInfo::HTTPMethod::GET && method != ClientInfo::HTTPMethod::POST)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        auto result_column = ColumnString::create();

        const String default_value;
        const std::unordered_set<String> & forbidden_header_list = getContext()->getClientHTTPHeaderForbiddenHeaders();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            auto header_name = arg_string->getDataAt(row).toString();

            if (!headers.has(header_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} is not in HTTP request headers.", header_name);
            else
            {
                auto it = forbidden_header_list.find(header_name);
                if (it != forbidden_header_list.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The header {} is in headers_forbidden_to_return_list, you can config it in config file.", header_name);

                const String & value = headers[header_name];
                result_column->insertData(value.data(), value.size());
            }
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(GetHttpHeader)
{
    factory.registerFunction<FunctionGetClientHTTPHeader>();
}

}
