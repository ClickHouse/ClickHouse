#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/DNSResolver.h>
#include <Poco/Net/IPAddress.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int FUNCTION_NOT_ALLOWED;
}

class ReverseDNSQuery : public IFunction
{
public:
    static constexpr auto name = "reverseDNSQuery";
    static constexpr auto allow_function_config_name = "allow_reverse_dns_query_function";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<ReverseDNSQuery>();
    }

    String getName() const override
    {
        return name;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & data_type, size_t input_rows_count) const override
    {
        if (!Context::getGlobalContextInstance()->getConfigRef().getBool(allow_function_config_name, false))
        {
            throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "Function {} is not allowed because {} is not set", name, allow_function_config_name);
        }

        if (arguments.empty())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", name);
        }

        auto res_type = getReturnTypeImpl({data_type});

        if (input_rows_count == 0u)
        {
            return res_type->createColumnConstWithDefaultValue(input_rows_count);
        }

        if (!isString(arguments[0].type))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires the input column to be of type String", name);
        }

        auto input_column = arguments[0].column;

        auto ip_address = Poco::Net::IPAddress(input_column->getDataAt(0).toString());

        auto ptr_records = DNSResolver::instance().reverseResolve(ip_address);

        if (ptr_records.empty())
            return res_type->createColumnConstWithDefaultValue(input_rows_count);

        Array res;

        for (const auto & ptr_record : ptr_records)
        {
            res.push_back(ptr_record);
        }

        return res_type->createColumnConst(input_rows_count, res);
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 1u;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

};


REGISTER_FUNCTION(ReverseDNSQuery)
{
    factory.registerFunction<ReverseDNSQuery>(
        Documentation(
            R"(Performs a reverse DNS query to get the PTR records associated with the IP address.

                **Syntax**

                ``` sql
                reverseDNSQuery(address)
                ```

                This function performs reverse DNS resolutions on both IPv4 and IPv6.

                **Arguments**

                -   `address` — An IPv4 or IPv6 address. [String](../../sql-reference/data-types/string.md).

                **Returned value**

                -   Associated domains (PTR records).

                Type: Type: [Array(String)](../../sql-reference/data-types/array.md).

                **Example**

                Query:

                ``` sql
                SELECT reverseDNSQuery('192.168.0.2');
                ```

                Result:

                ``` text
                ┌─reverseDNSQuery('192.168.0.2')────────────┐
                │ ['test2.example.com','test3.example.com'] │
                └───────────────────────────────────────────┘
                ```
                )")
    );
}

}
