#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/DNSResolver.h>
#include <Poco/Net/IPAddress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class ReverseDNSQuery : public IFunction
{
public:
    static constexpr auto name = "reverseDNSQuery";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<ReverseDNSQuery>();
    }

    String getName() const override
    {
        return name;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        if (arguments.empty())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function " + String(name) + "requires at least one argument");
        }

        auto ip_address_input = arguments[0].column;

        auto ip_address = Poco::Net::IPAddress(ip_address_input->getDataAt(0).toString());

        auto ptr_records = DNSResolver::instance().reverseResolve(ip_address);

        auto string_column = ColumnString::create();

        auto offsets_column = ColumnArray::ColumnOffsets::create();

        if (!ptr_records.empty())
        {
            for (const auto & ptr_record : ptr_records)
            {
                string_column->insertData(ptr_record.c_str(), ptr_record.size());
            }

            offsets_column->insert(string_column->size());
        }
        else
        {
            offsets_column->insertDefault();
        }

        return ColumnArray::create(std::move(string_column), std::move(offsets_column));
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
