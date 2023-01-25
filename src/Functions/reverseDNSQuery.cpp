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
    factory.registerFunction<ReverseDNSQuery>();
}

}
