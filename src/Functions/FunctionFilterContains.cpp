#include <memory>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/BloomFilter.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionFilterContains : public IFunction
{
public:
    static constexpr auto name = "filterContains";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFilterContains>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "Number of arguments for function {} can't be {}, should be 2",
                            getName(), arguments.size());

        /// TODO: check that 1st agrument is const string

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto filter_name_column = arguments[0].column;
        if (/*!filter_name_column->isConst() || */filter_name_column->getDataType() != TypeIndex::String)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument of function '{}' must be a String filter name",
                            getName());

        const auto filter_name = filter_name_column->getDataAt(0);
        (void)filter_name; /// TODO: lookup the filter by name
        std::shared_ptr<BloomFilter> filter = std::make_shared<BloomFilter>(10*1024, 4, 42);

        auto data_column = arguments[1].column;

        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            /// TODO: implement efficiently
            const auto & value = data_column->getDataAt(row);
            dst_data[row] = filter->find(value.data, value.size);
        }

        return dst;
    }
};

REGISTER_FUNCTION(FilterContains)
{
    factory.registerFunction<FunctionFilterContains>({}, FunctionFactory::Case::Insensitive);
}

}
