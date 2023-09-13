#include <random>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Poco/Logger.h>
#include "Columns/ColumnsNumber.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// arrayRandomSample(arr, k) - Returns k random elements from the input array
class FunctionArrayRandomSample : public IFunction
{
public:
    static constexpr auto name = "arrayRandomSample";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayRandomSample>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & first_arg = arguments[0];
        if (!isArray(first_arg))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected Array",
                arguments[0]->getName(),
                getName());

        const DataTypeArray * array_type = checkAndGetDataType<const DataTypeArray>(first_arg.get());

        if (!isUnsignedInteger(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument must be a unsigned integer");

        // Return an array with the same nested type as the input array
        return std::make_shared<DataTypeArray>(array_type->getNestedType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!column_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument must be an array");

        const IColumn * col_num = arguments[1].column.get();

        if (!col_num || col_num->empty())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "The second argument column is empty or null, type = {}", arguments[1].type->getName());
        }

        UInt64 K;
        try
        {
            K = col_num->getUInt(0);
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Failed to fetch UInt64 from the second argument column, type = {}",
                arguments[1].type->getName());
        }

        Poco::Logger::get("FunctionRandomSampleFromArray").debug("The number of samples K = " + std::to_string(K));

        const auto & offsets = column_array->getOffsets();
        size_t num_elements = offsets[0];

        Poco::Logger::get("FunctionRandomSampleFromArray").debug("The number of elements in the array = " + std::to_string(num_elements));

        std::random_device rd;
        std::mt19937 gen(rd());

        K = std::min(K, static_cast<UInt64>(num_elements));

        // Create an empty ColumnArray with the same structure as column_array
        auto nested_column = column_array->getDataPtr()->cloneEmpty();
        auto offsets_column = ColumnUInt64::create(); // Create an empty offsets column

        auto res_data = ColumnArray::create(std::move(nested_column), std::move(offsets_column));

        if (K == 0)
        {
            // Handle edge cases where input array is empty or K is 0
            res_data->getOffsets().push_back(0);
            return res_data;
        }

        std::vector<size_t> indices(num_elements);
        std::iota(indices.begin(), indices.end(), 0);
        std::shuffle(indices.begin(), indices.end(), gen);

        for (size_t i = 0; i < K; ++i)
        {
            size_t source_index = indices[i];

            // Insert the corresponding element from the source array
            res_data->getData().insertFrom(column_array->getData(), source_index);
        }

        // Update offsets manually for the single row
        auto & res_offsets = res_data->getOffsets();
        res_offsets.push_back(K);

        return res_data;
    }
};

REGISTER_FUNCTION(ArrayRandomSample)
{
    factory.registerFunction<FunctionArrayRandomSample>();
}

}
