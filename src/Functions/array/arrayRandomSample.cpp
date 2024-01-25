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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"array", &isArray<IDataType>, nullptr, "Array"},
            {"samples", &isUnsignedInteger<IDataType>, isColumnConst, "const UInt*"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        // Return an array with the same nested type as the input array
        const DataTypePtr & array_type = arguments[0].type;
        const DataTypeArray * array_data_type = checkAndGetDataType<DataTypeArray>(array_type.get());

        // Get the nested data type of the array
        const DataTypePtr & nested_type = array_data_type->getNestedType();

        return std::make_shared<DataTypeArray>(nested_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!column_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument must be an array");

        const IColumn * col_samples = arguments[1].column.get();
        if (!col_samples)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The second argument is empty or null, type = {}", arguments[1].type->getName());

        UInt64 samples;
        try
        {
            samples = col_samples->getUInt(0);
        }
        catch (...)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Failed to fetch UInt64 from the second argument column, type = {}",
                arguments[1].type->getName());
        }

        std::random_device rd;
        std::mt19937 gen(rd());

        auto nested_column = column_array->getDataPtr()->cloneEmpty();
        auto offsets_column = ColumnUInt64::create();

        auto res_data = ColumnArray::create(std::move(nested_column), std::move(offsets_column));

        const auto & input_offsets = column_array->getOffsets();
        auto & res_offsets = res_data->getOffsets();
        res_offsets.resize(input_rows_count);

        UInt64 cur_samples;
        size_t current_offset = 0;

        for (size_t row = 0; row < input_rows_count; row++)
        {
            size_t row_size = input_offsets[row] - current_offset;

            std::vector<size_t> indices(row_size);
            std::iota(indices.begin(), indices.end(), 0);
            std::shuffle(indices.begin(), indices.end(), gen);

            cur_samples = std::min(samples, static_cast<UInt64>(row_size));

            for (UInt64 j = 0; j < cur_samples; j++)
            {
                size_t source_index = indices[j];
                res_data->getData().insertFrom(column_array->getData(), source_index);
            }

            res_offsets[row] = current_offset + cur_samples;
            current_offset += cur_samples;
        }

        return res_data;
    }
};

REGISTER_FUNCTION(ArrayRandomSample)
{
    factory.registerFunction<FunctionArrayRandomSample>();
}

}
