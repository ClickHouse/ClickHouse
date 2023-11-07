#include <random>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Poco/Logger.h>

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
        const DataTypePtr & nested_type = array_data_type->getNestedType();
        return std::make_shared<DataTypeArray>(nested_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an array", getName());

        const IColumn * col_samples = arguments[1].column.get();
        if (!col_samples)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "The second argument of function {} is empty or null, type = {}",
                    getName(), arguments[1].type->getName());

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

        auto col_res_data = col_array->getDataPtr()->cloneEmpty();
        auto col_res_offsets = ColumnUInt64::create(input_rows_count);
        auto col_res = ColumnArray::create(std::move(col_res_data), std::move(col_res_offsets));

        const auto & array_offsets = col_array->getOffsets();
        auto & res_offsets = col_res->getOffsets();

        std::vector<size_t> indices;
        size_t prev_array_offset = 0;

        for (size_t row = 0; row < input_rows_count; row++)
        {
            const size_t num_elements = array_offsets[row] - prev_array_offset;

            indices.resize(num_elements);
            std::iota(indices.begin(), indices.end(), 0);
            std::shuffle(indices.begin(), indices.end(), gen);

            const size_t cur_samples = std::min(num_elements, samples);

            for (UInt64 i = 0; i < cur_samples; i++)
                col_res->getData().insertFrom(col_array->getData(), indices[i]);

            res_offsets[row] = prev_array_offset + cur_samples;
            prev_array_offset += cur_samples;
            indices.clear();
        }

        return col_res;
    }
};

REGISTER_FUNCTION(ArrayRandomSample)
{
    factory.registerFunction<FunctionArrayRandomSample>();
}

}
