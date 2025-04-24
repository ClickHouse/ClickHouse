#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

class FunctionArrayShingles : public IFunction
{
public:
    static constexpr auto name = "arrayShingles";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayShingles>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
            FunctionArgumentDescriptors args{
                {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
                {"length", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"}
            };
            validateFunctionArguments(*this, arguments, args);

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(array_type->getNestedType()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected array column for function {}", getName());

        const ColumnPtr & col_length = arguments[1].column;

        const auto & arr_offsets = col_array->getOffsets();
        const auto & arr_values = col_array->getData();

        auto col_res_data = arr_values.cloneEmpty();
        auto col_res_inner_offsets = ColumnArray::ColumnOffsets::create();
        auto col_res_outer_offsets = ColumnArray::ColumnOffsets::create();
        IColumn::Offsets & out_offsets_2 = col_res_inner_offsets->getData();
        IColumn::Offsets & out_offsets_1 = col_res_outer_offsets->getData();

        size_t pos1 = 0, pos2 = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const Int64 shingle_length = col_length->getInt(row);
            if (shingle_length < 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shingle argument of function {} must be a positive integer.", getName());

            const size_t array_length = arr_offsets[row] - arr_offsets[row - 1];
            if (static_cast<size_t>(shingle_length) > array_length)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shingle argument of function {} must less or equal than the array length.", getName());

            for (size_t i = 0; i < array_length - shingle_length + 1; ++i)
            {
                col_res_data->insertRangeFrom(arr_values, arr_offsets[row - 1] + i, shingle_length);
                pos1 += shingle_length;
                out_offsets_2.push_back(pos1);
            }
            pos2 += array_length - shingle_length + 1;
            out_offsets_1.push_back(pos2);
        }

        return ColumnArray::create(
            ColumnArray::create(
                std::move(col_res_data),
                std::move(col_res_inner_offsets)),
            std::move(col_res_outer_offsets)
        );
    }
};

REGISTER_FUNCTION(ArrayShingles)
{
    factory.registerFunction<FunctionArrayShingles>(
        FunctionDocumentation{
            .description = R"(
Generates an array of "shingles", i.e. consecutive sub-arrays with specified length of the input array.
)",
            .examples{
                {"example 1", "SELECT arrayShingles([1,2,3,4,5], 3)", "[[1,2,3],[2,3,4],[3,4,5]]"}
            },
            .categories = {"Array"},
        });
}

}

