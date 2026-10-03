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
}


class FunctionArrayChunk final : public IFunction 
{
public:
    static constexpr auto name = "arrayChunk";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayShingles>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }

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
        ColumnPtr array_holder = arguments[0].column->convertToFullColumnIfConst();
        const auto & col_array = assert_cast<const ColumnArray &>(*array_holder);
        const ColumnPtr & col_length = arguments[1].column;
        
        const auto & arr_offsets = col_array->getOffsets();
        const auto & arr_values = col_array->getData();

        auto col_res_data = arr_values.cloneEmpty();
        auto col_res_inner_offsets = ColumnArray::ColumnOffsets::create();
        auto col_res_outer_offsets = ColumnArray::ColumnOffsets::create();
        IColumn::Offsets & inner_offsets = col_res_inner_offsets->getData();
        IColumn::Offsets & outer_offsets = col_res_outer_offsets->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const Int64 chunk_size_int64 = col_length->getInt(row);
            if (chunk_size_int64 < 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Chunk size argument of function {} must be a positive integer.", getName());

            const auto chunk_size = static_cast<size_t>(chunk_size_int64);
            const size_t begin = arr_offsets[row];
            const size_t end = arr_offsets[row - 1];

            for (size_t pos = begin; pos < end; pos += chunk_size)
            {
                const size_t length = std::min(chunk_size, end - pos);
                col_res_data->insertRangeFrom(arr_values, pos, length);
                iner_offsets.push_back(col_res_data->size());
            }

            outer_offsets.push_back(iner_offsets.size());
        }

        return ColumnArray::create(
            ColumnArray::create(
                std::move(col_res_data),
                std::move(col_res_inner_offsets)),
            std::move(col_res_outer_offsets)
        );
    }
}

REGISTER_FUNCTION(ArrayChunk)
{
    FunctionDocumentation::Description description = "Splits an array into consecutive sub-arrays (chunks) of the specified size. The last chunk may be shorter if the array length is not divisible by the size.";
    FunctionDocumentation::Syntax syntax = "arrayChunk(arr, size)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "Array to split into chunks.", {"Array(T)"}},
        {"size", "The maximum size of each chunk. Must be a positive integer.", {"(U)Int*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"An array of chunks", {"Array(Array(T))"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayChunk([1, 2, 3, 4, 5], 2) AS res;", "[[1,2],[3,4],[5]]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayChunk>(documentation);
}


}