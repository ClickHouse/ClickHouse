#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// `array_length(array, dimension)` - PostgreSQL compatibility function.
/// For the first dimension it is `length(array)`, except that an empty array gives `NULL`: an empty array
/// has no dimensions in PostgreSQL. A dimension that the array does not have gives `NULL` as well.
/// An array of PostgreSQL is rectangular, so every dimension has one length. A nested array of ClickHouse can
/// hold arrays of different sizes, so a dimension other than the first has no single length, and asking for
/// it is refused instead of answering with the length of some of the nested arrays.
class FunctionArrayLengthPostgreSQL final : public IFunction
{
public:
    static constexpr auto name = "array_length";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayLengthPostgreSQL>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"dimension", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), nullptr, "Integer"},
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr array_column = arguments[0].column->convertToFullColumnIfConst();
        const ColumnPtr dimension_column = arguments[1].column->convertToFullColumnIfConst();

        const auto & offsets = assert_cast<const ColumnArray &>(*array_column).getOffsets();
        const Int64 number_of_dimensions
            = static_cast<Int64>(assert_cast<const DataTypeArray &>(*arguments[0].type).getNumberOfDimensions());

        auto result = ColumnInt32::create(input_rows_count);
        auto null_map = ColumnUInt8::create(input_rows_count);
        auto & result_data = result->getData();
        auto & null_map_data = null_map->getData();

        ColumnArray::Offset previous_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const ColumnArray::Offset size = offsets[i] - previous_offset;
            previous_offset = offsets[i];

            const Int64 dimension = dimension_column->getInt(i);
            if (dimension > 1 && dimension <= number_of_dimensions)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Function {} supports only the first dimension of an array: the nested arrays of dimension {} "
                    "can have different sizes",
                    getName(), dimension);

            const bool has_length = dimension == 1 && size != 0;
            result_data[i] = has_length ? static_cast<Int32>(size) : 0;
            null_map_data[i] = has_length ? 0 : 1;
        }

        return ColumnNullable::create(std::move(result), std::move(null_map));
    }
};

}

REGISTER_FUNCTION(ArrayLengthPostgreSQL)
{
    FunctionDocumentation::Description description = R"(
PostgreSQL compatibility function. Returns the length of the requested dimension of an array.

For the first dimension it is `length` of the array, except that an empty array gives `NULL`, as in PostgreSQL.
A dimension that the array does not have gives `NULL`.
A dimension other than the first of a nested array is not supported: unlike in PostgreSQL, the nested arrays can have different sizes.
)";
    FunctionDocumentation::Syntax syntax = "array_length(arr, dimension)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The array.", {"Array(T)"}},
        {"dimension", "The dimension, counted from 1.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of elements of the array, or `NULL` if the array is empty or does not have the dimension.", {"Nullable(Int32)"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT array_length([1, 2, 3], 1)", "3"},
        {"Empty array", "SELECT array_length([], 1)", "ᴺᵁᴸᴸ"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayLengthPostgreSQL>(documentation, FunctionFactory::Case::Insensitive);
}

}
