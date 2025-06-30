#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

#include "FunctionArrayMapped.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

enum class ArrayFirstLastStrategy : uint8_t
{
    First,
    Last
};

enum class ArrayFirstLastElementNotExistsStrategy : uint8_t
{
    Default,
    Null
};

template <ArrayFirstLastStrategy strategy, ArrayFirstLastElementNotExistsStrategy element_not_exists_strategy>
struct ArrayFirstLastImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
            return makeNullable(array_element);

        return array_element;
    }

    static ColumnPtr createNullableColumn(MutableColumnPtr && column, ColumnUInt8::MutablePtr && null_map)
    {
        if (auto * nullable_column = typeid_cast<ColumnNullable *>(column.get()))
        {
            nullable_column->applyNullMap(*null_map);
            return std::move(column);
        }
        return ColumnNullable::create(std::move(column), std::move(null_map));
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const auto * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of filter column: {}; The result of the lambda is expected to be a UInt8", mapped->getDataType());

            if (column_filter_const->getValue<UInt8>())
            {
                const auto & offsets = array.getOffsets();
                const auto & data = array.getData();
                auto out = data.cloneEmpty();
                out->reserve(data.size());

                size_t offsets_size = offsets.size();

                ColumnUInt8::MutablePtr col_null_map_to;
                ColumnUInt8::Container * vec_null_map_to = nullptr;

                if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                {
                    col_null_map_to = ColumnUInt8::create(offsets_size, false);
                    vec_null_map_to = &col_null_map_to->getData();
                }

                for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
                {
                    size_t start_offset = offsets[offset_index - 1];
                    size_t end_offset = offsets[offset_index];

                    if (end_offset > start_offset)
                    {
                        if constexpr (strategy == ArrayFirstLastStrategy::First)
                            out->insert(data[start_offset]);
                        else
                            out->insert(data[end_offset - 1]);
                    }
                    else
                    {
                        out->insertDefault();

                        if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                            (*vec_null_map_to)[offset_index] = true;
                    }
                }

                if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                    return createNullableColumn(std::move(out), std::move(col_null_map_to));

                return out;
            }

            auto out = array.getData().cloneEmpty();
            out->insertManyDefaults(array.size());

            if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
            {
                auto col_null_map_to = ColumnUInt8::create(out->size(), true);
                return createNullableColumn(std::move(out), std::move(col_null_map_to));
            }

            return out;
        }

        const auto & filter = column_filter->getData();
        const auto & offsets = array.getOffsets();
        const auto & data = array.getData();
        auto out = data.cloneEmpty();
        out->reserve(data.size());

        size_t offsets_size = offsets.size();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to = nullptr;

        if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
        {
            col_null_map_to = ColumnUInt8::create(offsets_size, false);
            vec_null_map_to = &col_null_map_to->getData();
        }

        for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
        {
            size_t start_offset = offsets[offset_index - 1];
            size_t end_offset = offsets[offset_index];

            bool exists = false;

            if constexpr (strategy == ArrayFirstLastStrategy::First)
            {
                for (; start_offset != end_offset; ++start_offset)
                {
                    if (filter[start_offset])
                    {
                        out->insert(data[start_offset]);
                        exists = true;
                        break;
                    }
                }
            }
            else
            {
                for (; end_offset != start_offset; --end_offset)
                {
                    if (filter[end_offset - 1])
                    {
                        out->insert(data[end_offset - 1]);
                        exists = true;
                        break;
                    }
                }
            }

            if (!exists)
            {
                out->insertDefault();

                if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                    (*vec_null_map_to)[offset_index] = true;
            }
        }

        if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
            return createNullableColumn(std::move(out), std::move(col_null_map_to));

        return out;
    }
};

struct NameArrayFirst { static constexpr auto name = "arrayFirst"; };
using ArrayFirstImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::First, ArrayFirstLastElementNotExistsStrategy::Default>;
using FunctionArrayFirst = FunctionArrayMapped<ArrayFirstImpl, NameArrayFirst>;

struct NameArrayFirstOrNull { static constexpr auto name = "arrayFirstOrNull"; };
using ArrayFirstOrNullImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::First, ArrayFirstLastElementNotExistsStrategy::Null>;
using FunctionArrayFirstOrNull = FunctionArrayMapped<ArrayFirstOrNullImpl, NameArrayFirstOrNull>;

struct NameArrayLast { static constexpr auto name = "arrayLast"; };
using ArrayLastImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::Last, ArrayFirstLastElementNotExistsStrategy::Default>;
using FunctionArrayLast = FunctionArrayMapped<ArrayLastImpl, NameArrayLast>;

struct NameArrayLastOrNull { static constexpr auto name = "arrayLastOrNull"; };
using ArrayLastOrNullImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::Last, ArrayFirstLastElementNotExistsStrategy::Null>;
using FunctionArrayLastOrNull = FunctionArrayMapped<ArrayLastOrNullImpl, NameArrayLastOrNull>;

REGISTER_FUNCTION(ArrayFirst)
{
    /// arrayFirst

    FunctionDocumentation::Description description = R"(
Returns the first element in the source array for which `func(x[, y1, y2, ... yN])` returns true, otherwise it returns a default value.
    )";
    FunctionDocumentation::Syntax syntax = "arrayFirst(func(x[, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments = {
        {"func(x[, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`). [Lambda function](/sql-reference/functions/overview#arrow-operator-and-lambda)."},
        {"source_arr", "The source array to process. [`Array(T)`](/sql-reference/data-types/array)."},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function. [`Array(T)`](/sql-reference/data-types/array)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the first element of the source array for which `λ` is true, otherwise returns the default value of `T`."};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT arrayFirst(x, y -> x=y, ['a', 'b', 'c'], ['c', 'b', 'a'])", "b"},
        {"No match", "SELECT arrayFirst(x, y -> x=y, [0, 1, 2], [3, 3, 3]) AS res, toTypeName(res)", "0 UInt8"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayFirst>(documentation);

    /// arrayFirstOrNull

    FunctionDocumentation::Description description_orNull = R"(
Returns the first element in the source array for which `func(x[, y1, y2, ... yN])` returns true, otherwise it returns `NULL`.
    )";
    FunctionDocumentation::Syntax syntax_orNull = "arrayFirstOrNull(func(x[, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_orNull = {
        {"func(x[, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_orNull = {"Returns the first element of the source array for which `func` is true, otherwise returns `NULL`."};
    FunctionDocumentation::Examples examples_orNull = {
        {"Usage example", "SELECT arrayFirstOrNull(x, y -> x=y, ['a', 'b', 'c'], ['c', 'b', 'a'])", "b"},
        {"No match", "SELECT arrayFirstOrNull(x, y -> x=y, [0, 1, 2], [3, 3, 3]) AS res, toTypeName(res)", "NULL Nullable(UInt8)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_orNull = {1, 1};
    FunctionDocumentation::Category category_orNull = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_orNull = {description_orNull, syntax_orNull, arguments_orNull, returned_value_orNull, examples_orNull, introduced_in_orNull, category_orNull};

    factory.registerFunction<FunctionArrayFirstOrNull>(documentation_orNull);

    /// arrayLast

    FunctionDocumentation::Description description_last = R"(
Returns the last element in the source array for which a lambda `func(x [, y1, y2, ... yN])` returns true, otherwise it returns a default value.
    )";
    FunctionDocumentation::Syntax syntax_last = "arrayLast(func(x[, y1, ..., yN]), source[, cond1, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_last = {
        {"func(x[, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`). [Lambda function](/sql-reference/functions/overview#arrow-operator-and-lambda)."},
        {"source", "The source array to process. [`Array(T)`](/sql-reference/data-types/array)."},
        {"[, cond1, ... , condN]", "Optional. N condition arrays providing additional arguments to the lambda function. [`Array(T)`](/sql-reference/data-types/array)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_last = {"Returns the last element of the source array for which `func` is true, otherwise returns the default value of `T`."};
    FunctionDocumentation::Examples examples_last = {
        {"Usage example", "SELECT arrayLast(x, y -> x=y, ['a', 'b', 'c'], ['a', 'b', 'c'])", "c"},
        {
            "No match",
            "SELECT arrayFirst(x, y -> x=y, [0, 1, 2], [3, 3, 3]) AS res, toTypeName(res)",
            "0 UInt8"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_last = {1, 1};
    FunctionDocumentation::Category category_last = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_last = {description_last, syntax_last, arguments_last, returned_value_last, examples_last, introduced_in_last, category_last};
    factory.registerFunction<FunctionArrayLast>(documentation_last);

    /// arrayLastOrNull

    FunctionDocumentation::Description description_last_null = R"(
Returns the last element in the source array for which a lambda `func(x [, y1, y2, ... yN])` returns true, otherwise it returns `NULL`.
    )";
    FunctionDocumentation::Syntax syntax_last_null = "arrayLastOrNull(func(x[, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_last_null = {
        {"func(x [, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`). [Lambda function](/sql-reference/functions/overview#arrow-operator-and-lambda)."},
        {"source_arr", "The source array to process. [`Array(T)`](/sql-reference/data-types/array)."},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function. [`Array(T)`](/sql-reference/data-types/array)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_last_null = {"Returns the last element of the source array for which `λ` is not true, otherwise returns `NULL`."};
    FunctionDocumentation::Examples examples_last_null = {
        {"Usage example", "SELECT arrayLastOrNull(x, y -> x=y, ['a', 'b', 'c'], ['a', 'b', 'c'])", "c"},
        {
            "No match",
            "SELECT arrayLastOrNull(x, y -> x=y, [0, 1, 2], [3, 3, 3]) AS res, toTypeName(res)",
            "NULL Nullable(UInt8)"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_last_null = {1, 1};
    FunctionDocumentation::Category category_last_null = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_last_null = {description_last_null, syntax_last_null, arguments_last_null, returned_value_last_null, examples_last_null, introduced_in_last_null, category_last_null};

    factory.registerFunction<FunctionArrayLastOrNull>(documentation_last_null);
}

}

