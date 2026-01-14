#include <base/StringRef.h>
#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>
#include <Common/HashTable/ClearableHashSet.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray_fwd.h>
#include <Common/register_objects.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename ColT, bool is_const = false>
struct ColumnInfoImpl
{
    using ColumnType = std::conditional_t<is_const, const ColT, ColT>;
    using OffsetsType = std::conditional_t<is_const, const ColumnArray::Offsets, ColumnArray::Offsets>;

    ColumnType & col;
    OffsetsType & offsets;
    std::conditional_t<is_const, const PaddedPODArray<UInt8>, PaddedPODArray<UInt8>> * null_map;
};

template <typename T>
using ColVecType = ColumnVector<T>;

template <typename T>
using ColumnInfo = ColumnInfoImpl<T, false>;

template <typename T>
using ConstColumnInfo = ColumnInfoImpl<T, true>;


template <typename ColumnType>
struct ValueHandler;

template <typename T>
struct ValueHandler<ColumnVector<T>>
{
    using ValueType = T;

    static ValueType getValue(const ColumnVector<T> & col, size_t pos) { return col.getData()[pos]; }

    static void insertValue(ColumnVector<T> & col, ValueType value) { col.insertValue(value); }

    static void insertDefault(ColumnVector<T> & col) { col.insertDefault(); }
};

template <>
struct ValueHandler<ColumnString>
{
    using ValueType = StringRef;

    static StringRef getValue(const ColumnString & col, size_t pos) { return col.getDataAt(pos); }

    static void insertValue(ColumnString & col, StringRef value) { col.insertData(value.data, value.size); }

    static void insertDefault(ColumnString & col) { col.insertDefault(); }
};

template <>
struct ValueHandler<ColumnFixedString>
{
    using ValueType = StringRef;

    static StringRef getValue(const ColumnFixedString & col, size_t pos)
    {
        const size_t fixed_size = col.getN();
        return StringRef(&col.getChars()[pos * fixed_size], fixed_size);
    }

    static void insertValue(ColumnFixedString & col, StringRef value) { col.insertData(value.data, value.size); }

    static void insertDefault(ColumnFixedString & col) { col.insertDefault(); }
};

template <bool nullable, bool exclude_single_row, typename ColumnT>
void processImpl(ConstColumnInfo<ColumnT> source, ConstColumnInfo<ColumnT> exclude, ColumnInfo<ColumnT> result, size_t input_rows_count)
{
    using ValueType = typename ValueHandler<ColumnT>::ValueType;
    using Handler = ValueHandler<ColumnT>;
    constexpr size_t initial_size_degree = 9;
    using Set = ClearableHashSetWithStackMemory<ValueType, DefaultHash<ValueType>, initial_size_degree>;
    Set exclude_set;

    // Initialize result columns
    result.offsets.resize(input_rows_count);
    if constexpr (std::is_same_v<ColumnT, ColumnString>)
    {
        result.col.reserve(source.col.size());
        result.col.getChars().reserve(source.col.getChars().size());
    }
    else if constexpr (std::is_same_v<ColumnT, ColumnFixedString>)
    {
        result.col.getChars().reserve(source.col.size() * source.col.getN());
    }
    else
    {
        result.col.getData().reserve(source.col.getData().size());
    }
    if constexpr (nullable)
        result.null_map->resize(source.null_map->size());

    size_t current_source_offset = 0;
    size_t current_exclude_offset = 0;
    size_t current_result_offset = 0;
    bool exclude_has_null = false;

    auto reset = [&]
    {
        exclude_set.clear();
        if constexpr (nullable)
            exclude_has_null = false;
    };
    auto populate_excludes = [&](size_t offset, size_t size)
    {
        for (size_t i = 0; i != size; ++i)
        {
            if (exclude.null_map && (*exclude.null_map)[offset + i])
            {
                if constexpr (nullable)
                    exclude_has_null = true;
                else
                    continue; // No need to bother - input is not nullable
            }
            else
            {
                exclude_set.insert(Handler::getValue(exclude.col, offset + i));
            }
        }
    };

    if constexpr (exclude_single_row)
        populate_excludes(0, exclude.col.size());


    for (size_t row = 0; row < input_rows_count; ++row)
    {
        const size_t source_size = source.offsets[row] - current_source_offset;

        if constexpr (!exclude_single_row)
        {
            reset();
            const size_t exclude_size = exclude.offsets[row] - current_exclude_offset;
            populate_excludes(current_exclude_offset, exclude_size);
        }

        // Process source elements
        for (size_t idx = 0; idx < source_size; ++idx)
        {
            const size_t abs_pos = current_source_offset + idx;

            if constexpr (nullable)
            {
                if ((*source.null_map)[abs_pos])
                {
                    // Deal with source NULL
                    if (!exclude_has_null)
                    {
                        (*result.null_map)[current_result_offset] = 1;
                        Handler::insertDefault(result.col);
                        ++current_result_offset;
                    }
                }
                else
                {
                    // Deal with normal value
                    const auto current = Handler::getValue(source.col, abs_pos);
                    if (!exclude_set.has(current))
                    {
                        (*result.null_map)[current_result_offset] = 0;
                        Handler::insertValue(result.col, current);
                        ++current_result_offset;
                    }
                }
            }
            else
            {
                const auto current = Handler::getValue(source.col, abs_pos);
                if (!exclude_set.has(current))
                {
                    Handler::insertValue(result.col, current);
                    ++current_result_offset;
                }
            }
        }

        result.offsets[row] = current_result_offset;
        current_source_offset = source.offsets[row];
        if constexpr (!exclude_single_row)
            current_exclude_offset = exclude.offsets[row];
    }

    if constexpr (nullable)
        result.null_map->resize(current_result_offset);
}

class FunctionArrayExcept : public IFunction
{
public:
    static constexpr auto name = "arrayExcept";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayExcept>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto mandatory_args = FunctionArgumentDescriptors{
            {"source", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"exclude", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
        };

        validateFunctionArguments(*this, arguments, mandatory_args);

        auto get_nested_type = [](const DataTypePtr & type)
        {
            const DataTypeArray * array_type = assert_cast<const DataTypeArray *>(type.get());
            if (const DataTypeNullable * nullable = typeid_cast<const DataTypeNullable *>(array_type->getNestedType().get()))
                return nullable->getNestedType();
            return array_type->getNestedType();
        };

        DataTypePtr source_nested = get_nested_type(arguments[0].type);
        DataTypePtr exclude_nested = get_nested_type(arguments[1].type);

        if (!source_nested->equals(*exclude_nested))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} requires both arrays to have exactly the same element type. "
                "Got {} and {}",
                getName(),
                arguments[0].type->getName(),
                arguments[1].type->getName());
        }

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        if (return_type->onlyNull())
            return return_type->createColumnConstWithDefaultValue(input_rows_count);

        auto source_full_col = arguments[0].column->convertToFullColumnIfConst();
        const bool exclude_is_const = isColumnConst(*arguments[1].column);

        // We expect some form of arrays for both params
        const ColumnArray * source_col = checkAndGetColumn<ColumnArray>(source_full_col.get());
        const ColumnArray * exclude_col = exclude_is_const
            ? typeid_cast<const ColumnArray *>(&typeid_cast<const ColumnConst *>(arguments[1].column.get())->getDataColumn())
            : checkAndGetColumn<ColumnArray>(arguments[1].column.get());

        if (!source_col || !exclude_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Arguments must be arrays");

        // With a little twist that they might be nullable...
        const IColumn * source_data = &source_col->getData();
        const IColumn * exclude_data = &exclude_col->getData();
        const ColumnUInt8::Container * source_null_map = nullptr;
        const ColumnUInt8::Container * exclude_null_map = nullptr;
        if (const auto * n = checkAndGetColumn<ColumnNullable>(source_data))
        {
            source_data = &n->getNestedColumn();
            source_null_map = &n->getNullMapData();
        }

        if (const auto * n = checkAndGetColumn<ColumnNullable>(exclude_data))
        {
            exclude_data = &n->getNestedColumn();
            exclude_null_map = &n->getNullMapData();
        }
        const ColumnArray::Offsets & source_offsets = source_col->getOffsets();
        const ColumnArray::Offsets & exclude_offsets = exclude_col->getOffsets();
        auto const exclude_single_row = exclude_offsets.size() == 1;

        // Prepare the result column
        auto actual_return_type = arguments[0].type;

        auto result = return_type->createColumn();
        ColumnArray * result_col = assert_cast<ColumnArray *>(result.get());
        IColumn * result_data = &result_col->getData();
        ColumnUInt8::Container * result_null_map = nullptr;

        if (ColumnNullable * n = typeid_cast<ColumnNullable *>(result_data))
        {
            result_data = &n->getNestedColumn();
            result_null_map = &n->getNullMapData();
        }

        ColumnArray::Offsets & result_offsets = result_col->getOffsets();

// NOLINTBEGIN(bugprone-macro-parentheses)
#define DISPATCH(COLTYPE, PROCESSOR, TYPE) \
    if (const auto * source_tcol##TYPE = typeid_cast<const COLTYPE *>(source_data)) \
    { \
        const auto * exclude_tcol = typeid_cast<const COLTYPE *>(exclude_data); \
        auto * result_tcol = typeid_cast<COLTYPE *>(result_data); \
        ConstColumnInfo<COLTYPE> s{*source_tcol##TYPE, source_offsets, source_null_map}; \
        ConstColumnInfo<COLTYPE> e{*exclude_tcol, exclude_offsets, exclude_null_map}; \
        ColumnInfo<COLTYPE> r{*result_tcol, result_offsets, result_null_map}; \
        if (source_null_map) \
            if (exclude_single_row) \
                PROCESSOR<true, true>(s, e, r, input_rows_count); \
            else \
                PROCESSOR<true, false>(s, e, r, input_rows_count); \
        else if (exclude_single_row) \
            PROCESSOR<false, true>(s, e, r, input_rows_count); \
        else \
            PROCESSOR<false, false>(s, e, r, input_rows_count); \
    }
#define DISPATCH_VEC(TYPE) DISPATCH(ColumnVector<TYPE>, processImpl, TYPE)
        // clang-format off
                DISPATCH_VEC(Int8)
                else DISPATCH_VEC(Int16)
                else DISPATCH_VEC(Int32)
                else DISPATCH_VEC(Int64)
                else DISPATCH_VEC(Int128)
                else DISPATCH_VEC(Int256)
                else DISPATCH_VEC(UInt8)
                else DISPATCH_VEC(UInt16)
                else DISPATCH_VEC(UInt32)
                else DISPATCH_VEC(UInt64)
                else DISPATCH_VEC(UInt128)
                else DISPATCH_VEC(UInt256)
                else DISPATCH_VEC(BFloat16)
                else DISPATCH_VEC(Float32)
                else DISPATCH_VEC(Float64)
                else DISPATCH(ColumnString, processImpl, String)
                else DISPATCH(ColumnFixedString, processImpl, FixedString)
                else
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported type {}. Consider arrayFilter(x -> NOT has (exclude), source)", arguments[0].type->getName());
        // clang-format on
#undef DISPATCH_VEC
#undef DISPATCH
        // NOLINTEND(bugprone-macro-parentheses)
        return result;
    }
};

}

REGISTER_FUNCTION(FunctionArrayExcept)
{
    FunctionDocumentation::Description description = R"(
Returns an array containing elements from `source` that are not present in `except`, preserving the original order.

This function performs a set difference operation between two arrays. For each element in `source`, it checks if the element exists in `except` (using exact comparison). If not, the element is included in the result.

The operation maintains these properties:
1. Order of elements from `source` is preserved
2. Duplicates in `source` are preserved if they don't exist in `except`
3. NULL is handled as a separate value
    )";

    FunctionDocumentation::Syntax syntax = "arrayExcept(source, except)";
    FunctionDocumentation::Arguments arguments
        = {{"source",
            "The source array containing elements to filter. ",
            {"Array(T)"}
           },
           {"except",
            "The array containing elements to exclude from the result. ",
            {"Array(T)"}
           }};

    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the same type as the input array containing elements from `source` that weren't found in `except`. ", {"Array(T)"}};

    FunctionDocumentation::Examples examples
        = {{"basic", "SELECT arrayExcept([1, 2, 3, 2, 4], [3, 5])", "[1, 2, 2, 4]"},
           {"with_nulls1", "SELECT arrayExcept([1, NULL, 2, NULL], [2])", "[1, NULL, NULL]"},
           {"with_nulls2", "SELECT arrayExcept([1, NULL, 2, NULL], [NULL, 2, NULL])", "[1]"},
           {"strings", "SELECT arrayExcept(['apple', 'banana', 'cherry'], ['banana', 'date'])", "['apple', 'cherry']"}};

    FunctionDocumentation::IntroducedIn introduced_in = {25, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayExcept>(documentation);
}

}
