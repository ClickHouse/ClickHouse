#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include "Columns/ColumnDecimal.h"
#include <Columns/ColumnNullable.h>

#include <Common/HashTable/HashTable.h>
#include <Common/assert_cast.h>

#include <DataTypes/DataTypeArray.h>
#include "DataTypes/DataTypeNothing.h"
#include <DataTypes/DataTypeNullable.h>

#include <Functions/FunctionFactory.h>
#include "Functions/FunctionHelpers.h"
#include <Functions/array/arrayRemove.h>

#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypePtr FunctionArrayRemove::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 2",
            getName(), arguments.size());

    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "First argument for function {} must be an array but it has type {}.",
                        getName(), arguments[0]->getName());

    const auto & array_inner_type = array_type->getNestedType();
    const auto & elem_type = arguments[1];

    auto array_inner_no_null = removeNullable(array_inner_type);
    bool array_inner_is_nothing = typeid_cast<const DataTypeNothing *>(array_inner_no_null.get());
    auto elem_no_null = removeNullable(elem_type);

    // TODO: Should we use tryGetLeastSupertype to allow elem to be a different integer size from
    // the array element?
    // How would compareAt work in that case?
    if (!array_inner_is_nothing && !array_inner_no_null->equals(*elem_no_null))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument for function {} must be of same type as array elements ({}), got {}",
            getName(), array_inner_no_null->getName(), elem_no_null->getName());

    return arguments[0];
}

template <typename T>
static bool executeType(
    const ColumnArray * src_array,
    const IColumn * elem_values_col,
    ColumnPtr & res_ptr)
{
    using ColVecType = ColumnVectorOrDecimal<T>;

    const ColVecType * src_values_column = checkAndGetColumn<ColVecType>(&src_array->getData());
    const ColVecType * elem_values_column = checkAndGetColumn<ColVecType>(elem_values_col);

    if (!src_values_column || !elem_values_column)
        return false;

    const auto & src_values = src_values_column->getData();
    const auto & src_offsets = src_array->getOffsets();

    chassert(elem_values_column->getData().size() == 1); // validated in executeImpl
    const T elem_value = elem_values_column->getData()[0];

    typename ColVecType::MutablePtr res_values_column;
    if constexpr (is_decimal<T>)
        res_values_column = ColVecType::create(src_values.size(), src_values_column->getScale());
    else
        res_values_column = ColVecType::create(src_values.size());

    typename ColVecType::Container & res_values = res_values_column->getData();

    size_t src_offsets_size = src_offsets.size();
    auto res_offsets_column = ColumnArray::ColumnOffsets::create(src_offsets_size);
    IColumn::Offsets & res_offsets = res_offsets_column->getData();

    size_t src_pos = 0;
    size_t res_pos = 0;

    for (size_t i = 0; i < src_offsets_size; ++i)
    {
        const size_t src_end = src_offsets[i];

        for (; src_pos < src_end; ++src_pos)
        {
            if (!bitEquals(src_values[src_pos], elem_value))
            {
                res_values[res_pos] = src_values[src_pos];
                ++res_pos;
            }
        }

        res_offsets[i] = res_pos;
    }

    res_values.resize(res_pos);
    res_ptr = ColumnArray::create(std::move(res_values_column), std::move(res_offsets_column));
    return true;
}

ColumnPtr FunctionArrayRemove::executeImpl(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & /*result_type*/,
    size_t /*input_rows_count*/) const
{
    const auto * src_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!src_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "First argument for function {} must be Array", getName());

    const auto elem_column = arguments[1].column;
    bool elem_is_const = isColumnConst(*arguments[1].column);
    const IColumn * elem_values_col =
        (elem_is_const) ?
            &checkAndGetColumn<ColumnConst>(arguments[1].column.get())->getDataColumn() :
            elem_column.get();

    if (elem_values_col->size() != 1)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Second argument of {} must have exactly 1 row, got {} rows", getName(), elem_values_col->size());

    ColumnPtr res;
    if ((executeType< UInt8 >(src_array, elem_values_col, res) ||
            executeType< UInt16>(src_array, elem_values_col, res) ||
            executeType< UInt32>(src_array, elem_values_col, res) ||
            executeType< UInt64>(src_array, elem_values_col, res) ||
            executeType< Int8  >(src_array, elem_values_col, res) ||
            executeType< Int16 >(src_array, elem_values_col, res) ||
            executeType< Int32 >(src_array, elem_values_col, res) ||
            executeType< Int64 >(src_array, elem_values_col, res) ||
            executeType<Float32>(src_array, elem_values_col, res) ||
            executeType<Float64>(src_array, elem_values_col, res)) ||
            executeType<Decimal32>(src_array, elem_values_col, res) ||
            executeType<Decimal64>(src_array, elem_values_col, res) ||
            executeType<Decimal128>(src_array, elem_values_col, res) ||
            executeType<Decimal256>(src_array, elem_values_col, res))
    {
        return res;
    }

    const IColumn & src_values = src_array->getData();
    const auto & src_offsets = src_array->getOffsets();

    const auto * src_nullable = checkAndGetColumn<ColumnNullable>(&src_values);
    const auto * src_null_map = src_nullable ? &src_nullable->getNullMapData() : nullptr;
    const IColumn * src_nested = src_nullable ? &src_nullable->getNestedColumn() : &src_values;

    const auto * elem_nullable = checkAndGetColumn<ColumnNullable>(elem_values_col);
    const IColumn * elem_nested = elem_nullable ? &elem_nullable->getNestedColumn() : elem_values_col;
    const bool elem_is_null = elem_nullable && elem_nullable->isNullAt(0);

    auto res_values_column = src_values.cloneEmpty();
    res_values_column->reserve(src_values.size());
    auto res_offsets_column = ColumnArray::ColumnOffsets::create(src_offsets.size());
    auto & res_offsets = res_offsets_column->getData();
    auto & res_values = *res_values_column;

    size_t src_pos = 0;
    size_t res_pos = 0;

    for (size_t i = 0; i < src_offsets.size(); ++i)
    {
        size_t src_offset = src_offsets[i];

        for (; src_pos < src_offset; ++src_pos)
        {
            bool is_null = src_null_map && (*src_null_map)[src_pos];

            if (is_null)
            {
                if (elem_is_null)
                    continue;

                res_values.insertFrom(src_values, src_pos);
                ++res_pos;
                continue;
            }

            if (elem_is_null)
            {
                res_values.insertFrom(src_values, src_pos);
                ++res_pos;
                continue;
            }

            if (src_nested->compareAt(src_pos, 0, *elem_nested, /*nan_direction_hint*/1) != 0)
            {
                res_values.insertFrom(src_values, src_pos);
                ++res_pos;
            }
        }

        res_offsets[i] = res_pos;
    }

    return ColumnArray::create(std::move(res_values_column), std::move(res_offsets_column));
}

REGISTER_FUNCTION(ArrayRemove)
{
    FunctionDocumentation::Description description = R"(
Removes all elements equal to a given value from an array.  
NULLs are treated as equal.
)";
    FunctionDocumentation::Syntax syntax = "arrayRemove(arr, elem)";
    FunctionDocumentation::Examples examples = {
        {"Example 1", "SELECT arrayRemove([1, 2, 2, 3], 2)", "[1, 3]"},
        {"Example 2", "SELECT arrayRemove(['a', NULL, 'b', NULL], NULL)", "['a', 'b']"}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a subset of the source array", {"Array(T)"}};

    FunctionDocumentation documentation = {
        description, syntax,
        {{"arr", "Array(T)"}, {"elem", "T"}},
        returned_value,
        examples,
        {1, 1},
        FunctionDocumentation::Category::Array
    };

    factory.registerFunction<FunctionArrayRemove>(documentation);
    factory.registerAlias("array_remove", "arrayRemove", FunctionFactory::Case::Insensitive);
}

}
