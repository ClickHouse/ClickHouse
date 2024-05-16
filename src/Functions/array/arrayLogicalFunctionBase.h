#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/getMostSubtype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <base/TypeLists.h>
#include <base/range.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <bool intersect>
class FunctionArrayLogicalBase : public IFunction
{
public:
    explicit FunctionArrayLogicalBase(ContextPtr context_) : context(context_) { }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    ContextPtr context;
    const char * name = intersect ? "arrayIntersect" : "arraySymmetricDifference";

    /// Initially allocate a piece of memory for 64 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 6;

    struct UnpackedArrays
    {
        size_t base_rows = 0;

        struct UnpackedArray
        {
            bool is_const = false;
            const NullMap * null_map = nullptr;
            const NullMap * overflow_mask = nullptr;
            const ColumnArray::ColumnOffsets::Container * offsets = nullptr;
            const IColumn * nested_column = nullptr;
        };

        std::vector<UnpackedArray> args;
        Columns column_holders;

        UnpackedArrays() = default;
    };

    /// Cast column to data_type removing nullable if data_type hasn't.
    /// It's expected that column can represent data_type after removing some NullMap's.
    ColumnPtr castRemoveNullable(const ColumnPtr & column, const DataTypePtr & data_type) const;

    struct CastArgumentsResult
    {
        ColumnsWithTypeAndName initial;
        ColumnsWithTypeAndName casted;
    };

    static CastArgumentsResult
    castColumns(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, const DataTypePtr & return_type_with_nulls);

    UnpackedArrays prepareArrays(const ColumnsWithTypeAndName & columns, ColumnsWithTypeAndName & initial_columns) const;

    template <typename ValueType, typename ColumnType, bool is_numeric_column>
    static ColumnPtr execute(const FunctionArrayLogicalBase<intersect>::UnpackedArrays & arrays, MutableColumnPtr result_data);

    struct NumberExecutor
    {
        const UnpackedArrays & arrays;
        const DataTypePtr & data_type;

        ColumnPtr & result;

        NumberExecutor(const UnpackedArrays & arrays_, const DataTypePtr & data_type_, ColumnPtr & result_)
            : arrays(arrays_), data_type(data_type_), result(result_)
        {
        }

        template <class T>
        void operator()(TypeList<T>);
    };

    struct DecimalExecutor
    {
        const UnpackedArrays & arrays;
        const DataTypePtr & data_type;

        ColumnPtr & result;

        DecimalExecutor(const UnpackedArrays & arrays_, const DataTypePtr & data_type_, ColumnPtr & result_)
            : arrays(arrays_), data_type(data_type_), result(result_)
        {
        }

        template <class T>
        void operator()(TypeList<T>);
    };
};

static ColumnPtr callFunctionNotEquals(ColumnWithTypeAndName first, ColumnWithTypeAndName second, ContextPtr context)
{
    ColumnsWithTypeAndName args{first, second};
    auto eq_func = FunctionFactory::instance().get("notEquals", context)->build(args);
    return eq_func->execute(args, eq_func->getResultType(), args.front().column->size());
}

template <bool intersect>
ColumnPtr FunctionArrayLogicalBase<intersect>::castRemoveNullable(const ColumnPtr & column, const DataTypePtr & data_type) const
{
    if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(data_type.get());
        const auto & nested = column_nullable->getNestedColumnPtr();
        if (nullable_type)
        {
            auto casted_column = castRemoveNullable(nested, nullable_type->getNestedType());
            return ColumnNullable::create(casted_column, column_nullable->getNullMapColumnPtr());
        }
        return castRemoveNullable(nested, data_type);
    }
    else if (const auto * column_array = checkAndGetColumn<ColumnArray>(column.get()))
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(data_type.get());
        if (!array_type)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot cast array column to column with type {} in function {}",
                data_type->getName(),
                getName());

        auto casted_column = castRemoveNullable(column_array->getDataPtr(), array_type->getNestedType());
        return ColumnArray::create(casted_column, column_array->getOffsetsPtr());
    }
    else if (const auto * column_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
    {
        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(data_type.get());

        if (!tuple_type)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Cannot cast tuple column to type {} in function {}", data_type->getName(), getName());

        auto columns_number = column_tuple->tupleSize();
        Columns columns(columns_number);

        const auto & types = tuple_type->getElements();

        for (size_t i = 0; i < columns_number; i++)
            columns[i] = castRemoveNullable(column_tuple->getColumnPtr(i), types[i]);
        return ColumnTuple::create(columns);
    }

    return column;
}

template <bool intersect>
FunctionArrayLogicalBase<intersect>::CastArgumentsResult FunctionArrayLogicalBase<intersect>::castColumns(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, const DataTypePtr & return_type_with_nulls)
{
    size_t num_args = arguments.size();
    ColumnsWithTypeAndName initial_columns(num_args);
    ColumnsWithTypeAndName casted_columns(num_args);

    const auto * type_array = checkAndGetDataType<DataTypeArray>(return_type.get());
    const auto & type_nested = type_array->getNestedType();
    auto type_not_nullable_nested = removeNullable(type_nested);

    const bool is_numeric_or_string = isNumber(type_not_nullable_nested) || isDate(type_not_nullable_nested)
        || isDateTime(type_not_nullable_nested) || isDateTime64(type_not_nullable_nested) || isDate32(type_not_nullable_nested)
        || isStringOrFixedString(type_not_nullable_nested);

    DataTypePtr nullable_return_type;

    if (is_numeric_or_string)
    {
        auto type_nullable_nested = makeNullable(type_nested);
        nullable_return_type = std::make_shared<DataTypeArray>(type_nullable_nested);
    }

    const bool nested_is_nullable = type_nested->isNullable();

    for (size_t i = 0; i < num_args; ++i)
    {
        const ColumnWithTypeAndName & arg = arguments[i];
        initial_columns[i] = arg;
        casted_columns[i] = arg;
        auto & column = casted_columns[i];

        if (is_numeric_or_string)
        {
            /// Cast to Array(T) or Array(Nullable(T)).
            if (nested_is_nullable)
            {
                if (!arg.type->equals(*return_type))
                {
                    column.column = castColumn(arg, return_type);
                    column.type = return_type;
                }
            }
            else if (!arg.type->equals(*return_type) && !arg.type->equals(*nullable_return_type))
            {
                /// If result has array type Array(T) still cast Array(Nullable(U)) to Array(Nullable(T))
                ///  because cannot cast Nullable(T) to T.
                if (static_cast<const DataTypeArray &>(*arg.type).getNestedType()->isNullable())
                {
                    column.column = castColumn(arg, nullable_return_type);
                    column.type = nullable_return_type;
                }
                else
                {
                    column.column = castColumn(arg, return_type);
                    column.type = return_type;
                }
            }
        }
        else if (!arg.type->equals(*return_type_with_nulls))
        {
            /// return_type_with_nulls is the most common subtype with possible nullable parts.
            column.column = castColumn(arg, return_type_with_nulls);
            column.type = return_type_with_nulls;
        }
    }

    return {.initial = initial_columns, .casted = casted_columns};
}


template <bool intersect>
FunctionArrayLogicalBase<intersect>::UnpackedArrays
FunctionArrayLogicalBase<intersect>::prepareArrays(const ColumnsWithTypeAndName & columns, ColumnsWithTypeAndName & initial_columns) const
{
    UnpackedArrays arrays;

    size_t columns_number = columns.size();
    arrays.args.resize(columns_number);

    bool all_const = true;

    for (size_t i = 0; i < columns_number; ++i)
    {
        auto & arg = arrays.args[i];
        const auto * argument_column = columns[i].column.get();
        const auto * initial_column = initial_columns[i].column.get();

        if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(argument_column))
        {
            arg.is_const = true;
            argument_column = argument_column_const->getDataColumnPtr().get();
            initial_column = typeid_cast<const ColumnConst &>(*initial_column).getDataColumnPtr().get();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(argument_column))
        {
            if (!arg.is_const)
                all_const = false;

            arg.offsets = &argument_column_array->getOffsets();
            arg.nested_column = &argument_column_array->getData();

            initial_column = &typeid_cast<const ColumnArray &>(*initial_column).getData();

            if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(arg.nested_column))
            {
                arg.null_map = &column_nullable->getNullMapData();
                arg.nested_column = &column_nullable->getNestedColumn();

                if (initial_column->isNullable())
                    initial_column = &typeid_cast<const ColumnNullable &>(*initial_column).getNestedColumn();
            }

            /// In case the column was casted, we need to create an overflow mask for integer types.
            if (arg.nested_column != initial_column)
            {
                const auto & nested_init_type
                    = typeid_cast<const DataTypeArray &>(*removeNullable(initial_columns[i].type)).getNestedType();
                const auto & nested_cast_type = typeid_cast<const DataTypeArray &>(*removeNullable(columns[i].type)).getNestedType();

                if (isInteger(nested_init_type) || isDate(nested_init_type) || isDateTime(nested_init_type)
                    || isDateTime64(nested_init_type))
                {
                    /// Compare original and casted columns. It seem to be the easiest way.
                    auto overflow_mask = callFunctionNotEquals(
                        {arg.nested_column->getPtr(), nested_init_type, ""}, {initial_column->getPtr(), nested_cast_type, ""}, context);

                    arg.overflow_mask = &typeid_cast<const ColumnUInt8 &>(*overflow_mask).getData();
                    arrays.column_holders.emplace_back(std::move(overflow_mask));
                }
            }
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Arguments for function {} must be arrays.", getName());
    }

    if (all_const)
    {
        arrays.base_rows = arrays.args.front().offsets->size();
    }
    else
    {
        for (size_t i = 0; i < columns_number; ++i)
        {
            if (arrays.args[i].is_const)
                continue;

            size_t rows = arrays.args[i].offsets->size();
            if (arrays.base_rows == 0 && rows > 0)
                arrays.base_rows = rows;
            else if (arrays.base_rows != rows)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Non-const array columns in function {} should have the same number of rows", getName());
        }
    }

    return arrays;
}

template <bool intersect>
template <class T>
void FunctionArrayLogicalBase<intersect>::NumberExecutor::operator()(TypeList<T>)
{
    if (!result && typeid_cast<const DataTypeNumber<T> *>(data_type.get()))
        result = execute<T, ColumnVector<T>, true>(arrays, ColumnVector<T>::create());
}

template <bool intersect>
template <class T>
void FunctionArrayLogicalBase<intersect>::DecimalExecutor::operator()(TypeList<T>)
{
    if (!result)
        if (auto * decimal = typeid_cast<const DataTypeDecimal<T> *>(data_type.get()))
            result = FunctionArrayLogicalBase<intersect>::execute<T, ColumnDecimal<T>, true>(
                arrays, ColumnDecimal<T>::create(0, decimal->getScale()));
}

template <bool intersect>
template <typename ValueType, typename ColumnType, bool is_numeric_column>
ColumnPtr FunctionArrayLogicalBase<intersect>::execute(
    const FunctionArrayLogicalBase<intersect>::UnpackedArrays & arrays, MutableColumnPtr result_data_ptr)
{
    auto args = arrays.args.size();
    auto rows = arrays.base_rows;

    size_t nullable_amount = 0;

    std::vector<const ColumnType *> columns;
    columns.reserve(args);
    // We can define whether the result is nullable by the number of nullable columns
    for (const auto & arg : arrays.args)
    {
        if constexpr (std::is_same_v<ColumnType, IColumn>)
            columns.push_back(arg.nested_column);
        else
            columns.push_back(checkAndGetColumn<ColumnType>(arg.nested_column));

        if (!columns.back())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected array type");

        if (arg.null_map)
            ++nullable_amount;
    }

    auto & result_data = static_cast<ColumnType &>(*result_data_ptr);
    auto result_offsets_ptr = ColumnArray::ColumnOffsets::create(rows);
    auto & result_offsets = assert_cast<ColumnArray::ColumnOffsets &>(*result_offsets_ptr);
    auto null_map_column = ColumnUInt8::create();
    NullMap & null_map = assert_cast<ColumnUInt8 &>(*null_map_column).getData();

    using TypeMap = ClearableHashMapWithStackMemory<ValueType, size_t, DefaultHash<ValueType>, INITIAL_SIZE_DEGREE>;
    using TypeSet = ClearableHashSetWithStackMemory<ValueType, DefaultHash<ValueType>, INITIAL_SIZE_DEGREE>;

    Arena arena;
    TypeMap map;
    TypeSet set;
    std::vector<size_t> prev_off(args, 0);
    size_t result_offset = 0;
    const size_t needed_amount = intersect ? args : 1;
    const size_t max_amount = intersect ? args : args - 1;

    // Common approach here is to count the number of occurrences of each element in each array once
    for (size_t row = 0; row < rows; ++row)
    {
        map.clear();
        bool current_has_null;
        size_t null_amount = 0;
        for (size_t arg_num = 0; arg_num < args; ++arg_num)
        {
            set.clear();
            const auto & arg = arrays.args[arg_num];
            current_has_null = false;

            size_t off;
            // const array has only one row
            if (arg.is_const)
                off = (*arg.offsets)[0];
            else
                off = (*arg.offsets)[row];

            for (size_t i = prev_off[arg_num]; i < off; i++)
            {
                if (arg.null_map && (*arg.null_map)[i])
                {
                    if (!current_has_null)
                        ++null_amount;
                    current_has_null = true;
                }

                if (!arg.overflow_mask || (*arg.overflow_mask)[i] == 0)
                {
                    typename TypeMap::mapped_type * value = nullptr;
                    typename TypeMap::key_type elem;


                    if constexpr (is_numeric_column)
                    {
                        elem = columns[arg_num]->getElement(i);
                    }
                    else if constexpr (std::is_same_v<ColumnType, ColumnString> || std::is_same_v<ColumnType, ColumnFixedString>)
                    {
                        elem = columns[arg_num]->getDataAt(i);
                    }
                    else
                    {
                        const char * data = nullptr;
                        elem = columns[arg_num]->serializeValueIntoArena(i, arena, data);
                    }

                    value = &map[elem];
                    if (!set.find(elem))
                    {
                        ++(*value);
                        set.insert(elem);
                    }
                }
            }

            prev_off[arg_num] = off;
            if (arg.is_const)
                prev_off[arg_num] = 0;
        }

        // After we counted the number of occurrences we should get the elements directly from source arrays to keep the order
        set.clear();
        prev_off.assign(args, 0);
        bool null_added = false;
        for (size_t arg_num = 0; arg_num < args; ++arg_num)
        {
            const auto & arg = arrays.args[arg_num];

            size_t off;
            if (arg.is_const)
                off = (*arg.offsets)[0];
            else
                off = (*arg.offsets)[row];
            typename TypeMap::key_type elem;

            for (size_t i = prev_off[arg_num]; i < off; i++)
            {
                if (arg.null_map && (*arg.null_map)[i])
                {
                    if (!null_added && null_amount >= needed_amount && null_amount <= max_amount)
                    {
                        ++result_offset;
                        result_data.insertDefault();
                        null_map.push_back(1);
                        null_added = true;
                    }
                    else
                        continue;
                }
                else if constexpr (is_numeric_column)
                {
                    elem = columns[arg_num]->getElement(i);
                    if (map[elem] >= needed_amount && map[elem] <= max_amount && !set.find(elem))
                    {
                        result_data.insertValue(elem);
                        ++result_offset;
                        set.insert(elem);
                        null_map.push_back(0);
                    }
                }
                else if constexpr (std::is_same_v<ColumnType, ColumnString> || std::is_same_v<ColumnType, ColumnFixedString>)
                {
                    elem = columns[arg_num]->getDataAt(i);
                    if (map[elem] >= needed_amount && map[elem] <= max_amount && !set.find(elem))
                    {
                        result_data.insertData(elem.data, elem.size);
                        ++result_offset;
                        set.insert(elem);
                        null_map.push_back(0);
                    }
                }
                else
                {
                    const char * data = nullptr;
                    elem = columns[arg_num]->serializeValueIntoArena(i, arena, data);
                    if (map[elem] >= needed_amount && map[elem] <= max_amount && !set.find(elem))
                    {
                        std::ignore = result_data.deserializeAndInsertFromArena(elem.data);
                        set.insert(elem);
                        ++result_offset;
                        null_map.push_back(0);
                    }
                }
            }
            prev_off[arg_num] = off;
            if (arg.is_const)
                prev_off[arg_num] = 0;


            // For arrayIntersect we can find all elements in the first array
            if constexpr (intersect)
                break;
        }
        result_offset = result_data.size();
        result_offsets.getElement(row) = result_offset;
    }

    ColumnPtr result_column = std::move(result_data_ptr);
    //     The result of arrayIntersection is nullable if all arrays are nullable,
    //     result of arraySymmetricDifference can be nullable if at least one array is nullable
    if (nullable_amount >= needed_amount)
        result_column = ColumnNullable::create(result_column, std::move(null_map_column));
    return ColumnArray::create(result_column, std::move(result_offsets_ptr));
}


template <bool intersect>
DataTypePtr FunctionArrayLogicalBase<intersect>::getReturnTypeImpl(const DataTypes & arguments) const
{
    DataTypes nested_types;
    nested_types.reserve(arguments.size());

    bool has_nothing = false;
    bool has_nullable = false;

    if (arguments.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());

    for (auto i : collections::range(0, arguments.size()))
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument {} for function {} must be an array but it has type {}.",
                            i, getName(), arguments[i]->getName());

        const auto & nested_type = array_type->getNestedType();

        if (nested_type->isNullable())
            has_nullable = true;

        if (typeid_cast<const DataTypeNothing *>(nested_type.get()))
            has_nothing = true;
        else
            nested_types.push_back(nested_type);
    }

    DataTypePtr result_type;
    if (intersect) {
        if (!nested_types.empty())
            result_type = getMostSubtype(nested_types, true);
        if (has_nothing)
            result_type = std::make_shared<DataTypeNothing>();
    } else {
        if (!nested_types.empty())
            result_type = getLeastSupertype(nested_types);
        if (has_nullable)
            result_type = makeNullable(result_type);
    }

    return std::make_shared<DataTypeArray>(result_type);
}
//{
//    DataTypes nested_types;
//    nested_types.reserve(arguments.size());
//
//    bool has_nullable = false;
//
//    if (arguments.empty())
//        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());
//
//    for (size_t i = 0; i < arguments.size(); i++)
//    {
//        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
//        if (!array_type)
//            throw Exception(
//                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
//                "Argument {} for function {} must be an array but it has type {}.",
//                i,
//                getName(),
//                arguments[i]->getName());
//
//        const auto & nested_type = array_type->getNestedType();
//
//        nested_types.push_back(nested_type);
//        if (nested_type->isNullable())
//            has_nullable = true;
//    }
//
//    DataTypePtr result_type;
//
//    if (!nested_types.empty())
//        result_type = getLeastSupertype(nested_types);
//
//    if (!intersect && has_nullable)
//        result_type = makeNullable(result_type);
//
//    return std::make_shared<DataTypeArray>(result_type);
//}

template <bool intersect>
ColumnPtr FunctionArrayLogicalBase<intersect>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    const auto * return_type_array = checkAndGetDataType<DataTypeArray>(result_type.get());

    if (!return_type_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Return type for function {} must be array.", getName());

    const auto & nested_return_type = return_type_array->getNestedType();

    if (typeid_cast<const DataTypeNothing *>(nested_return_type.get()))
        return result_type->createColumnConstWithDefaultValue(input_rows_count);

    auto num_args = arguments.size();
    DataTypes data_types;
    data_types.reserve(num_args);
    for (size_t i = 0; i < num_args; ++i)
        data_types.push_back(arguments[i].type);

    DataTypePtr return_type_with_nulls;
    if (intersect)
        return_type_with_nulls = getMostSubtype(data_types, true, true);
    else
        return_type_with_nulls = getLeastSupertype(data_types);
    auto casted_columns = castColumns(arguments, result_type, return_type_with_nulls);

    UnpackedArrays arrays = prepareArrays(casted_columns.casted, casted_columns.initial);

    ColumnPtr result_column;
    auto not_nullable_nested_return_type = removeNullable(nested_return_type);
    TypeListUtils::forEach(TypeListIntAndFloat{}, NumberExecutor(arrays, not_nullable_nested_return_type, result_column));
    TypeListUtils::forEach(TypeListDecimal{}, DecimalExecutor(arrays, not_nullable_nested_return_type, result_column));


    if (!result_column)
    {
        auto column = not_nullable_nested_return_type->createColumn();
        WhichDataType which(not_nullable_nested_return_type);

        if (which.isDate())
            result_column
                = FunctionArrayLogicalBase<intersect>::execute<DataTypeDate::FieldType, ColumnVector<DataTypeDate::FieldType>, true>(
                    arrays, std::move(column));
        else if (which.isDate32())
            result_column
                = FunctionArrayLogicalBase<intersect>::execute<DataTypeDate32::FieldType, ColumnVector<DataTypeDate32::FieldType>, true>(
                    arrays, std::move(column));
        else if (which.isDateTime())
            result_column = FunctionArrayLogicalBase<intersect>::
                execute<DataTypeDateTime::FieldType, ColumnVector<DataTypeDateTime::FieldType>, true>(arrays, std::move(column));
        else if (which.isString())
            result_column = FunctionArrayLogicalBase<intersect>::execute<StringRef, ColumnString, false>(arrays, std::move(column));
        else if (which.isFixedString())
            result_column = FunctionArrayLogicalBase<intersect>::execute<StringRef, ColumnFixedString, false>(arrays, std::move(column));
        else
        {
            column = assert_cast<const DataTypeArray &>(*return_type_with_nulls).getNestedType()->createColumn();
            result_column = castRemoveNullable(execute<StringRef, IColumn, false>(arrays, std::move(column)), result_type);
        }
    }

    return result_column;
}
}
