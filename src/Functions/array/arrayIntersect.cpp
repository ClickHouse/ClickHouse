#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getMostSubtype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/assert_cast.h>
#include <Core/TypeListNumber.h>
#include <Interpreters/castColumn.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionArrayIntersect : public IFunction
{
public:
    static constexpr auto name = "arrayIntersect";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayIntersect>(context); }
    explicit FunctionArrayIntersect(const Context & context_) : context(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    const Context & context;

    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

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

    static CastArgumentsResult castColumns(Block & block, const ColumnNumbers & arguments,
                        const DataTypePtr & return_type, const DataTypePtr & return_type_with_nulls);
    UnpackedArrays prepareArrays(const ColumnsWithTypeAndName & columns, ColumnsWithTypeAndName & initial_columns) const;

    template <typename Map, typename ColumnType, bool is_numeric_column>
    static ColumnPtr execute(const UnpackedArrays & arrays, MutableColumnPtr result_data);

    struct NumberExecutor
    {
        const UnpackedArrays & arrays;
        const DataTypePtr & data_type;
        ColumnPtr & result;

        NumberExecutor(const UnpackedArrays & arrays_, const DataTypePtr & data_type_, ColumnPtr & result_)
            : arrays(arrays_), data_type(data_type_), result(result_) {}

        template <typename T, size_t>
        void operator()();
    };

    struct DecimalExecutor
    {
        const UnpackedArrays & arrays;
        const DataTypePtr & data_type;
        ColumnPtr & result;

        DecimalExecutor(const UnpackedArrays & arrays_, const DataTypePtr & data_type_, ColumnPtr & result_)
            : arrays(arrays_), data_type(data_type_), result(result_) {}

        template <typename T, size_t>
        void operator()();
    };
};


DataTypePtr FunctionArrayIntersect::getReturnTypeImpl(const DataTypes & arguments) const
{
    DataTypes nested_types;
    nested_types.reserve(arguments.size());

    bool has_nothing = false;

    if (arguments.empty())
        throw Exception{"Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    for (auto i : ext::range(0, arguments.size()))
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
        if (!array_type)
            throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array but it has type "
                            + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto & nested_type = array_type->getNestedType();

        if (typeid_cast<const DataTypeNothing *>(nested_type.get()))
            has_nothing = true;
        else
            nested_types.push_back(nested_type);
    }

    DataTypePtr result_type;

    if (!nested_types.empty())
        result_type = getMostSubtype(nested_types, true);

    if (has_nothing)
        result_type = std::make_shared<DataTypeNothing>();

    return std::make_shared<DataTypeArray>(result_type);
}

ColumnPtr FunctionArrayIntersect::castRemoveNullable(const ColumnPtr & column, const DataTypePtr & data_type) const
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
            throw Exception{"Cannot cast array column to column with type "
                            + data_type->getName() + " in function " + getName(), ErrorCodes::LOGICAL_ERROR};

        auto casted_column = castRemoveNullable(column_array->getDataPtr(), array_type->getNestedType());
        return ColumnArray::create(casted_column, column_array->getOffsetsPtr());
    }
    else if (const auto * column_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
    {
        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(data_type.get());

        if (!tuple_type)
            throw Exception{"Cannot cast tuple column to type "
                            + data_type->getName() + " in function " + getName(), ErrorCodes::LOGICAL_ERROR};

        auto columns_number = column_tuple->tupleSize();
        Columns columns(columns_number);

        const auto & types = tuple_type->getElements();

        for (auto i : ext::range(0, columns_number))
        {
            columns[i] = castRemoveNullable(column_tuple->getColumnPtr(i), types[i]);
        }
        return ColumnTuple::create(columns);
    }

    return column;
}

FunctionArrayIntersect::CastArgumentsResult FunctionArrayIntersect::castColumns(
        Block & block, const ColumnNumbers & arguments, const DataTypePtr & return_type,
        const DataTypePtr & return_type_with_nulls)
{
    size_t num_args = arguments.size();
    ColumnsWithTypeAndName initial_columns(num_args);
    ColumnsWithTypeAndName columns(num_args);

    const auto * type_array = checkAndGetDataType<DataTypeArray>(return_type.get());
    const auto & type_nested = type_array->getNestedType();
    auto type_not_nullable_nested = removeNullable(type_nested);

    const bool is_numeric_or_string = isNativeNumber(type_not_nullable_nested)
                                      || isDateOrDateTime(type_not_nullable_nested)
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
        const ColumnWithTypeAndName & arg = block.getByPosition(arguments[i]);
        initial_columns[i] = arg;
        columns[i] = arg;
        auto & column = columns[i];

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
            else
            {

                if (!arg.type->equals(*return_type) && !arg.type->equals(*nullable_return_type))
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
        }
        else
        {
            /// return_type_with_nulls is the most common subtype with possible nullable parts.
            if (!arg.type->equals(*return_type_with_nulls))
            {
                column.column = castColumn(arg, return_type_with_nulls);
                column.type = return_type_with_nulls;
            }
        }
    }

    return {.initial = initial_columns, .casted = columns};
}

static ColumnPtr callFunctionNotEquals(ColumnWithTypeAndName first, ColumnWithTypeAndName second, const Context & context)
{
    ColumnsWithTypeAndName args;
    args.reserve(2);
    args.emplace_back(std::move(first));
    args.emplace_back(std::move(second));

    auto eq_func = FunctionFactory::instance().get("notEquals", context)->build(args);

    Block block = args;
    block.insert({nullptr, eq_func->getReturnType(), ""});

    eq_func->execute(block, {0, 1}, 2, args.front().column->size());

    return block.getByPosition(2).column;
}

FunctionArrayIntersect::UnpackedArrays FunctionArrayIntersect::prepareArrays(
    const ColumnsWithTypeAndName & columns, ColumnsWithTypeAndName & initial_columns) const
{
    UnpackedArrays arrays;

    size_t columns_number = columns.size();
    arrays.args.resize(columns_number);

    bool all_const = true;

    for (auto i : ext::range(0, columns_number))
    {
        auto & arg = arrays.args[i];
        const auto * argument_column = columns[i].column.get();
        const auto * initial_column = initial_columns[i].column.get();

        if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(argument_column))
        {
            arg.is_const = true;
            argument_column = argument_column_const->getDataColumnPtr().get();
            initial_column = typeid_cast<const ColumnConst *>(initial_column)->getDataColumnPtr().get();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(argument_column))
        {
            if (!arg.is_const)
                all_const = false;

            arg.offsets = &argument_column_array->getOffsets();
            arg.nested_column = &argument_column_array->getData();

            initial_column = &typeid_cast<const ColumnArray *>(initial_column)->getData();

            if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(arg.nested_column))
            {
                arg.null_map = &column_nullable->getNullMapData();
                arg.nested_column = &column_nullable->getNestedColumn();
                initial_column = &typeid_cast<const ColumnNullable *>(initial_column)->getNestedColumn();
            }

            /// In case column was casted need to create overflow mask for integer types.
            if (arg.nested_column != initial_column)
            {
                const auto & nested_init_type = typeid_cast<const DataTypeArray *>(removeNullable(initial_columns[i].type).get())->getNestedType();
                const auto & nested_cast_type = typeid_cast<const DataTypeArray *>(removeNullable(columns[i].type).get())->getNestedType();

                if (isInteger(nested_init_type) || isDateOrDateTime(nested_init_type))
                {
                    /// Compare original and casted columns. It seem to be the easiest way.
                    auto overflow_mask = callFunctionNotEquals(
                            {arg.nested_column->getPtr(), nested_init_type, ""},
                            {initial_column->getPtr(), nested_cast_type, ""},
                            context);

                    arg.overflow_mask = &typeid_cast<const ColumnUInt8 *>(overflow_mask.get())->getData();
                    arrays.column_holders.emplace_back(std::move(overflow_mask));
                }
            }
        }
        else
            throw Exception{"Arguments for function " + getName() + " must be arrays.", ErrorCodes::LOGICAL_ERROR};
    }

    if (all_const)
    {
        arrays.base_rows = arrays.args.front().offsets->size();
    }
    else
    {
        for (auto i : ext::range(0, columns_number))
        {
            if (arrays.args[i].is_const)
                continue;

            size_t rows = arrays.args[i].offsets->size();
            if (arrays.base_rows == 0 && rows > 0)
                arrays.base_rows = rows;
            else if (arrays.base_rows != rows)
                throw Exception("Non-const array columns in function " + getName() + "should have same rows", ErrorCodes::LOGICAL_ERROR);
        }
    }

    return arrays;
}

void FunctionArrayIntersect::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    const auto & return_type = block.getByPosition(result).type;
    const auto * return_type_array = checkAndGetDataType<DataTypeArray>(return_type.get());

    if (!return_type_array)
        throw Exception{"Return type for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};

    const auto & nested_return_type = return_type_array->getNestedType();

    if (typeid_cast<const DataTypeNothing *>(nested_return_type.get()))
    {
        block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(input_rows_count);
        return;
    }

    auto num_args = arguments.size();
    DataTypes data_types;
    data_types.reserve(num_args);
    for (size_t i = 0; i < num_args; ++i)
        data_types.push_back(block.getByPosition(arguments[i]).type);

    auto return_type_with_nulls = getMostSubtype(data_types, true, true);

    auto columns = castColumns(block, arguments, return_type, return_type_with_nulls);

    UnpackedArrays arrays = prepareArrays(columns.casted, columns.initial);

    ColumnPtr result_column;
    auto not_nullable_nested_return_type = removeNullable(nested_return_type);
    TypeListNativeNumbers::forEach(NumberExecutor(arrays, not_nullable_nested_return_type, result_column));
    TypeListDecimalNumbers::forEach(DecimalExecutor(arrays, not_nullable_nested_return_type, result_column));

    using DateMap = HASH_TABLE_WITH_STACK_MEMORY(ClearableHashMap,
        DataTypeDate::FieldType, size_t, DefaultHash<DataTypeDate::FieldType>,
        HashTableGrower<INITIAL_SIZE_DEGREE>);

    using DateTimeMap = HASH_TABLE_WITH_STACK_MEMORY(ClearableHashMap,
        DataTypeDateTime::FieldType, size_t,
        DefaultHash<DataTypeDateTime::FieldType>,
        HashTableGrower<INITIAL_SIZE_DEGREE>);

    using StringMap = HASH_TABLE_WITH_STACK_MEMORY(ClearableHashMap,
        StringRef, size_t, StringRefHash,
        HashTableGrower<INITIAL_SIZE_DEGREE>);

    if (!result_column)
    {
        auto column = not_nullable_nested_return_type->createColumn();
        WhichDataType which(not_nullable_nested_return_type);

        if (which.isDate())
            result_column = execute<DateMap, ColumnVector<DataTypeDate::FieldType>, true>(arrays, std::move(column));
        else if (which.isDateTime())
            result_column = execute<DateTimeMap, ColumnVector<DataTypeDateTime::FieldType>, true>(arrays, std::move(column));
        else if (which.isString())
            result_column = execute<StringMap, ColumnString, false>(arrays, std::move(column));
        else if (which.isFixedString())
            result_column = execute<StringMap, ColumnFixedString, false>(arrays, std::move(column));
        else
        {
            column = assert_cast<const DataTypeArray &>(*return_type_with_nulls).getNestedType()->createColumn();
            result_column = castRemoveNullable(execute<StringMap, IColumn, false>(arrays, std::move(column)), return_type);
        }
    }

    block.getByPosition(result).column = std::move(result_column);
}

template <typename T, size_t>
void FunctionArrayIntersect::NumberExecutor::operator()()
{
    using Map = HASH_TABLE_WITH_STACK_MEMORY(ClearableHashMap,
        T, size_t, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>);

    if (!result && typeid_cast<const DataTypeNumber<T> *>(data_type.get()))
        result = execute<Map, ColumnVector<T>, true>(arrays, ColumnVector<T>::create());
}

template <typename T, size_t>
void FunctionArrayIntersect::DecimalExecutor::operator()()
{
    using Map = HASH_TABLE_WITH_STACK_MEMORY(ClearableHashMap,
        T, size_t, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>);

    if (!result)
        if (auto * decimal = typeid_cast<const DataTypeDecimal<T> *>(data_type.get()))
            result = execute<Map, ColumnDecimal<T>, true>(arrays, ColumnDecimal<T>::create(0, decimal->getScale()));
}

template <typename Map, typename ColumnType, bool is_numeric_column>
ColumnPtr FunctionArrayIntersect::execute(const UnpackedArrays & arrays, MutableColumnPtr result_data_ptr)
{
    auto args = arrays.args.size();
    auto rows = arrays.base_rows;

    bool all_nullable = true;

    std::vector<const ColumnType *> columns;
    columns.reserve(args);
    for (const auto & arg : arrays.args)
    {
        if constexpr (std::is_same<ColumnType, IColumn>::value)
            columns.push_back(arg.nested_column);
        else
            columns.push_back(checkAndGetColumn<ColumnType>(arg.nested_column));

        if (!columns.back())
            throw Exception("Unexpected array type for function arrayIntersect", ErrorCodes::LOGICAL_ERROR);

        if (!arg.null_map)
            all_nullable = false;
    }

    auto & result_data = static_cast<ColumnType &>(*result_data_ptr);
    auto result_offsets_ptr = ColumnArray::ColumnOffsets::create(rows);
    auto & result_offsets = assert_cast<ColumnArray::ColumnOffsets &>(*result_offsets_ptr);
    auto null_map_column = ColumnUInt8::create();
    NullMap & null_map = assert_cast<ColumnUInt8 &>(*null_map_column).getData();

    Arena arena;

    Map map;
    std::vector<size_t> prev_off(args, 0);
    size_t result_offset = 0;
    for (auto row : ext::range(0, rows))
    {
        map.clear();

        bool all_has_nullable = all_nullable;

        for (auto arg_num : ext::range(0, args))
        {
            const auto & arg = arrays.args[arg_num];
            bool current_has_nullable = false;

            size_t off;
            // const array has only one row
            if (arg.is_const)
                off = (*arg.offsets)[0];
            else
                off = (*arg.offsets)[row];

            for (auto i : ext::range(prev_off[arg_num], off))
            {
                if (arg.null_map && (*arg.null_map)[i])
                    current_has_nullable = true;
                else if (!arg.overflow_mask || (*arg.overflow_mask)[i] == 0)
                {
                    typename Map::mapped_type * value = nullptr;

                    if constexpr (is_numeric_column)
                        value = &map[columns[arg_num]->getElement(i)];
                    else if constexpr (std::is_same<ColumnType, ColumnString>::value || std::is_same<ColumnType, ColumnFixedString>::value)
                        value = &map[columns[arg_num]->getDataAt(i)];
                    else
                    {
                        const char * data = nullptr;
                        value = &map[columns[arg_num]->serializeValueIntoArena(i, arena, data)];
                    }

                    /// Here we count the number of element appearances, but no more than once per array.
                    if (*value == arg_num)
                        ++(*value);
                }
            }

            prev_off[arg_num] = off;
            if (arg.is_const)
                prev_off[arg_num] = 0;

            if (!current_has_nullable)
                all_has_nullable = false;
        }

        if (all_has_nullable)
        {
            ++result_offset;
            result_data.insertDefault();
            null_map.push_back(1);
        }

        for (const auto & pair : map)
        {
            if (pair.getMapped() == args)
            {
                ++result_offset;
                if constexpr (is_numeric_column)
                    result_data.insertValue(pair.getKey());
                else if constexpr (std::is_same<ColumnType, ColumnString>::value || std::is_same<ColumnType, ColumnFixedString>::value)
                    result_data.insertData(pair.getKey().data, pair.getKey().size);
                else
                    result_data.deserializeAndInsertFromArena(pair.getKey().data);

                if (all_nullable)
                    null_map.push_back(0);
            }
        }
        result_offsets.getElement(row) = result_offset;
    }

    ColumnPtr result_column = std::move(result_data_ptr);
    if (all_nullable)
        result_column = ColumnNullable::create(result_column, std::move(null_map_column));
    return ColumnArray::create(result_column, std::move(result_offsets_ptr));
}


void registerFunctionArrayIntersect(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayIntersect>();
}

}
