#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getMostSubtype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Common/HashTable/ClearableHashMap.h>
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
    FunctionArrayIntersect(const Context & context) : context(context) {}

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
        std::vector<char> is_const;
        std::vector<const NullMap *> null_maps;
        std::vector<const ColumnArray::ColumnOffsets::Container *> offsets;
        ColumnRawPtrs nested_columns;

        UnpackedArrays() = default;
    };

    /// Cast column to data_type removing nullable if data_type hasn't.
    /// It's expected that column can represent data_type after removing some NullMap's.
    ColumnPtr castRemoveNullable(const ColumnPtr & column, const DataTypePtr & data_type) const;
    Columns castColumns(Block & block, const ColumnNumbers & arguments,
                        const DataTypePtr & return_type, const DataTypePtr & return_type_with_nulls) const;
    UnpackedArrays prepareArrays(const Columns & columns) const;

    template <typename Map, typename ColumnType, bool is_numeric_column>
    static ColumnPtr execute(const UnpackedArrays & arrays, MutableColumnPtr result_data);

    struct NumberExecutor
    {
        const UnpackedArrays & arrays;
        const DataTypePtr & data_type;
        ColumnPtr & result;

        NumberExecutor(const UnpackedArrays & arrays, const DataTypePtr & data_type, ColumnPtr & result)
            : arrays(arrays), data_type(data_type), result(result) {}

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
        auto array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
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
    if (auto column_nullable = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        auto nullable_type = checkAndGetDataType<DataTypeNullable>(data_type.get());
        const auto & nested = column_nullable->getNestedColumnPtr();
        if (nullable_type)
        {
            auto casted_column = castRemoveNullable(nested, nullable_type->getNestedType());
            return ColumnNullable::create(casted_column, column_nullable->getNullMapColumnPtr());
        }
        return castRemoveNullable(nested, data_type);
    }
    else if (auto column_array = checkAndGetColumn<ColumnArray>(column.get()))
    {
        auto array_type = checkAndGetDataType<DataTypeArray>(data_type.get());
        if (!array_type)
            throw Exception{"Cannot cast array column to column with type "
                            + data_type->getName() + " in function " + getName(), ErrorCodes::LOGICAL_ERROR};

        auto casted_column = castRemoveNullable(column_array->getDataPtr(), array_type->getNestedType());
        return ColumnArray::create(casted_column, column_array->getOffsetsPtr());
    }
    else if (auto column_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
    {
        auto tuple_type = checkAndGetDataType<DataTypeTuple>(data_type.get());

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

Columns FunctionArrayIntersect::castColumns(
        Block & block, const ColumnNumbers & arguments, const DataTypePtr & return_type,
        const DataTypePtr & return_type_with_nulls) const
{
    size_t num_args = arguments.size();
    Columns columns(num_args);

    auto type_array = checkAndGetDataType<DataTypeArray>(return_type.get());
    auto & type_nested = type_array->getNestedType();
    auto type_not_nullable_nested = removeNullable(type_nested);

    const bool is_numeric_or_string = isNumber(type_not_nullable_nested)
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
        auto & column = columns[i];

        if (is_numeric_or_string)
        {
            /// Cast to Array(T) or Array(Nullable(T)).
            if (nested_is_nullable)
            {
                if (arg.type->equals(*return_type))
                    column = arg.column;
                else
                    column = castColumn(arg, return_type, context);
            }
            else
            {
                /// If result has array type Array(T) still cast Array(Nullable(U)) to Array(Nullable(T))
                ///  because cannot cast Nullable(T) to T.
                if (arg.type->equals(*return_type) || arg.type->equals(*nullable_return_type))
                    column = arg.column;
                else if (static_cast<const DataTypeArray &>(*arg.type).getNestedType()->isNullable())
                    column = castColumn(arg, nullable_return_type, context);
                else
                    column = castColumn(arg, return_type, context);
            }
        }
        else
        {
            /// return_type_with_nulls is the most common subtype with possible nullable parts.
            if (arg.type->equals(*return_type_with_nulls))
                column = arg.column;
            else
                column = castColumn(arg, return_type_with_nulls, context);
        }
    }

    return columns;
}

FunctionArrayIntersect::UnpackedArrays FunctionArrayIntersect::prepareArrays(const Columns & columns) const
{
    UnpackedArrays arrays;

    size_t columns_number = columns.size();
    arrays.is_const.assign(columns_number, false);
    arrays.null_maps.resize(columns_number);
    arrays.offsets.resize(columns_number);
    arrays.nested_columns.resize(columns_number);

    bool all_const = true;

    for (auto i : ext::range(0, columns_number))
    {
        auto argument_column = columns[i].get();
        if (auto argument_column_const = typeid_cast<const ColumnConst *>(argument_column))
        {
            arrays.is_const[i] = true;
            argument_column = argument_column_const->getDataColumnPtr().get();
        }

        if (auto argument_column_array = typeid_cast<const ColumnArray *>(argument_column))
        {
            if (!arrays.is_const[i])
                all_const = false;

            arrays.offsets[i] = &argument_column_array->getOffsets();
            arrays.nested_columns[i] = &argument_column_array->getData();
            if (auto column_nullable = typeid_cast<const ColumnNullable *>(arrays.nested_columns[i]))
            {
                arrays.null_maps[i] = &column_nullable->getNullMapData();
                arrays.nested_columns[i] = &column_nullable->getNestedColumn();
            }
        }
        else
            throw Exception{"Arguments for function " + getName() + " must be arrays.", ErrorCodes::LOGICAL_ERROR};
    }

    if (all_const)
    {
        arrays.base_rows = arrays.offsets.front()->size();
    }
    else
    {
        for (auto i : ext::range(0, columns_number))
        {
            if (arrays.is_const[i])
                continue;

            size_t rows = arrays.offsets[i]->size();
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
    auto return_type_array = checkAndGetDataType<DataTypeArray>(return_type.get());

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

    Columns columns = castColumns(block, arguments, return_type, return_type_with_nulls);

    UnpackedArrays arrays = prepareArrays(columns);

    ColumnPtr result_column;
    auto not_nullable_nested_return_type = removeNullable(nested_return_type);
    TypeListNumbers::forEach(NumberExecutor(arrays, not_nullable_nested_return_type, result_column));

    using DateMap = ClearableHashMap<DataTypeDate::FieldType, size_t, DefaultHash<DataTypeDate::FieldType>,
            HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(DataTypeDate::FieldType)>>;

    using DateTimeMap = ClearableHashMap<DataTypeDateTime::FieldType, size_t, DefaultHash<DataTypeDateTime::FieldType>,
            HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(DataTypeDateTime::FieldType)>>;

    using StringMap = ClearableHashMap<StringRef, size_t, StringRefHash, HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(StringRef)>>;

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
            column = static_cast<const DataTypeArray &>(*return_type_with_nulls).getNestedType()->createColumn();
            result_column = castRemoveNullable(execute<StringMap, IColumn, false>(arrays, std::move(column)), return_type);
        }
    }

    block.getByPosition(result).column = std::move(result_column);
}

template <typename T, size_t>
void FunctionArrayIntersect::NumberExecutor::operator()()
{
    using Map = ClearableHashMap<T, size_t, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(T)>>;

    if (!result && typeid_cast<const DataTypeNumber<T> *>(data_type.get()))
        result = execute<Map, ColumnVector<T>, true>(arrays, ColumnVector<T>::create());
}

template <typename Map, typename ColumnType, bool is_numeric_column>
ColumnPtr FunctionArrayIntersect::execute(const UnpackedArrays & arrays, MutableColumnPtr result_data_ptr)
{
    auto args = arrays.nested_columns.size();
    auto rows = arrays.base_rows;

    bool all_nullable = true;

    std::vector<const ColumnType *> columns;
    columns.reserve(args);
    for (auto arg : ext::range(0, args))
    {
        if constexpr (std::is_same<ColumnType, IColumn>::value)
            columns.push_back(arrays.nested_columns[arg]);
        else
            columns.push_back(checkAndGetColumn<ColumnType>(arrays.nested_columns[arg]));

        if (!columns.back())
            throw Exception("Unexpected array type for function arrayIntersect", ErrorCodes::LOGICAL_ERROR);

        if (!arrays.null_maps[arg])
            all_nullable = false;
    }

    auto & result_data = static_cast<ColumnType &>(*result_data_ptr);
    auto result_offsets_ptr = ColumnArray::ColumnOffsets::create(rows);
    auto & result_offsets = static_cast<ColumnArray::ColumnOffsets &>(*result_offsets_ptr);
    auto null_map_column = ColumnUInt8::create();
    NullMap & null_map = static_cast<ColumnUInt8 &>(*null_map_column).getData();

    Arena arena;

    Map map;
    std::vector<size_t> prev_off(args, 0);
    size_t result_offset = 0;
    for (auto row : ext::range(0, rows))
    {
        map.clear();

        bool all_has_nullable = all_nullable;

        for (auto arg : ext::range(0, args))
        {
            bool current_has_nullable = false;

            size_t off;
            // const array has only one row
            bool const_arg = arrays.is_const[arg];
            if (const_arg)
                off = (*arrays.offsets[arg])[0];
            else
                off = (*arrays.offsets[arg])[row];

            for (auto i : ext::range(prev_off[arg], off))
            {
                if (arrays.null_maps[arg] && (*arrays.null_maps[arg])[i])
                    current_has_nullable = true;
                else
                {
                    typename Map::mapped_type * value = nullptr;

                    if constexpr (is_numeric_column)
                        value = &map[columns[arg]->getElement(i)];
                    else if constexpr (std::is_same<ColumnType, ColumnString>::value || std::is_same<ColumnType, ColumnFixedString>::value)
                        value = &map[columns[arg]->getDataAt(i)];
                    else
                    {
                        const char * data = nullptr;
                        value = &map[columns[arg]->serializeValueIntoArena(i, arena, data)];
                    }

                    if (*value == arg)
                        ++(*value);
                }
            }

            prev_off[arg] = off;
            if (const_arg)
                prev_off[arg] = 0;

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
            if (pair.getSecond() == args)
            {
                ++result_offset;
                if constexpr (is_numeric_column)
                    result_data.insertValue(pair.getFirst());
                else if constexpr (std::is_same<ColumnType, ColumnString>::value || std::is_same<ColumnType, ColumnFixedString>::value)
                    result_data.insertData(pair.getFirst().data, pair.getFirst().size);
                else
                    result_data.deserializeAndInsertFromArena(pair.getFirst().data);

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
