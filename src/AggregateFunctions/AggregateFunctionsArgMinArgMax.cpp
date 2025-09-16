#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/SingleValueData.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/IDataType.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <class ResultType, class ValueType>
struct AggregateFunctionArgMinMaxData
{
private:
    ResultType result_data;
    ValueType value_data;

public:
    ResultType & result() { return result_data; }
    const ResultType & result() const { return result_data; }
    ValueType & value() { return value_data; }
    const ValueType & value() const { return value_data; }

    AggregateFunctionArgMinMaxData() = default;
    explicit AggregateFunctionArgMinMaxData(const DataTypePtr &) {}

    static bool allocatesMemoryInArena(TypeIndex)
    {
        return ResultType::allocatesMemoryInArena() || ValueType::allocatesMemoryInArena();
    }
};

template <class ValueType>
struct AggregateFunctionArgMinMaxDataGeneric
{
private:
    SingleValueDataBaseMemoryBlock result_data;
    ValueType value_data;

public:
    SingleValueDataBase & result() { return result_data.get(); }
    const SingleValueDataBase & result() const { return result_data.get(); }
    ValueType & value() { return value_data; }
    const ValueType & value() const { return value_data; }

    [[noreturn]] AggregateFunctionArgMinMaxDataGeneric()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionArgMinMaxData initialized empty");
    }

    explicit AggregateFunctionArgMinMaxDataGeneric(const DataTypePtr & result_type) : value_data()
    {
        generateSingleValueFromType(result_type, result_data);
    }

    static bool allocatesMemoryInArena(TypeIndex result_type_index)
    {
        return singleValueTypeAllocatesMemoryInArena(result_type_index) || ValueType::allocatesMemoryInArena();
    }

    ~AggregateFunctionArgMinMaxDataGeneric() { result().~SingleValueDataBase(); }
};

static_assert(
    sizeof(AggregateFunctionArgMinMaxDataGeneric<Int8>) <= 2 * SingleValueDataBase::MAX_STORAGE_SIZE,
    "Incorrect size of AggregateFunctionArgMinMaxData struct");

/// Returns the first arg value found for the minimum/maximum value. Example: argMin(arg, value).
template <typename Data, bool isMin>
class AggregateFunctionArgMinMax final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data, isMin>>
{
private:
    const DataTypePtr & type_val;
    const DataTypePtr data_type_res;
    const SerializationPtr serialization_res;
    const DataTypePtr data_type_val;
    const SerializationPtr serialization_val;
    const TypeIndex result_type_index;


    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data, isMin>>;

public:
    explicit AggregateFunctionArgMinMax(const DataTypes & argument_types_)
        : Base(argument_types_, {}, argument_types_[0])
        , type_val(this->argument_types[1])
        , data_type_res(this->argument_types[0])
        , serialization_res(this->argument_types[0]->getDefaultSerialization())
        , data_type_val(this->argument_types[1])
        , serialization_val(this->argument_types[1]->getDefaultSerialization())
        , result_type_index(WhichDataType(this->argument_types[0]).idx)
    {
        if (!type_val->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of aggregate function {} because the values of that data type are not comparable",
                type_val->getName(),
                getName());

        if (isDynamic(this->type_val) || isVariant(this->type_val))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of aggregate function {} because the column of that type can contain values with different "
                "data types. Consider using typed subcolumns or cast column to a specific data type",
                this->type_val->getName(),
                getName());
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        new (place) Data(data_type_res);
    }

    String getName() const override
    {
        if constexpr (isMin)
            return "argMin";
        else
            return "argMax";
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (isMin)
        {
            if (this->data(place).value().setIfSmaller(*columns[1], row_num, arena))
                this->data(place).result().set(*columns[0], row_num, arena);
        }
        else
        {
            if (this->data(place).value().setIfGreater(*columns[1], row_num, arena))
                this->data(place).result().set(*columns[0], row_num, arena);
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        add(place, columns, 0, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        std::optional<size_t> idx;
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if constexpr (isMin)
                idx = this->data(place).value().getSmallestIndexNotNullIf(*columns[1], nullptr, if_map.data(), row_begin, row_end);
            else
                idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], nullptr, if_map.data(), row_begin, row_end);
        }
        else
        {
            if constexpr (isMin)
                idx = this->data(place).value().getSmallestIndex(*columns[1], row_begin, row_end);
            else
                idx = this->data(place).value().getGreatestIndex(*columns[1], row_begin, row_end);
        }

        if (idx)
            add(place, columns, *idx, arena);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        std::optional<size_t> idx;
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if constexpr (isMin)
                idx = this->data(place).value().getSmallestIndexNotNullIf(*columns[1], null_map, if_map.data(), row_begin, row_end);
            else
                idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], null_map, if_map.data(), row_begin, row_end);
        }
        else
        {
            if constexpr (isMin)
                idx = this->data(place).value().getSmallestIndexNotNullIf(*columns[1], null_map, nullptr, row_begin, row_end);
            else
                idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], null_map, nullptr, row_begin, row_end);
        }

        if (idx)
            add(place, columns, *idx, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if constexpr (isMin)
        {
            if (this->data(place).value().setIfSmaller(this->data(rhs).value(), arena))
                this->data(place).result().set(this->data(rhs).result(), arena);
        }
        else
        {
            if (this->data(place).value().setIfGreater(this->data(rhs).value(), arena))
                this->data(place).result().set(this->data(rhs).result(), arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).result().write(buf, *serialization_res);
        this->data(place).value().write(buf, *serialization_val);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).result().read(buf, *serialization_res, data_type_res, arena);
        this->data(place).value().read(buf, *serialization_val, data_type_val, arena);
        if (unlikely(this->data(place).value().has() != this->data(place).result().has()))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Invalid state of the aggregate function {}: has_value ({}) != has_result ({})",
                getName(),
                this->data(place).value().has(),
                this->data(place).result().has());
    }

    bool allocatesMemoryInArena() const override
    {
        return Data::allocatesMemoryInArena(result_type_index);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).result().insertResultInto(to, this->result_type);
    }
};


template <bool isMin, typename ResultType>
IAggregateFunction * createWithTwoTypesSecond(const DataTypes & argument_types)
{
    const DataTypePtr & value_type = argument_types[1];
    WhichDataType which_value(value_type);

    if (which_value.idx == TypeIndex::UInt8)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt8>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::UInt16)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt16>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::UInt32)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt32>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::UInt64)
    {
       using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt64>>;
       return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::Int8)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Int8>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::Int16)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Int16>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::Int32)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Int32>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::Int64)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Int64>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::Float32)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Float32>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::Float64)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Float64>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::Date)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt16>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }
    if (which_value.idx == TypeIndex::DateTime)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt32>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types);
    }

    return nullptr;
}

template <bool isMin>
IAggregateFunction * createWithTwoTypes(const DataTypes & argument_types)
{
    const DataTypePtr & result_type = argument_types[0];
    WhichDataType which_result(result_type);

    if (which_result.idx == TypeIndex::UInt8) return createWithTwoTypesSecond<isMin, UInt8>(argument_types);
    if (which_result.idx == TypeIndex::UInt16) return createWithTwoTypesSecond<isMin, UInt16>(argument_types);
    if (which_result.idx == TypeIndex::UInt32) return createWithTwoTypesSecond<isMin, UInt32>(argument_types);
    if (which_result.idx == TypeIndex::UInt64) return createWithTwoTypesSecond<isMin, UInt64>(argument_types);
    if (which_result.idx == TypeIndex::Int8) return createWithTwoTypesSecond<isMin, Int8>(argument_types);
    if (which_result.idx == TypeIndex::Int16) return createWithTwoTypesSecond<isMin, Int16>(argument_types);
    if (which_result.idx == TypeIndex::Int32) return createWithTwoTypesSecond<isMin, Int32>(argument_types);
    if (which_result.idx == TypeIndex::Int64) return createWithTwoTypesSecond<isMin, Int64>(argument_types);
    if (which_result.idx == TypeIndex::Float32) return createWithTwoTypesSecond<isMin, Float32>(argument_types);
    if (which_result.idx == TypeIndex::Float64) return createWithTwoTypesSecond<isMin, Float64>(argument_types);

    return nullptr;
}


template <bool isMin>
AggregateFunctionPtr createAggregateFunctionArgMinMax(const std::string & name, const DataTypes & argument_types, const Array &, const Settings *)
{
    assertBinary(name, argument_types);

    AggregateFunctionPtr result = AggregateFunctionPtr(createWithTwoTypes<isMin>(argument_types));

    if (!result)
    {
        const DataTypePtr & value_type = argument_types[1];
        WhichDataType which(value_type);
#define DISPATCH(TYPE) \
        if (which.idx == TypeIndex::TYPE) \
            return AggregateFunctionPtr(new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataFixed<TYPE>>, isMin>(argument_types));  /// NOLINT
        FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

        if (which.idx == TypeIndex::Date)
            return AggregateFunctionPtr(new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataFixed<DataTypeDate::FieldType>>, isMin>(argument_types));
        if (which.idx == TypeIndex::DateTime)
            return AggregateFunctionPtr(new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataFixed<DataTypeDateTime::FieldType>>, isMin>(argument_types));
        if (which.idx == TypeIndex::String)
            return AggregateFunctionPtr(new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataString>, isMin>(argument_types));

        if (canUseFieldForValueData(value_type))
            return AggregateFunctionPtr(new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataGeneric>, isMin>(argument_types));
        return AggregateFunctionPtr(new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataGenericWithColumn>, isMin>(argument_types));
    }
    return result;
}

}

void registerAggregateFunctionsArgMinArgMax(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    FunctionDocumentation::Description min_description = R"(
Calculates the `arg` value for a minimum `val` value.
If there are multiple rows with equal `val` being the minimum, which of the associated `arg` is returned is not deterministic.
Both parts the `arg` and the `min` behave as aggregate functions, they both skip `NULL` during processing and return not `NULL` values if not `NULL` values are available.
    )";
    FunctionDocumentation::Syntax min_syntax = "argMin(arg, val)";
    FunctionDocumentation::Arguments min_arguments = {
        {"arg", "Argument.", {"Any"}},
        {"val", "Value.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue min_returned_value = {"Returns arg value that corresponds to minimum val value. Type matches arg type.", {"Any"}};
    FunctionDocumentation::Examples min_examples = {
    {
        "Extended example",
        R"(
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES((NULL, 0), ('a', 1), ('b', 2), ('c', 2), (NULL, NULL), ('d', NULL));

SELECT * FROM test;
SELECT argMin(a, b), min(b) FROM test;
SELECT argMin(tuple(a), b) FROM test;
SELECT (argMin((a, b), b) as t).1 argMinA, t.2 argMinB from test;
SELECT argMin(a, b), min(b) FROM test WHERE a IS NULL and b IS NULL;
SELECT argMin(a, (b, a)), min(tuple(b, a)) FROM test;
SELECT argMin((a, b), (b, a)), min(tuple(b, a)) FROM test;
SELECT argMin(a, tuple(b)) FROM test;
        )",
        R"(
┌─a────┬────b─┐
│ ᴺᵁᴸᴸ │    0 │
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘
┌─argMin(a, b)─┬─min(b)─┐
│ a            │      0 │ -- argMin = 'a' because it the first not `NULL` value, min(b) is from another row!
└──────────────┴────────┘
┌─argMin(tuple(a), b)─┐
│ (NULL)              │ -- The a `Tuple` that contains only a `NULL` value is not `NULL`, so the aggregate functions won't skip that row because of that `NULL` value
└─────────────────────┘
┌─argMinA─┬─argMinB─┐
│ ᴺᵁᴸᴸ    │       0 │ -- you can use `Tuple` and get both (all - tuple(*)) columns for the according min(b)
└─────────┴─────────┘
┌─argMin(a, b)─┬─min(b)─┐
│ ᴺᵁᴸᴸ         │   ᴺᵁᴸᴸ │ -- All aggregated rows contains at least one `NULL` value because of the filter, so all rows are skipped, therefore the result will be `NULL`
└──────────────┴────────┘
┌─argMin(a, tuple(b, a))─┬─min(tuple(b, a))─┐
│ d                      │ (NULL,NULL)      │ -- 'd' is the first not `NULL` value for the min
└────────────────────────┴──────────────────┘
┌─argMin(tuple(a, b), tuple(b, a))─┬─min(tuple(b, a))─┐
│ (NULL,NULL)                      │ (NULL,NULL)      │ -- argMin returns (NULL,NULL) here because `Tuple` allows to don't skip `NULL` and min(tuple(b, a)) in this case is minimal value for this dataset
└──────────────────────────────────┴──────────────────┘
┌─argMin(a, tuple(b))─┐
│ d                   │ -- `Tuple` can be used in `min` to not skip rows with `NULL` values as b.
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn min_introduced_in = {1, 1};
    FunctionDocumentation::Category min_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation min_documentation = {min_description, min_syntax, min_arguments, {}, min_returned_value, min_examples, min_introduced_in, min_category};

    factory.registerFunction("argMin", createAggregateFunctionArgMinMax<true>, properties, min_documentation, AggregateFunctionFactory::Case::Sensitive);

    FunctionDocumentation::Description description = R"(
Calculates the `arg` value for a maximum `val` value.
If there are multiple rows with equal `val` being the maximum, which of the associated `arg` is returned is not deterministic.
Both parts the `arg` and the `max` behave as aggregate functions, they both skip `NULL` during processing and return not `NULL` values if not `NULL` values are available.
    )";
    FunctionDocumentation::Syntax syntax = "argMax(arg, val)";
    FunctionDocumentation::Arguments arguments = {
        {"arg", "Argument.", {"Any"}},
        {"val", "Value.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns arg value that corresponds to maximum val value. Type matches arg type.", {"Any"}};
    FunctionDocumentation::Examples examples = {
    {
        "Extended example",
        R"(
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES(('a', 1), ('b', 2), ('c', 2), (NULL, 3), (NULL, NULL), ('d', NULL));

SELECT * FROM test;
SELECT argMax(a, b), max(b) FROM test;
SELECT argMax(tuple(a), b) FROM test;
SELECT (argMax((a, b), b) as t).1 argMaxA, t.2 argMaxB FROM test;
SELECT argMax(a, b), max(b) FROM test WHERE a IS NULL AND b IS NULL;
SELECT argMax(a, (b,a)) FROM test;
SELECT argMax(a, tuple(b)) FROM test;
        )",
        R"(
┌─a────┬────b─┐
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │    3 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘
┌─argMax(a, b)─┬─max(b)─┐
│ b            │      3 │ -- argMax = 'b' because it the first not Null value, max(b) is from another row!
└──────────────┴────────┘
┌─argMax(tuple(a), b)─┐
│ (NULL)              │ -- The a `Tuple` that contains only a `NULL` value is not `NULL`, so the aggregate functions won't skip that row because of that `NULL` value
└─────────────────────┘
┌─argMaxA─┬─argMaxB─┐
│ ᴺᵁᴸᴸ    │       3 │ -- you can use Tuple and get both (all - tuple(*)) columns for the according max(b)
└─────────┴─────────┘
┌─argMax(a, b)─┬─max(b)─┐
│ ᴺᵁᴸᴸ         │   ᴺᵁᴸᴸ │ -- All aggregated rows contains at least one `NULL` value because of the filter, so all rows are skipped, therefore the result will be `NULL`
└──────────────┴────────┘
┌─argMax(a, tuple(b, a))─┐
│ c                      │ -- There are two rows with b=2, `Tuple` in the `Max` allows to get not the first `arg`
└────────────────────────┘
┌─argMax(a, tuple(b))─┐
│ b                   │ -- `Tuple` can be used in `Max` to not skip Nulls in `Max`
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("argMax", createAggregateFunctionArgMinMax<false>, properties, documentation, AggregateFunctionFactory::Case::Sensitive);
}

}
