#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/SingleValueData.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
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
/// When return_both is true, returns tuple (arg, value). Example: argAndMin(arg, value).
template <typename Data, bool isMin>
class AggregateFunctionArgMinMax final : public IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data, isMin>>
{
private:
    const DataTypePtr & type_val;
    const DataTypePtr data_type_res;
    const SerializationPtr serialization_res;
    const DataTypePtr data_type_val;
    const SerializationPtr serialization_val;
    const TypeIndex result_type_index;
    const bool return_both;

    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data, isMin>>;

public:
    explicit AggregateFunctionArgMinMax(const DataTypes & argument_types_, bool return_both_)
        : Base(argument_types_, {}, createResultType(argument_types_, return_both_))
        , type_val(this->argument_types[1])
        , data_type_res(this->argument_types[0])
        , serialization_res(this->argument_types[0]->getDefaultSerialization())
        , data_type_val(this->argument_types[1])
        , serialization_val(this->argument_types[1]->getDefaultSerialization())
        , result_type_index(WhichDataType(this->argument_types[0]).idx)
        , return_both(return_both_)
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

    static DataTypePtr createResultType(const DataTypes & argument_types_, bool return_both_)
    {
        if (return_both_)
        {
            DataTypes types = {argument_types_[0], argument_types_[1]};
            return std::make_shared<DataTypeTuple>(std::move(types));
        }
        return argument_types_[0];
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        new (place) Data(data_type_res);
    }

    String getName() const override
    {
        if (return_both)
        {
            if constexpr (isMin)
                return "argAndMin";
            else
                return "argAndMax";
        }
        else
        {
            if constexpr (isMin)
                return "argMin";
            else
                return "argMax";
        }
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
        if (return_both)
        {
            auto & col_tuple = assert_cast<ColumnTuple &>(to);

            this->data(place).result().insertResultInto(col_tuple.getColumn(0), data_type_res);
            this->data(place).value().insertResultInto(col_tuple.getColumn(1), data_type_val);
        }
        else
        {
            this->data(place).result().insertResultInto(to, data_type_res);
        }
    }
};


template <bool isMin, typename ResultType>
IAggregateFunction * createWithTwoTypesSecond(const DataTypes & argument_types, const bool return_both) //NOLINT(misc-unused-parameters)
{
    const DataTypePtr & value_type = argument_types[1];
    WhichDataType which_value(value_type);

    if (which_value.idx == TypeIndex::UInt8)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt8>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::UInt16)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt16>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::UInt32)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt32>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::UInt64)
    {
       using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt64>>;
       return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::Int8)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Int8>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::Int16)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Int16>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::Int32)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Int32>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::Int64)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Int64>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::Float32)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Float32>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::Float64)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<Float64>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::Date)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt16>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }
    if (which_value.idx == TypeIndex::DateTime)
    {
        using Data = AggregateFunctionArgMinMaxData<SingleValueDataFixed<ResultType>, SingleValueDataFixed<UInt32>>;
        return new AggregateFunctionArgMinMax<Data, isMin>(argument_types, return_both);
    }

    return nullptr;
}

template <bool isMin>
IAggregateFunction * createWithTwoTypes(const DataTypes & argument_types, const bool return_both)
{
    const DataTypePtr & result_type = argument_types[0];
    WhichDataType which_result(result_type);

    if (which_result.idx == TypeIndex::UInt8)
        return createWithTwoTypesSecond<isMin, UInt8>(argument_types, return_both);
    if (which_result.idx == TypeIndex::UInt16)
        return createWithTwoTypesSecond<isMin, UInt16>(argument_types, return_both);
    if (which_result.idx == TypeIndex::UInt32)
        return createWithTwoTypesSecond<isMin, UInt32>(argument_types, return_both);
    if (which_result.idx == TypeIndex::UInt64)
        return createWithTwoTypesSecond<isMin, UInt64>(argument_types, return_both);
    if (which_result.idx == TypeIndex::Int8)
        return createWithTwoTypesSecond<isMin, Int8>(argument_types, return_both);
    if (which_result.idx == TypeIndex::Int16)
        return createWithTwoTypesSecond<isMin, Int16>(argument_types, return_both);
    if (which_result.idx == TypeIndex::Int32)
        return createWithTwoTypesSecond<isMin, Int32>(argument_types, return_both);
    if (which_result.idx == TypeIndex::Int64)
        return createWithTwoTypesSecond<isMin, Int64>(argument_types, return_both);
    if (which_result.idx == TypeIndex::Float32)
        return createWithTwoTypesSecond<isMin, Float32>(argument_types, return_both);
    if (which_result.idx == TypeIndex::Float64)
        return createWithTwoTypesSecond<isMin, Float64>(argument_types, return_both);

    return nullptr;
}


template <bool isMin>
AggregateFunctionPtr createAggregateFunctionArgMinMax(
    const std::string & name, const DataTypes & argument_types, const Array &, const Settings *, const bool return_both)
{
    assertBinary(name, argument_types);

    AggregateFunctionPtr result = AggregateFunctionPtr(createWithTwoTypes<isMin>(argument_types, return_both));

    if (!result)
    {
        const DataTypePtr & value_type = argument_types[1];
        WhichDataType which(value_type);
#define DISPATCH(TYPE) \
        if (which.idx == TypeIndex::TYPE) \
            return AggregateFunctionPtr(new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataFixed<TYPE>>, isMin>(argument_types, return_both)); /// NOLINT
        FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

        if (which.idx == TypeIndex::Date)
            return AggregateFunctionPtr(
                new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataFixed<DataTypeDate::FieldType>>, isMin>(
                    argument_types, return_both));
        if (which.idx == TypeIndex::DateTime)
            return AggregateFunctionPtr(new AggregateFunctionArgMinMax<
                                        AggregateFunctionArgMinMaxDataGeneric<SingleValueDataFixed<DataTypeDateTime::FieldType>>,
                                        isMin>(argument_types, return_both));
        if (which.idx == TypeIndex::String)
            return AggregateFunctionPtr(new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataString>, isMin>(
                argument_types, return_both));

        if (canUseFieldForValueData(value_type))
            return AggregateFunctionPtr(
                new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataGeneric>, isMin>(
                    argument_types, return_both));
        return AggregateFunctionPtr(
            new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxDataGeneric<SingleValueDataGenericWithColumn>, isMin>(
                argument_types, return_both));
    }
    return result;
}

}

void registerAggregateFunctionsArgMinArgMax(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    FunctionDocumentation::Description description_argMin = R"(
Calculates the `arg` value for a minimum `val` value. If there are multiple rows with equal `val` being the maximum, which of the associated `arg` is returned is not deterministic.
Both parts the `arg` and the `min` behave as [aggregate functions](/sql-reference/aggregate-functions/index.md), they both [skip `Null`](/sql-reference/aggregate-functions/index.md#null-processing) during processing and return not `Null` values if not `Null` values are available.

**See also**

- [Tuple](/sql-reference/data-types/tuple.md)
    )";
    FunctionDocumentation::Syntax syntax_argMin = R"(
argMin(arg, val)
    )";
    FunctionDocumentation::Parameters parameters_argMin = {};
    FunctionDocumentation::Arguments arguments_argMin = {
        {"arg", "Argument for which to find the maximum value.", {"const String"}},
        {"val", "The minimum value.", {"(U)Int8/16/32/64", "Float*", "Date", "DateTime", "Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_argMin = {"Returns the `arg` value that corresponds to minimum `val` value. Type matches `arg` type.", {}};
    FunctionDocumentation::Examples examples_argMin = {
    {
        "Basic usage",
        R"(
SELECT argMin(user, salary) FROM salary;
        )",
        R"(
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
        )"
    },
    {
        "Extended example with NULL handling",
        R"(
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES((NULL, 0), ('a', 1), ('b', 2), ('c', 2), (NULL, NULL), ('d', NULL));

SELECT argMin(a, b), min(b) FROM test;
        )",
        R"(
┌─argMin(a, b)─┬─min(b)─┐
│ a            │      0 │
└──────────────┴────────┘
        )"
    },
    {
        "Using Tuple in arguments",
        R"(
SELECT argMin(a, (b, a)), min(tuple(b, a)) FROM test;
        )",
        R"(
┌─argMin(a, tuple(b, a))─┬─min(tuple(b, a))─┐
│ d                      │ (NULL,NULL)      │
└────────────────────────┴──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_argMin = {1, 1};
    FunctionDocumentation::Category category_argMin = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_argMin = {description_argMin, syntax_argMin, arguments_argMin, parameters_argMin, returned_value_argMin, examples_argMin, introduced_in_argMin, category_argMin};

    factory.registerFunction(
        "argMin",
        {[](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
         { return createAggregateFunctionArgMinMax<true>(name, argument_types, params, settings, false); },
         documentation_argMin, properties});

    FunctionDocumentation::Description description_argMax = R"(
Calculates the `arg` value for a maximum `val` value. If there are multiple rows with equal `val` being the maximum, which of the associated `arg` is returned is not deterministic.
Both parts the `arg` and the `max` behave as [aggregate functions](/sql-reference/aggregate-functions/index.md), they both [skip `Null`](/sql-reference/aggregate-functions/index.md#null-processing) during processing and return not `Null` values if not `Null` values are available.

**See also**

- [Tuple](/sql-reference/data-types/tuple.md)
    )";
    FunctionDocumentation::Syntax syntax_argMax = R"(
argMax(arg, val)
    )";
    FunctionDocumentation::Parameters parameters_argMax = {};
    FunctionDocumentation::Arguments arguments_argMax = {
        {"arg", "Argument for which to find the maximum value.", {"const String"}},
        {"val", "The maximum value.", {"(U)Int8/16/32/64", "Float*", "Date", "DateTime", "Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_argMax = {"Returns the `arg` value that corresponds to maximum `val` value. Type matches `arg` type.", {}};
    FunctionDocumentation::Examples examples_argMax = {
    {
        "Basic usage",
        R"(
SELECT argMax(user, salary) FROM salary;
        )",
        R"(
┌─argMax(user, salary)─┐
│ director             │
└──────────────────────┘
        )"
    },
    {
        "Extended example with NULL handling",
        R"(
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES(('a', 1), ('b', 2), ('c', 2), (NULL, 3), (NULL, NULL), ('d', NULL));

SELECT argMax(a, b), max(b) FROM test;
        )",
        R"(
┌─argMax(a, b)─┬─max(b)─┐
│ b            │      3 │
└──────────────┴────────┘
        )"
    },
    {
        "Using Tuple in arguments",
        R"(
SELECT argMax(a, (b,a)) FROM test;
        )",
        R"(
┌─argMax(a, tuple(b, a))─┐
│ c                      │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_argMax = {1, 1};
    FunctionDocumentation::Category category_argMax = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_argMax = {description_argMax, syntax_argMax, arguments_argMax, parameters_argMax, returned_value_argMax, examples_argMax, introduced_in_argMax, category_argMax};

    factory.registerFunction(
        "argMax",
        {[](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
         { return createAggregateFunctionArgMinMax<false>(name, argument_types, params, settings, false); },
         documentation_argMax, properties});

    FunctionDocumentation::Description description_argAndMin = R"(
Calculates the `arg` and `val` value for a minimum `val` value.
If there are multiple rows with equal `val` being the minimum, which of the associated `arg` and `val` is returned is not deterministic.
Both parts the `arg` and the `min` behave as [aggregate functions](/sql-reference/aggregate-functions/index.md), they both [skip `Null`](/sql-reference/aggregate-functions/index.md#null-processing) during processing and return not `Null` values if not `Null` values are available.

:::note
The only difference with `argMin` is that `argAndMin` returns both argument and value.
:::

**See also**

- [argMin](/sql-reference/aggregate-functions/reference/argMin.md)
- [Tuple](/sql-reference/data-types/tuple.md)
    )";
    FunctionDocumentation::Syntax syntax_argAndMin = R"(
argAndMin(arg, val)
    )";
    FunctionDocumentation::Parameters parameters_argAndMin = {};
    FunctionDocumentation::Arguments arguments_argAndMin = {
        {"arg", "Argument for which to find the minimum value.", {"const String"}},
        {"val", "The minimum value.", {"(U)Int8/16/32/64", "Float*", "Date", "DateTime", "Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_argAndMin = {"Returns a tuple containing the `arg` value that corresponds to minimum `val` value and the minimum `val` value.", {"Tuple"}};
    FunctionDocumentation::Examples examples_argAndMin = {
    {
        "Basic usage",
        R"(
SELECT argAndMin(user, salary) FROM salary;
        )",
        R"(
┌─argAndMin(user, salary)─┐
│ ('worker',1000)         │
└─────────────────────────┘
        )"
    },
    {
        "Extended example with NULL handling",
        R"(
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES((NULL, 0), ('a', 1), ('b', 2), ('c', 2), (NULL, NULL), ('d', NULL));

SELECT argMin(a,b), argAndMin(a, b), min(b) FROM test;
        )",
        R"(
┌─argMin(a, b)─┬─argAndMin(a, b)─┬─min(b)─┐
│ a            │ ('a',1)         │      0 │
└──────────────┴─────────────────┴────────┘
        )"
    },
    {
        "Using Tuple in arguments",
        R"(
SELECT argAndMin(a, (b, a)), min(tuple(b, a)) FROM test;
        )",
        R"(
┌─argAndMin(a, (b, a))─┬─min((b, a))─┐
│ ('a',(1,'a'))        │ (0,NULL)    │
└──────────────────────┴─────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_argAndMin = {1, 1};
    FunctionDocumentation::Category category_argAndMin = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_argAndMin = {description_argAndMin, syntax_argAndMin, arguments_argAndMin, parameters_argAndMin, returned_value_argAndMin, examples_argAndMin, introduced_in_argAndMin, category_argAndMin};

    factory.registerFunction(
        "argAndMin",
        {[](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
         { return createAggregateFunctionArgMinMax<true>(name, argument_types, params, settings, true); },
         documentation_argAndMin, properties});

    FunctionDocumentation::Description description_argAndMax = R"(
Calculates the `arg` and `val` value for a maximum `val` value.
If there are multiple rows with equal `val` being the maximum, which of the associated `arg` and `val` is returned is not deterministic.
Both parts the `arg` and the `max` behave as [aggregate functions](/sql-reference/aggregate-functions/index.md), they both [skip `Null`](/sql-reference/aggregate-functions/index.md#null-processing) during processing and return not `Null` values if not `Null` values are available.

:::note
The only difference with `argMax` is that `argAndMax` returns both argument and value.
:::

**See also**

- [argMax](/sql-reference/aggregate-functions/reference/argMax.md)
- [Tuple](/sql-reference/data-types/tuple.md)
    )";
    FunctionDocumentation::Syntax syntax_argAndMax = R"(
argAndMax(arg, val)
    )";
    FunctionDocumentation::Parameters parameters_argAndMax = {};
    FunctionDocumentation::Arguments arguments_argAndMax = {
        {"arg", "Argument for which to find the maximum value.", {"const String"}},
        {"val", "The maximum value.", {"(U)Int8/16/32/64", "Float*", "Date", "DateTime", "Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_argAndMax = {"Returns a tuple containing the `arg` value that corresponds to maximum `val` value and the maximum `val` value.", {"Tuple"}};
    FunctionDocumentation::Examples examples_argAndMax = {
    {
        "Basic usage",
        R"(
SELECT argAndMax(user, salary) FROM salary;
        )",
        R"(
┌─argAndMax(user, salary)─┐
│ ('director',5000)       │
└─────────────────────────┘
        )"
    },
    {
        "Extended example with NULL handling",
        R"(
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES(('a', 1), ('b', 2), ('c', 2), (NULL, 3), (NULL, NULL), ('d', NULL));

SELECT argMax(a, b), argAndMax(a, b), max(b) FROM test;
        )",
        R"(
┌─argMax(a, b)─┬─argAndMax(a, b)─┬─max(b)─┐
│ b            │ ('b',2)         │      3 │
└──────────────┴─────────────────┴────────┘
        )"
    },
    {
        "Using Tuple in arguments",
        R"(
SELECT argAndMax(a, (b,a)) FROM test;
        )",
        R"(
┌─argAndMax(a, (b, a))─┐
│ ('c',(2,'c'))        │
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_argAndMax = {1, 1};
    FunctionDocumentation::Category category_argAndMax = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_argAndMax = {description_argAndMax, syntax_argAndMax, arguments_argAndMax, parameters_argAndMax, returned_value_argAndMax, examples_argAndMax, introduced_in_argAndMax, category_argAndMax};

    factory.registerFunction(
        "argAndMax",
        {[](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
         { return createAggregateFunctionArgMinMax<false>(name, argument_types, params, settings, true); },
         documentation_argAndMax, properties});
}

}
