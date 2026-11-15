#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/SingleValueData.h>
#include <DataTypes/DataTypeFixedString.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename Data>
class AggregateFunctionAny final : public IAggregateFunctionDataHelper<Data, AggregateFunctionAny<Data>>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionAny(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionAny<Data>>(argument_types_, {}, argument_types_[0])
    {
        if constexpr (!std::is_same_v<Data, SingleValueReference>)
            serialization = this->result_type->getDefaultSerialization();
    }

    using IAggregateFunction::argument_types;

    AggregateFunctionPtr getAggregateFunctionForMergingFinal() const override
    {
        /// For types that are potentially large, we use SingleValueReference to avoid unnecessary memory allocations during merging final
        /// Large types are: String, FixedString (N > 20), Array, Map, Object, Variant, Dynamic
        const auto which = WhichDataType(argument_types[0]);
        auto * type_fixed_string = typeid_cast<const DataTypeFixedString *>(argument_types[0].get());
        if (which.isString() || which.isArray() || which.isMap() || which.isObject() || which.isVariant()
            || which.isDynamic() || (type_fixed_string && type_fixed_string->getN() > 20))
        {
            return std::make_shared<AggregateFunctionAny<SingleValueReference>>(argument_types);
        }

        return IAggregateFunction::getAggregateFunctionForMergingFinal();
    }

    String getName() const override { return "any"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (!this->data(place).has())
            this->data(place).set(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (this->data(place).has() || row_begin >= row_end)
            return;

        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (if_map.data()[i] != 0)
                {
                    this->data(place).set(*columns[0], i, arena);
                    return;
                }
            }
        }
        else
        {
            this->data(place).set(*columns[0], row_begin, arena);
        }
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
        if (this->data(place).has() || row_begin >= row_end)
            return;

        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (if_map.data()[i] != 0 && null_map[i] == 0)
                {
                    this->data(place).set(*columns[0], i, arena);
                    return;
                }
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (null_map[i] == 0)
                {
                    this->data(place).set(*columns[0], i, arena);
                    return;
                }
            }
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        if (!this->data(place).has())
            this->data(place).set(*columns[0], 0, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (!this->data(place).has())
            this->data(place).set(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, this->result_type, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to, this->result_type);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilable() const override
    {
        if constexpr (!Data::is_compilable)
            return false;
        else
            return Data::isCompilable(*this->argument_types[0]);
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (Data::is_compilable)
            Data::compileCreate(builder, aggregate_data_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        if constexpr (Data::is_compilable)
            Data::compileAny(builder, aggregate_data_ptr, arguments[0].value);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        if constexpr (Data::is_compilable)
            Data::compileAnyMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (Data::is_compilable)
            return Data::compileGetResult(builder, aggregate_data_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }
#endif
};

AggregateFunctionPtr
createAggregateFunctionAny(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionAny, /* unary */ true>(name, argument_types, parameters, settings));
}


template <typename Data>
class AggregateFunctionAnyLast final : public IAggregateFunctionDataHelper<Data, AggregateFunctionAnyLast<Data>>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionAnyLast(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionAnyLast<Data>>(argument_types_, {}, argument_types_[0])
    {
        if constexpr (!std::is_same_v<Data, SingleValueReference>)
            serialization = this->result_type->getDefaultSerialization();
    }

    using IAggregateFunction::argument_types;

    AggregateFunctionPtr getAggregateFunctionForMergingFinal() const override
    {
        /// For types that are potentially large, we use SingleValueReference to avoid unnecessary memory allocations during merging final
        /// Large types are: String, FixedString (N >= 20), Array, Map, Object, Variant, Dynamic
        const auto which = WhichDataType(argument_types[0]);
        auto * type_fixed_string = typeid_cast<const DataTypeFixedString *>(argument_types[0].get());
        if (which.isString() || which.isArray() || which.isMap() || which.isObject() || which.isVariant()
            || which.isDynamic() || (type_fixed_string && type_fixed_string->getN() >= 20))
        {
            return std::make_shared<AggregateFunctionAnyLast<SingleValueReference>>(argument_types);
        }

        return IAggregateFunction::getAggregateFunctionForMergingFinal();
    }

    String getName() const override { return "anyLast"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).set(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (row_begin >= row_end)
            return;

        size_t batch_size = row_end - row_begin;
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; i++)
            {
                size_t pos = (row_end - 1) - i;
                if (if_map.data()[pos] != 0)
                {
                    this->data(place).set(*columns[0], pos, arena);
                    return;
                }
            }
        }
        else
        {
            this->data(place).set(*columns[0], row_end - 1, arena);
        }
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
        if (row_begin >= row_end)
            return;

        size_t batch_size = row_end - row_begin;
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; i++)
            {
                size_t pos = (row_end - 1) - i;
                if (if_map.data()[pos] != 0 && null_map[pos] == 0)
                {
                    this->data(place).set(*columns[0], pos, arena);
                    return;
                }
            }
        }
        else
        {
            for (size_t i = 0; i < batch_size; i++)
            {
                size_t pos = (row_end - 1) - i;
                if (null_map[pos] == 0)
                {
                    this->data(place).set(*columns[0], pos, arena);
                    return;
                }
            }
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        this->data(place).set(*columns[0], 0, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).set(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, this->result_type, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to, this->result_type);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilable() const override
    {
        if constexpr (!Data::is_compilable)
            return false;
        else
            return Data::isCompilable(*this->argument_types[0]);
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (Data::is_compilable)
            Data::compileCreate(builder, aggregate_data_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        if constexpr (Data::is_compilable)
            Data::compileAnyLast(builder, aggregate_data_ptr, arguments[0].value);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        if constexpr (Data::is_compilable)
            Data::compileAnyLastMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (Data::is_compilable)
            return Data::compileGetResult(builder, aggregate_data_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }
#endif
};

AggregateFunctionPtr createAggregateFunctionAnyLast(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionAnyLast, /* unary */ true>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionsAny(AggregateFunctionFactory & factory)
{
    /// any documentation
    FunctionDocumentation::Description description = R"(
Selects the first encountered value of a column.

:::warning
As a query can be executed in arbitrary order, the result of this function is non-deterministic. If you need an arbitrary but deterministic result, use functions min or max.
:::

By default, the function never returns NULL, i.e. ignores NULL values in the input column.
However, if the function is used with the `RESPECT NULLS` modifier, it returns the first value reads no matter if NULL or not.

**Implementation details**

In some cases, you can rely on the order of execution.
This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY`.

When a `SELECT` query has the `GROUP BY` clause or at least one aggregate function, ClickHouse (in contrast to MySQL) requires that all expressions in the `SELECT`, `HAVING`, and `ORDER BY` clauses be calculated from keys or from aggregate functions.
In other words, each column selected from the table must be used either in keys or inside aggregate functions.
To get behavior like in MySQL, you can put the other columns in the `any` aggregate function.

:::note
The return type of the function is the same as the input, except for LowCardinality which is discarded.
This means that given no rows as input it will return the default value of that type (0 for integers, or Null for a Nullable() column).
You might use the -OrNull combinator to modify this behaviour.
:::
    )";
    FunctionDocumentation::Syntax syntax = "any(column)[ RESPECT NULLS]";
    FunctionDocumentation::Arguments arguments = {
        {"column", "The column name.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the first value encountered.
    )",
    {"Any"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE tab (city Nullable(String)) ENGINE=Memory;
INSERT INTO tab (city) VALUES (NULL), ('Amsterdam'), ('New York'), ('Tokyo'), ('Valencia'), (NULL);
SELECT any(city), anyRespectNulls(city) FROM tab;
        )",
        R"(
┌─any(city)─┬─anyRespectNulls(city)─┐
│ Amsterdam │ ᴺᵁᴸᴸ                  │
└───────────┴───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties default_properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("any", {createAggregateFunctionAny, documentation, default_properties});
    factory.registerAlias("any_value", "any", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("first_value", "any", AggregateFunctionFactory::Case::Insensitive);

    /// anyLast documentation
    FunctionDocumentation::Description anyLast_description = R"(
Selects the last encountered value of a column.

:::warning
As a query can be executed in arbitrary order, the result of this function is non-deterministic.
If you need an arbitrary but deterministic result, use functions [min](/sql-reference/aggregate-functions/reference/min) or [max](/sql-reference/aggregate-functions/reference/max).
:::

By default, the function never returns NULL, i.e. ignores NULL values in the input column.
However, if the function is used with the `RESPECT NULLS` modifier, it returns the last value reads no matter if NULL or not.
    )";
    FunctionDocumentation::Syntax anyLast_syntax = "anyLast(column) [RESPECT NULLS]";
    FunctionDocumentation::Arguments anyLast_arguments = {
        {"column", "The column name.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue anyLast_returned_value = {"Returns the last value encountered.", {"Any"}};
    FunctionDocumentation::Examples anyLast_examples = {
    {
        "Usage example",
        R"(
CREATE TABLE tab(city Nullable(String)) ENGINE=Memory;
INSERT INTO tab (city) VALUES ('Amsterdam'), (NULL), ('New York'), ('Tokyo'), ('Valencia'), (NULL);
SELECT anyLast(city), anyLastRespectNulls(city) FROM tab;
        )",
        R"(
┌─anyLast(city)─┬─anyLastRespectNulls(city)─┐
│ Valencia      │ ᴺᵁᴸᴸ                      │
└───────────────┴───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn anyLast_introduced_in = {1, 1};
    FunctionDocumentation::Category anyLast_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation anyLast_documentation = {anyLast_description, anyLast_syntax, anyLast_arguments, {}, anyLast_returned_value, anyLast_examples, anyLast_introduced_in, anyLast_category};

    factory.registerFunction("anyLast", {createAggregateFunctionAnyLast, anyLast_documentation, default_properties}, AggregateFunctionFactory::Case::Sensitive);
    factory.registerAlias("last_value", "anyLast", AggregateFunctionFactory::Case::Insensitive);
}
}
