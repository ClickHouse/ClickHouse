#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/SingleValueData.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/** Implement 'heavy hitters' algorithm.
  * Selects most frequent value if its frequency is more than 50% in each thread of execution.
  * Otherwise, selects some arbitrary value.
  * http://www.cs.umd.edu/~samir/498/karp.pdf
  */
struct AggregateFunctionAnyHeavyData
{
    using Self = AggregateFunctionAnyHeavyData;

private:
    SingleValueDataBaseMemoryBlock v_data;
    UInt64 counter = 0;

public:
    [[noreturn]] explicit AggregateFunctionAnyHeavyData()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionAnyHeavyData initialized empty");
    }

    explicit AggregateFunctionAnyHeavyData(const DataTypePtr & value_type) { generateSingleValueFromType(value_type, v_data); }

    ~AggregateFunctionAnyHeavyData() { data().~SingleValueDataBase(); }

    SingleValueDataBase & data() { return v_data.get(); }
    const SingleValueDataBase & data() const { return v_data.get(); }

    void add(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (data().isEqualTo(column, row_num))
        {
            ++counter;
        }
        else if (counter == 0)
        {
            data().set(column, row_num, arena);
            ++counter;
        }
        else
        {
            --counter;
        }
    }

    void add(const Self & to, Arena * arena)
    {
        if (!to.data().has())
            return;

        if (data().isEqualTo(to.data()))
            counter += to.counter;
        else if (!data().has() || counter < to.counter)
        {
            data().set(to.data(), arena);
            counter = to.counter - counter;
        }
        else
            counter -= to.counter;
    }

    void addManyDefaults(const IColumn & column, size_t length, Arena * arena)
    {
        for (size_t i = 0; i < length; ++i)
            add(column, 0, arena);
    }

    void write(WriteBuffer & buf, const ISerialization & serialization) const
    {
        data().write(buf, serialization);
        writeBinaryLittleEndian(counter, buf);
    }

    void read(ReadBuffer & buf, const ISerialization & serialization, const DataTypePtr & type, Arena * arena)
    {
        data().read(buf, serialization, type, arena);
        readBinaryLittleEndian(counter, buf);
    }

    void insertResultInto(IColumn & to, const DataTypePtr & type) const { data().insertResultInto(to, type); }
};


class AggregateFunctionAnyHeavy final : public IAggregateFunctionDataHelper<AggregateFunctionAnyHeavyData, AggregateFunctionAnyHeavy>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionAnyHeavy(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<AggregateFunctionAnyHeavyData, AggregateFunctionAnyHeavy>({type}, {}, type)
        , serialization(type->getDefaultSerialization())
    {
    }

    void create(AggregateDataPtr __restrict place) const override { new (place) AggregateFunctionAnyHeavyData(result_type); }

    String getName() const override { return "anyHeavy"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        data(place).add(*columns[0], row_num, arena);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        data(place).addManyDefaults(*columns[0], 0, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).add(data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        data(place).read(buf, *serialization, result_type, arena);
    }

    bool allocatesMemoryInArena() const override { return singleValueTypeAllocatesMemoryInArena(result_type->getTypeId()); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        data(place).insertResultInto(to, result_type);
    }
};


AggregateFunctionPtr
createAggregateFunctionAnyHeavy(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    return AggregateFunctionPtr(new AggregateFunctionAnyHeavy(res_type));
}

}

void registerAggregateFunctionAnyHeavy(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description_anyHeavy = R"(
Selects a frequently occurring value using the [heavy hitters](https://doi.org/10.1145/762471.762473) algorithm.
If there is a value that occurs more than in half the cases in each of the query's execution threads, this value is returned.
Normally, the result is nondeterministic.
    )";
    FunctionDocumentation::Syntax syntax_anyHeavy = R"(
anyHeavy(column)
    )";
    FunctionDocumentation::Arguments arguments_anyHeavy = {
        {"column", "The column name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_anyHeavy = {"Returns a frequently occurring value. The result is nondeterministic.", {"Any"}};
    FunctionDocumentation::Examples examples_anyHeavy = {
    {
        "Usage example",
        R"(
SELECT anyHeavy(AirlineID) AS res
FROM ontime;
        )",
        R"(
┌───res─┐
│ 19690 │
└───────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_anyHeavy = {1, 1};
    FunctionDocumentation::Category category_anyHeavy = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_anyHeavy = {description_anyHeavy, syntax_anyHeavy, arguments_anyHeavy, {}, returned_value_anyHeavy, examples_anyHeavy, introduced_in_anyHeavy, category_anyHeavy};

    AggregateFunctionProperties default_properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("anyHeavy", {createAggregateFunctionAnyHeavy, documentation_anyHeavy, default_properties});
}

}
