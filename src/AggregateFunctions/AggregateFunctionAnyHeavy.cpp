#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/SingleValueData.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/defines.h>


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

    explicit AggregateFunctionAnyHeavyData(TypeIndex value_type) { generateSingleValueFromTypeIndex(value_type, v_data); }

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

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena * arena)
    {
        data().read(buf, serialization, arena);
        readBinaryLittleEndian(counter, buf);
    }

    void insertResultInto(IColumn & to) const { data().insertResultInto(to); }
};


class AggregateFunctionAnyHeavy final : public IAggregateFunctionDataHelper<AggregateFunctionAnyHeavyData, AggregateFunctionAnyHeavy>
{
private:
    SerializationPtr serialization;
    const TypeIndex value_type_index;

public:
    explicit AggregateFunctionAnyHeavy(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<AggregateFunctionAnyHeavyData, AggregateFunctionAnyHeavy>({type}, {}, type)
        , serialization(type->getDefaultSerialization())
        , value_type_index(WhichDataType(type).idx)
    {
    }

    void create(AggregateDataPtr __restrict place) const override { new (place) AggregateFunctionAnyHeavyData(value_type_index); }

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
        data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return singleValueTypeAllocatesMemoryInArena(value_type_index); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        data(place).insertResultInto(to);
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
    AggregateFunctionProperties default_properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("anyHeavy", {createAggregateFunctionAnyHeavy, default_properties});
}

}
