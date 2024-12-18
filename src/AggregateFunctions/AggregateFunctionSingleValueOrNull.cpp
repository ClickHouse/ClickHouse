#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/SingleValueData.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{
/** The aggregate function 'singleValueOrNull' is used to implement subquery operators,
  * such as x = ALL (SELECT ...)
  * It checks if there is only one unique non-NULL value in the data.
  * If there is only one unique value - returns it.
  * If there are zero or at least two distinct values - returns NULL.
  */

struct AggregateFunctionSingleValueOrNullData
{
    using Self = AggregateFunctionSingleValueOrNullData;

private:
    SingleValueDataBaseMemoryBlock v_data;
    bool first_value = true;
    bool is_null = false;

public:
    [[noreturn]] explicit AggregateFunctionSingleValueOrNullData()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionSingleValueOrNullData initialized empty");
    }

    explicit AggregateFunctionSingleValueOrNullData(TypeIndex value_type) { generateSingleValueFromTypeIndex(value_type, v_data); }

    ~AggregateFunctionSingleValueOrNullData() { data().~SingleValueDataBase(); }

    SingleValueDataBase & data() { return v_data.get(); }
    const SingleValueDataBase & data() const { return v_data.get(); }

    bool isNull() const { return is_null; }

    void add(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (first_value)
        {
            first_value = false;
            data().set(column, row_num, arena);
        }
        else if (!data().isEqualTo(column, row_num))
        {
            is_null = true;
        }
    }

    void add(const Self & to, Arena * arena)
    {
        if (!to.data().has())
            return;

        if (first_value && !to.first_value)
        {
            first_value = false;
            data().set(to.data(), arena);
        }
        else if (!data().isEqualTo(to.data()))
        {
            is_null = true;
        }
    }

    /// TODO: Methods write and read lose data (first_value and is_null)
    /// Fixing it requires a breaking change (but it's probably necessary)
    void write(WriteBuffer & buf, const ISerialization & serialization) const { data().write(buf, serialization); }

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena * arena) { data().read(buf, serialization, arena); }

    void insertResultInto(IColumn & to) const
    {
        if (is_null || first_value)
        {
            to.insertDefault();
        }
        else
        {
            ColumnNullable & col = typeid_cast<ColumnNullable &>(to);
            col.getNullMapColumn().insertDefault();
            data().insertResultInto(col.getNestedColumn());
        }
    }
};


class AggregateFunctionSingleValueOrNull final
    : public IAggregateFunctionDataHelper<AggregateFunctionSingleValueOrNullData, AggregateFunctionSingleValueOrNull>
{
private:
    SerializationPtr serialization;
    const TypeIndex value_type_index;

public:
    explicit AggregateFunctionSingleValueOrNull(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<AggregateFunctionSingleValueOrNullData, AggregateFunctionSingleValueOrNull>(
            {type}, {}, makeNullable(type))
        , serialization(type->getDefaultSerialization())
        , value_type_index(WhichDataType(type).idx)
    {
    }

    void create(AggregateDataPtr __restrict place) const override { new (place) AggregateFunctionSingleValueOrNullData(value_type_index); }

    String getName() const override { return "singleValueOrNull"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        data(place).add(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (data(place).isNull())
            return;
        IAggregateFunctionDataHelper<Data, AggregateFunctionSingleValueOrNull>::addBatchSinglePlace(
            row_begin, row_end, place, columns, arena, if_argument_pos);
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
        if (data(place).isNull())
            return;
        IAggregateFunctionDataHelper<Data, AggregateFunctionSingleValueOrNull>::addBatchSinglePlaceNotNull(
            row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        data(place).add(*columns[0], 0, arena);
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

AggregateFunctionPtr createAggregateFunctionSingleValueOrNull(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    return AggregateFunctionPtr(new AggregateFunctionSingleValueOrNull(res_type));
}

}

void registerAggregateFunctionSingleValueOrNull(AggregateFunctionFactory & factory)
{
    factory.registerFunction("singleValueOrNull", createAggregateFunctionSingleValueOrNull);
}
}
