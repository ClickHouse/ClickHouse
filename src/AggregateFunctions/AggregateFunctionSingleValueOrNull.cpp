#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{
struct Settings;


namespace
{
/** The aggregate function 'singleValueOrNull' is used to implement subquery operators,
  * such as x = ALL (SELECT ...)
  * It checks if there is only one unique non-NULL value in the data.
  * If there is only one unique value - returns it.
  * If there are zero or at least two distinct values - returns NULL.
  */

template <typename Data>
struct AggregateFunctionSingleValueOrNullData
{
    using Self = AggregateFunctionSingleValueOrNullData;
    using Data_t = Data;

    Data data;
    bool first_value = true;
    bool is_null = false;

    void add(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (first_value)
        {
            first_value = false;
            data.set(column, row_num, arena);
        }
        else if (!data.isEqualTo(column, row_num))
        {
            is_null = true;
        }
    }

    void add(const Self & to, Arena * arena)
    {
        if (!to.data.has())
            return;

        if (first_value && !to.first_value)
        {
            first_value = false;
            data.set(to.data, arena);
        }
        else if (!data.isEqualTo(to.data))
        {
            is_null = true;
        }
    }

    /// TODO: Methods write and read lose data (first_value and is_null)
    /// Fixing it requires a breaking change (but it's probably necessary)
    void write(WriteBuffer & buf, const ISerialization & serialization) const { data.write(buf, serialization); }

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena * arena) { data.read(buf, serialization, arena); }

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
            data.insertResultInto(col.getNestedColumn());
        }
    }
};


template <typename Data>
class AggregateFunctionSingleValueOrNull final : public IAggregateFunctionDataHelper<Data, AggregateFunctionSingleValueOrNull<Data>>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionSingleValueOrNull(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSingleValueOrNull<Data>>({type}, {}, makeNullable(type))
        , serialization(type->getDefaultSerialization())
    {
    }

    String getName() const override { return "singleValueOrNull"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).add(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (this->data(place).is_null)
            return;
        IAggregateFunctionDataHelper<Data, AggregateFunctionSingleValueOrNull<Data>>::addBatchSinglePlace(
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
        if (this->data(place).is_null)
            return;
        IAggregateFunctionDataHelper<Data, AggregateFunctionSingleValueOrNull<Data>>::addBatchSinglePlaceNotNull(
            row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        this->data(place).add(*columns[0], 0, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).add(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::Data_t::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

AggregateFunctionPtr createAggregateFunctionSingleValueOrNull(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValueComposite<AggregateFunctionSingleValueOrNull, AggregateFunctionSingleValueOrNullData>(
            name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionSingleValueOrNull(AggregateFunctionFactory & factory)
{
    factory.registerFunction("singleValueOrNull", createAggregateFunctionSingleValueOrNull);
}
}
