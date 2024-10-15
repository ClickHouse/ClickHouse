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
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

namespace
{
struct AggregateFunctionAnyRespectNullsData
{
    enum class Status : UInt8
    {
        NotSet = 1,
        SetNull = 2,
        SetOther = 3
    };

    Status status = Status::NotSet;
    Field value;

    bool isSet() const { return status != Status::NotSet; }
    void setNull() { status = Status::SetNull; }
    void setOther() { status = Status::SetOther; }
};

template <bool First>
class AggregateFunctionAnyRespectNulls final
    : public IAggregateFunctionDataHelper<AggregateFunctionAnyRespectNullsData, AggregateFunctionAnyRespectNulls<First>>
{
public:
    using Data = AggregateFunctionAnyRespectNullsData;

    SerializationPtr serialization;
    const bool returns_nullable_type = false;

    explicit AggregateFunctionAnyRespectNulls(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionAnyRespectNulls<First>>({type}, {}, type)
        , serialization(type->getDefaultSerialization())
        , returns_nullable_type(type->isNullable())
    {
    }

    String getName() const override
    {
        if constexpr (First)
            return "any_respect_nulls";
        else
            return "anyLast_respect_nulls";
    }

    bool allocatesMemoryInArena() const override { return false; }

    void addNull(AggregateDataPtr __restrict place) const
    {
        chassert(returns_nullable_type);
        auto & d = this->data(place);
        if (First && d.isSet())
            return;
        d.setNull();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if (columns[0]->isNullable())
        {
            if (columns[0]->isNullAt(row_num))
                return addNull(place);
        }
        auto & d = this->data(place);
        if (First && d.isSet())
            return;
        d.setOther();
        columns[0]->get(row_num, d.value);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        if (columns[0]->isNullable())
            addNull(place);
        else
            add(place, columns, 0, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos)
        const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            size_t size = row_end - row_begin;
            for (size_t i = 0; i < size; ++i)
            {
                size_t pos = First ? row_begin + i : row_end - 1 - i;
                if (flags[pos])
                {
                    add(place, columns, pos, arena);
                    break;
                }
            }
        }
        else if (row_begin < row_end)
        {
            size_t pos = First ? row_begin : row_end - 1;
            add(place, columns, pos, arena);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t, size_t, AggregateDataPtr __restrict, const IColumn **, const UInt8 *, Arena *, ssize_t) const override
    {
        /// This should not happen since it means somebody else has preprocessed the data (NULLs or IFs) and might
        /// have discarded values that we need (NULLs)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionAnyRespectNulls::addBatchSinglePlaceNotNull called");
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & d = this->data(place);
        if (First && d.isSet())
            return;

        auto & other = this->data(rhs);
        if (other.isSet())
        {
            d.status = other.status;
            d.value = other.value;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & d = this->data(place);
        UInt8 k = static_cast<UInt8>(d.status);

        writeBinaryLittleEndian<UInt8>(k, buf);
        if (d.status == Data::Status::SetOther)
            serialization->serializeBinary(d.value, buf, {});
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & d = this->data(place);
        UInt8 k = 0;
        readBinaryLittleEndian<UInt8>(k, buf);
        d.status = static_cast<Data::Status>(k);
        if (d.status == Data::Status::NotSet)
            return;
        if (d.status == Data::Status::SetNull)
        {
            if (!returns_nullable_type)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect type (NULL) in non-nullable {}State", getName());
            return;
        }
        if (d.status == Data::Status::SetOther)
        {
            serialization->deserializeBinary(d.value, buf, {});
            return;
        }
        throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect type ({}) in {}State", static_cast<Int8>(k), getName());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & d = this->data(place);
        if (d.status == Data::Status::SetOther)
            to.insert(d.value);
        else
            to.insertDefault();
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & original_function,
        const DataTypes & /*arguments*/,
        const Array & /*params*/,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        return original_function;
    }
};


template <bool First>
IAggregateFunction * createAggregateFunctionSingleValueRespectNulls(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    return new AggregateFunctionAnyRespectNulls<First>(argument_types[0]);
}

AggregateFunctionPtr createAggregateFunctionAnyRespectNulls(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValueRespectNulls<true>(name, argument_types, parameters, settings));
}

AggregateFunctionPtr createAggregateFunctionAnyLastRespectNulls(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValueRespectNulls<false>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionsAnyRespectNulls(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties default_properties_for_respect_nulls
        = {.returns_default_when_only_null = false, .is_order_dependent = true, .is_window_function = true};

    factory.registerFunction("any_respect_nulls", {createAggregateFunctionAnyRespectNulls, default_properties_for_respect_nulls});
    factory.registerAlias("any_value_respect_nulls", "any_respect_nulls", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("first_value_respect_nulls", "any_respect_nulls", AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("anyLast_respect_nulls", {createAggregateFunctionAnyLastRespectNulls, default_properties_for_respect_nulls});
    factory.registerAlias("last_value_respect_nulls", "anyLast_respect_nulls", AggregateFunctionFactory::Case::Insensitive);

    /// Must happen after registering any and anyLast
    factory.registerNullsActionTransformation("any", "any_respect_nulls");
    factory.registerNullsActionTransformation("anyLast", "anyLast_respect_nulls");
}

}
