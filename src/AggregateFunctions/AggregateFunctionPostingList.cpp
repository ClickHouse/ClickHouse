#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionPostingListData.h>
#include <algorithm>
#include <utility>
#include <Common/RadixSort.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>

#pragma clang optimize off
namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

class AggregateFunctionPostingList final : public IAggregateFunctionDataHelper<PostingListData, AggregateFunctionPostingList>
{
public:
    using DataType = PostingListData::PostingListDataType;
    explicit AggregateFunctionPostingList(const DataTypePtr & data_type_)
        : IAggregateFunctionDataHelper<PostingListData, AggregateFunctionPostingList>({data_type_}, {}, std::make_shared<DataTypeArray>(data_type_))
        , serialization(data_type_->getDefaultSerialization())
    {
    }

    String getName() const override { return PostingListData::name(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto row_value = assert_cast<const ColumnVector<DataType> &>(*columns[0]).getData()[row_num];
        this->data(place).add(std::move(row_value), arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & rhs_data = this->data(rhs);
        this->data(place).merge(rhs_data, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        this->data(place).serialize(buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        this->data(place).deserialize(buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    SerializationPtr serialization;
};

AggregateFunctionPtr createAggregateFunctionPostingList(
    const std::string & name, const DataTypes & argument_types, const Array &, const Settings *)
{
    assertUnary(name, argument_types);
    WhichDataType which(argument_types[0]);
    if (which.idx == TypeIndex::UInt32 || which.idx == TypeIndex::UInt64)
        return std::make_shared<AggregateFunctionPostingList>(argument_types[0]);

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);
}

}

void registerAggregateFunctionsTextSearch(AggregateFunctionFactory & factory)
{
    factory.registerFunction("postingList", { createAggregateFunctionPostingList });
}

}
