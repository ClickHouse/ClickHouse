#include <Storages/Statistics/UniqStatistics.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_STATISTICS;
}

UniqStatistics::UniqStatistics(const SingleStatisticsDescription & stat_, const DataTypePtr & data_type)
    : IStatistics(stat_)
{
    arena = std::make_unique<Arena>();
    AggregateFunctionProperties properties;
    collector = AggregateFunctionFactory::instance().get("uniq", NullsAction::IGNORE_NULLS, {data_type}, Array(), properties);
    data = arena->alignedAlloc(collector->sizeOfData(), collector->alignOfData());
    collector->create(data);
}

UniqStatistics::~UniqStatistics()
{
    collector->destroy(data);
}

UInt64 UniqStatistics::getCardinality()
{
    auto column = DataTypeUInt64().createColumn();
    collector->insertResultInto(data, *column, nullptr);
    return column->getUInt(0);
}

void UniqStatistics::serialize(WriteBuffer & buf)
{
    collector->serialize(data, buf);
}

void UniqStatistics::deserialize(ReadBuffer & buf)
{
    collector->deserialize(data, buf);
}

void UniqStatistics::update(const ColumnPtr & column)
{
    /// TODO(hanfei): For low cardinality, it's very slow to convert to full column. We can read the dictionary directly.
    /// Here we intend to avoid crash in CI.
    auto col_ptr = column->convertToFullColumnIfLowCardinality();
    const IColumn * raw_ptr = col_ptr.get();
    collector->addBatchSinglePlace(0, column->size(), data, &(raw_ptr), nullptr);
}

void UniqValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'uniq' does not support type {}", data_type->getName());
}

StatisticsPtr UniqCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type)
{
    return std::make_shared<UniqStatistics>(stat, data_type);
}

}
