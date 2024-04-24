#include <Storages/Statistics/UniqStatistics.h>
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
    AggregateFunctionProperties property;
    property.returns_default_when_only_null = true;
    uniq_collector = AggregateFunctionFactory::instance().get("uniq", NullsAction::IGNORE_NULLS, {data_type}, Array(), property);
    data = arena->alignedAlloc(uniq_collector->sizeOfData(), uniq_collector->alignOfData());
    uniq_collector->create(data);
}

UniqStatistics::~UniqStatistics()
{
    uniq_collector->destroy(data);
}

UInt64 UniqStatistics::getCardinality()
{
    auto column = DataTypeUInt64().createColumn();
    uniq_collector->insertResultInto(data, *column, nullptr);
    return column->getUInt(0);
}

void UniqStatistics::serialize(WriteBuffer & buf)
{
    uniq_collector->serialize(data, buf);
}

void UniqStatistics::deserialize(ReadBuffer & buf)
{
    uniq_collector->deserialize(data, buf);
}

void UniqStatistics::update(const ColumnPtr & column)
{
    const IColumn * col_ptr = column.get();
    uniq_collector->addBatchSinglePlace(0, column->size(), data, &col_ptr, nullptr);
}

void UniqValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type Uniq does not support type {}", data_type->getName());
}

StatisticsPtr UniqCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type)
{
    return std::make_shared<UniqStatistics>(stat, data_type);
}

}
