#include <Storages/Statistics/StatisticsUniq.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_STATISTICS;
}

StatisticsUniq::StatisticsUniq(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
    : ISingleStatistics(description)
{
    arena = std::make_unique<Arena>();
    AggregateFunctionProperties properties;
    collector = AggregateFunctionFactory::instance().get("uniq", NullsAction::IGNORE_NULLS, {data_type}, Array(), properties);
    data = arena->alignedAlloc(collector->sizeOfData(), collector->alignOfData());
    collector->create(data);
}

StatisticsUniq::~StatisticsUniq()
{
    collector->destroy(data);
}

void StatisticsUniq::update(const ColumnPtr & column)
{
    /// TODO(hanfei): For low cardinality, it's very slow to convert to full column. We can read the dictionary directly.
    /// Here we intend to avoid crash in CI.
    auto col_ptr = column->convertToFullColumnIfLowCardinality();
    const IColumn * raw_ptr = col_ptr.get();
    collector->addBatchSinglePlace(0, column->size(), data, &(raw_ptr), nullptr);
}

void StatisticsUniq::merge(const SingleStatisticsPtr & other)
{
    if (const auto * other_stat = dynamic_cast<const StatisticsUniq *>(other.get()))
        collector->merge(other_stat->data, data, arena.get());
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to merge statistics of type {} to Uniq statistics", toString(other->getTypeName()));
}

void StatisticsUniq::serialize(WriteBuffer & buf)
{
    collector->serialize(data, buf);
}

void StatisticsUniq::deserialize(ReadBuffer & buf)
{
    collector->deserialize(data, buf);
}

UInt64 StatisticsUniq::estimateCardinality() const
{
    auto column = DataTypeUInt64().createColumn();
    collector->insertResultInto(data, *column, nullptr);
    return column->getUInt(0);
}

void uniqStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    DataTypePtr inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    if (!inner_data_type->isValueRepresentedByNumber() && !isStringOrFixedString(inner_data_type))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'uniq' do not support type {}", data_type->getName());
}

SingleStatisticsPtr uniqStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsUniq>(description, data_type);
}

}
