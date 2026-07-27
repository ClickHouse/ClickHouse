#include <Storages/Statistics/StatisticsUniq.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

StatisticsUniq::StatisticsUniq(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
    : IStatistics(description)
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

void StatisticsUniq::build(const ColumnPtr & column)
{
    const IColumn * raw_column_ptr = nullptr;

    /// For sparse and low cardinality columns an extra default
    /// value may be added. That is ok since the uniq count is an estimation.
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(column.get()))
    {
        raw_column_ptr = &column_sparse->getValuesColumn();
    }
    else if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
    {
        raw_column_ptr = column_low_cardinality->getDictionary().getNestedColumn().get();
    }
    else
    {
        raw_column_ptr = column.get();
    }

    collector->addBatchSinglePlace(0, raw_column_ptr->size(), data, &(raw_column_ptr), nullptr);
}

void StatisticsUniq::merge(const StatisticsPtr & other_stats)
{
    const StatisticsUniq * other = typeid_cast<const StatisticsUniq *>(other_stats.get());
    collector->merge(data, other->data, arena.get());
}

void StatisticsUniq::serialize(WriteBuffer & buf)
{
    if (collector->getNestedFunction())
        writeBinary(true, buf);
    else
        writeBinary(false, buf);
    collector->serialize(data, buf);
}

void StatisticsUniq::deserialize(ReadBuffer & buf)
{
    bool is_null;
    readBinary(is_null, buf);
    auto nested_func = collector->getNestedFunction();
    /// when serialize is nullable, but we removed the nullable
    if (is_null && !nested_func)
    {
        bool serialize_flag;
        readBinary(serialize_flag, buf);
        if (!serialize_flag)
            return;
    }

    /// when serialize is not nullable, but we changed it to nullable
    if (!is_null && nested_func)
    {
        nested_func->deserialize(data, buf);
        return;
    }

    collector->deserialize(data, buf);
}

UInt64 StatisticsUniq::estimateCardinality() const
{
    auto column = DataTypeUInt64().createColumn();
    collector->insertResultInto(data, *column, nullptr);
    return column->getUInt(0);
}

bool uniqStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    DataTypePtr inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    return inner_data_type->isValueRepresentedByNumber() || isStringOrFixedString(inner_data_type);
}

StatisticsPtr uniqStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsUniq>(description, data_type);
}

}
