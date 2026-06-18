#include <Storages/Statistics/StatisticsUniqCombined.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

/// Use uniqCombined with K=12: Small(16 exact) → HashSet(up to 256) → HLL(~3 KB).
static constexpr UInt64 UNIQ_COMBINED_PRECISION = 12;

StatisticsUniqCombined::StatisticsUniqCombined(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
    : IStatistics(description)
{
    arena = std::make_unique<Arena>();
    AggregateFunctionProperties properties;
    collector = AggregateFunctionFactory::instance().get(
        "uniqCombined", NullsAction::IGNORE_NULLS, {data_type}, Array{UNIQ_COMBINED_PRECISION}, properties);
    data = arena->alignedAlloc(collector->sizeOfData(), collector->alignOfData());
    collector->create(data);
}

StatisticsUniqCombined::~StatisticsUniqCombined()
{
    collector->destroy(data);
}

void StatisticsUniqCombined::build(const ColumnPtr & column)
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

void StatisticsUniqCombined::merge(const StatisticsPtr & other_stats)
{
    const StatisticsUniqCombined * other = typeid_cast<const StatisticsUniqCombined *>(other_stats.get());
    /// If the column type changed between parts (e.g. via a concurrent ALTER that wraps it in Nullable),
    /// the null-wrapper shifts the HLL state layout and makes the two states incompatible.
    /// Merging them would corrupt memory; skip instead.
    if (collector->sizeOfData() != other->collector->sizeOfData())
        return;
    collector->merge(data, other->data, arena.get());
}

void StatisticsUniqCombined::serialize(WriteBuffer & buf)
{
    if (collector->getNestedFunction())
        writeBinary(true, buf);
    else
        writeBinary(false, buf);
    collector->serialize(data, buf);
}

void StatisticsUniqCombined::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    bool is_null = false;
    readBinary(is_null, buf);
    auto nested_func = collector->getNestedFunction();
    /// when serialize is nullable, but we removed the nullable
    if (is_null && !nested_func)
    {
        bool serialize_flag = false;
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

UInt64 StatisticsUniqCombined::estimateCardinality() const
{
    auto column = collector->getResultType()->createColumn();
    collector->insertResultInto(data, *column, nullptr);
    /// When all input values are NULL the null-wrapper returns NULL (no non-null values seen).
    /// That means 0 distinct non-null values.
    if (column->isNullAt(0))
        return 0;
    return column->getUInt(0);
}

bool uniqCombinedStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    DataTypePtr inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    return inner_data_type->isValueRepresentedByNumber() || isStringOrFixedString(inner_data_type);
}

StatisticsPtr uniqCombinedStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsUniqCombined>(description, data_type);
}

}
