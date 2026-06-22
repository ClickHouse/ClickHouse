#include <Storages/Statistics/StatisticsUniqV2.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

/// Use uniqCombined64 with K=12: Small(16 exact) → HashSet(up to 256) → HLL(~3 KB).
/// The 64-bit hash avoids collisions for high-cardinality columns (32-bit hashes degrade above ~65K distinct values).
static constexpr UInt64 UNIQ_COMBINED_PRECISION = 12;

StatisticsUniqV2::StatisticsUniqV2(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
    : IStatistics(description)
{
    arena = std::make_unique<Arena>();
    AggregateFunctionProperties properties;
    collector = AggregateFunctionFactory::instance().get(
        "uniqCombined64", NullsAction::IGNORE_NULLS, {data_type}, Array{UNIQ_COMBINED_PRECISION}, properties);
    data = arena->alignedAlloc(collector->sizeOfData(), collector->alignOfData());
    collector->create(data);
}

StatisticsUniqV2::~StatisticsUniqV2()
{
    collector->destroy(data);
}

void StatisticsUniqV2::build(const ColumnPtr & column)
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

void StatisticsUniqV2::merge(const StatisticsPtr & other_stats)
{
    const StatisticsUniqV2 * other = typeid_cast<const StatisticsUniqV2 *>(other_stats.get());
    collector->merge(data, other->data, arena.get());
}

bool StatisticsUniqV2::isCompatibleWith(const IStatistics & other) const
{
    const auto * other_v2 = typeid_cast<const StatisticsUniqV2 *>(&other);
    if (!other_v2)
        return false;
    return StatisticsUtils::aggregateEqual(*collector, *other_v2->collector);
}

void StatisticsUniqV2::serialize(WriteBuffer & buf)
{
    if (collector->getNestedFunction())
        writeBinary(true, buf);
    else
        writeBinary(false, buf);
    collector->serialize(data, buf);
}

void StatisticsUniqV2::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
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

UInt64 StatisticsUniqV2::estimateCardinality() const
{
    auto column = collector->getResultType()->createColumn();
    collector->insertResultInto(data, *column, nullptr);
    /// When all input values are NULL the null-wrapper returns NULL (no non-null values seen).
    /// That means 0 distinct non-null values.
    if (column->isNullAt(0))
        return 0;
    return column->getUInt(0);
}

bool uniqV2StatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    DataTypePtr inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    return inner_data_type->isValueRepresentedByNumber() || isStringOrFixedString(inner_data_type);
}

StatisticsPtr uniqV2StatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsUniqV2>(description, data_type);
}

}
