#include <Storages/Statistics/StatisticsUniqV2.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <mutex>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_STATISTICS;
    extern const int LOGICAL_ERROR;
}

/// Use uniqCombined64 with K=12: Small(16 exact) → HashSet(up to 256) → HLL(~3 KB).
/// The 64-bit hash avoids collisions for high-cardinality columns (32-bit hashes degrade above ~65K distinct values).
static constexpr UInt64 UNIQ_COMBINED_PRECISION = 12;

/// The statistics of all columns of a data part are loaded at once, so getting the aggregate function from
/// the factory for every column would dominate the loading. An aggregate function is immutable, so it is
/// shared by all statistics of the same type. The cache outlives the tables, so it is cleared when it has too many types.
static AggregateFunctionPtr getCollector(const DataTypePtr & data_type)
{
    static constexpr size_t max_cached_types = 10000;

    static std::mutex mutex;
    static std::unordered_map<String, AggregateFunctionPtr> collectors;

    String type_name = data_type->getName();
    {
        std::lock_guard lock(mutex);
        if (auto it = collectors.find(type_name); it != collectors.end())
            return it->second;
    }

    AggregateFunctionProperties properties;
    auto function = AggregateFunctionFactory::instance().get(
        "uniqCombined64", NullsAction::IGNORE_NULLS, {data_type}, Array{UNIQ_COMBINED_PRECISION}, properties);
    if (function->allocatesMemoryInArena())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregate function {} for uniq_v2 statistics allocates memory in arena", function->getName());

    std::lock_guard lock(mutex);
    if (collectors.size() >= max_cached_types)
        collectors.clear();
    collectors.emplace(std::move(type_name), function);
    return function;
}

StatisticsUniqV2::StatisticsUniqV2(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
    : IStatistics(description)
    , collector(getCollector(data_type))
    , state(collector->sizeOfData(), collector->alignOfData())
    , data(state.data())
{
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

    collector->addBatchSinglePlace(0, raw_column_ptr->size(), data, &raw_column_ptr, nullptr);
    serialized_size = 0;
}

void StatisticsUniqV2::merge(const StatisticsPtr & other_stats)
{
    const StatisticsUniqV2 * other = typeid_cast<const StatisticsUniqV2 *>(other_stats.get());
    collector->merge(data, other->data, nullptr);
    serialized_size = 0;
}

size_t StatisticsUniqV2::allocatedBytes() const
{
    /// The aggregate function state keeps its hash table or HyperLogLog counters on the heap,
    /// and there is no interface to get their size. A hash table is kept between a quarter and a half full, so its
    /// buffer is up to four times larger than the serialized elements; the HyperLogLog counters take about as much
    /// memory as their serialized form. Take the upper bound, so the cache does not grow over its limit.
    size_t state_size = serialized_size;
    if (!state_size)
    {
        NullWriteBuffer serialized;
        collector->serialize(data, serialized);
        serialized.finalize();
        state_size = serialized.count();
    }
    return sizeof(*this) + collector->sizeOfData() + 4 * state_size;
}

bool StatisticsUniqV2::isCompatibleWith(const IStatistics & other) const
{
    const auto * other_v2 = typeid_cast<const StatisticsUniqV2 *>(&other);
    if (!other_v2)
        return false;
    return StatisticsUtils::isSame(*collector, *other_v2->collector);
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

    /// Sanity check: If the nullable metadata disagrees, abort the load
    const bool collector_is_nullable = collector->getNestedFunction() != nullptr;
    if (is_null != collector_is_nullable)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "uniq_v2 statistics nullability ({}) does not match the current column type ({}); "
            "rebuild with ALTER TABLE ... MATERIALIZE STATISTICS.",
            is_null, collector_is_nullable);

    const size_t count_before = buf.count();
    collector->deserialize(data, buf);
    serialized_size = buf.count() - count_before;
}

UInt64 StatisticsUniqV2::estimateCardinality() const
{
    if (UInt64 cached = cached_cardinality_plus_one)
        return cached - 1;

    auto column = collector->getResultType()->createColumn();
    collector->insertResultInto(data, *column, nullptr);
    /// When all input values are NULL the null-wrapper returns NULL (no non-null values seen).
    /// That means 0 distinct non-null values.
    const UInt64 cardinality = column->isNullAt(0) ? 0 : column->getUInt(0);

    cached_cardinality_plus_one = cardinality + 1;

    return cardinality;
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
