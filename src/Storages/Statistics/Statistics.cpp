#include <Storages/Statistics/Statistics.h>

#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/Statistics/StatisticsCountMinSketch.h>
#include <Storages/Statistics/StatisticsMinMax.h>
#include <Storages/Statistics/StatisticsTDigest.h>
#include <Storages/Statistics/StatisticsUniq.h>
#include <Storages/Statistics/StatisticDefaults.h>
#include <Storages/StatisticsDescription.h>


#include "config.h" /// USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int ILLEGAL_STATISTICS;
}

enum StatisticsFileVersion : UInt16
{
    V0 = 0,
};

std::optional<Float64> StatisticsUtils::tryConvertToFloat64(const Field & value, const DataTypePtr & data_type)
{
    if (data_type->isValueRepresentedByNumber())
    {
        Field value_converted;

        if (isInteger(data_type) && (value.getType() == Field::Types::Float64 || value.getType() == Field::Types::String))
            /// For case val_int32 < 10.5 or val_int32 < '10.5' we should convert 10.5 to Float64.
            value_converted = convertFieldToType(value, *DataTypeFactory::instance().get("Float64"));
        else
            /// We should convert value to the real column data type and then translate it to Float64.
            /// For example for expression col_date > '2024-08-07', if we directly convert '2024-08-07' to Float64, we will get null.
            value_converted = convertFieldToType(value, *data_type);

        if (value_converted.isNull())
            return {};

        Float64 value_as_float = applyVisitor(FieldVisitorConvertToNumber<Float64>(), value_converted);
        return value_as_float;
    }
    return {};
}

IStatistics::IStatistics(const SingleStatisticsDescription & stat_)
    : stat(stat_)
{
}

ColumnStatistics::ColumnStatistics(const ColumnStatisticsDescription & stats_desc_)
    : stats_desc(stats_desc_)
{
}

void ColumnStatistics::build(const ColumnPtr & column)
{
    rows += column->size();
    for (const auto & stat : stats)
        stat.second->build(column);
}

void ColumnStatistics::merge(const ColumnStatisticsPtr & other)
{
    rows += other->rows;
    for (const auto & stat_it : stats)
    {
        if (auto it = other->stats.find(stat_it.first); it != other->stats.end())
        {
            stat_it.second->merge(it->second);
        }
    }
    for (const auto & stat_it : other->stats)
    {
        if (!stats.contains(stat_it.first))
        {
            stats.insert(stat_it);
        }
    }
}

UInt64 IStatistics::estimateCardinality() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cardinality estimation is not implemented for this type of statistics");
}

UInt64 IStatistics::estimateDefaults() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Defaults estimation is not implemented for this type of statistics");
}

Float64 IStatistics::estimateEqual(const Field & /*val*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Equality estimation is not implemented for this type of statistics");
}

Float64 IStatistics::estimateLess(const Field & /*val*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Less-than estimation is not implemented for this type of statistics");
}

Float64 IStatistics::estimateRange(const Range & /*range*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Range estimation is not implemented for this type of statistics");
}

/// Notes:
/// - Statistics object usually only support estimation for certain types of predicates, e.g.
///    - TDigest: '< X' (less-than predicates)
///    - Count-min sketches: '= X' (equal predicates)
///    - Uniq (HyperLogLog): 'count distinct(*)' (column cardinality)
///
/// If multiple statistics objects in a column support estimating a predicate, we want to try statistics in order of descending accuracy
/// (e.g. MinMax statistics are simpler than TDigest statistics and thus worse for estimating 'less' predicates).
///
/// Sometimes, it is possible to combine multiple statistics in a clever way. For that reason, all estimation are performed in a central
/// place (here), and we don't simply pass the predicate to the first statistics object that supports it natively.

Float64 ColumnStatistics::estimateLess(const Field & val) const
{
    if (stats.contains(StatisticsType::TDigest))
        return stats.at(StatisticsType::TDigest)->estimateLess(val);
    if (stats.contains(StatisticsType::MinMax))
        return stats.at(StatisticsType::MinMax)->estimateLess(val);
    return rows * ConditionSelectivityEstimator::default_cond_range_factor;
}

Float64 ColumnStatistics::estimateGreater(const Field & val) const
{
    return rows - estimateLess(val);
}

Float64 ColumnStatistics::estimateEqual(const Field & val) const
{
    if (stats_desc.data_type->isValueRepresentedByNumber() && stats.contains(StatisticsType::Uniq) && stats.contains(StatisticsType::TDigest))
    {
        /// 2048 is the default number of buckets in TDigest. In this case, TDigest stores exactly one value (with many rows) for every bucket.
        if (stats.at(StatisticsType::Uniq)->estimateCardinality() < 2048)
        {
            return stats.at(StatisticsType::TDigest)->estimateEqual(val);
        }
    }
#if USE_DATASKETCHES
    if (stats.contains(StatisticsType::CountMinSketch))
    {
        return stats.at(StatisticsType::CountMinSketch)->estimateEqual(val);
    }
#endif
    if (stats.contains(StatisticsType::Uniq))
    {
        UInt64 cardinality = stats.at(StatisticsType::Uniq)->estimateCardinality();
        if (cardinality == 0 || rows == 0)
            return 0;
        return Float64(rows) / cardinality; /// assume uniform distribution
    }

    return rows * ConditionSelectivityEstimator::default_cond_equal_factor;
}

Float64 ColumnStatistics::estimateRange(const Range & range) const
{
    if (range.empty())
    {
        return 0;
    }

    if (range.isInfinite())
    {
        return rows;
    }

    if (range.left == range.right)
    {
        return estimateEqual(range.left);
    }

    if (range.left.isNegativeInfinity())
    {
        return estimateLess(range.right);
    }

    if (range.right.isPositiveInfinity())
    {
        return estimateGreater(range.left);
    }

    Float64 right_count = estimateLess(range.right);
    Float64 left_count = estimateLess(range.left);
    return right_count - left_count;
}

UInt64 ColumnStatistics::rowCount() const
{
    return rows;
}

UInt64 ColumnStatistics::estimateCardinality() const
{
    if (stats.contains(StatisticsType::Uniq))
    {
        return stats.at(StatisticsType::Uniq)->estimateCardinality();
    }
    /// if we don't have uniq statistics, we use a mock one, assuming there are 90% different unique values.
    return UInt64(rows * ConditionSelectivityEstimator::default_cardinality_ratio);
}

UInt64 ColumnStatistics::estimateDefaults() const
{
    if (stats.contains(StatisticsType::Defaults))
    {
        return stats.at(StatisticsType::Defaults)->estimateDefaults();
    }
    return 0;
}

void ColumnStatistics::serialize(WriteBuffer & buf) const
{
    writeIntBinary(V0, buf);

    UInt64 stat_types_mask = 0;
    for (const auto & [type, _]: stats)
        stat_types_mask |= 1LL << UInt8(type);

    writeIntBinary(stat_types_mask, buf);

    /// As the column row count is always useful, save it in any case
    writeIntBinary(rows, buf);

    /// Write the actual statistics object
    for (const auto & [type, stat_ptr] : stats)
        stat_ptr->serialize(buf);
}

void ColumnStatistics::deserialize(ReadBuffer &buf)
{
    UInt16 version;
    readIntBinary(version, buf);
    if (version != V0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown file format version: {}", version);

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, buf);
    readIntBinary(rows, buf);

    for (auto it = stats.begin(); it != stats.end();)
    {
        if (!(stat_types_mask & 1LL << UInt8(it->first)))
        {
            stats.erase(it++);
        }
        else
        {
            it->second->deserialize(buf);
            ++it;
        }
    }
}

Estimate ColumnStatistics::getEstimate() const
{
    Estimate info;
    info.rows_count = rows;

    for (const auto & [type, _] : stats)
        info.types.insert(type);

    if (stats.contains(StatisticsType::Uniq))
        info.estimated_cardinality = stats.at(StatisticsType::Uniq)->estimateCardinality();

    if (stats.contains(StatisticsType::Defaults))
        info.estimated_defaults = stats.at(StatisticsType::Defaults)->estimateDefaults();

    return info;
}

ColumnsStatistics::ColumnsStatistics(const ColumnsDescription & columns)
{
    const auto & factory = MergeTreeStatisticsFactory::instance();

    for (const auto & column : columns)
    {
        if (!column.statistics.empty())
            emplace(column.name, factory.get(column));
    }
}

ColumnsStatistics ColumnsStatistics::cloneEmpty() const
{
    ColumnsStatistics result;
    for (const auto & [column_name, stat] : *this)
    {
        auto new_stat = MergeTreeStatisticsFactory::instance().get(stat->getDescription());
        result.emplace(column_name, std::move(new_stat));
    }
    return result;
}

void ColumnsStatistics::serialize(WriteBuffer & buf) const
{
    static constexpr UInt8 version = 0;

    writeIntBinary(version, buf);
    writeIntBinary(size(), buf);

    for (const auto & [column_name, stat] : *this)
    {
        writeStringBinary(column_name, buf);
        stat->serialize(buf);
    }
}

void ColumnsStatistics::deserialize(ReadBuffer & buf)
{
    UInt8 version;
    readIntBinary(version, buf);

    if (version != 0)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown file format version: {}", UInt64(version));

    size_t size;
    readIntBinary(size, buf);
    NameSet loaded_stats;

    for (size_t i = 0; i < size; ++i)
    {
        String column_name;
        readStringBinary(column_name, buf);

        auto it = find(column_name);

        if (it == end())
            continue;

        it->second->deserialize(buf);
        loaded_stats.insert(column_name);
    }

    for (auto it = begin(); it != end();)
    {
        if (loaded_stats.contains(it->first))
            ++it;
        else
            erase(it++);
    }
}

void ColumnsStatistics::build(const Block & block)
{
    for (const auto & [column_name, stat] : *this)
        stat->build(block.getByName(column_name).column);
}

void ColumnsStatistics::merge(const ColumnsStatistics & other)
{
    for (const auto & [column_name, stat] : other)
    {
        auto it = find(column_name);
        if (it == end())
            emplace(column_name, stat);
        else
            it->second->merge(stat);
    }
}

void ColumnsStatistics::replace(const ColumnsStatistics & other)
{
    for (const auto & [column_name, stat] : other)
    {
        auto it = find(column_name);
        if (it == end())
            emplace(column_name, stat);
        else
            it->second = stat;
    }
}

Estimates ColumnsStatistics::getEstimates() const
{
    Estimates estimates;
    for (const auto & [column_name, stat] : *this)
        estimates.emplace(column_name, stat->getEstimate());
    return estimates;
}

void MergeTreeStatisticsFactory::registerCreator(StatisticsType stats_type, Creator creator)
{
    if (!creators.emplace(stats_type, std::move(creator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticsFactory: the statistics creator type {} is not unique", stats_type);
}

void MergeTreeStatisticsFactory::registerValidator(StatisticsType stats_type, Validator validator)
{
    if (!validators.emplace(stats_type, std::move(validator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticsFactory: the statistics validator type {} is not unique", stats_type);
}

MergeTreeStatisticsFactory::MergeTreeStatisticsFactory()
{
    registerValidator(StatisticsType::MinMax, minMaxStatisticsValidator);
    registerCreator(StatisticsType::MinMax, minMaxStatisticsCreator);

    registerValidator(StatisticsType::TDigest, tdigestStatisticsValidator);
    registerCreator(StatisticsType::TDigest, tdigestStatisticsCreator);

    registerValidator(StatisticsType::Uniq, uniqStatisticsValidator);
    registerCreator(StatisticsType::Uniq, uniqStatisticsCreator);

    registerValidator(StatisticsType::Defaults, defaultsStatisticsValidator);
    registerCreator(StatisticsType::Defaults, defaultsStatisticsCreator);

#if USE_DATASKETCHES
    registerValidator(StatisticsType::CountMinSketch, countMinSketchStatisticsValidator);
    registerCreator(StatisticsType::CountMinSketch, countMinSketchStatisticsCreator);
#endif
}

MergeTreeStatisticsFactory & MergeTreeStatisticsFactory::instance()
{
    static MergeTreeStatisticsFactory instance;
    return instance;
}

void MergeTreeStatisticsFactory::validate(const ColumnStatisticsDescription & stats, const DataTypePtr & data_type) const
{
    for (const auto & [type, desc] : stats.types_to_desc)
    {
        auto it = validators.find(type);
        if (it == validators.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistic type '{}'", type);

        if (!it->second(desc, data_type))
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type '{}' does not support data type type {}", type, data_type->getName());
    }
}

ColumnStatisticsDescription MergeTreeStatisticsFactory::cloneWithSupportedStatistics(const ColumnStatisticsDescription & stats, const DataTypePtr & data_type) const
{
    ColumnStatisticsDescription result;
    result.data_type = data_type;

    for (const auto & entry : stats.types_to_desc)
    {
        auto it = validators.find(entry.first);
        if (it == validators.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistic type '{}'", entry.first);

        if (it->second(entry.second, data_type))
            result.types_to_desc.insert(entry);
    }

    return result;
}

ColumnStatisticsPtr MergeTreeStatisticsFactory::get(const ColumnDescription & column_desc) const
{
    return get(column_desc.statistics);
}

ColumnStatisticsPtr MergeTreeStatisticsFactory::get(const ColumnStatisticsDescription & stats_desc) const
{
    auto column_stat = std::make_shared<ColumnStatistics>(stats_desc);

    for (const auto & [type, desc] : stats_desc.types_to_desc)
    {
        auto it = creators.find(type);
        if (it == creators.end())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'. Available types: 'countmin', 'minmax', 'tdigest', 'uniq' and 'defaults'", type);

        auto stat_ptr = (it->second)(desc, stats_desc.data_type);
        column_stat->stats[type] = stat_ptr;
    }

    return column_stat;
}

}
