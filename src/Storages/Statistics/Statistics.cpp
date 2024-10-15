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
#include <Storages/StatisticsDescription.h>


#include "config.h" /// USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
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

ColumnPartStatistics::ColumnPartStatistics(const ColumnStatisticsDescription & stats_desc_, const String & column_name_)
    : stats_desc(stats_desc_), column_name(column_name_)
{
}

void ColumnPartStatistics::build(const ColumnPtr & column)
{
    rows += column->size();
    for (const auto & stat : stats)
        stat.second->build(column);
}

UInt64 IStatistics::estimateCardinality() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cardinality estimation is not implemented for this type of statistics");
}

Float64 IStatistics::estimateEqual(const Field & /*val*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Equality estimation is not implemented for this type of statistics");
}

Float64 IStatistics::estimateLess(const Field & /*val*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Less-than estimation is not implemented for this type of statistics");
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

Float64 ColumnPartStatistics::estimateLess(const Field & val) const
{
    if (stats.contains(StatisticsType::TDigest))
        return stats.at(StatisticsType::TDigest)->estimateLess(val);
    if (stats.contains(StatisticsType::MinMax))
        return stats.at(StatisticsType::MinMax)->estimateLess(val);
    return rows * ConditionSelectivityEstimator::default_cond_range_factor;
}

Float64 ColumnPartStatistics::estimateGreater(const Field & val) const
{
    return rows - estimateLess(val);
}

Float64 ColumnPartStatistics::estimateEqual(const Field & val) const
{
    if (stats_desc.data_type->isValueRepresentedByNumber() && stats.contains(StatisticsType::Uniq) && stats.contains(StatisticsType::TDigest))
    {
        /// 2048 is the default number of buckets in TDigest. In this case, TDigest stores exactly one value (with many rows) for every bucket.
        if (stats.at(StatisticsType::Uniq)->estimateCardinality() < 2048)
            return stats.at(StatisticsType::TDigest)->estimateEqual(val);
    }
#if USE_DATASKETCHES
    if (stats.contains(StatisticsType::CountMinSketch))
        return stats.at(StatisticsType::CountMinSketch)->estimateEqual(val);
#endif
    if (stats.contains(StatisticsType::Uniq))
    {
        UInt64 cardinality = stats.at(StatisticsType::Uniq)->estimateCardinality();
        if (cardinality == 0 || rows == 0)
            return 0;
        return 1.0 / cardinality * rows; /// assume uniform distribution
    }

    return rows * ConditionSelectivityEstimator::default_cond_equal_factor;
}

/// -------------------------------------

void ColumnPartStatistics::serialize(WriteBuffer & buf)
{
    writeIntBinary(V0, buf);

    UInt64 stat_types_mask = 0;
    for (const auto & [type, _]: stats)
        stat_types_mask |= 1 << UInt8(type);
    writeIntBinary(stat_types_mask, buf);

    /// as the column row count is always useful, save it in any case
    writeIntBinary(rows, buf);

    /// write the actual statistics object
    for (const auto & [type, stat_ptr] : stats)
        stat_ptr->serialize(buf);
}

void ColumnPartStatistics::deserialize(ReadBuffer &buf)
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
        if (!(stat_types_mask & 1 << UInt8(it->first)))
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

String ColumnPartStatistics::getFileName() const
{
    return STATS_FILE_PREFIX + columnName();
}

const String & ColumnPartStatistics::columnName() const
{
    return column_name;
}

UInt64 ColumnPartStatistics::rowCount() const
{
    return rows;
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
        it->second(desc, data_type);
    }
}

ColumnStatisticsPartPtr MergeTreeStatisticsFactory::get(const ColumnDescription & column_desc) const
{
    ColumnStatisticsPartPtr column_stat = std::make_shared<ColumnPartStatistics>(column_desc.statistics, column_desc.name);
    for (const auto & [type, desc] : column_desc.statistics.types_to_desc)
    {
        auto it = creators.find(type);
        if (it == creators.end())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'. Available types: 'countmin', 'minmax', 'tdigest' and 'uniq'", type);
        auto stat_ptr = (it->second)(desc, column_desc.type);
        column_stat->stats[type] = stat_ptr;
    }
    return column_stat;
}

ColumnsStatistics MergeTreeStatisticsFactory::getMany(const ColumnsDescription & columns) const
{
    ColumnsStatistics result;
    for (const auto & col : columns)
        if (!col.statistics.empty())
            result.push_back(get(col));
    return result;
}

}
