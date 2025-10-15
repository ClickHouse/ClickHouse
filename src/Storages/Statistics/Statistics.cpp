#include <Storages/Statistics/Statistics.h>

#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>
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
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>


#include "config.h" /// USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int ILLEGAL_STATISTICS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

enum StatisticsFileVersion : UInt16
{
    V0 = 0,
};

std::optional<Float64> StatisticsUtils::tryConvertToFloat64(const Field & value, const DataTypePtr & data_type)
{
    if (!data_type->isValueRepresentedByNumber())
        return {};

    Field value_converted;
    if (isInteger(data_type) && !isBool(data_type))
        /// For case val_int32 < 10.5 or val_int32 < '10.5' we should convert 10.5 to Float64.
        value_converted = convertFieldToType(value, DataTypeFloat64());
    else
        /// We should convert value to the real column data type and then translate it to Float64.
        /// For example for expression col_date > '2024-08-07', if we directly convert '2024-08-07' to Float64, we will get null.
        value_converted = convertFieldToType(value, *data_type);

    if (value_converted.isNull())
        return {};

    return applyVisitor(FieldVisitorConvertToNumber<Float64>(), value_converted);
}

IStatistics::IStatistics(const SingleStatisticsDescription & stat_)
    : stat(stat_)
{
}

ColumnStatistics::ColumnStatistics(const ColumnStatisticsDescription & stats_desc_, const String & column_name_)
    : stats_desc(stats_desc_), column_name(column_name_)
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
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cardinality estimation is not implemented for this type of statistics");
}

Float64 IStatistics::estimateEqual(const Field & /*val*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Equality estimation is not implemented for this type of statistics");
}

Float64 IStatistics::estimateLess(const Field & /*val*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Less-than estimation is not implemented for this type of statistics");
}

Float64 IStatistics::estimateRange(const Range & /*range*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Range estimation is not implemented for this type of statistics");
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

UInt64 ColumnStatistics::estimateCardinality() const
{
    if (stats.contains(StatisticsType::Uniq))
    {
        return stats.at(StatisticsType::Uniq)->estimateCardinality();
    }
    /// if we don't have uniq statistics, we use a mock one, assuming there are 90% different unique values.
    return UInt64(rows * ConditionSelectivityEstimator::default_cardinality_ratio);
}

Estimate ColumnStatistics::getEstimate() const
{
    Estimate info;
    info.rows_count = rows;

    for (const auto & [type, _] : stats)
        info.types.insert(type);

    if (stats.contains(StatisticsType::Uniq))
        info.estimated_cardinality = stats.at(StatisticsType::Uniq)->estimateCardinality();

    if (stats.contains(StatisticsType::MinMax))
    {
        const auto & minmax_stats = assert_cast<const StatisticsMinMax &>(*stats.at(StatisticsType::MinMax));
        info.estimated_min = minmax_stats.getMin();
        info.estimated_max = minmax_stats.getMax();
    }

    return info;
}

/// -------------------------------------

void ColumnStatistics::serialize(WriteBuffer & buf)
{
    writeIntBinary(V0, buf);

    UInt64 stat_types_mask = 0;
    for (const auto & [type, _]: stats)
        stat_types_mask |= 1LL << UInt8(type);
    writeIntBinary(stat_types_mask, buf);

    /// as the column row count is always useful, save it in any case
    writeIntBinary(rows, buf);

    /// write the actual statistics object
    for (const auto & [type, stat_ptr] : stats)
        stat_ptr->serialize(buf);
}

void ColumnStatistics::deserialize(ReadBuffer &buf)
{
    UInt16 version;
    readIntBinary(version, buf);
    if (version != V0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown file format version: {}", version);

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

String ColumnStatistics::getStatisticName() const
{
    return STATS_FILE_PREFIX + column_name;
}

const String & ColumnStatistics::getColumnName() const
{
    return column_name;
}

UInt64 ColumnStatistics::rowCount() const
{
    return rows;
}

String ColumnStatistics::getNameForLogs() const
{
    String ret;
    for (const auto & [type, single_stats] : stats)
    {
        ret += single_stats->getNameForLogs();
        ret += " | ";
    }
    ret += "rows: " + std::to_string(rows);
    return ret;
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
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'", type);

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
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'", entry.first);

        if (it->second(entry.second, data_type))
            result.types_to_desc.insert(entry);
    }

    return result;
}

ColumnStatisticsPtr MergeTreeStatisticsFactory::get(const ColumnDescription & column_desc) const
{
    ColumnStatisticsPtr column_stat = std::make_shared<ColumnStatistics>(column_desc.statistics, column_desc.name);
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

static ColumnStatisticsDescription::StatisticsTypeDescMap parseColumnStatisticsFromString(const String & str)
{
    ParserStatisticsType stat_type_parser;
    auto stats_ast = parseQuery(stat_type_parser, "(" + str + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    ColumnStatisticsDescription::StatisticsTypeDescMap result;

    for (const auto & arg : stats_ast->as<ASTFunction &>().arguments->children)
    {
        const auto * arg_func = arg->as<ASTFunction>();
        if (!arg_func)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a function for statistic type, got: {}", arg->formatForLogging());

        auto stat_type = stringToStatisticsType(arg_func->name);
        result.emplace(stat_type, SingleStatisticsDescription(stat_type, arg, true));
    }

    return result;
}

void removeImplicitStatistics(ColumnsDescription & columns)
{
    for (const auto & column : columns)
    {
        auto default_kind = column.default_desc.kind;
        if (default_kind == ColumnDefaultKind::Alias || default_kind == ColumnDefaultKind::Ephemeral)
            continue;

        columns.modify(column.name, [&](ColumnDescription & column_desc)
        {
            auto & stats = column_desc.statistics.types_to_desc;
            for (auto it = stats.begin(); it != stats.end();)
            {
                if (it->second.is_implicit)
                    it = stats.erase(it);
                else
                    ++it;
            }
        });
    }
}

void addImplicitStatistics(ColumnsDescription & columns, const String & statistics_types_str)
{
    if (statistics_types_str.empty())
        return;

    auto stats_ast_map = parseColumnStatisticsFromString(statistics_types_str);
    const auto & factory = MergeTreeStatisticsFactory::instance();

    for (const auto & column : columns)
    {
        auto default_kind = column.default_desc.kind;
        if (default_kind == ColumnDefaultKind::Alias || default_kind == ColumnDefaultKind::Ephemeral)
            continue;

        ColumnStatisticsDescription stats_desc;
        stats_desc.data_type = column.type;
        stats_desc.types_to_desc = stats_ast_map;
        stats_desc = factory.cloneWithSupportedStatistics(stats_desc, column.type);

        if (!stats_desc.empty())
        {
            columns.modify(column.name, [&](ColumnDescription & column_desc)
            {
                column_desc.statistics.merge(stats_desc, column.name, column.type, /*if_not_exists=*/ true);
            });
        }
    }
}

}
