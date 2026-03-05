#include <Storages/Statistics/Statistics.h>

#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
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
#include <Parsers/ASTIdentifier.h>

#include "config.h" /// USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_STATISTICS;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

enum StatisticsFileVersion : UInt16
{
    V0 = 0,
    V1 = 1, /// modify the format of uniq, https://github.com/ClickHouse/ClickHouse/pull/90311
};

std::optional<Float64> StatisticsUtils::tryConvertToFloat64(const Field & value, const DataTypePtr & data_type)
{
    if (!data_type->isValueRepresentedByNumber())
        return {};

    auto column = data_type->createColumn();
    column->insert(value);
    ColumnsWithTypeAndName arguments({ColumnWithTypeAndName(std::move(column), data_type, "stats_const")});

    auto cast_resolver = FunctionFactory::instance().get("toFloat64", nullptr);
    auto cast_function = cast_resolver->build(arguments);
    ColumnPtr result = cast_function->execute(arguments, std::make_shared<DataTypeFloat64>(), 1, false);
    return result->getFloat64(0);
}

IStatistics::IStatistics(const SingleStatisticsDescription & stat_)
    : stat(stat_)
{
}

ColumnStatistics::ColumnStatistics(const ColumnStatisticsDescription & stats_desc_) : stats_desc(stats_desc_)
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

bool ColumnStatistics::structureEquals(const ColumnStatistics & other) const
{
    if (stats.size() != other.stats.size())
        return false;

    auto i = stats.begin();
    auto j = other.stats.begin();

    for (; i != stats.end(); ++i, ++j)
    {
        if (i->first != j->first)
            return false;
    }

    return true;
}

std::shared_ptr<ColumnStatistics> ColumnStatistics::cloneEmpty() const
{
    return MergeTreeStatisticsFactory::instance().get(stats_desc);
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
    /// Comparisons with NaN are always false, so `x < NaN` has zero selectivity.
    if (val.isNaN())
        return 0;

    if (stats.contains(StatisticsType::TDigest))
        return stats.at(StatisticsType::TDigest)->estimateLess(val);
    if (stats.contains(StatisticsType::MinMax))
        return stats.at(StatisticsType::MinMax)->estimateLess(val);
    return static_cast<Float64>(rows) * ConditionSelectivityEstimator::default_cond_range_factor;
}

Float64 ColumnStatistics::estimateGreater(const Field & val) const
{
    /// Comparisons with NaN are always false, so `x > NaN` has zero selectivity.
    if (val.isNaN())
        return 0;

    return static_cast<Float64>(rows) - estimateLess(val);
}

Float64 ColumnStatistics::estimateEqual(const Field & val) const
{
    /// Comparisons with NaN are always false, so `x = NaN` has zero selectivity.
    if (val.isNaN())
        return 0;

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
        return static_cast<Float64>(rows) / static_cast<Float64>(cardinality); /// assume uniform distribution
    }

    return static_cast<Float64>(rows) * ConditionSelectivityEstimator::default_cond_equal_factor;
}

Float64 ColumnStatistics::estimateRange(const Range & range) const
{
    if (range.empty())
    {
        return 0;
    }

    if (range.isInfinite())
    {
        return static_cast<Float64>(rows);
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
    return UInt64(static_cast<Float64>(rows) * ConditionSelectivityEstimator::default_cardinality_ratio);
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

void ColumnStatistics::serialize(WriteBuffer & buf) const
{
    writeIntBinary(V1, buf);

    UInt64 stat_types_mask = 0;
    for (const auto & [type, _]: stats)
        stat_types_mask |= 1ULL << static_cast<UInt8>(type);

    writeIntBinary(stat_types_mask, buf);

    /// As the column row count is always useful, save it in any case
    writeIntBinary(rows, buf);

    /// Write the actual statistics object
    for (const auto & [type, stat_ptr] : stats)
        stat_ptr->serialize(buf);
}

std::shared_ptr<ColumnStatistics> ColumnStatistics::deserialize(ReadBuffer & buf, const DataTypePtr & data_type)
{
    UInt16 version;
    readIntBinary(version, buf);

    /// TODO: we should check the version of statistics format when we start clickhouse server, and do materialize statistics automatically.
    if (version != V1)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "We try to read stale file format version: {}. Please run `ALTER TABLE [db.]table MATERIALIZE STATISTICS ALL` to regenerate the statistics", version);

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, buf);

    std::vector<StatisticsType> stat_types;
    for (size_t i = 0; i < static_cast<UInt8>(StatisticsType::Max); ++i)
    {
        if (stat_types_mask & (1ULL << i))
            stat_types.push_back(static_cast<StatisticsType>(i));
    }

    const auto & factory = MergeTreeStatisticsFactory::instance();
    ColumnStatisticsDescription stats_desc;
    stats_desc.data_type = data_type;
    stats_desc.types_to_desc = factory.get(stat_types, data_type);

    auto result = factory.get(stats_desc);
    readIntBinary(result->rows, buf);

    for (const auto & [_, desc] : result->stats)
        desc->deserialize(buf);

    return result;
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
        result.emplace(column_name, stat->cloneEmpty());
    return result;
}

void ColumnsStatistics::build(const Block & block)
{
    for (const auto & [column_name, stat] : *this)
        stat->build(block.getByName(column_name).column);
}

void ColumnsStatistics::buildIfExists(const Block & block)
{
    for (const auto & [column_name, stat] : *this)
    {
        if (block.has(column_name))
            stat->build(block.getByName(column_name).column);
    }
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
    return get(column_desc.statistics);
}

ColumnStatisticsPtr MergeTreeStatisticsFactory::get(const ColumnStatisticsDescription & stats_desc) const
{
    auto column_stat = std::make_shared<ColumnStatistics>(stats_desc);

    for (const auto & [type, desc] : stats_desc.types_to_desc)
    {
        auto it = creators.find(type);
        if (it == creators.end())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'. Available types: 'countmin', 'minmax', 'tdigest' and 'uniq'", type);

        auto stat_ptr = (it->second)(desc, stats_desc.data_type);
        column_stat->stats[type] = stat_ptr;
    }

    return column_stat;
}

ColumnStatisticsDescription::StatisticsTypeDescMap MergeTreeStatisticsFactory::get(const std::vector<StatisticsType> & stat_types, const DataTypePtr & data_type) const
{
    ColumnStatisticsDescription::StatisticsTypeDescMap result;
    for (const auto & type : stat_types)
    {
        auto it = validators.find(type);
        if (it == validators.end())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'. Available types: 'countmin', 'minmax', 'tdigest' and 'uniq'", type);

        auto ast = make_intrusive<ASTIdentifier>(statisticsTypeToString(type));
        SingleStatisticsDescription desc(type, ast, false);

        if (!it->second(desc, data_type))
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type '{}' does not support data type {}", type, data_type->getName());

        result.emplace(type, desc);
    }
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
