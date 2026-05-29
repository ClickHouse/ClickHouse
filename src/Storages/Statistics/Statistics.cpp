#include <Storages/Statistics/Statistics.h>

#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/Statistics/StatisticsBasic.h>
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


std::optional<Float64> StatisticsUtils::tryConvertToFloat64(const Field & value, const DataTypePtr & data_type)
{
    if (!data_type->isValueRepresentedByNumber())
        return {};

    try
    {
        auto column = data_type->createColumn();
        column->insert(value);
        ColumnsWithTypeAndName arguments({ColumnWithTypeAndName(std::move(column), data_type, "stats_const")});

        auto cast_resolver = FunctionFactory::instance().get("toFloat64", nullptr);
        auto cast_function = cast_resolver->build(arguments);
        ColumnPtr result = cast_function->execute(arguments, std::make_shared<DataTypeFloat64>(), 1, false);
        return result->getFloat64(0);
    }
    catch (...)
    {
        tryLogCurrentException("StatisticsUtils", "Cannot convert field to Float64", LogsLevel::information);
        return {};
    }
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

std::optional<Float64> IStatistics::estimateLess(const Field & /*val*/) const
{
    return std::nullopt;
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

std::optional<Float64> ColumnStatistics::estimateLess(const Field & val) const
{
    /// Comparisons with NaN are always false, so `x < NaN` has zero selectivity.
    if (val.isNaN())
        return 0;

    if (stats.contains(StatisticsType::TDigest))
        if (auto result = stats.at(StatisticsType::TDigest)->estimateLess(val))
            return result;
    if (stats.contains(StatisticsType::Basic))
        if (auto result = stats.at(StatisticsType::Basic)->estimateLess(val))
            return result;
    if (stats.contains(StatisticsType::MinMax))
        if (auto result = stats.at(StatisticsType::MinMax)->estimateLess(val))
            return result;
    return std::nullopt;
}

std::optional<Float64> ColumnStatistics::estimateGreater(const Field & val) const
{
    /// Comparisons with NaN are always false, so `x > NaN` has zero selectivity.
    if (val.isNaN())
        return 0;

    if (auto less = estimateLess(val))
        return static_cast<Float64>(getNonNullRowCount()) - *less;
    return std::nullopt;
}

std::optional<Float64> ColumnStatistics::estimateEqual(const Field & val) const
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
        /// Uniq ignores NULLs, so divide non-NULL row count by distinct values.
        /// `getNonNullRowCount` returns `rows` when null-count tracking is absent.
        return static_cast<Float64>(getNonNullRowCount()) / static_cast<Float64>(cardinality);
    }

    return std::nullopt;
}

std::optional<Float64> ColumnStatistics::estimateRange(const Range & range) const
{
    if (range.empty())
        return 0;

    if (range.isInfinite())
        return static_cast<Float64>(getNonNullRowCount());

    if (range.left == range.right)
        return estimateEqual(range.left);

    if (range.left.isNegativeInfinity())
        return estimateLess(range.right);

    if (range.right.isPositiveInfinity())
        return estimateGreater(range.left);

    auto right_count = estimateLess(range.right);
    auto left_count = estimateLess(range.left);
    if (!right_count || !left_count)
        return std::nullopt;
    return *right_count - *left_count;
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

bool ColumnStatistics::hasNullCount() const
{
    if (auto it = stats.find(StatisticsType::Basic); it != stats.end())
        return assert_cast<const StatisticsBasic &>(*it->second).hasNullCount();
    return false;
}

UInt64 ColumnStatistics::getNullCount() const
{
    if (auto it = stats.find(StatisticsType::Basic); it != stats.end())
    {
        const auto & basic = assert_cast<const StatisticsBasic &>(*it->second);
        if (basic.hasNullCount())
            return basic.getNullCount();
    }
    return 0;
}

UInt64 ColumnStatistics::getNonNullRowCount() const
{
    if (hasNullCount())
    {
        UInt64 null_count = getNullCount();
        return null_count <= rows ? rows - null_count : 0;
    }
    return rows;
}

Float64 ColumnStatistics::estimateIsNull() const
{
    if (rows == 0)
        return 0.0;
    if (hasNullCount())
        return static_cast<Float64>(getNullCount()) / static_cast<Float64>(rows);
    return ConditionSelectivityEstimator::default_cond_equal_factor;
}

Float64 ColumnStatistics::estimateIsNotNull() const
{
    if (rows == 0)
        return 0.0;
    if (hasNullCount())
        return static_cast<Float64>(getNonNullRowCount()) / static_cast<Float64>(rows);
    return 1.0 - ConditionSelectivityEstimator::default_cond_equal_factor;
}

Estimate ColumnStatistics::getEstimate() const
{
    Estimate info;
    info.rows_count = rows;

    for (const auto & [type, _] : stats)
        info.types.insert(type);

    if (stats.contains(StatisticsType::Uniq))
        info.estimated_cardinality = stats.at(StatisticsType::Uniq)->estimateCardinality();

    if (auto it = stats.find(StatisticsType::Basic); it != stats.end())
    {
        const auto & basic_stats = assert_cast<const StatisticsBasic &>(*it->second);
        if (basic_stats.hasNumericMinMax())
        {
            if (!basic_stats.getMin().isNull())
                info.estimated_min = basic_stats.getMin();
            if (!basic_stats.getMax().isNull())
                info.estimated_max = basic_stats.getMax();
        }
        if (basic_stats.hasNullCount())
            info.estimated_null_count = basic_stats.getNullCount();
    }
    else if (auto minmax_it = stats.find(StatisticsType::MinMax); minmax_it != stats.end())
    {
        const auto & minmax_stats = assert_cast<const StatisticsMinMax &>(*minmax_it->second);
        if (!minmax_stats.getMin().isNull())
            info.estimated_min = minmax_stats.getMin();
        if (!minmax_stats.getMax().isNull())
            info.estimated_max = minmax_stats.getMax();
    }

    return info;
}

void ColumnStatistics::serialize(WriteBuffer & buf) const
{
    /// Layout (V4):
    ///   UInt16  version (= V4)
    ///   UInt64  stat_types_mask
    ///   UInt64  rows
    ///   For each set bit in mask (in ascending bit order):
    ///       UInt64  stat_size
    ///       <stat_size> bytes of per-statistics payload
    /// The per-stat size prefix lets a reader skip statistics types it doesn't recognize.
    writeIntBinary(StatisticsFileVersion::V4, buf);

    UInt64 stat_types_mask = 0;
    for (const auto & [type, _]: stats)
        stat_types_mask |= 1ULL << static_cast<UInt8>(type);

    writeIntBinary(stat_types_mask, buf);

    /// As the column row count is always useful, save it in any case
    writeIntBinary(rows, buf);

    /// Write each statistics blob with a length prefix, by serializing into a temp buffer first.
    for (const auto & [type, stat_ptr] : stats)
    {
        String temp_data;
        WriteBufferFromString temp_buf(temp_data);
        stat_ptr->serialize(temp_buf);
        temp_buf.finalize();

        writeIntBinary(static_cast<UInt64>(temp_data.size()), buf);
        buf.write(temp_data.data(), temp_data.size());
    }
}

std::shared_ptr<ColumnStatistics> ColumnStatistics::deserialize(ReadBuffer & buf, const DataTypePtr & data_type)
{
    UInt16 version_raw;
    readIntBinary(version_raw, buf);
    auto version = static_cast<StatisticsFileVersion>(version_raw);

    /// `V3` was briefly written by reverted PR #102356 and is permanently reserved. Refuse to read it
    /// rather than silently misinterpret a `V3` payload as `V4`.
    if (version == StatisticsFileVersion::V3)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Statistics file version V3 is reserved and is never produced by any released build. "
            "Please run `ALTER TABLE [db.]table MATERIALIZE STATISTICS ALL` to regenerate the statistics.");

    if (version != StatisticsFileVersion::V1
        && version != StatisticsFileVersion::V2
        && version != StatisticsFileVersion::V4)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Tried to read statistics file with unsupported format version {}. "
            "Please run `ALTER TABLE [db.]table MATERIALIZE STATISTICS ALL` to regenerate the statistics.",
            version_raw);

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, buf);

    const auto & factory = MergeTreeStatisticsFactory::instance();
    ColumnStatisticsDescription stats_desc;
    stats_desc.data_type = data_type;

    if (version == StatisticsFileVersion::V4)
    {
        /// V4 layout: per-stat size prefix. Iterate every type bit in order; if the current build
        /// doesn't recognize the type, skip its `stat_size` payload bytes.
        UInt64 rows_value;
        readIntBinary(rows_value, buf);

        auto result = std::make_shared<ColumnStatistics>(stats_desc);
        result->rows = rows_value;

        for (size_t i = 0; i < static_cast<size_t>(StatisticsType::Max); ++i)
        {
            if (!(stat_types_mask & (1ULL << i)))
                continue;

            UInt64 stat_size;
            readIntBinary(stat_size, buf);

            auto type = static_cast<StatisticsType>(i);
            if (auto stat_ptr = factory.tryCreateSingle(type, data_type))
            {
                /// Track bytes consumed so we can detect a per-stat parser drift and either pad the
                /// remainder or refuse to continue on overrun (which would corrupt the next stat).
                const auto count_before = buf.count();
                stat_ptr->deserialize(buf, version);
                const auto consumed = static_cast<UInt64>(buf.count() - count_before);
                if (consumed < stat_size)
                    buf.ignore(stat_size - consumed);
                else if (consumed > stat_size)
                    throw Exception(
                        ErrorCodes::ILLEGAL_STATISTICS,
                        "Statistics deserialization for type {} consumed {} bytes but stat_size was {}. "
                        "The statistics file may be corrupted.",
                        statisticsTypeToString(type), consumed, stat_size);

                auto ast = make_intrusive<ASTIdentifier>(statisticsTypeToString(type));
                result->stats_desc.types_to_desc.emplace(type, SingleStatisticsDescription(type, ast, false));
                result->stats[type] = std::move(stat_ptr);
            }
            else
            {
                /// Unknown / unsupported statistics type for this column type. Skip it.
                buf.ignore(stat_size);
            }
        }

        return result;
    }

    /// V1 / V2 legacy path: no per-stat size prefix. All listed types must be known.
    std::vector<StatisticsType> stat_types;
    for (size_t i = 0; i < static_cast<UInt8>(StatisticsType::Max); ++i)
    {
        if (stat_types_mask & (1ULL << i))
            stat_types.push_back(static_cast<StatisticsType>(i));
    }
    stats_desc.types_to_desc = factory.get(stat_types, data_type);

    auto result = factory.get(stats_desc);
    readIntBinary(result->rows, buf);

    for (const auto & [_, desc] : result->stats)
        desc->deserialize(buf, version);

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

    registerValidator(StatisticsType::Basic, basicStatisticsValidator);
    registerCreator(StatisticsType::Basic, basicStatisticsCreator);

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
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'. Available types: 'basic', 'countmin', 'minmax', 'tdigest' and 'uniq'", type);

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
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type '{}'. Available types: 'basic', 'countmin', 'minmax', 'tdigest' and 'uniq'", type);

        auto ast = make_intrusive<ASTIdentifier>(statisticsTypeToString(type));
        SingleStatisticsDescription desc(type, ast, false);

        if (!it->second(desc, data_type))
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type '{}' does not support data type {}", type, data_type->getName());

        result.emplace(type, desc);
    }
    return result;
}

StatisticsPtr MergeTreeStatisticsFactory::tryCreateSingle(StatisticsType type, const DataTypePtr & data_type) const
{
    auto vit = validators.find(type);
    if (vit == validators.end())
        return nullptr;

    auto ast = make_intrusive<ASTIdentifier>(statisticsTypeToString(type));
    SingleStatisticsDescription desc(type, ast, false);

    if (!vit->second(desc, data_type))
        return nullptr;

    auto cit = creators.find(type);
    if (cit == creators.end())
        return nullptr;

    return cit->second(desc, data_type);
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
