#include <Processors/QueryPlan/numbersLikeUtils.h>

#include <algorithm>

#include <Core/Settings.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/SizeLimits.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 max_rows_to_read;
extern const SettingsUInt64 max_rows_to_read_leaf;
extern const SettingsOverflowMode read_overflow_mode;
extern const SettingsOverflowMode read_overflow_mode_leaf;
}

namespace ErrorCodes
{
extern const int TOO_MANY_ROWS;
}

namespace NumbersLikeUtils
{

ExtractedRanges extractRanges(const KeyCondition & condition)
{
    ExtractedRanges result;

    if (condition.extractPlainRanges(result.ranges))
    {
        result.kind = ExtractedRanges::Kind::ExactRanges;
        return result;
    }

    result.kind = ExtractedRanges::Kind::ConservativeRanges;
    result.ranges = condition.extractBounds();
    return result;
}

void applyQueryLimit(std::optional<UInt64> & effective_limit, const std::optional<size_t> & query_limit)
{
    if (!query_limit)
        return;

    const UInt64 query_limit_64 = static_cast<UInt64>(*query_limit);
    effective_limit = std::min(effective_limit.value_or(query_limit_64), query_limit_64);
}

void addNullSource(Pipe & pipe, SharedHeader header)
{
    pipe.addSource(std::make_shared<NullSource>(std::move(header)));
}

namespace
{

bool shouldPushdownLimit(const SelectQueryInfo & query_info, const InterpreterSelectQuery::LimitInfo & lim_info)
{
    /// Reject negative, fractional, and zero limits for pushdown
    if (lim_info.is_limit_length_negative
        || lim_info.fractional_limit > 0
        || lim_info.fractional_offset > 0
        || lim_info.limit_length == 0)
        return false;

    chassert(query_info.query);

    const auto & query = query_info.query->as<ASTSelectQuery &>();

    /// Just ignore some minor cases, such as:
    ///     select * from system.numbers order by number asc limit 10
    return !query.distinct
        && !query.limitBy()
        && !query_info.has_order_by
        && !query_info.need_aggregate
        /// For the analyzer, window will be deleted from AST, so we should not use query.window()
        && !query_info.has_window
        && !query_info.additional_filter_ast
        && !query.limit_with_ties;
}

}

std::optional<size_t> getLimitFromQueryInfo(const SelectQueryInfo & query_info, const ContextPtr & context)
{
    if (!query_info.query)
        return {};

    const auto lim_info = InterpreterSelectQuery::getLimitLengthAndOffset(query_info.query->as<ASTSelectQuery &>(), context);

    if (!shouldPushdownLimit(query_info, lim_info))
        return {};

    return lim_info.limit_length + lim_info.limit_offset;
}

void checkLimits(const Settings & settings, size_t rows)
{
    if (settings[Setting::read_overflow_mode] == OverflowMode::THROW && settings[Setting::max_rows_to_read])
    {
        const auto limits = SizeLimits(settings[Setting::max_rows_to_read], 0, settings[Setting::read_overflow_mode]);
        limits.check(rows, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
    }

    if (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW && settings[Setting::max_rows_to_read_leaf])
    {
        const auto leaf_limits = SizeLimits(settings[Setting::max_rows_to_read_leaf], 0, settings[Setting::read_overflow_mode_leaf]);
        leaf_limits.check(rows, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
    }
}

}

}
