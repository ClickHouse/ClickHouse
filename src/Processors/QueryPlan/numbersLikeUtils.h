#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/SelectQueryInfo.h>

#include <cstddef>
#include <optional>

namespace DB
{

struct Settings;

namespace NumbersLikeUtils
{

/// Whether we should push limit down to scan.
inline bool shouldPushdownLimit(const SelectQueryInfo & query_info, const InterpreterSelectQuery::LimitInfo & lim_info)
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
        /// For new analyzer, window will be deleted from AST, so we should not use query.window()
        && !query_info.has_window
        && !query_info.additional_filter_ast
        && !query.limit_with_ties;
}

/// TODO: This is ideologically wrong. We should only get it from the query plan optimization.
inline std::optional<size_t> getLimitFromQueryInfo(const SelectQueryInfo & query_info, const ContextPtr & context)
{
    if (!query_info.query)
        return {};

    const auto lim_info = InterpreterSelectQuery::getLimitLengthAndOffset(query_info.query->as<ASTSelectQuery &>(), context);

    if (!shouldPushdownLimit(query_info, lim_info))
        return {};

    return lim_info.limit_length + lim_info.limit_offset;
}

/// Fail fast if estimated number of rows to read exceeds the limit.
void checkLimits(const Settings & settings, size_t rows);

}

}
