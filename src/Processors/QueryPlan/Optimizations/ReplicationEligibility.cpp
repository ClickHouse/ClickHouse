#include <Processors/QueryPlan/Optimizations/ReplicationEligibility.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>

#include <fmt/format.h>

namespace DB
{

namespace
{

String formatOptional(const std::optional<UInt64> & value)
{
    return value ? std::to_string(*value) : "unknown";
}

}

ReplicationDecision decideReplication(const ReplicationSize & size, const ReplicationBudget & budget)
{
    ReplicationDecision decision;

    if (!budget.max_rows && budget.max_bytes == 0)
    {
        decision.allowed = true;
        decision.reason = "no replication limit";
        return decision;
    }

    if (budget.max_rows)
    {
        const std::optional<UInt64> rows_for_limit = size.estimated_rows ? size.estimated_rows : size.max_rows;
        if (!rows_for_limit)
        {
            decision.reason = "the row count is unknown and has no bound";
            return decision;
        }
        if (*rows_for_limit > *budget.max_rows)
        {
            decision.counted_rows = rows_for_limit;
            decision.reason = "more rows than the row limit";
            return decision;
        }
    }

    if (budget.max_bytes == 0)
    {
        decision.allowed = true;
        decision.counted_rows = size.estimated_rows ? size.estimated_rows : size.max_rows;
        decision.reason = "within the row limit, no byte budget";
        return decision;
    }

    const bool trusted_estimate = size.estimated_rows && !size.estimate_from_defaults;
    decision.counted_rows = trusted_estimate ? size.estimated_rows : size.max_rows;
    if (!decision.counted_rows)
    {
        decision.reason = size.estimated_rows
            ? "the estimate took defaults and the rows have no bound"
            : "the row count is unknown and has no bound";
        return decision;
    }

    decision.modeled_bytes = ceilToRowCount(Float64(*decision.counted_rows) * size.bytes_per_row);
    if (*decision.modeled_bytes > budget.max_bytes)
    {
        decision.reason = trusted_estimate
            ? "the estimated bytes exceed the byte budget"
            : "the bytes of the row bound exceed the byte budget";
        return decision;
    }

    if (size.scan_bytes && *size.scan_bytes > budget.max_bytes)
    {
        decision.reason = "the bytes every node would read exceed the byte budget";
        return decision;
    }

    decision.allowed = true;
    decision.reason = trusted_estimate
        ? "the estimated bytes fit the byte budget"
        : "the bytes of the row bound fit the byte budget";
    return decision;
}

String ReplicationDecision::describe(const ReplicationSize & size, const ReplicationBudget & budget) const
{
    return fmt::format(
        "{}: {}; rows {}{}, bound {}, {:.1f} bytes per row, scan {} bytes, modeled {} bytes; limit {} rows, budget {} bytes",
        allowed ? "replication allowed" : "replication declined",
        reason,
        formatOptional(size.estimated_rows),
        size.estimated_rows && size.estimate_from_defaults ? " (from defaults)" : "",
        formatOptional(size.max_rows),
        size.bytes_per_row,
        formatOptional(size.scan_bytes),
        formatOptional(modeled_bytes),
        budget.max_rows ? std::to_string(*budget.max_rows) : "none",
        budget.max_bytes == 0 ? "none" : std::to_string(budget.max_bytes));
}

}
