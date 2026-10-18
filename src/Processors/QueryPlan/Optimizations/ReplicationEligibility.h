#pragma once

#include <base/types.h>
#include <optional>
#include <string>
#include <string_view>

namespace DB
{

/// What a planner knows about the size of a result it wants to replicate to every node: a
/// broadcast join side, a table read that every node repeats, a subplan that every node computes.
struct ReplicationSize
{
    /// Estimated rows; none when there is no estimate.
    std::optional<UInt64> estimated_rows = std::nullopt;
    /// The estimate has an imprecise part: a default selectivity, a key without statistics, statistics of only some
    /// parts, the rows of a past execution, and the like. Such an estimate is a guess that cannot decide a replication alone.
    bool imprecise_estimate = false;
    /// Proven upper bound on the rows; none when there is none.
    std::optional<UInt64> max_rows = std::nullopt;
    /// Average bytes of one row.
    Float64 bytes_per_row = 1.0;
    /// Bytes every node reads to produce the result, when known: the scan volume of a read, which a
    /// filter the primary key cannot prune leaves far above the rows the read outputs.
    std::optional<UInt64> scan_bytes = std::nullopt;
};

struct ReplicationBudget
{
    /// Most rows to replicate; none for no row limit.
    std::optional<UInt64> max_rows = std::nullopt;
    /// Most modeled bytes to replicate; zero for no byte budget.
    UInt64 max_bytes = 0;
};

struct ReplicationDecision
{
    bool allowed = false;
    /// Rows the limits counted: the estimate when it is precise, the bound otherwise.
    std::optional<UInt64> counted_rows = std::nullopt;
    /// `counted_rows` times the row width.
    std::optional<UInt64> modeled_bytes = std::nullopt;
    std::string_view reason = {};

    /// The inputs, the budget and the reason in one line, for a log.
    String describe(const ReplicationSize & size, const ReplicationBudget & budget) const;
};

/// Decides whether a result may be replicated to every node: the row limit and the byte budget (rows times the row
/// width) on the estimate when it is precise, else on the bound; a result with neither is never replicated. It checks
/// the planners' model only: the row width is an average and the bytes have no hash table or concurrency terms, so an
/// allowed replication has no memory bound, and a precise estimate can still be wrong.
ReplicationDecision decideReplication(const ReplicationSize & size, const ReplicationBudget & budget);

}
