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
    /// A default selectivity stood in for a predicate somewhere in the estimate, so the estimate is a guess.
    bool estimate_from_defaults = false;
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
    /// Rows the byte budget counted: the estimate when it is trusted, the bound otherwise.
    std::optional<UInt64> counted_rows = std::nullopt;
    /// `counted_rows` times the row width.
    std::optional<UInt64> modeled_bytes = std::nullopt;
    std::string_view reason = {};

    /// The inputs, the budget and the reason in one line, for a log.
    String describe(const ReplicationSize & size, const ReplicationBudget & budget) const;
};

/// Decides whether a result may be replicated to every node: the row limit on the estimate, or on
/// the bound without one; the byte budget (rows times the row width) on the estimate when no
/// default went into it, else on the bound. A result with neither is never replicated.
ReplicationDecision decideReplication(const ReplicationSize & size, const ReplicationBudget & budget);

}
