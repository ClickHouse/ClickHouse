#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <optional>


namespace DB
{

struct PromQLRangeSumByQuery
{
    Strings labels_to_keep;
    PrometheusQueryTree::MatcherList matchers;
    PrometheusQueryTree::DurationType window{};
};

/// The native topk/bottomk extension is intentionally bounded. The range-sum
/// kernel bounds the number of groups; this cap bounds the requested number
/// of retained candidates per step. Exact ranking still needs the merged
/// candidate grids, whose size depends on input cardinality and query range.
inline constexpr UInt64 PROMQL_NATIVE_TOPK_MAX_K = 1024;

struct PromQLRangeTopKByQuery
{
    PromQLRangeSumByQuery range_sum;
    UInt64 k = 0;
    bool bottomk = false;
};

/// Extracts the complete input needed by the native execution path for the exact query-range shape
/// `sum by (<labels>) (rate(<instant-selector>[<range>]))`.
std::optional<PromQLRangeSumByQuery> extractPromQLRangeSumByQuery(
    const PrometheusQueryTree & query_tree,
    bool is_query_range);

/// Extracts the same native range-sum shape from a subtree. The endpoint owner
/// still supplies the query-range flag; callers must ensure that the subtree's
/// evaluation range is compatible with the native fragment settings.
std::optional<PromQLRangeSumByQuery> extractPromQLRangeSumByQuery(
    const PrometheusQueryTree::Node * root,
    bool is_query_range);

/// Extracts the exact bounded extension
/// `topk|bottomk(<constant-k>, sum by (<labels>) (rate(<instant-selector>[<range>])))`.
/// The topk/bottomk operator itself must not have a by/without modifier.
std::optional<PromQLRangeTopKByQuery> extractPromQLRangeTopKByQuery(
    const PrometheusQueryTree & query_tree,
    bool is_query_range);

/// Returns true only for the supported query-range shape:
/// sum by (<labels>) (rate(<instant-selector>[<range>])).
/// The query-range flag is supplied by the endpoint owner; this helper does not inspect or execute any data.
bool isSupportedPromQLRangeQuery(const PrometheusQueryTree & query_tree, bool is_query_range);

}
