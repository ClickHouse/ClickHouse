#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <array>
#include <optional>


namespace DB
{

struct PromQLRangeSumByQuery
{
    Strings labels_to_keep;
    PrometheusQueryTree::MatcherList matchers;
    PrometheusQueryTree::DurationType window{};
};

struct PromQLRangeRateQuery
{
    PrometheusQueryTree::MatcherList matchers;
    PrometheusQueryTree::DurationType window{};
    const PrometheusQueryTree::Node * node = nullptr;
};

/// The narrow contract for the two leaves of the exact D06 binary candidate.
/// `common_matchers` is a canonical multiset with the `__name__` matcher
/// removed; `node` remains available to callers that need to clone the leaf.
struct PromQLTwoRangeRateQuery
{
    String metric_name;
    PrometheusQueryTree::MatcherList common_matchers;
    PrometheusQueryTree::DurationType window{};
    const PrometheusQueryTree::Node * node = nullptr;
};

/// Exact default one-to-one addition of two compatible rate selectors.
struct PromQLTwoRangeRatesQuery
{
    std::array<PromQLTwoRangeRateQuery, 2> rates;
    const PrometheusQueryTree::Node * binary_node = nullptr;
};

/// Exact hybrid shape used by ClockBench D06. `binary_node` lets the planner
/// replace both `rate` leaves and their one-to-one addition with one fused
/// native fragment while SQL remains responsible for the outer aggregation
/// and `ceil`.
struct PromQLTwoRangeRatesSumByQuery
{
    Strings labels_to_keep;
    std::array<PromQLTwoRangeRateQuery, 2> rates;
    const PrometheusQueryTree::Node * binary_node = nullptr;
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

/// Extracts one exact `rate(<instant-selector>[<positive-range>])` subtree.
std::optional<PromQLRangeRateQuery> extractPromQLRangeRateQuery(
    const PrometheusQueryTree::Node * root,
    bool is_query_range);

/// Extracts the exact native island
/// `rate(<selector-a>[<range>]) + rate(<selector-b>[<range>])`.
/// Only default one-to-one vector matching is accepted.
std::optional<PromQLTwoRangeRatesQuery> extractPromQLTwoRangeRatesQuery(
    const PrometheusQueryTree::Node * root,
    bool is_query_range);

/// Extracts the bounded hybrid shape
/// `ceil(sum by (<labels>) (rate(<selector>[<range>]) + rate(<selector>[<range>])))`.
/// Only default one-to-one vector matching is accepted.
std::optional<PromQLTwoRangeRatesSumByQuery> extractPromQLTwoRangeRatesSumByQuery(
    const PrometheusQueryTree & query_tree,
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
