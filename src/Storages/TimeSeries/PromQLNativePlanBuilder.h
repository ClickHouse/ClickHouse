#pragma once

#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>


namespace DB
{

class PrometheusQueryTree;
class QueryPlan;

/// Checks whether an exact range-sum subtree and every samples target which the
/// selector may choose satisfy the native `VECTOR_GRID` contract.
bool canBuildPromQLNativeVectorGridPlan(
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    ContextPtr context);

/// Builds and materializes the exact selector-id set used by a hybrid native fragment.
/// Returns null when the fragment is unsupported or its selected identifiers do not satisfy
/// the native one-identifier-per-full-tag-set invariant. The returned ready set is reused by
/// the fragment plan, so admission and execution observe the same identifier snapshot.
BuiltSetsByHashPtr tryPreparePromQLNativeVectorGridPlan(
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings);

/// Builds the native execution plan for a supported PromQL expression.
/// Returns false without changing `query_plan` when the expression or storage layout is unsupported.
bool tryBuildPromQLNativePlan(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    size_t max_output_groups);

/// Builds the native plan for one exact range-sum subtree and exposes its
/// internal `VECTOR_GRID` contract: `group UInt64`, `values Array(Nullable(T))`.
bool tryBuildPromQLNativeVectorGridPlan(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    const PrometheusQueryTree & promql_query,
    const PrometheusQueryEvaluationSettings & evaluation_settings,
    size_t max_output_groups,
    BuiltSetsByHashPtr prepared_identifier_sets = nullptr);

}
