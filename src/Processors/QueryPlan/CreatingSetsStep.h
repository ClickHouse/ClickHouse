#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

class PreparedSetsCache;
using PreparedSetsCachePtr = std::shared_ptr<PreparedSetsCache>;

struct QueryPlanOptimizationSettings;

/// Creates sets for subqueries and JOIN. See CreatingSetsTransform.
class CreatingSetStep : public ITransformingStep
{
public:
    CreatingSetStep(
        const SharedHeader & input_header_,
        SetAndKeyPtr set_and_key_,
        SizeLimits network_transfer_limits_,
        PreparedSetsCachePtr prepared_sets_cache_);

    String getName() const override { return "CreatingSet"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool hasExternalTable() const { return set_and_key->external_table != nullptr; }

    /// Deduplicate each input stream independently before the single set-filling transform. Correct for
    /// any input (the set deduplicates anyway); enabled when the input streams carry disjoint sets of the
    /// key values, so per-stream deduplication is complete and the filling transform only hashes unique rows.
    void enablePreliminaryDistinct() { preliminary_distinct = true; }

private:
    void updateOutputHeader() override;

    SetAndKeyPtr set_and_key;
    SizeLimits network_transfer_limits;
    PreparedSetsCachePtr prepared_sets_cache;
    bool preliminary_distinct = false;
};

class CreatingSetsStep : public IQueryPlanStep
{
public:
    explicit CreatingSetsStep(SharedHeaders input_headers_);

    String getName() const override { return "CreatingSets"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override { output_header = getInputHeaders().front(); }
};

/// This is a temporary step which is converted to CreatingSetStep after plan optimization.
/// Can't be used by itself.
class DelayedCreatingSetsStep final : public IQueryPlanStep
{
public:
    DelayedCreatingSetsStep(
        SharedHeader input_header,
        PreparedSets::Subqueries subqueries_,
        SizeLimits network_transfer_limits_,
        PreparedSetsCachePtr prepared_sets_cache_);

    String getName() const override { return "DelayedCreatingSets"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &) override;

    static std::vector<std::unique_ptr<QueryPlan>> makePlansForSets(
        DelayedCreatingSetsStep && step,
        const QueryPlanOptimizationSettings & optimization_settings);

    SizeLimits getNetworkTransferLimits() const { return network_transfer_limits; }
    PreparedSetsCachePtr getPreparedSetsCache() const { return prepared_sets_cache; }

    const PreparedSets::Subqueries & getSets() const { return subqueries; }
    PreparedSets::Subqueries detachSets() { return std::move(subqueries); }

    void serialize(Serialization &) const override {}
    bool isSerializable() const override { return true; }

private:
    void updateOutputHeader() override { output_header = getInputHeaders().front(); }

    PreparedSets::Subqueries subqueries;
    SizeLimits network_transfer_limits;
    PreparedSetsCachePtr prepared_sets_cache;
};

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSets::Subqueries subqueries, ContextPtr context);

void addDelayedCreatingSetsStep(QueryPlan & query_plan, PreparedSetsPtr prepared_sets, ContextPtr context);

QueryPipelineBuilderPtr addCreatingSetsTransform(QueryPipelineBuilderPtr pipeline, PreparedSets::Subqueries subqueries, ContextPtr context);

}
