#pragma once

#include <unordered_set>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Waits for every producer's publication stream to finish, then adds the ordinary aggregation
/// merge processors to the executing pipeline. Completion ports carry only stream completion.
class AdaptiveAggregationMergeTransform final : public IProcessor
{
public:
    AdaptiveAggregationMergeTransform(
        AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_,
        size_t max_threads_, size_t temporary_data_merge_threads_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "AdaptiveAggregationMergeTransform"; }
    Status prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts &) override;
    void work() override;
    PipelineUpdate updatePipeline() override;
    void onCancel() noexcept override;

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataPtr many_data;
    AdaptiveAggregationSessionPtr session;
    size_t max_threads;
    size_t temporary_data_merge_threads;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    Processors processors;
    std::unordered_set<const InputPort *> unfinished_inputs;
    bool inputs_initialized = false;
    enum class Stage
    {
        WaitingForInputs,
        ExpandingPipeline,
        ReadingMerge,
    };
    Stage stage = Stage::WaitingForInputs;
};

}
