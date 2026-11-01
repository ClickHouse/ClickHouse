#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopK.h>

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/BoundingMode.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/CandidateMode.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Memo.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Mode.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopKImpl.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Passes.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Pilot.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <deque>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationMultiPassTopKDeclines;
}

namespace DB
{

namespace MultiPassTopKImpl
{

namespace
{

/// The pipeline of a pass, with one output.
Pipe buildPass(const MultiPassTopKTemplate & templ, QueryPlan plan, QueryPlanResourceHolder & resources)
{
    auto builder = plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(templ.input->context), BuildQueryPipelineSettings(templ.input->context), /*do_optimize=*/false);
    auto pipe = QueryPipelineBuilder::getPipe(std::move(*builder), resources);
    pipe.resize(1);
    return pipe;
}

/// The rows of `chunks`, of the structure of `header`, in one block.
Block concatenate(const Block & header, const Chunks & chunks)
{
    MutableColumns columns = header.cloneEmptyColumns();
    for (const auto & chunk : chunks)
    {
        const size_t num_rows = chunk.getNumRows();
        const auto & chunk_columns = chunk.getColumns();
        for (size_t i = 0; i < columns.size(); ++i)
            columns[i]->insertRangeFrom(*chunk_columns[i], 0, num_rows);
    }
    return header.cloneWithColumns(std::move(columns));
}

/// Whether the controller of a multi-pass top-K aggregation runs the original aggregation. Set before the
/// controller sends anything to the gates.
using TopKDecisionPtr = std::shared_ptr<std::atomic<bool>>;

/// Passes one stream of the output of a multi-pass top-K aggregation: the stream of the original aggregation when
/// the controller declines, or else what the controller sends on its control input, the result for the first gate.
/// Each stream of the original aggregation has its gate, so a declined aggregation runs with the parallelism it has
/// without the strategy, and the original aggregation runs only when the controller declines.
class MultiPassTopKGateTransform final : public IProcessor
{
public:
    MultiPassTopKGateTransform(SharedHeader header, TopKDecisionPtr run_original_)
        : IProcessor({header, header}, {header})
        , run_original(std::move(run_original_))
    {
    }

    String getName() const override { return "MultiPassTopKGateTransform"; }

    Status prepare() override
    {
        auto & original = inputs.front();
        auto & control = inputs.back();
        auto & output = outputs.front();

        if (output.isFinished())
        {
            original.close();
            control.close();
            return Status::Finished;
        }

        if (!source)
        {
            control.setNeeded();
            if (!control.hasData() && !control.isFinished())
                return Status::NeedData;
            if (run_original->load(std::memory_order_acquire))
            {
                source = &original;
                control.close();
            }
            else
            {
                source = &control;
                original.close();
            }
        }

        if (source->isFinished())
        {
            output.finish();
            return Status::Finished;
        }
        if (!output.canPush())
        {
            source->setNotNeeded();
            return Status::PortFull;
        }
        source->setNeeded();
        if (!source->hasData())
            return Status::NeedData;
        output.push(source->pull());
        return Status::PortFull;
    }

private:
    const TopKDecisionPtr run_original;
    InputPort * source = nullptr;
};

/// Runs the passes of a multi-pass top-K aggregation, which its mode plans (see `TopKMode`). Its first input is the
/// output of its first pass (see `startFirstPass`); the passes it starts later are plans of their own, built when they
/// start and added to the running pipeline as inputs. It collects the output of every pass, and its mode decides what
/// follows it. Its outputs are the control inputs of the gates of the streams (see `MultiPassTopKGateTransform`): it
/// sends its result to the first gate, or makes all the gates pass the streams of the original aggregation when it
/// declines.
class MultiPassTopKController final : public IProcessor
{
public:
    MultiPassTopKController(
        const SharedHeader & header,
        size_t num_streams,
        MultiPassTopKTemplatePtr templ_,
        TopKRunMemoPtr memo_,
        TopKModePtr mode_,
        TopKDecisionPtr run_original_)
        : IProcessor(InputPorts{}, OutputPorts(num_streams, header))
        , templ(std::move(templ_))
        , memo(std::move(memo_))
        , mode(std::move(mode_))
        , run_original(std::move(run_original_))
    {
    }

    String getName() const override { return "MultiPassTopKController"; }

    /// Builds the first pass of the mode and returns its pipeline, whose output is the first input. Called once, before
    /// the pipeline runs.
    Pipe startFirstPass(QueryPlanResourceHolder & first_resources)
    {
        Pipe pipe = buildPass(*templ, mode->firstPass(), first_resources);
        inputs.emplace_back(pipe.getHeader(), this);
        current = &inputs.back();
        return pipe;
    }

    Status prepare() override
    {
        if (new_output)
            return Status::UpdatePipeline;

        if (allOutputsFinished())
        {
            closeInputs();
            return Status::Finished;
        }

        switch (stage)
        {
            case Stage::Collecting:
                return prepareCollecting();
            case Stage::PassFinished:
                return Status::Ready;
            case Stage::Declined:
                run_original->store(true, std::memory_order_release);
                for (auto & output : outputs)
                    output.finish();
                closeInputs();
                return Status::Finished;
            case Stage::Emitting:
                return prepareEmitting();
        }
        UNREACHABLE();
    }

    /// Asks the mode what follows the pass that finished: `prepare` returns `Ready` only then.
    void work() override
    {
        TopKNext next = mode->afterPass(takeOutput());
        for (auto & chunk : next.result)
            to_emit.push_back(std::move(chunk));
        if (next.decline)
        {
            decline(next.decline);
        }
        else if (next.next_mode)
        {
            mode = std::move(next.next_mode);
            startPass(mode->firstPass());
        }
        else if (next.next_pass)
        {
            startPass(std::move(*next.next_pass));
        }
        else
        {
            stage = Stage::Emitting;
        }
    }

    PipelineUpdate updatePipeline() override
    {
        inputs.emplace_back(new_output->getHeader(), this);
        connect(*new_output, inputs.back());
        current = &inputs.back();
        current->setNeeded();
        new_output = nullptr;
        return PipelineUpdate{.to_add = std::move(new_processors), .to_remove = {}};
    }

private:
    enum class Stage : uint8_t
    {
        /// Collects the output of the running pass.
        Collecting,
        /// The running pass finished, and `work` asks the mode what follows it.
        PassFinished,
        /// The original aggregation runs.
        Declined,
        /// Emits the result on the first output.
        Emitting,
    };

    const MultiPassTopKTemplatePtr templ;
    const TopKRunMemoPtr memo;
    /// The mode whose pass runs.
    TopKModePtr mode;
    const TopKDecisionPtr run_original;
    const LoggerPtr log = getLogger("MultiPassTopK");

    Stage stage = Stage::Collecting;

    /// The output of the running pass, the pipeline of the next pass until `updatePipeline` adds it, and the resources
    /// of the passes.
    InputPort * current = nullptr;
    OutputPort * new_output = nullptr;
    Processors new_processors;
    std::vector<QueryPlanResourceHolder> resources;

    /// The output of the running pass, collected for its end.
    Chunks collected;

    /// The chunks to emit on the first output.
    std::deque<Chunk> to_emit;
    bool emitting_started = false;

    void closeInputs()
    {
        for (auto & input : inputs)
            input.close();
    }

    bool allOutputsFinished() const
    {
        return std::ranges::all_of(outputs, [](const OutputPort & output) { return output.isFinished(); });
    }

    Status prepareCollecting()
    {
        while (true)
        {
            if (current->isFinished())
            {
                stage = Stage::PassFinished;
                return Status::Ready;
            }
            current->setNeeded();
            if (!current->hasData())
                return Status::NeedData;
            collected.push_back(current->pull());
        }
    }

    /// The output of the pass that finished, in one block.
    Block takeOutput()
    {
        Block output = concatenate(current->getHeader(), collected);
        collected.clear();
        return output;
    }

    /// Emits `to_emit` on the first output.
    Status prepareEmitting()
    {
        if (!emitting_started)
        {
            emitting_started = true;
            for (auto output = std::next(outputs.begin()); output != outputs.end(); ++output)
                output->finish();
        }

        auto & output = outputs.front();
        if (output.isFinished())
        {
            closeInputs();
            return Status::Finished;
        }
        if (!output.canPush())
            return Status::PortFull;
        if (!to_emit.empty())
        {
            output.push(std::move(to_emit.front()));
            to_emit.pop_front();
            return Status::PortFull;
        }
        output.finish();
        closeInputs();
        return Status::Finished;
    }

    void startPass(QueryPlan plan)
    {
        QueryPlanResourceHolder pass_resources;
        auto pipe = buildPass(*templ, std::move(plan), pass_resources);
        new_output = pipe.getOutputPort(0);
        new_processors = Pipe::detachProcessors(std::move(pipe));
        resources.push_back(std::move(pass_resources));
        stage = Stage::Collecting;
    }

    void decline(const char * reason)
    {
        LOG_DEBUG(log, "Running the original aggregation: {}", reason);
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMultiPassTopKDeclines);
        memo->keepOriginal();
        stage = Stage::Declined;
    }
};

MultiPassTopKTemplatePtr makeTemplate(std::unique_ptr<AggregatingStep> aggregating, MultiPassTopKInputPtr input)
{
    auto templ = std::make_shared<MultiPassTopKTemplate>();
    const auto & params = aggregating->getParams();
    templ->top = input->top;
    templ->rank_index = input->rank_index;
    for (size_t i = 0; i < params.aggregates.size(); ++i)
    {
        const auto & aggregate = params.aggregates[i];
        if (aggregate.function->getName() == "count" && aggregate.argument_names.empty() && aggregate.parameters.empty())
        {
            templ->rows_index = i;
            break;
        }
    }
    templ->input = std::move(input);
    templ->aggregating = std::move(aggregating);
    return templ;
}

}

}

using namespace MultiPassTopKImpl;

void addMultiPassTopK(QueryPipelineBuilder & pipeline, std::unique_ptr<AggregatingStep> aggregating, MultiPassTopKInputPtr input)
{
    const auto templ = makeTemplate(std::move(aggregating), std::move(input));

    /// A query for which an earlier run chose the original aggregation keeps its pipeline intact. Another memo chooses
    /// the mode that the run starts with, and a run without one starts with the pilot.
    auto start = memoForRun(*templ);
    if (start && start->mode == TopKMemo::Mode::Original)
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMultiPassTopKDeclines);
        return;
    }
    auto memo = std::make_shared<TopKRunMemo>(*templ, start);
    TopKModePtr mode;
    if (!start)
        mode = makePilot(templ, memo);
    else if (start->mode == TopKMemo::Mode::Candidates)
        mode = makeCandidateMode(templ, memo, start->candidate_keys);
    else
        /// Without a sample, only the n-th best rank of the first restricted pass bounds the values that the hot pass
        /// takes.
        mode = makeBoundingMode(templ, memo, /*min_key_rows=*/0);

    /// The first pass is part of the pipeline from the start, so its sources start with all the threads of the
    /// query, as those of the original aggregation would.
    const auto header = pipeline.getSharedHeader();
    auto run_original = std::make_shared<std::atomic<bool>>(false);
    auto controller = std::make_shared<MultiPassTopKController>(
        header, pipeline.getNumStreams(), templ, std::move(memo), std::move(mode), run_original);
    QueryPlanResourceHolder first_resources;
    Pipe first = controller->startFirstPass(first_resources);
    pipeline.addResources(first_resources);

    pipeline.transform([&](OutputPortRawPtrs streams)
    {
        connect(*first.getOutputPort(0), controller->getInputs().front());
        Processors processors = Pipe::detachProcessors(std::move(first));
        auto control = controller->getOutputs().begin();
        for (auto * stream : streams)
        {
            auto gate = std::make_shared<MultiPassTopKGateTransform>(header, run_original);
            connect(*stream, gate->getInputs().front());
            connect(*control++, gate->getInputs().back());
            processors.push_back(std::move(gate));
        }
        processors.push_back(std::move(controller));
        return processors;
    });
}

}
