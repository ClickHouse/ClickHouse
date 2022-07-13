#include <Processors/Transforms/Cuda/CudaAggregatingTransform.h>

#include <Formats/NativeReader.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Core/ProtocolDefines.h>

#include <Common/logger_useful.h>
#include <Common/StackTrace.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Convert block to chunk.
/// Adds additional info about aggregation.
Chunk convertToChunk(const Block & block);


/// Generates chunks with aggregated data.
/// In single level case, aggregates data itself.
/// In two-level case, creates `ConvertingAggregatedToChunksSource` workers:
///
/// ConvertingAggregatedToChunksSource ->
/// ConvertingAggregatedToChunksSource -> ConvertingAggregatedToChunksTransform -> AggregatingTransform
/// ConvertingAggregatedToChunksSource ->
///
/// Result chunks guaranteed to be sorted by bucket number.
class CudaConvertingAggregatedToChunksTransform : public IProcessor
{
public:
    CudaConvertingAggregatedToChunksTransform(CudaAggregatingTransformParamsPtr params_, CudaAggregatedDataVariantsPtr variants_, size_t /*num_threads_*/)
        : IProcessor({}, {params_->getHeader()})
        , params(std::move(params_)), variants(std::move(variants_)) {
            // LOG_FATAL(&Poco::Logger::root(), "# CudaConvertingAggregatedToChunksTransform::constructor()");
        }

    String getName() const override { return "CudaConvertingAggregatedToChunksTransform"; }

    void work() override
    {
        if (variants->empty())
        {
            finished = true;
            return;
        }

        if (!is_initialized)
        {
            initialize();
            return;
        }

        mergeSingleLevel();
    }

    Processors expandPipeline() override
    {
        for (auto & source : processors)
        {
            auto & out = source->getOutputs().front();
            inputs.emplace_back(out.getHeader(), this);
            connect(out, inputs.back());
            inputs.back().setNeeded();
        }

        return std::move(processors);
    }

    IProcessor::Status prepare() override
    {
        auto & output = outputs.front();

        if (finished && !has_input)
        {
            output.finish();
            return Status::Finished;
        }

        /// Check can output.
        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();

            return Status::Finished;
        }

        if (!output.canPush())
            return Status::PortFull;

        if (!is_initialized)
            return Status::Ready;

        if (!processors.empty())
            return Status::ExpandPipeline;

        if (has_input)
            return preparePushToOutput();

        /// Single level case.
        if (inputs.empty())
            return Status::Ready;

        /// Two-level case.
        throw std::runtime_error("Something Bad happened here");
        // return prepareTwoLevel();
    }

private:
    IProcessor::Status preparePushToOutput()
    {
        // LOG_FATAL(&Poco::Logger::root(), "# {}:{} - {}::preparePushToOutput(1)", __FILE__, __LINE__, getName());
        auto & output = outputs.front();
        output.push(std::move(current_chunk));
        has_input = false;

        if (finished)
        {
            output.finish();
            return Status::Finished;
        }

        return Status::PortFull;
    }

    CudaAggregatingTransformParamsPtr params;
    CudaAggregatedDataVariantsPtr variants;

    bool is_initialized = false;
    bool has_input = false;
    bool finished = false;

    Chunk current_chunk;

    static constexpr Int32 NUM_BUCKETS = 256;
    std::array<Chunk, NUM_BUCKETS> chunks;

    Processors processors;

    void setCurrentChunk(Chunk chunk)
    {
        if (has_input)
            throw Exception("Current chunk was already set in "
                            "CudaConvertingAggregatedToChunksTransform.", ErrorCodes::LOGICAL_ERROR);

        has_input = true;
        current_chunk = std::move(chunk);
    }

    void initialize()
    {
        is_initialized = true;
    }

    void mergeSingleLevel()
    {
        if (!variants->waitProcessed())
        {
            finished = true;
            return;
        }

        auto block = params->aggregator.prepareBlockAndFillSingleLevel(*variants, params->final);

        setCurrentChunk(convertToChunk(block));
        finished = true;
    }

};

CudaAggregatingTransform::CudaAggregatingTransform(Block header, CudaAggregatingTransformParamsPtr params_, ContextPtr context_)
    : CudaAggregatingTransform(std::move(header), std::move(params_)
    , std::make_unique<CudaAggregatedDataVariants>(1), 0, 1, context_)
{
}

CudaAggregatingTransform::CudaAggregatingTransform(
    Block header,
    CudaAggregatingTransformParamsPtr params_,
    CudaAggregatedDataVariantsPtr variants_,
    size_t /*current_variant*/,
    size_t /*max_threads_*/,
    ContextPtr context_)
    // size_t temporary_data_merge_threads_)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , context(context_)
    , params(std::move(params_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , variants(std::move(variants_))
{
    // LOG_FATAL(&Poco::Logger::root(), "# CudaAggregatingTransform::constructor()");
}

CudaAggregatingTransform::~CudaAggregatingTransform() = default;

IProcessor::Status CudaAggregatingTransform::prepare()
{
    /// There are one or two input ports.
    /// The first one is used at aggregation step, the second one - while reading merged data from ConvertingAggregated

    auto & output = outputs.front();
    /// Last output is current. All other outputs should already be closed.
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Finish data processing, prepare to generating.
    if (is_consume_finished && !is_generate_initialized)
    {
        /// Close input port in case max_rows_to_group_by was reached but not all data was read.
        inputs.front().close();

        return Status::Ready;
    }

    if (is_generate_initialized && !is_pipeline_created && !processors.empty())
        return Status::ExpandPipeline;

    /// Only possible while consuming.
    if (read_current_chunk)
        return Status::Ready;

    /// Get chunk from input.
    if (input.isFinished())
    {
        if (is_consume_finished)
        {
            // LOG_FATAL(&Poco::Logger::root(), "# CudaAggregatingTransform - input.isFinished() && is_consume_finished");
            output.finish();
            return Status::Finished;
        }
        else
        {
            // LOG_FATAL(&Poco::Logger::root(), "# CudaAggregatingTransform - input.isFinished()");
            variants->stop();
            /// Finish data processing and create another pipe.
            is_consume_finished = true;
            return Status::Ready;
        }
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    if (is_consume_finished)
        input.setNeeded();

    current_chunk = input.pull(/*set_not_needed = */ !is_consume_finished);
    read_current_chunk = true;

    if (is_consume_finished)
    {
        output.push(std::move(current_chunk));
        read_current_chunk = false;
        return Status::PortFull;
    }

    return Status::Ready;
}

void CudaAggregatingTransform::work()
{
    if (is_consume_finished)
        initGenerate();
    else
    {
        consume(std::move(current_chunk));
        read_current_chunk = false;
    }
}

Processors CudaAggregatingTransform::expandPipeline()
{
    auto & out = processors.back()->getOutputs().front();
    inputs.emplace_back(out.getHeader(), this);
    connect(out, inputs.back());
    is_pipeline_created = true;
    return std::move(processors);
}

void CudaAggregatingTransform::consume(Chunk chunk)
{
    const UInt64 num_rows = chunk.getNumRows();

    if (num_rows == 0 && params->params.empty_result_for_aggregation_by_empty_set)
        return;

    if (!is_consume_started)
    {
        LOG_TRACE(log, "Aggregating");
        is_consume_started = true;
    }

    src_rows += num_rows;
    src_bytes += chunk.bytes();

    if (!params->aggregator.executeOnBlock(chunk.detachColumns(), num_rows, *variants, key_columns, aggregate_columns, no_more_keys))
        is_consume_finished = true;
}

void CudaAggregatingTransform::initGenerate()
{
    if (is_generate_initialized)
        return;

    is_generate_initialized = true;
    processors.emplace_back(std::make_shared<CudaConvertingAggregatedToChunksTransform>(params, variants, max_threads));
}

}
