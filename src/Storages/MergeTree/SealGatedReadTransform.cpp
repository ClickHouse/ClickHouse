#include <Storages/MergeTree/SealGatedReadTransform.h>

#include <Processors/Transforms/JoiningTransform.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

namespace DB
{

SealGatedReadTransform::SealGatedReadTransform(
    SharedHeader header,
    MergeTreeSelectProcessorPtr processor_,
    std::shared_ptr<RuntimeFilterReadRangesRefiner> refiner_,
    std::string log_name_)
    : IProcessor({std::make_shared<const Block>(Block{})}, {std::move(header)})
    , processor(std::move(processor_))
    , refiner(std::move(refiner_))
    , log_name(std::move(log_name_))
{
}

SealGatedReadTransform::~SealGatedReadTransform() = default;

IProcessor::Status SealGatedReadTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

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

    if (ready_chunk)
    {
        output.push(std::move(ready_chunk));
        return Status::PortFull;
    }

    if (finished)
    {
        output.finish();
        input.close();
        return Status::Finished;
    }

    if (reading)
        return Status::Ready;

    if (input.isFinished())
    {
        /// No seal will ever arrive (cancellation, or the join pipeline did not wire the
        /// seal). Fail-open: read without the runtime filter, which is always correct.
        reading = true;
        return Status::Ready;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    seal = input.pull(true);
    return Status::Ready;
}

void SealGatedReadTransform::work()
{
    if (!reading)
    {
        if (auto info = seal.getChunkInfos().get<RuntimeFilterSealInfo>(); info && info->filter && refiner)
            refiner->setFilter(info->filter);

        seal = Chunk();
        reading = true;
        return;
    }

    auto res = processor->read();

    if (res.num_read_rows || res.num_read_bytes)
    {
        std::lock_guard lock(read_progress_mutex);
        read_progress.read_rows += res.num_read_rows;
        read_progress.read_bytes += res.num_read_bytes;
    }

    if (res.chunk)
        ready_chunk = std::move(res.chunk);

    if (res.is_finished)
    {
        processor->onFinish();
        finished = true;
    }
}

std::optional<IProcessor::ReadProgress> SealGatedReadTransform::getReadProgress()
{
    std::lock_guard lock(read_progress_mutex);
    if (finished && read_progress.read_bytes == 0 && read_progress.total_rows_approx == 0)
        return {};

    ReadProgressCounters res_progress;
    std::swap(read_progress, res_progress);

    if (storage_limits)
        return ReadProgress{res_progress, *storage_limits};

    static const StorageLimitsList empty_limits;
    return ReadProgress{res_progress, empty_limits};
}

void SealGatedReadTransform::addTotalRowsApprox(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_rows_approx += value;
}

void SealGatedReadTransform::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_)
{
    storage_limits = storage_limits_;
}

void SealGatedReadTransform::onCancel() noexcept
{
    if (processor)
        processor->cancel();
}

}
