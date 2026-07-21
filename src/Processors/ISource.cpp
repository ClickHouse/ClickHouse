#include <Processors/ISource.h>
#include <QueryPipeline/StreamLocalLimits.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ISource::~ISource() = default;

ISource::ISource(SharedHeader header, bool enable_auto_progress)
    : IProcessor({}, {std::move(header)})
    , auto_progress(enable_auto_progress)
    , output(outputs.front())
{
}

void ISource::cancel(CancelReason) noexcept
{
    /// Run `onCancel` on the first cancellation, regardless of reason. `onCancel` is where a
    /// source's real interruption/cleanup lives (e.g. `ReadFromDistributedPlanSource::onCancel`
    /// sets the `cancellation_flag` that stops its blocking `tryGenerate`, `MergeTreeSource`
    /// forwards the cancel to its read processor), so it must fire on the `PartialResult`
    /// short stop that `PipelineExecutor::cancelReading` issues once a `LIMIT` /
    /// `partial_result_on_first_cancel` consumer already has enough rows — otherwise those
    /// sources would keep running after the consumer is done.
    ///
    /// The success-path drain that `PipelineExecutor::finalizeExecution` performs on normal
    /// completion is not routed through here: it targets only the sources that need it
    /// (`RemoteSource`, see `IProcessor::drainsProgressOnFinalize`), so ordinary sources do
    /// not get spurious `onCancel` calls (e.g. `SQLiteSource::onCancel` calling
    /// `sqlite3_interrupt`) on every successful query.
    ///
    /// `is_cancelled` is published unconditionally first so `prepare` returns `Finished` even
    /// for sources whose `onCancel` is a no-op (e.g. `NumbersSource`).
    if (is_cancelled.exchange(true, std::memory_order_acq_rel))
        return;

    onCancel();
}

ISource::Status ISource::prepare()
{
    if (finished)
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;

    output.pushData(std::move(current_chunk));
    has_input = false;

    if (isCancelled())
    {
        output.finish();
        return Status::Finished;
    }

    if (got_exception)
    {
        output.finish();
        return Status::Finished;
    }

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

void ISource::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_)
{
    storage_limits = storage_limits_;
}

void ISource::progress(size_t read_rows, size_t read_bytes)
{
    //std::cerr << "========= Progress " << read_rows << " from " << getName() << std::endl << StackTrace().toString() << std::endl;
    read_progress_was_set = true;
    std::lock_guard lock(read_progress_mutex);
    read_progress.read_rows += read_rows;
    read_progress.read_bytes += read_bytes;
}

std::optional<ISource::ReadProgress> ISource::getReadProgress()
{
    std::lock_guard lock(read_progress_mutex);
    /// If the source has finished and there is genuinely nothing to report, skip the
    /// downstream `onProgress` call. Check every counter to avoid dropping accumulated
    /// `read_rows` in cases where a `Progress` packet reports rows without bytes.
    if (finished
        && read_progress.read_rows == 0
        && read_progress.read_bytes == 0
        && read_progress.total_rows_approx == 0
        && read_progress.total_bytes == 0)
        return {};

    ReadProgressCounters res_progress;
    std::swap(read_progress, res_progress);

    if (storage_limits)
        return ReadProgress{res_progress, *storage_limits};

    static StorageLimitsList empty_limits;
    return ReadProgress{res_progress, empty_limits};
}

void ISource::addTotalRowsApprox(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_rows_approx += value;
}

void ISource::addTotalBytes(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_bytes += value;
}

void ISource::work()
{
    try
    {
        if (finished)
            return;

        read_progress_was_set = false;

        if (auto chunk = tryGenerate())
        {
            current_chunk.chunk = std::move(*chunk);
            if (current_chunk.chunk)
            {
                has_input = true;
                if (auto_progress && !read_progress_was_set)
                    progress(current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes());
            }
        }
        else
            finished = true;

        if (finished)
            onFinish();
    }
    catch (...)
    {
        got_exception = true;

        if (!std::exchange(finished, true))
            onFinish();

        throw;
    }
}

Chunk ISource::generate()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "generate is not implemented for {}", getName());
}

std::optional<Chunk> ISource::tryGenerate()
{
    auto chunk = generate();
    if (!chunk)
        return std::nullopt;

    return chunk;
}

}
