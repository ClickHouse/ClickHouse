#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/SharedThreadPools.h>
#include <Common/EventFD.h>

namespace DB
{

#if defined(OS_LINUX)
struct MergeTreeSource::AsyncReadingState
{
    /// NotStarted -> InProgress -> IsFinished -> NotStarted ...
    enum class Stage : uint8_t
    {
        NotStarted,
        InProgress,
        IsFinished,
    };

    struct Control
    {
        /// setResult and setException are the only methods
        /// which can be called from background thread.
        /// Invariant:
        ///   * background thread changes status InProgress -> IsFinished
        ///   * (status == InProgress) => (MergeTreeSelectProcessor is alive)

        void setResult(ChunkAndProgress chunk_)
        {
            chassert(stage == Stage::InProgress);
            chunk = std::move(chunk_);
            finish();
        }

        void setException(std::exception_ptr exception_)
        {
            chassert(stage == Stage::InProgress);
            exception = exception_;
            finish();
        }

    private:

        /// Executor requires file descriptor (which can be polled) to be returned for async execution.
        /// We are using EventFD here.
        /// Thread from background pool writes to fd when task is finished.
        /// Working thread should read from fd when task is finished or canceled to wait for bg thread.
        EventFD event;
        std::atomic<Stage> stage = Stage::NotStarted;

        ChunkAndProgress chunk;
        std::exception_ptr exception;

        void finish()
        {
            stage = Stage::IsFinished;
            event.write();
        }

        ChunkAndProgress getResult()
        {
            chassert(stage == Stage::IsFinished);
            event.read();
            stage = Stage::NotStarted;

            if (exception)
                std::rethrow_exception(exception);

            return std::move(chunk);
        }

        friend struct AsyncReadingState;
    };

    std::shared_ptr<Control> start()
    {
        chassert(control->stage == Stage::NotStarted);
        control->stage = Stage::InProgress;
        return control;
    }

    void schedule(ThreadPool::Job job)
    {
        try
        {
            callback_runner(std::move(job), Priority{});
        }
        catch (...)
        {
            /// Roll back stage in case of exception from ThreadPool::schedule
            control->stage = Stage::NotStarted;
            throw;
        }
    }

    ChunkAndProgress getResult()
    {
        return control->getResult();
    }

    Stage getStage() const { return control->stage; }
    int getFD() const { return control->event.fd; }

    AsyncReadingState()
    {
        control = std::make_shared<Control>();
        callback_runner = threadPoolCallbackRunnerUnsafe<void>(getIOThreadPool().get(), "MergeTreeRead");
    }

    ~AsyncReadingState()
    {
        /// Here we wait for async task if needed.
        /// ~AsyncReadingState and Control::finish can be run concurrently.
        /// It's important to store std::shared_ptr<Control> into bg pool task.
        /// Otherwise following is possible:
        ///
        ///  (executing thread)                         (bg pool thread)
        ///                                             Control::finish()
        ///                                             stage = Stage::IsFinished;
        ///  ~MergeTreeSelectProcessor()
        ///  ~AsyncReadingState()
        ///  control->stage != Stage::InProgress
        ///  ~EventFD()
        ///                                             event.write()
        if (control->stage == Stage::InProgress)
            control->event.read();
    }

private:
    ThreadPoolCallbackRunnerUnsafe<void> callback_runner;
    std::shared_ptr<Control> control;
};
#endif

MergeTreeSource::MergeTreeSource(MergeTreeSelectProcessorPtr processor_, const std::string & log_name_)
    : ISource(processor_->getHeader()), processor(std::move(processor_)), log_name(log_name_)
{
#if defined(OS_LINUX)
    if (processor->getSettings().use_asynchronous_read_from_pool)
        async_reading_state = std::make_unique<AsyncReadingState>();
#endif
}

MergeTreeSource::~MergeTreeSource() = default;

std::string MergeTreeSource::getName() const
{
    return processor->getName();
}

void MergeTreeSource::onCancel() noexcept
{
    processor->cancel();
}

ISource::Status MergeTreeSource::prepare()
{
#if defined(OS_LINUX)
    if (!async_reading_state)
        return ISource::prepare();

    /// Check if query was cancelled before returning Async status. Otherwise it may lead to infinite loop.
    if (isCancelled())
    {
        getPort().finish();
        return ISource::Status::Finished;
    }

    if (async_reading_state && async_reading_state->getStage() == AsyncReadingState::Stage::InProgress)
        return ISource::Status::Async;
#endif

    return ISource::prepare();
}


Chunk MergeTreeSource::processReadResult(ChunkAndProgress chunk)
{
    if (chunk.num_read_rows || chunk.num_read_bytes)
        progress(chunk.num_read_rows, chunk.num_read_bytes);

    finished = chunk.is_finished;

    /// We can return a chunk with no rows even if are not finished.
    /// This allows to report progress when all the rows are filtered out inside MergeTreeSelectProcessor by PREWHERE logic.
    return std::move(chunk.chunk);
}


std::optional<Chunk> MergeTreeSource::tryGenerate()
{
#if defined(OS_LINUX)
    if (async_reading_state)
    {
        if (async_reading_state->getStage() == AsyncReadingState::Stage::IsFinished)
            return processReadResult(async_reading_state->getResult());

        chassert(async_reading_state->getStage() == AsyncReadingState::Stage::NotStarted);

        /// It is important to store control into job.
        /// Otherwise, race between job and ~MergeTreeSelectProcessor is possible.
        auto job = [this, control = async_reading_state->start()]() mutable
        {
            auto holder = std::move(control);

            try
            {
                OpenTelemetry::SpanHolder span{fmt::format("MergeTreeSource({})::tryGenerate", log_name)};
                holder->setResult(processor->read());
            }
            catch (...)
            {
                holder->setException(std::current_exception());
            }
        };

        async_reading_state->schedule(std::move(job));

        return Chunk();
    }
#endif

    OpenTelemetry::SpanHolder span{fmt::format("MergeTreeSource({})::tryGenerate", log_name)};
    return processReadResult(processor->read());
}

#if defined(OS_LINUX)
int MergeTreeSource::schedule()
{
    return async_reading_state->getFD();
}
#endif

}
