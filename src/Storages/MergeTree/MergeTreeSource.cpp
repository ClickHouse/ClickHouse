#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <IO/IOThreadPool.h>
#include <Common/EventFD.h>

namespace DB
{

MergeTreeSource::MergeTreeSource(MergeTreeSelectAlgorithmPtr algorithm_)
    : ISource(algorithm_->getHeader())
    , algorithm(std::move(algorithm_))
{
    if (algorithm->getSettings().use_asynchronous_read_from_pool)
        async_reading_state = std::make_unique<AsyncReadingState>();
}

MergeTreeSource::~MergeTreeSource() = default;

std::string MergeTreeSource::getName() const
{
    return algorithm->getName();
}

void MergeTreeSource::onCancel()
{
    algorithm->cancel();
}

struct MergeTreeSource::AsyncReadingState
{
    /// NotStarted -> InProgress -> IsFinished -> NotStarted ...
    enum class Stage
    {
        NotStarted,
        InProgress,
        IsFinished,
    };

    struct Control
    {
        EventFD event;
        std::atomic<Stage> stage = Stage::NotStarted;

        void finish()
        {
            stage = Stage::IsFinished;
            event.write();
        }
    };

    /// setResult and setException are the only methods
    /// which can be called from background thread.
    /// Invariant:
    ///   * background thread changes status InProgress -> IsFinished
    ///   * (status == InProgress) => (MergeTreeBaseSelectProcessor is alive)

    void setResult(ChunkAndProgress chunk_)
    {
        assert(control->stage == Stage::InProgress);
        chunk = std::move(chunk_);
        control->finish();
    }

    void setException(std::exception_ptr exception_)
    {
        assert(control->stage == Stage::InProgress);
        exception = exception_;
        control->finish();
    }

    std::shared_ptr<Control> start()
    {
        assert(control->stage == Stage::NotStarted);
        control->stage = Stage::InProgress;
        return control;
    }

    void schedule(ThreadPool::Job job)
    {
        callback_runner(std::move(job), 0);
    }

    ChunkAndProgress getResult()
    {
        assert(control->stage == Stage::IsFinished);
        control->event.read();
        control->stage = Stage::NotStarted;

        if (exception)
            std::rethrow_exception(exception);

        return std::move(chunk);
    }

    Stage getStage() const { return control->stage; }
    int getFD() const { return control->event.fd; }

    AsyncReadingState()
    {
        control = std::make_shared<Control>();
        callback_runner = threadPoolCallbackRunner<void>(IOThreadPool::get(), "MergeTreeRead");
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
        ///  ~MergeTreeBaseSelectProcessor()
        ///  ~AsyncReadingState()
        ///  control->stage != Stage::InProgress
        ///  ~EventFD()
        ///                                             event.write()
        if (control->stage == Stage::InProgress)
            control->event.read();
    }

private:
    ThreadPoolCallbackRunner<void> callback_runner;
    ChunkAndProgress chunk;
    std::exception_ptr exception;
    std::shared_ptr<Control> control;
};


ISource::Status MergeTreeSource::prepare()
{
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

    return ISource::prepare();
}


std::optional<Chunk> MergeTreeSource::reportProgress(ChunkAndProgress chunk)
{
    if (chunk.num_read_rows || chunk.num_read_bytes)
        progress(chunk.num_read_rows, chunk.num_read_bytes);

    if (chunk.chunk.hasRows())
        return std::move(chunk.chunk);

    return {};
}


std::optional<Chunk> MergeTreeSource::tryGenerate()
{
    if (!async_reading_state)
        return reportProgress(algorithm->read());

    if (async_reading_state->getStage() == AsyncReadingState::Stage::IsFinished)
        return reportProgress(async_reading_state->getResult());

    assert(async_reading_state->getStage() == AsyncReadingState::Stage::NotStarted);
    async_reading_state->start();

    /// It is important to store control into job.
    /// Otherwise, race between job and ~MergeTreeBaseSelectProcessor is possible.
    auto job = [this, control = async_reading_state->start()]() mutable
    {
        auto holder = std::move(control);

        try
        {
            async_reading_state->setResult(algorithm->read());
        }
        catch (...)
        {
            async_reading_state->setException(std::current_exception());
        }
    };

    async_reading_state->schedule(std::move(job));

    return Chunk();
}


int MergeTreeSource::schedule()
{
    return async_reading_state->getFD();
}

}
