#include <Client/BuzzHouse/Utils/BackgroundWorker.h>

#include <Common/Exception.h>

namespace BuzzHouse
{

/// Enqueue a function to be executed
void BackgroundWorker::enqueue(BackgroundWorkerTask task)
{
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    cv_.notify_one();
}

void BackgroundWorker::stop()
{
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stop_ = true;
    }
    cv_.notify_one();

    if (worker_thread_.joinable())
    {
        worker_thread_.join();
    }
}

void BackgroundWorker::worker_loop()
{
    while (true)
    {
        BackgroundWorkerTask task;

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            cv_.wait(lock, [this] { return stop_ || !task_queue_.empty(); });

            /// Drain the remaining tasks before stopping.
            if (task_queue_.empty())
            {
                break;
            }
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }

        /// Execute the task outside the lock
        if (task)
        {
            try
            {
                task();
            }
            catch (...)
            {
                /// Any exception, not only `std::exception`, must not escape and terminate the worker thread.
                LOG_ERROR(log, "Error while executing background task {}", DB::getCurrentExceptionMessage(false));
            }
        }
    }
}

}
