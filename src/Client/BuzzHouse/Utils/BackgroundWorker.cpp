#include <Client/BuzzHouse/Utils/BackgroundWorker.h>

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

bool BackgroundWorker::is_empty() const
{
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return task_queue_.empty();
}

size_t BackgroundWorker::queue_size() const
{
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return task_queue_.size();
}

void BackgroundWorker::worker_loop()
{
    while (true)
    {
        BackgroundWorkerTask task;

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            cv_.wait(lock, [this] { return stop_ || !task_queue_.empty(); });

            if (stop_ && task_queue_.empty())
            {
                break;
            }

            if (!task_queue_.empty())
            {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        /// Execute the task outside the lock
        if (task)
        {
            try
            {
                task();
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(log, "Error while executing background task {}", e.what());
            }
        }
    }
}

}
