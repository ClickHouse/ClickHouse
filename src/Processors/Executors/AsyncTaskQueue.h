#include <cstddef>
#include <mutex>
#include <atomic>

namespace DB
{

class AsyncTaskQueue
{
private:
    int epoll_fd;
    size_t num_tasks;
    std::atomic_bool is_finished = false;
    std::condition_variable condvar;

public:
    AsyncTaskQueue();
    ~AsyncTaskQueue();

    size_t size() const { return num_tasks; }
    bool empty() const { return num_tasks == 0; }

    /// Add new task to queue.
    void addTask(void * data, int fd);

    /// Remove task.
    void removeTask(int fd) const;

    /// Wait for any descriptor. If no descriptors in queue, blocks.
    /// Returns ptr which was inserted into queue or nullptr if finished was called.
    /// Lock is used to wait on condvar.
    void * wait(std::unique_lock<std::mutex> & lock);

    /// Interrupt waiting.
    void finish();
};

}
