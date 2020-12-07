#include <Processors/Executors/AsyncTaskQueue.h>
#include <Common/Exception.h>
#include <sys/epoll.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_SOCKET;
}


AsyncTaskQueue::AsyncTaskQueue()
{
    epoll_fd = epoll_create(1);
    if (-1 == epoll_fd)
        throwFromErrno("Cannot create epoll descriptor", ErrorCodes::CANNOT_OPEN_FILE);
}

AsyncTaskQueue::~AsyncTaskQueue()
{
    close(epoll_fd);
}

void AsyncTaskQueue::addTask(size_t thread_number, void * data, int fd)
{
    auto it = tasks.insert(tasks.end(), TaskData{thread_number, data, fd, {}});
    it->self = it;

    epoll_event socket_event;
    socket_event.events = EPOLLIN | EPOLLPRI;
    socket_event.data.ptr = &(*it);

    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &socket_event))
        throwFromErrno("Cannot add socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);

    if (size() == 1)
        condvar.notify_one();
}

AsyncTaskQueue::TaskData AsyncTaskQueue::wait(std::unique_lock<std::mutex> & lock)
{
    condvar.wait(lock, [&] { return !empty() || is_finished; });

    if (is_finished)
        return {};

    lock.unlock();

    epoll_event event;
    int num_events = 0;

    while (num_events == 0)
    {
        num_events = epoll_wait(epoll_fd, &event, 1, 0);
        if (num_events == -1)
            throwFromErrno("Failed to epoll_wait", ErrorCodes::CANNOT_READ_FROM_SOCKET);
    }

    lock.lock();

    auto it = static_cast<TaskData *>(event.data.ptr)->self;

    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_DEL, it->fd, &event))
        throwFromErrno("Cannot remove socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);

    auto res = *it;
    tasks.erase(it);
    return res;
}

void AsyncTaskQueue::finish()
{
    is_finished = true;
    tasks.clear();
    condvar.notify_one();
}

}
