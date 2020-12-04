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

void AsyncTaskQueue::addTask(void * data, int fd)
{
    epoll_event socket_event;
    socket_event.events = EPOLLIN | EPOLLPRI;
    socket_event.data.ptr = data;

    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &socket_event))
        throwFromErrno("Cannot add socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);

    ++num_tasks;
    if (num_tasks == 1)
        condvar.notify_one();
}

void AsyncTaskQueue::removeTask(int fd) const
{
    epoll_event event;
    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &event))
        throwFromErrno("Cannot remove socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
}

void * AsyncTaskQueue::wait(std::unique_lock<std::mutex> & lock)
{
    condvar.wait(lock, [&] { return !empty() || is_finished; });

    if (is_finished)
        return nullptr;

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
    --num_tasks;
    return event.data.ptr;
}

void AsyncTaskQueue::finish()
{
    is_finished = true;
    num_tasks = 0;
    condvar.notify_one();
}

}
