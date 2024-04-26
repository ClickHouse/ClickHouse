#pragma once

#include <list>
#include <mutex>

#include <Common/EventFD.h>

namespace DB
{

template <class T>
class SCMPQueue
{
public:
    void add(T value);
    std::list<T> extractAll();

    bool isEmpty() const;

    /// returns event_fd's native handle for unix systems
    /// otherwise returns nullopt
    std::optional<int> fd() const;

    /// closes queue.
    /// after this, all operations will return immediately
    void close();

private:
    // data
    mutable std::mutex mutex;
    std::list<T> queue;

    // synchronization
    std::atomic<bool> is_disabled{false};

#if defined(OS_LINUX)
    EventFD new_values_event;
#else
    std::condition_variable new_values;
#endif
};

template <class T>
bool SCMPQueue<T>::isEmpty() const
{
    std::unique_lock guard(mutex);
    return queue.empty();
}

#if defined(OS_LINUX)

template <class T>
void SCMPQueue<T>::add(T value)
{
    {
        std::unique_lock guard(mutex);

        if (is_disabled.load())
            return;

        queue.emplace_back(std::move(value));
    }
    new_values_event.write(1);
}

template <class T>
std::list<T> SCMPQueue<T>::extractAll()
{
    if (is_disabled.load())
        return {};

    new_values_event.read();

    std::unique_lock guard(mutex);
    return std::exchange(queue, {});
}

template <class T>
std::optional<int> SCMPQueue<T>::fd() const
{
    return new_values_event.fd;
}

template <class T>
void SCMPQueue<T>::close()
{
    {
        std::unique_lock guard(mutex);
        is_disabled.store(true);
        queue.clear();
    }

    new_values_event.write(1);
}

#else

template <class T>
void SCMPQueue<T>::add(T value)
{
    std::unique_lock guard(mutex);

    if (is_disabled.load())
        return;

    queue.emplace_back(std::move(value));
    new_values.notify_one();
}

template <class T>
std::list<T> SCMPQueue<T>::extractAll()
{
    std::unique_lock guard(mutex);

    while (queue.empty() && !is_disabled.load())
        new_values.wait(guard);

    return std::exchange(queue, {});
}

template <class T>
std::optional<int> SCMPQueue<T>::fd() const
{
    return std::nullopt;
}

template <class T>
void SCMPQueue<T>::close()
{
    std::unique_lock guard(mutex);
    is_disabled.store(true);
    queue.clear();
    new_values.notify_one();
}

#endif

}
