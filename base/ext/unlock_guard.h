#pragma once

namespace ext
{

template <typename T>
class unlock_guard
{
public:
    unlock_guard(T & mutex_) : mutex(mutex_)
    {
        mutex.unlock();
    }

    ~unlock_guard()
    {
        mutex.lock();
    }

    unlock_guard(const unlock_guard &) = delete;
    unlock_guard & operator=(const unlock_guard &) = delete;

private:
    T & mutex;
};

}
