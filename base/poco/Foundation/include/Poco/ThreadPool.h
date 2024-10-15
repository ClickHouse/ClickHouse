//
// ThreadPool.h
//
// Library: Foundation
// Package: Threading
// Module:  ThreadPool
//
// Definition of the ThreadPool class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ThreadPool_INCLUDED
#define Foundation_ThreadPool_INCLUDED


#include <vector>
#include "Poco/Foundation.h"
#include "Poco/Mutex.h"
#include "Poco/Thread.h"


namespace Poco
{


class Runnable;
class PooledThread;


class Foundation_API ThreadPool
/// A thread pool always keeps a number of threads running, ready
/// to accept work.
/// Creating and starting a threads can impose a significant runtime
/// overhead to an application. A thread pool helps to improve
/// the performance of an application by reducing the number
/// of threads that have to be created (and destroyed again).
/// Threads in a thread pool are re-used once they become
/// available again.
/// The thread pool always keeps a minimum number of threads
/// running. If the demans for threads increases, additional
/// threads are created. Once the demand for threads sinks
/// again, no-longer used threads are stopped and removed
/// from the pool.
{
public:
    explicit ThreadPool(
        int minCapacity = 2,
        int maxCapacity = 16,
        int idleTime = 60,
        int stackSize = POCO_THREAD_STACK_SIZE,
        size_t global_profiler_real_time_period_ns_ = 0,
        size_t global_profiler_cpu_time_period_ns_ = 0);
    /// Creates a thread pool with minCapacity threads.
    /// If required, up to maxCapacity threads are created
    /// a NoThreadAvailableException exception is thrown.
    /// If a thread is running idle for more than idleTime seconds,
    /// and more than minCapacity threads are running, the thread
    /// is killed. Threads are created with given stack size.

    explicit ThreadPool(
        const std::string & name,
        int minCapacity = 2,
        int maxCapacity = 16,
        int idleTime = 60,
        int stackSize = POCO_THREAD_STACK_SIZE,
        size_t global_profiler_real_time_period_ns_ = 0,
        size_t global_profiler_cpu_time_period_ns_ = 0);
    /// Creates a thread pool with the given name and minCapacity threads.
    /// If required, up to maxCapacity threads are created
    /// a NoThreadAvailableException exception is thrown.
    /// If a thread is running idle for more than idleTime seconds,
    /// and more than minCapacity threads are running, the thread
    /// is killed. Threads are created with given stack size.

    ~ThreadPool();
    /// Currently running threads will remain active
    /// until they complete.

    void addCapacity(int n);
    /// Increases (or decreases, if n is negative)
    /// the maximum number of threads.

    int capacity() const;
    /// Returns the maximum capacity of threads.

    void setStackSize(int stackSize);
    /// Sets the stack size for threads.
    /// New stack size applies only for newly created threads.

    int getStackSize() const;
    /// Returns the stack size used to create new threads.

    int used() const;
    /// Returns the number of currently used threads.

    int allocated() const;
    /// Returns the number of currently allocated threads.

    int available() const;
    /// Returns the number available threads.

    void start(Runnable & target);
    /// Obtains a thread and starts the target.
    /// Throws a NoThreadAvailableException if no more
    /// threads are available.

    void start(Runnable & target, const std::string & name);
    /// Obtains a thread and starts the target.
    /// Assigns the given name to the thread.
    /// Throws a NoThreadAvailableException if no more
    /// threads are available.

    void startWithPriority(Thread::Priority priority, Runnable & target);
    /// Obtains a thread, adjusts the thread's priority, and starts the target.
    /// Throws a NoThreadAvailableException if no more
    /// threads are available.

    void startWithPriority(Thread::Priority priority, Runnable & target, const std::string & name);
    /// Obtains a thread, adjusts the thread's priority, and starts the target.
    /// Assigns the given name to the thread.
    /// Throws a NoThreadAvailableException if no more
    /// threads are available.

    void stopAll();
    /// Stops all running threads and waits for their completion.
    ///
    /// Will also delete all thread objects.
    /// If used, this method should be the last action before
    /// the thread pool is deleted.
    ///
    /// Note: If a thread fails to stop within 10 seconds
    /// (due to a programming error, for example), the
    /// underlying thread object will not be deleted and
    /// this method will return anyway. This allows for a
    /// more or less graceful shutdown in case of a misbehaving
    /// thread.

    void joinAll();
    /// Waits for all threads to complete.
    ///
    /// Note that this will not actually join() the underlying
    /// thread, but rather wait for the thread's runnables
    /// to finish.

    void collect();
    /// Stops and removes no longer used threads from the
    /// thread pool. Can be called at various times in an
    /// application's life time to help the thread pool
    /// manage its threads. Calling this method is optional,
    /// as the thread pool is also implicitly managed in
    /// calls to start(), addCapacity() and joinAll().

    const std::string & name() const;
    /// Returns the name of the thread pool,
    /// or an empty string if no name has been
    /// specified in the constructor.

    static ThreadPool & defaultPool();
    /// Returns a reference to the default
    /// thread pool.

protected:
    PooledThread * getThread();
    PooledThread * createThread();

    void housekeep();

private:
    ThreadPool(const ThreadPool & pool);
    ThreadPool & operator=(const ThreadPool & pool);

    typedef std::vector<PooledThread *> ThreadVec;

    std::string _name;
    int _minCapacity;
    int _maxCapacity;
    int _idleTime;
    int _serial;
    int _age;
    int _stackSize;
    size_t _globalProfilerRealTimePeriodNs;
    size_t _globalProfilerCPUTimePeriodNs;
    ThreadVec _threads;
    mutable FastMutex _mutex;
};


//
// inlines
//
inline void ThreadPool::setStackSize(int stackSize)
{
    _stackSize = stackSize;
}


inline int ThreadPool::getStackSize() const
{
    return _stackSize;
}


inline const std::string & ThreadPool::name() const
{
    return _name;
}


} // namespace Poco


#endif // Foundation_ThreadPool_INCLUDED
