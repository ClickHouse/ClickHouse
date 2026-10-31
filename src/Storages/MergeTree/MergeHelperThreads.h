#pragma once

#include <Common/CurrentMetrics.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <cstddef>
#include <memory>

namespace DB
{

/// Limits the number of threads that merges use in addition to their own threads, such as a thread that reads
/// the columns of a Vertical merge.
/// A merge that does not get a slot does this work in its own thread, as it does when these threads are disabled.
/// The limit is the server setting `max_merge_helper_threads`.
class MergeHelperThreads
{
public:
    /// Holds one of the threads until it is destroyed.
    class Slot : private boost::noncopyable
    {
    public:
        ~Slot();

    private:
        friend class MergeHelperThreads;
        Slot();

        CurrentMetrics::Increment metric_increment;
    };

    using SlotPtr = std::unique_ptr<Slot>;

    /// Returns `nullptr` if all threads are used.
    static SlotPtr tryAcquire();

    static void setMaxThreads(size_t max_threads_);

private:
    static std::atomic<size_t> max_threads;
    static std::atomic<size_t> used_threads;
};

}
