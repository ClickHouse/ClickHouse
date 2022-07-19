#pragma once

#include <stddef.h>
#include <cstdint>
#include <utility>
#include <atomic>
#include <common/types.h>

/** Allows to count number of simultaneously happening processes or current value of some metric.
  *  - for high-level profiling.
  *
  * See also ProfileEvents.h
  * ProfileEvents counts number of happened events - for example, how many times queries was executed.
  * CurrentMetrics counts number of simultaneously happening events - for example, number of currently executing queries, right now,
  *  or just current value of some metric - for example, replica delay in seconds.
  *
  * CurrentMetrics are updated instantly and are correct for any point in time.
  * For periodically (asynchronously) updated metrics, see AsynchronousMetrics.h
  */

namespace CurrentMetrics
{
    /// Metric identifier (index in array).
    using Metric = size_t;
    using Value = DB::Int64;

    /// Get name of metric by identifier. Returns statically allocated string.
    const char * getName(Metric event);
    /// Get text description of metric by identifier. Returns statically allocated string.
    const char * getDocumentation(Metric event);

    /// Metric identifier -> current value of metric.
    extern std::atomic<Value> values[];

    /// Get index just after last metric identifier.
    Metric end();

    /// Set value of specified metric.
    inline void set(Metric metric, Value value)
    {
        values[metric].store(value, std::memory_order_relaxed);
    }

    /// Add value for specified metric. You must subtract value later; or see class Increment below.
    inline void add(Metric metric, Value value = 1)
    {
        values[metric].fetch_add(value, std::memory_order_relaxed);
    }

    inline void sub(Metric metric, Value value = 1)
    {
        add(metric, -value);
    }

    /// For lifetime of object, add amount for specified metric. Then subtract.
    class Increment
    {
    private:
        std::atomic<Value> * what;
        Value amount;

        Increment(std::atomic<Value> * what_, Value amount_)
            : what(what_), amount(amount_)
        {
            *what += amount;
        }

    public:
        Increment(Metric metric, Value amount_ = 1)
            : Increment(&values[metric], amount_) {}

        ~Increment()
        {
            if (what)
                what->fetch_sub(amount, std::memory_order_relaxed);
        }

        Increment(Increment && old)
        {
            *this = std::move(old);
        }

        Increment & operator= (Increment && old)
        {
            what = old.what;
            amount = old.amount;
            old.what = nullptr;
            return *this;
        }

        void changeTo(Value new_amount)
        {
            what->fetch_add(new_amount - amount, std::memory_order_relaxed);
            amount = new_amount;
        }

        void sub(Value value = 1)
        {
            what->fetch_sub(value, std::memory_order_relaxed);
            amount -= value;
        }

        void add(Value value = 1)
        {
            what->fetch_add(value, std::memory_order_relaxed);
            amount += value;
        }

        /// Subtract value before destructor.
        void destroy()
        {
            what->fetch_sub(amount, std::memory_order_relaxed);
            what = nullptr;
        }
    };
}
