#pragma once

#include <functional>

#include <Common/CounterInFile.h>


/** Allows to get an auto-increment number, storing it in a file.
  * Intended for rare calls (not designed for performance).
  */
class Increment
{
public:
    /// path - the name of the file, including the path
    explicit Increment(const std::string & path_) : counter(path_) {}

    /** Get the next number.
      *
      * If `create_if_need` is true, a missing or empty file is treated as if it
      * contained zero (the file is created or repaired in place). See
      * `CounterInFile::add` for the recovery semantics around empty files.
      *
      * `min_initial_value`, if greater than zero, sets a lower bound for the
      * counter value used as the starting point when the file was missing or
      * empty. It is intended for recovery scenarios where the caller knows
      * from external context what the counter must be at.
      *
      * `min_initial_value_provider`, if set, supplies an additional lower
      * bound that is computed lazily and only on the recovery path (missing or
      * empty file), under the counter lock. See `CounterInFile::add`.
      *
      * `locked_callback` is called while the counter file is locked. The new
      * value is passed to it. It can be used to do something atomically with
      * the increment (for example, rename files).
      */
    template <typename Callback>
    UInt64 get(Callback && locked_callback, bool create_if_need = false, UInt64 min_initial_value = 0)
    {
        return static_cast<UInt64>(counter.add(
            1, std::forward<Callback>(locked_callback), create_if_need, static_cast<Int64>(min_initial_value)));
    }

    UInt64 get(bool create_if_need = false, UInt64 min_initial_value = 0,
               const std::function<UInt64()> & min_initial_value_provider = {})
    {
        return getBunch(1, create_if_need, min_initial_value, min_initial_value_provider);
    }

    /// Peek the next value.
    UInt64 peek(bool create_if_need = false, UInt64 min_initial_value = 0)
    {
        return getBunch(0, create_if_need, min_initial_value);
    }

    /** Get the next number and increase the counter by `count`.
      *
      * If `create_if_need` is true, a missing or empty file is treated as if
      * it contained zero. See `CounterInFile::add` for details.
      *
      * `min_initial_value` is a lower bound for the starting counter value
      * when the file was missing or empty (zero by default).
      */
    UInt64 getBunch(UInt64 count, bool create_if_need = false, UInt64 min_initial_value = 0,
                    const std::function<UInt64()> & min_initial_value_provider = {})
    {
        std::function<Int64()> provider;
        if (min_initial_value_provider)
            provider = [&min_initial_value_provider] { return static_cast<Int64>(min_initial_value_provider()); };

        return static_cast<UInt64>(
            counter.add(static_cast<Int64>(count), create_if_need, static_cast<Int64>(min_initial_value), provider)
            - count + 1);
    }

    /// Change the path to the file.
    void setPath(std::string path_)
    {
        counter.setPath(path_);
    }

    void fixIfBroken(UInt64 value)
    {
        counter.fixIfBroken(value);
    }

private:
    CounterInFile counter;
};
