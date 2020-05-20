#pragma once

#include <Common/CounterInFile.h>


/** Allows to get an auto-increment number, storing it in a file.
  * Intended for rare calls (not designed for performance).
  */
class Increment
{
public:
    /// path - the name of the file, including the path
    Increment(const std::string & path_) : counter(path_) {}

    /** Get the next number.
      * If the `create_if_need` parameter is not set to true, then
      *  the file must already have a number written (if not - create the file manually with zero).
      *
      * To protect against race conditions between different processes, file locks are used.
      * (But when the first file is created, the race condition is possible, so it's better to create the file in advance.)
      *
      * `locked_callback` is called when the counter file is locked. A new value is passed to it.
      * `locked_callback` can be used to do something atomically with the increment of the counter (for example, rename files).
      */
    template <typename Callback>
    UInt64 get(Callback && locked_callback, bool create_if_need = false)
    {
        return static_cast<UInt64>(counter.add(1, std::forward<Callback>(locked_callback), create_if_need));
    }

    UInt64 get(bool create_if_need = false)
    {
        return getBunch(1, create_if_need);
    }

    /// Peek the next value.
    UInt64 peek(bool create_if_need = false)
    {
        return getBunch(0, create_if_need);
    }

    /** Get the next number and increase the counter by `count`.
      * If the `create_if_need` parameter is not set to true, then
      *  the file should already have a number written (if not - create the file manually with zero).
      *
      * To protect against race conditions between different processes, file locks are used.
      * (But when the first file is created, the race condition is possible, so it's better to create the file in advance.)
      */
    UInt64 getBunch(UInt64 count, bool create_if_need = false)
    {
        return static_cast<UInt64>(counter.add(static_cast<Int64>(count), create_if_need) - count + 1);
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
