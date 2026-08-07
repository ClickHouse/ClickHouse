#pragma once

#include <base/types.h>
#include <atomic>


/** Is used for numbering of files.
  */
struct SimpleIncrement
{
    std::atomic<UInt64> value{0};

    void set(UInt64 new_value)
    {
        value = new_value;
    }

    UInt64 get()
    {
        return ++value;
    }
};
