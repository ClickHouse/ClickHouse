#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>

#include <base/types.h>

namespace DB::Runtime::Wyrd
{

class Gate
{
public:
    void pass();
    void open();
    void close();

private:
    Atomic<UInt32> is_open{0};
};

}
