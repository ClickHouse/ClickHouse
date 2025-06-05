#pragma once

#include <base/types.h>
#include <cstddef>

namespace DB
{

class Expected404Scope
{
public:
    Expected404Scope();
    ~Expected404Scope();

    static bool is404Expected();

private:
    UInt64 initial_thread_id;
};

}
