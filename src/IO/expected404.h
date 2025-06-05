#pragma once

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
    static thread_local size_t expected_404_scope_count;
    size_t & counter;
};

}
