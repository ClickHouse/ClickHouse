#include "expected404.h"
#include "base/defines.h"

namespace DB
{

thread_local size_t Expected404Scope::expected_404_scope_count = 0;

Expected404Scope::Expected404Scope()
    : counter(expected_404_scope_count)
{
    ++counter;
}

Expected404Scope::~Expected404Scope()
{
    // check that instance is destroyed in the same thread
    chassert(&counter == &expected_404_scope_count);
    chassert(counter);
    --counter;
}

bool Expected404Scope::is404Expected()
{
    return expected_404_scope_count == 0;
}

}
