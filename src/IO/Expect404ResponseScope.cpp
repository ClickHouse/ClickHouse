#include <IO/Expect404ResponseScope.h>

#include <base/defines.h>
#include <Common/FiberLocal.h>

namespace DB
{

constinit FiberLocal<size_t, FiberLocalSlot::EXPECTED_404_SCOPE_COUNT> expected_404_scope_count;

Expect404ResponseScope::Expect404ResponseScope()
    : initial_thread_id(std::this_thread::get_id())
{
    expected_404_scope_count = expected_404_scope_count + 1;
}

Expect404ResponseScope::~Expect404ResponseScope()
{
    // check that instance is destroyed in the same thread
    chassert(initial_thread_id == std::this_thread::get_id());
    chassert(expected_404_scope_count);
    expected_404_scope_count = expected_404_scope_count - 1;
}

bool Expect404ResponseScope::is404Expected()
{
    return expected_404_scope_count != 0;
}

}
