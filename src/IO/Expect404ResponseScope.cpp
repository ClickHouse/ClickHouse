#include <IO/Expect404ResponseScope.h>

#include <Common/FiberLocal.h>

#include <base/defines.h>

#include <cstddef>

namespace DB
{

namespace
{
/// Per fiber: a fiber that suspends inside the scope, for example in the `HeadObject` request of a multipart completion
/// recovery, must neither make the 404 responses of the other fibers on its thread expected nor lose its own scope
/// when it resumes on another thread.
constinit FiberLocal<size_t, FiberLocalSlot::EXPECTED_404_SCOPE_COUNT> expected_404_scope_count;
}

Expect404ResponseScope::Expect404ResponseScope()
{
    expected_404_scope_count = expected_404_scope_count + 1;
}

Expect404ResponseScope::~Expect404ResponseScope()
{
    chassert(expected_404_scope_count);
    expected_404_scope_count = expected_404_scope_count - 1;
}

bool Expect404ResponseScope::is404Expected()
{
    return expected_404_scope_count != 0;
}

}
