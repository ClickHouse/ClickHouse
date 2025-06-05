#include "Expect404ResponseScope.h"
#include <Common/CurrentThread.h>

namespace DB
{

thread_local size_t expected_404_scope_count = 0;

Expect404ResponseScope::Expect404ResponseScope()
    : initial_thread_id(CurrentThread::get().thread_id)
{
    ++expected_404_scope_count;
}

Expect404ResponseScope::~Expect404ResponseScope()
{
    // check that instance is destroyed in the same thread
    chassert(initial_thread_id == CurrentThread::get().thread_id);
    chassert(expected_404_scope_count);
    --expected_404_scope_count;
}

bool Expect404ResponseScope::is404Expected()
{
    return expected_404_scope_count != 0;
}

}
