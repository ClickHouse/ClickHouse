#include "expected404.h"
#include <Common/CurrentThread.h>

namespace DB
{

thread_local size_t Expected404Scope::expected_404_scope_count = 0;

Expected404Scope::Expected404Scope()
    : initial_thread_id(CurrentThread::get().thread_id)
{
    ++expected_404_scope_count;
}

Expected404Scope::~Expected404Scope()
{
    // check that instance is destroyed in the same thread
    chassert(initial_thread_id == CurrentThread::get().thread_id);
    chassert(expected_404_scope_count);
    --expected_404_scope_count;
}

bool Expected404Scope::is404Expected()
{
    return expected_404_scope_count == 0;
}

}
