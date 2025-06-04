#include "expected404.h"

namespace DB
{

thread_local size_t Expected404Scope::expected_404_scope_count = 0;

Expected404Scope::Expected404Scope()
{
    ++expected_404_scope_count;
}

Expected404Scope::~Expected404Scope()
{
    --expected_404_scope_count;
}

bool Expected404Scope::is404Expected()
{
    return expected_404_scope_count == 0;
}

}
