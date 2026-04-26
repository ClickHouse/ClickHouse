#include <IO/ExpectCredentialProbe4xxScope.h>

#include <base/defines.h>

namespace DB
{

thread_local size_t expected_credential_probe_4xx_scope_count = 0;

ExpectCredentialProbe4xxScope::ExpectCredentialProbe4xxScope()
    : initial_thread_id(std::this_thread::get_id())
{
    ++expected_credential_probe_4xx_scope_count;
}

ExpectCredentialProbe4xxScope::~ExpectCredentialProbe4xxScope()
{
    /// check that instance is destroyed in the same thread
    chassert(initial_thread_id == std::this_thread::get_id());
    chassert(expected_credential_probe_4xx_scope_count);
    --expected_credential_probe_4xx_scope_count;
}

bool ExpectCredentialProbe4xxScope::isActive()
{
    return expected_credential_probe_4xx_scope_count != 0;
}

}
