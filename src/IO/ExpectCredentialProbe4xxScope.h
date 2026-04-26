#pragma once

#include <thread>

namespace DB
{

/// Inside the inner scope
/// Remote storage 4xx responses received during AWS credential-provider probing
/// (e.g., EC2 IMDS metadata negotiation, STS web-identity refresh) are considered
/// expected. The AWS SDK silently falls through to the next provider in the chain
/// on these responses; without this scope, `PocoHTTPClient` would log them as
/// errors and pollute server logs / trigger false alerts.
///
/// Suppression is bounded to the 4xx range so that 5xx responses (genuine
/// regional STS or IMDS infrastructure outages) still surface as errors.
class ExpectCredentialProbe4xxScope
{
public:
    ExpectCredentialProbe4xxScope();
    ~ExpectCredentialProbe4xxScope();

    static bool isActive();

private:
    std::thread::id initial_thread_id;
};

}
