#pragma once

#include <base/types.h>

#include <time.h>
#include <vector>

namespace DB
{

/// Information about a TLS client certificate presented during the connection.
/// Server-side only (not serialized) and used for session_log enrichment.
/// Empty when the client did not present a certificate.
struct ClientCertificateInfo
{
    /// Certificate subjects in the form "CN:..." / "SAN:..." (Common Name and Subject Alternative Names).
    std::vector<String> subjects;
    String serial;
    String issuer;
    time_t not_before = 0;
    time_t not_after = 0;
};

}
