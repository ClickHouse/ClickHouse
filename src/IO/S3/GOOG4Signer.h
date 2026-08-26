#pragma once
#include "config.h"
#if USE_AWS_S3

#include <chrono>

namespace Aws::Http { class HttpRequest; }
namespace Aws::Auth { class AWSCredentials; }

namespace DB::S3
{

/// Sign `request` in place with GOOG4-HMAC-SHA256 — Google Cloud Storage's native V4 HMAC scheme
/// for the XML API. Structurally sigv4 with renamed constants: key prefix `GOOG4`, scope
/// terminator `goog4_request`, headers `x-goog-date` / `x-goog-content-sha256`. Bodies are never
/// hashed (`UNSIGNED-PAYLOAD`), so streaming uploads sign in O(1).
///
/// Signs the `host` header plus EVERY `x-goog-*` header present on the request (GCS requires all
/// x-goog headers to be signed); other headers ride unsigned. `now` is injected so unit tests can
/// pin the timestamp to fixed vectors.
///
/// Live-validated against GCS 2026-07-03 (see `utils/ca-soak/scripts/gcs_goog4_probe.py`, 12/12).
void signRequestGOOG4(
    Aws::Http::HttpRequest & request,
    const Aws::Auth::AWSCredentials & credentials,
    std::chrono::system_clock::time_point now);

}

#endif
