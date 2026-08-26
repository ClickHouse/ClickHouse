#pragma once
#include "config.h"
#if USE_AWS_S3

namespace Aws::Http { class HttpRequest; class HttpResponse; }
namespace Poco::Net { class HTTPResponse; }

namespace DB::S3
{

/// The GCS native-conditional adapter, request side. Applied at the wire boundary by the GCS-mode
/// Poco HTTP clients ONLY for a request marked `NativeConditional`, so ordinary traffic through the
/// same client keeps upstream AWS semantics. Translations:
///   - `If-None-Match: *` becomes `x-goog-if-generation-match: 0`;
///   - `If-Match: "<digits>"` (quotes optional) becomes `x-goog-if-generation-match: <digits>`;
///   - `x-amz-meta-*` becomes `x-goog-meta-*`, the prefix GCS documents for object metadata.
/// Fail-close guards, the request never leaves the process:
///   - `If-None-Match` with any value other than `*` (LOGICAL_ERROR: CAS only ever sends `*`);
///   - a non-numeric `If-Match` (CORRUPTED_DATA: a persisted token, or a storage response the
///     generation kind was stamped onto, that is not a generation number);
///   - the same metadata key under both prefixes with different values (BAD_ARGUMENTS);
///   - a CONDITIONAL CompleteMultipartUpload (POST with `uploadId` and no `partNumber`): GCS
///     silently ignores preconditions there (measured live 2026-07-03) — silent data loss.
void applyGcsConditionalDialectToRequest(Aws::Http::HttpRequest & request);

/// Authentication preparation for the native OAuth path, run only for a `NativeConditional` request:
/// drop the stale AWS signing artifacts so the Bearer token is the only credential on the wire.
/// Every other `x-amz-*` header passes through unchanged, matching the ordinary OAuth path — there is
/// deliberately no GOOG4-style allowlist here.
void prepareGcsRequestForOAuthAuthentication(Aws::Http::HttpRequest & request);

/// Authentication preparation for the GOOG4-HMAC path, run for EVERY request that client sends.
/// This path normalises prefixes deliberately: it signs with Google's native scheme, so every
/// `x-amz-*` header must have a decided fate before signing — dropped as an AWS signing artifact,
/// renamed to its `x-goog-` counterpart, or consumed because GCS has no counterpart. An `x-amz-*`
/// header with no rule raises BAD_ARGUMENTS rather than being guessed at or sent as-is. Whether GCS
/// would in fact reject a mixed-prefix request has not been measured, and the thrown message says so
/// too: the refusal is fail-closed under that uncertainty, not a consequence of a known rejection. No
/// request shape ClickHouse constructs on a normal bucket produces one.
void prepareGcsRequestForGoog4Authentication(Aws::Http::HttpRequest & request);

/// The adapter, response side, applied only for a `NativeConditional` request: copies the header
/// changes the AWS SDK parser needs from `poco_response` onto `sdk_response`. The generation IS the
/// incarnation token on GCS, so it is installed QUOTED as `ETag` and rides the entire existing
/// ETag/token plumbing unchanged; `x-goog-meta-*` is presented as `x-amz-meta-*`. The same metadata
/// key arriving under both prefixes with different values raises CORRUPTED_DATA. A `Default`
/// response is never passed here and so keeps its upstream ETag and headers byte-for-byte.
/// Consequence: CAS object attributes are legible only through a marked read. The AWS SDK parses only
/// `x-amz-meta-*` into its metadata map and this function holds the only reverse mapping, so a
/// `Default` read of a CAS object's attributes yields a silently empty map rather than an error.
void applyGcsConditionalDialectToResponse(const Poco::Net::HTTPResponse & poco_response, Aws::Http::HttpResponse & sdk_response);

}

#endif
