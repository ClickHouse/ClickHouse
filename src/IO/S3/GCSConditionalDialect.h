#pragma once
#include "config.h"
#if USE_AWS_S3

#include <optional>
#include <string>

namespace Aws::Http { class HttpRequest; }
namespace Poco::Net { class HTTPResponse; }

namespace DB::S3
{

/// The GCS conditional dialect, request side (spec: 2026-07-03-cas-gcs-generation-binding-design).
/// Applied at the wire boundary by the GCS-mode Poco HTTP clients, so everything above keeps
/// speaking AWS. Translations:
///   - AWS auth artifacts (`authorization`, `x-amz-date`, `x-amz-content-sha256`,
///     `x-amz-security-token`, `x-amz-api-version`) are DROPPED (the caller re-authenticates);
///   - every remaining `x-amz-*` header is renamed to `x-goog-*`;
///   - `If-None-Match: *` becomes `x-goog-if-generation-match: 0`;
///   - `If-Match: "<digits>"` (quotes optional) becomes `x-goog-if-generation-match: <digits>`.
/// Fail-close guards (throw LOGICAL_ERROR, the request never leaves the process):
///   - `If-None-Match` with any value other than `*` (no GCS equivalent);
///   - a non-numeric `If-Match` (an ETag-kind token leaked into a generation dialect);
///   - a CONDITIONAL CompleteMultipartUpload (POST with `uploadId` and no `partNumber`): GCS
///     silently ignores preconditions there (measured live 2026-07-03) — silent data loss.
void applyGcsConditionalDialectToRequest(Aws::Http::HttpRequest & request);

/// The dialect, response side: when the response carries `x-goog-generation`, returns it QUOTED —
/// the caller substitutes it for the `ETag` response header, making the generation ride the
/// entire existing ETag/token plumbing unchanged. Returns nullopt when no generation is present.
std::optional<std::string> gcsGenerationETagOverride(const Poco::Net::HTTPResponse & response);

}

#endif
