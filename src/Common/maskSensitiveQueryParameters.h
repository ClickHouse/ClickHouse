#pragma once

#include <string>
#include <string_view>
#include <vector>


namespace DB
{

/// Redacts the values of sensitive HTTP query-string parameters in a request URI so they are
/// safe to write to logs (system.text_log), OpenTelemetry spans and similar sinks.
///
/// A parameter is considered sensitive when its name contains (case-insensitively) any of:
/// "secret", "password", "passwd", "token", "credential", "signature", "sig", "key".
/// This covers the `param_*` query-parameter binding mechanism (e.g. `param_secret_key`,
/// `param_aws_secret_access_key`), the `password` HTTP parameter, and S3-style
/// `x-amz-*`/`signature` parameters. The value of every matching parameter is replaced with
/// "[HIDDEN]", while names, ordering and all other parameters are preserved so the URI stays
/// useful for debugging.
///
/// Only the query-string part (after the first '?') is processed; the path is left untouched.
/// If the URI has no query string, the original string is returned unchanged.
std::string maskSensitiveQueryParametersInURI(const std::string & uri);

/// Decode a query-string component (a parameter name or value) the same way the server interprets it:
/// HTMLForm::readQuery turns '+' into a space and percent-decodes ('%XX'). A name is classified on
/// this decoded form, otherwise an encoded name like "pass%77ord" (which the server accepts as
/// "password") would slip past the match and leak its value. Tolerant by design: a malformed escape
/// is copied through verbatim and nothing throws, since the result is only matched against, never logged.
std::string decodeQueryComponent(std::string_view name);

/// Whether a percent-decoded query parameter name belongs to an S3 or GCS presigned URL: `AWSAccessKeyId`, `Signature`,
/// `Expires`, `GoogleAccessId`, or `X-Amz-*`/`X-Goog-*` in any case. `S3::URI` then reads no key from the query.
bool isPresignedURLParameterName(std::string_view name);

/// Returns the credentials in the query string of a presigned S3, GCS or Azure SAS URL (SigV4 and SigV2): the signature, the
/// session token and the access key id, each as sent and percent-decoded, so that a text echoing them back can be
/// scrubbed. Names are matched percent-decoded and case-insensitively. URL metadata such as `X-Amz-Date` is not included.
std::vector<std::string> presignedURLCredentials(std::string_view uri);

}
