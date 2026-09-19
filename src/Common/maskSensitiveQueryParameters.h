#pragma once

#include <string>


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

/// Mask every credential a URI can carry in its own text: the password of a
/// `scheme://user:password@host` authority and the values of the sensitive query parameters above
/// (a presigned S3/GCS signature, an Azure SAS `sig`, ...). A URI that carries none -- an ordinary
/// object path -- comes back unchanged.
///
/// For a string that merely contains a URI rather than being one: the password scan finds it
/// anywhere, and the query-string scan runs from the first '?' to the end, so a trailing suffix is
/// treated as part of the query string.
std::string maskCredentialsInURI(const std::string & uri);

}
