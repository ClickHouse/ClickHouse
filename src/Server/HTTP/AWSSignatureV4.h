#pragma once

#include <base/types.h>

#include <ctime>
#include <memory>


namespace DB
{

class HTTPServerRequest;
class ReadBuffer;

/// Server-side support for requests signed with AWS Signature Version 4, as sent by S3-compatible clients.
/// The access key ID is the ClickHouse user name, and the secret access key is the user's password.
/// See https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
///
/// Both forms are supported:
/// - the `Authorization: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...` header;
/// - a presigned URL with the `X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
///   `X-Amz-SignedHeaders` and `X-Amz-Signature` query parameters.

/// Returns true if the request is signed with AWS Signature Version 4 in either form.
bool isAWSSignatureV4Request(const HTTPServerRequest & request);

/// Returns true for the query parameters which are a part of the AWS protocol rather than of the request to ClickHouse:
/// `X-Amz-*` (the presigned URL parameters) and `x-id` (the operation name appended by AWS SDKs, e.g. `x-id=GetObject`).
bool isAWSSignatureV4QueryParameter(const String & name);

struct AWSSignatureV4Request
{
    String access_key_id;
    /// The credential scope.
    String date;
    String region;
    String service;

    /// The string that the client has signed, built from the canonical request.
    String string_to_sign;
    /// The signature provided by the client (lowercase hex).
    String signature;

    /// The value describing the request body: a lowercase hex SHA-256 of the body, `UNSIGNED-PAYLOAD`
    /// or `STREAMING-UNSIGNED-PAYLOAD-TRAILER`.
    String payload_hash;
};

/// Validates the request and builds the string to sign from it.
/// Throws `AUTHENTICATION_FAILED` if the request is malformed, expired, or does not satisfy the requirements:
/// the `host` header and every `X-ClickHouse-*` header have to be signed, so that the signature covers everything
/// that defines the request. The signature itself is not verified here: it requires the user's secret.
AWSSignatureV4Request parseAWSSignatureV4Request(const HTTPServerRequest & request, time_t now);

/// Returns the payload hash of the request (see `AWSSignatureV4Request::payload_hash`).
String getAWSSignatureV4PayloadHash(const HTTPServerRequest & request);

/// Returns true if the payload hash is a SHA-256 of the body, so that the body is covered by the signature.
bool isAWSSignatureV4PayloadSigned(const String & payload_hash);

/// Wraps the request body so that it is verified against the payload hash (an exception is thrown at the end of
/// the body if it does not match) or decoded from the `aws-chunked` content encoding.
std::unique_ptr<ReadBuffer> wrapAWSSignatureV4RequestBody(
    std::unique_ptr<ReadBuffer> body, const String & payload_hash, const HTTPServerRequest & request);

/// Returns true if the value of the `Range` header is a single range from the beginning: `bytes=0-` or `bytes=0-<end>`.
bool isAWSRangeFromBeginning(const String & range);

/// Removes `aws-chunked` from the value of the `Content-Encoding` header.
String removeAWSChunkedContentEncoding(const String & content_encoding);

}
