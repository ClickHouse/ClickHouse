#pragma once

#include <string>
#include <string_view>

namespace DB
{

/** Hides the secrets of an Azure connection string as the Azure SDK reads it (`Azure::Storage::_internal::ParseConnectionString`,
  * `contrib/azure/sdk/storage/azure-storage-common/src/storage_credential.cpp`): a key runs to '=' and its value to ';', keys
  * match exactly, and the last copy of a repeated key wins. Splitting at every ';' first can only hide more than the SDK reads.
  *   - secret keys:   `AccountName=a;AccountKey=K;SharedAccessSignature=S` -> `AccountName=a;AccountKey=[HIDDEN];SharedAccessSignature=[HIDDEN]`
  *   - repeated keys: `AccountKey=K1;AccountKey=K2` -> `AccountKey=[HIDDEN];AccountKey=[HIDDEN]`
  *   - an endpoint is a url, so a query in it is a SAS: `BlobEndpoint=https://h/?sig=S` or `BlobEndpoint=https://u@h/` -> `BlobEndpoint=[HIDDEN]`
  *   - the whole string is hidden when a '?', '#' or '@' is still shown, or when it has one and holds a url outside the pair
  *     values (':' or '/' in a key or in a part without '='; no key has them): `https://h/?sig=S` or `https://u:p=s@h/` -> `[HIDDEN]`
  * Returns whether anything was hidden.
  */
bool maskAzureConnectionStringSecrets(std::string & str);

/// Where RFC 3986, section 3.2, ends the authority of a URI. `Poco::URI` ends it there too.
inline constexpr std::string_view URI_AUTHORITY_TERMINATORS = "/?#";

/// Replace the password `findURIPasswordRange` finds with `[HIDDEN]`. Returns whether anything was masked.
bool maskURIPassword(std::string & uri, std::string_view authority_terminators = "/");

/// Hides the secret option values and the URI password of a MongoDB connection string or option list; returns whether any was hidden.
/// As the driver reads them, an option name is case-insensitive and not percent-decoded, and a value runs to the next '&'.
bool maskMongoDBConnectionString(std::string & str);

/** The offset just past the `://` of a value that starts with an RFC 3986 scheme, `npos` otherwise.
  */
size_t findURIAuthority(std::string_view uri);

/** Mask the userinfo part of a URL: `scheme://anything@rest` becomes `scheme://[HIDDEN]@rest`.
  * Returns whether anything was masked.
  *
  * This used to be the regular expression `^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]+@` rewritten to
  * `\1[HIDDEN]@`. Only a match at the start of the string counts, and the userinfo is taken
  * greedily up to the last '@' before the path, so a password that itself contains an at-sign is
  * masked whole. `src/Common/tests/gtest_mask_uri_password.cpp` checks this against re2.
  */
bool maskURIUserinfo(std::string & url);

/// Mask the values of the query parameters that carry credentials in a presigned URL, so that
/// `...?X-Amz-Signature=abc&foo=1` becomes `...?X-Amz-Signature=[HIDDEN]&foo=1`, and of the
/// `connection_string` parameter of an ODBC/JDBC bridge request. Every occurrence is masked.
/// Returns whether anything was masked.
///
/// Apart from `connection_string`, this used to be the regular expression
/// `([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|sig|(?i:X-Amz-|X-Goog-)[^=?&]*)=)[^&#]*`
/// rewritten to `\1[HIDDEN]` globally; `src/Common/tests/gtest_mask_uri_password.cpp` checks this against re2.
/// A name is also matched percent-decoded, as the storage endpoint reads it: `?%73ig=` is `?sig=`.
/// `sig` is the signature of an Azure SAS.
bool maskPresignedURLParameters(std::string & url);

/** Mask both credential carriers of a URL: the userinfo (`user:password@`) and the values of the
  * presigned-URL query parameters. Returns whether anything was masked.
  *
  * This is the masking to use for a whole HTTP/S3 URL. Scanning for a presigned parameter is only
  * sound on a value that is one whole URL (see `maskPresignedURLParameters`), so the argument must be
  * one URL, not a URL embedded in a longer string.
  */
inline bool maskURICredentials(std::string & url)
{
    bool masked = maskURIUserinfo(url);
    masked |= maskPresignedURLParameters(url);
    return masked;
}

}
