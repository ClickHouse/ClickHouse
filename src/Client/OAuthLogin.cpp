#include <config.h>
#include <Client/OAuthLogin.h>

#if USE_JWT_CPP && USE_SSL

#include <Client/OAuthFlowRunner.h>

#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <system_error>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

std::string cacheKey(const std::string & client_id)
{
    std::string hash = encodeSHA256(client_id);
    std::string hex;
    hex.reserve(32);
    for (unsigned char c : hash)
    {
        constexpr char digits[] = "0123456789abcdef";
        hex += digits[(c >> 4) & 0xF];
        hex += digits[c & 0xF];
    }
    return hex.substr(0, 16);
}

std::string cacheFilePath()
{
    const char * home = std::getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
    if (!home)
        return "";
    return std::string(home) + "/.clickhouse-client/oauth_cache.json";
}

std::string readCachedRefreshTokenImpl(const std::string & client_id)
{
    const std::string path = cacheFilePath();
    if (path.empty())
        return "";

    std::ifstream f(path);
    if (!f.is_open())
        return "";

    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    try
    {
        Poco::JSON::Parser parser;
        auto result = parser.parse(content);
        const auto & obj = result.extract<Poco::JSON::Object::Ptr>();
        const std::string key = cacheKey(client_id);
        if (obj->has(key))
            return obj->getValue<std::string>(key);
    }
    catch (...)
    {
        std::cerr << "Note: OAuth token cache at '" << cacheFilePath()
                  << "' could not be parsed and will be ignored.\n";
    }
    return "";
}

}

namespace
{

/// RAII close + unlock for a POSIX fd used as an advisory lock. close(2)
/// implicitly releases the lock; we unlock first only to keep the intent
/// explicit at the close site.
class ScopedFlockFd
{
public:
    explicit ScopedFlockFd(int fd_) : fd(fd_) {}
    ~ScopedFlockFd()
    {
        if (fd >= 0)
        {
            ::flock(fd, LOCK_UN);
            ::close(fd);
        }
    }
    ScopedFlockFd(const ScopedFlockFd &) = delete;
    ScopedFlockFd & operator=(const ScopedFlockFd &) = delete;
    int get() const { return fd; }

private:
    int fd;
};

}

namespace
{

/// Run `mutator` against the on-disk OAuth refresh-token cache as a single
/// atomic read-modify-write under an advisory exclusive flock. Both the writer
/// (cache a new refresh token) and the evictor (drop a rejected one) go
/// through this helper so they share the crash-safe rename and the
/// concurrency lock — using two different paths for the two operations would
/// reintroduce the lost-update race that M1 fixed.
///
/// `warn_on_failure` controls the user-facing diagnostic policy:
///   - true  (writers): a failed FS operation triggers a single warning that
///     names both the underlying cause (syscall + errno or fs error) AND the
///     user-visible consequence ("you will be prompted to log in again on the
///     next invocation"). Without naming the consequence, the syscall warning
///     alone is too cryptic for users to connect to the symptom they later
///     observe (unexpected re-auth on the next run), so they cannot act on
///     it; this is the bug we are fixing here.
///   - false (evictors): failures are silent. Eviction is best-effort cleanup
///     after the IdP has rejected a cached refresh token; if the cache file
///     is unwritable, the next interactive auth's writer attempt will produce
///     exactly the same warning anyway, and that is the message users
///     actually need (because it tells them caching is broken going forward,
///     not just that an already-dead token couldn't be deleted).
template <typename Mutator>
void mutateRefreshTokenCache(Mutator && mutator, bool warn_on_failure)
{
    /// Two-part diagnostic: first a one-line headline that names the
    /// user-visible consequence (so the message is actionable even for users
    /// who don't recognise the underlying syscall), then a second line with
    /// the technical cause for operators / bug reports. Captured by reference
    /// so each error path is one line at the call site.
    auto fail = [&](const std::string & cause)
    {
        if (!warn_on_failure)
            return;
        std::cerr
            << "Warning: OAuth refresh token will NOT be persisted to disk; "
               "you may be prompted to log in again on the next invocation.\n"
            << "  Cause: " << cause << "\n";
    };
    /// errno-flavoured variant. Snapshot errno immediately on entry — the
    /// global is fragile across any intervening allocation/IO.
    auto fail_errno = [&](const char * what, const std::string & arg)
    {
        const int e = errno;
        if (!warn_on_failure)
            return;
        /// generic_category().message is the thread-safe equivalent of
        /// std::strerror, which is flagged by clang-tidy.
        fail(std::string(what) + " '" + arg + "' failed: " + std::generic_category().message(e));
    };

    const std::string path = cacheFilePath();
    if (path.empty())
    {
        /// Pre-fix this branch returned silently, so users running without
        /// $HOME (cron, systemd units without `User=`, sandboxed containers)
        /// would re-auth on every invocation with no diagnostic at all. Now
        /// we surface the cause on writes; reads still no-op silently because
        /// "no HOME" on first run is indistinguishable from "no cache yet".
        fail("cannot determine cache file path: HOME environment variable is unset");
        return;
    }

    namespace fs = std::filesystem;
    const fs::path cache_path(path);
    const fs::path cache_dir = cache_path.parent_path();

    std::error_code ec;
    fs::create_directories(cache_dir, ec);
    if (ec)
    {
        fail("failed to create directory '" + cache_dir.string() + "': " + ec.message());
        return;
    }

    /// Serialize concurrent writers via an advisory exclusive lock on a
    /// dedicated sibling file. cache_path itself cannot be locked because the
    /// rename(2) below swaps its inode; the .lock file is never renamed, so
    /// the lock survives the whole read-modify-write. Readers don't take this
    /// lock — rename(2) is atomic on POSIX, so a concurrent reader observes
    /// either the previous or the new cache, never a torn file.
    const fs::path lock_path = cache_dir / ".oauth_cache.lock";
    int raw_lock_fd = ::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (raw_lock_fd < 0)
    {
        fail_errno("open lock file", lock_path.string());
        return;
    }
    ScopedFlockFd lock_fd(raw_lock_fd);
    if (::flock(lock_fd.get(), LOCK_EX) != 0)
    {
        fail_errno("flock", lock_path.string());
        return;
    }

    /// Read existing entries under the lock so the read-modify-write is
    /// atomic with respect to other concurrent writers (lost-update fix).
    Poco::JSON::Object obj;
    {
        std::ifstream f(path);
        if (f.is_open())
        {
            std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
            try
            {
                Poco::JSON::Parser parser;
                auto result = parser.parse(content);
                const auto & existing = result.extract<Poco::JSON::Object::Ptr>();
                for (const auto & [key, value] : *existing)
                    obj.set(key, value);
            }
            catch (...)
            {
                std::cerr << "Note: OAuth token cache at '" << path
                          << "' could not be parsed; existing entries will be lost.\n";
            }
        }
    }

    mutator(obj);

    /// mkstemp gives a process- and thread-unique path with O_EXCL semantics,
    /// so concurrent invocations can no longer race on a fixed `.tmp` name and
    /// corrupt each other's writes. POSIX requires mkstemp to create the file
    /// 0600, so the refresh token is never on disk in a wider mode. The
    /// template lives in the cache directory so rename(2) is same-FS and
    /// atomic. We close the fd immediately and reopen via std::ofstream so
    /// the existing serialization path stays unchanged; the random suffix
    /// plus the held flock guarantee no other writer can interfere with the
    /// reopen.
    std::string tmpl = (cache_dir / "oauth_cache.XXXXXX").string();
    int tmp_fd = ::mkstemp(tmpl.data());
    if (tmp_fd < 0)
    {
        fail_errno("mkstemp", tmpl);
        return;
    }
    ::close(tmp_fd);

    {
        std::ofstream out(tmpl, std::ios::trunc | std::ios::binary);
        if (!out.is_open())
        {
            fail_errno("open for write", tmpl);
            ::unlink(tmpl.c_str());
            return;
        }
        Poco::JSON::Stringifier::stringify(obj, out);
        out.close();
        if (out.fail())
        {
            /// iostreams don't reliably surface errno through fail(); we
            /// can't blame a specific syscall, but the consequence message
            /// is still the actionable part for the user.
            fail("failed to serialize JSON to '" + tmpl + "'");
            ::unlink(tmpl.c_str());
            return;
        }
    }

    if (::rename(tmpl.c_str(), path.c_str()) != 0)
    {
        fail_errno("rename to", path);
        ::unlink(tmpl.c_str());
        return;
    }
}

void removeCachedRefreshToken(const std::string & client_id)
{
    mutateRefreshTokenCache(
        [&](Poco::JSON::Object & obj) { obj.remove(cacheKey(client_id)); },
        /*warn_on_failure=*/false);
}

}

void writeCachedRefreshToken(const std::string & client_id, const std::string & refresh_token)
{
    mutateRefreshTokenCache(
        [&](Poco::JSON::Object & obj) { obj.set(cacheKey(client_id), refresh_token); },
        /*warn_on_failure=*/true);
}

namespace
{

std::string tryRefreshToken(const OAuthCredentials & creds, const std::string & refresh_token)
{
    try
    {
        std::string body
            = "grant_type=refresh_token"
              "&client_id=" + urlEncodeOAuth(creds.client_id)
            + "&refresh_token=" + urlEncodeOAuth(refresh_token);
        /// Public clients (no registered secret) must omit the parameter
        /// entirely; see loadOAuthCredentials() for the rationale.
        if (!creds.client_secret.empty())
            body += "&client_secret=" + urlEncodeOAuth(creds.client_secret);

        auto resp = postOAuthForm(creds.token_uri, body);
        if (resp->has("error"))
        {
            const std::string err = resp->getValue<std::string>("error");
            std::cerr << "Note: cached refresh token was rejected (" << err << "); re-authenticating.\n";
            /// RFC 6749 §5.2: invalid_grant means the refresh token itself is
            /// no longer usable (revoked / expired / mismatched redirect).
            /// Evict it so subsequent invocations skip the doomed round-trip.
            /// Other error codes (invalid_client, invalid_request, ...) mean
            /// our request was wrong, not the token, so we keep the cache.
            if (err == "invalid_grant")
                removeCachedRefreshToken(creds.client_id);
            return "";
        }
        if (resp->has("refresh_token"))
            writeCachedRefreshToken(creds.client_id, resp->getValue<std::string>("refresh_token"));
        if (resp->has("id_token"))
            return resp->getValue<std::string>("id_token");
    }
    catch (const std::exception & e)
    {
        std::cerr << "Note: refresh token exchange failed (" << e.what()
                  << "); re-authenticating.\n";
    }
    return "";
}

}

OAuthCredentials loadOAuthCredentials(const std::string & path)
{
    std::ifstream f(path);
    if (!f.is_open())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "OAuth credentials file not found: '{}'\n"
            "Place a Google-format credentials JSON at that path, or specify "
            "--oauth-credentials /path/to/file.json",
            path);

    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed;
    try
    {
        parsed = parser.parse(content);
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse OAuth credentials file '{}': {}", path, e.what());
    }

    auto root = parsed.extract<Poco::JSON::Object::Ptr>();

    Poco::JSON::Object::Ptr app;
    if (root->has("installed"))
        app = root->getObject("installed");
    else if (root->has("web"))
        app = root->getObject("web");
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "OAuth credentials file '{}' must have an 'installed' or 'web' top-level key",
            path);

    auto require = [&](const std::string & key) -> std::string
    {
        if (!app->has(key))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "OAuth credentials file '{}' is missing required field '{}'",
                path,
                key);
        return app->getValue<std::string>(key);
    };

    OAuthCredentials creds;
    creds.client_id = require("client_id");
    creds.auth_uri = require("auth_uri");
    creds.token_uri = require("token_uri");

    /// client_secret is optional: per RFC 6749 §2.1 / RFC 8252 §8.4, native
    /// OIDC clients are typically registered as "public" and have no secret;
    /// PKCE (always used in the auth-code flow here) and the device_code
    /// itself (in the device flow) provide the client-binding guarantee that a
    /// secret would otherwise carry. An absent or empty value here causes the
    /// downstream POST bodies to omit the client_secret form parameter
    /// entirely — sending it with an empty value is treated by Auth0, Entra
    /// ID, Keycloak and others as a malformed confidential-client credential
    /// and rejected with invalid_client, so omission is required, not just
    /// preferred.
    if (app->has("client_secret"))
        creds.client_secret = app->getValue<std::string>("client_secret");

    if (app->has("device_authorization_uri"))
        creds.device_auth_uri = app->getValue<std::string>("device_authorization_uri");
    if (app->has("issuer"))
        creds.issuer = app->getValue<std::string>("issuer");

    auto warn_if_http = [&](const std::string & field, const std::string & uri)
    {
        if (uri.starts_with("http://"))
            std::cerr << "Warning: OAuth credentials field '" << field << "' uses plain HTTP ('"
                      << uri << "'). Token exchanges over HTTP expose client credentials.\n";
    };
    warn_if_http("token_uri", creds.token_uri);
    warn_if_http("auth_uri", creds.auth_uri);
    if (!creds.device_auth_uri.empty())
        warn_if_http("device_authorization_uri", creds.device_auth_uri);

    return creds;
}

std::string obtainIDToken(const OAuthCredentials & creds, OAuthFlowMode mode)
{
    const std::string cached_refresh = readCachedRefreshTokenImpl(creds.client_id);
    if (!cached_refresh.empty())
    {
        const std::string id_token = tryRefreshToken(creds, cached_refresh);
        if (!id_token.empty())
            return id_token;
    }

    if (mode == OAuthFlowMode::Device)
        return runOAuthDeviceFlow(creds);
    return runOAuthAuthCodeFlow(creds);
}

} // namespace DB

#endif // USE_JWT_CPP && USE_SSL
