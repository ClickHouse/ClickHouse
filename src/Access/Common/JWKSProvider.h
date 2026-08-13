#include <config.h>

#if USE_JWT_CPP
#include <IO/ConnectionTimeouts.h>
#include <base/types.h>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>
#include <filesystem>
#include <shared_mutex>

#include <Poco/URI.h>

namespace DB
{

using JWKSType = jwt::jwks<jwt::traits::kazuho_picojson>;

/// JWKS (JSON Web Key Set) is a kind of a set of public keys that are used to validate JWT authenticity locally.
/// They are usually exposed by identity providers (e.g. Keycloak) via a well-known URI (usually /.well-known/jwks.json)
/// This interface is responsible for managing JWKS. Retrieving, caching and refreshing of JWKS happens here.
/// JWKS can either be static (e.g. provided in config) or dynamic (fetched from a remote URI and).
class IJWKSProvider
{
public:
    virtual ~IJWKSProvider() = default;

    virtual JWKSType getJWKS() = 0;
};

class JWKSClient : public IJWKSProvider
{
public:
    explicit JWKSClient(const String & uri, const size_t refresh_ms_, const ConnectionTimeouts & timeouts_)
        : refresh_timeout(refresh_ms_), jwks_uri(uri), timeouts(timeouts_) {}

    ~JWKSClient() override = default;
    JWKSClient(const JWKSClient &) = delete;
    JWKSClient(JWKSClient &&) = delete;
    JWKSClient &operator=(const JWKSClient &) = delete;
    JWKSClient &operator=(JWKSClient &&) = delete;

    JWKSType getJWKS() override;

private:
    size_t refresh_timeout;
    Poco::URI jwks_uri;
    ConnectionTimeouts timeouts;

    std::shared_mutex mutex;
    std::optional<JWKSType> cached_jwks;
    /// `steady_clock` (not `system_clock`): refresh-cooldown is an elapsed-time
    /// measurement; a wall-clock jump must not skip or freeze it.
    /// `std::nullopt` means "no fetch has ever been attempted" -- needed to
    /// distinguish a never-attempted state from a recently-failed one, because
    /// the steady-clock epoch may sit only a short distance in the past on
    /// freshly-booted hosts / containers with isolated CLOCK_MONOTONIC, making
    /// a zero-initialized time_point look like a "recent" attempt.
    std::optional<std::chrono::time_point<std::chrono::steady_clock>> last_request_send;
};

struct StaticJWKSParams
{
    StaticJWKSParams(const std::string &static_jwks_, const std::string &static_jwks_file_);

    String static_jwks;
    String static_jwks_file;
};

class StaticJWKS : public IJWKSProvider
{
public:
    explicit StaticJWKS(const StaticJWKSParams &params);

    /// Reload the JWKS from disk if `static_jwks_file` was specified and the
    /// file's mtime has advanced since the last load. Inline `static_jwks`
    /// (no file path) is returned from the in-memory copy without I/O.
    /// Without this, rotating the underlying file did NOT refresh the
    /// in-memory keys -- admins had to trigger a full
    /// `setExternalAuthenticatorsConfig` reload to pick up the new file.
    JWKSType getJWKS() override;

private:
    void reloadFromFileIfChangedNoLock();

    /// Source path -- empty when JWKS came from inline `<static_jwks>` config.
    String static_jwks_file;
    /// `mtime` of the file at the most recent successful load. Used to detect
    /// rotation. `file_time_type::min()` means "not loaded from a file" or
    /// "never seen the file yet".
    std::filesystem::file_time_type last_loaded_mtime = std::filesystem::file_time_type::min();

    mutable std::shared_mutex mutex;
    JWKSType jwks;
};

}
#endif
