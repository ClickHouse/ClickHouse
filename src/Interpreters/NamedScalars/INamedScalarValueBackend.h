#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <functional>
#include <optional>
#include <string_view>
#include <utility>

namespace DB
{

/// Outcome of a refresh body's publish attempt. Storage failures
/// propagate as exceptions instead; those are recorded as refresh errors.
enum class RefreshPublishResult : UInt8
{
    Published,
    /// The lease was lost or the definition no longer points at this UUID
    /// (peer OR REPLACE / DROP). Caller treats this as a no-op outcome;
    /// the watcher will reconcile the local catalog.
    Diverged,
};

enum class NamedScalarCacheKind : UInt8
{
    Local,
    Shared,
};

inline std::string_view toString(NamedScalarCacheKind cache_kind)
{
    switch (cache_kind)
    {
        case NamedScalarCacheKind::Local:
            return "local";
        case NamedScalarCacheKind::Shared:
            return "shared";
    }
    UNREACHABLE();
}

class NamedScalarRefreshLease
{
public:
    NamedScalarRefreshLease() = default;
    explicit NamedScalarRefreshLease(std::function<RefreshPublishResult(const String &)> publish_callback_)
        : publish_callback(std::move(publish_callback_)) {}

    explicit operator bool() const { return static_cast<bool>(publish_callback); }

    RefreshPublishResult publish(const String & value_blob) const
    {
        return publish_callback(value_blob);
    }

private:
    std::function<RefreshPublishResult(const String &)> publish_callback;
};

/// Serialized value backend used by NamedScalar. Catalogs provide concrete
/// local/shared implementations; the scalar owns decoding, lifetime, and
/// refresh semantics.
class INamedScalarValueBackend
{
public:
    virtual ~INamedScalarValueBackend() = default;

    virtual bool supportsValueWatches() const = 0;
    virtual std::optional<String> readValueBlob(const String & value_key) = 0;
    virtual std::optional<String> readValueBlobAndWatch(const String & value_key, std::function<void()> on_change) = 0;
    virtual void removeValue(const String & value_key) = 0;
    virtual std::optional<NamedScalarRefreshLease> tryAcquireRefreshLease(
        const String & name,
        const String & value_key) = 0;
};

}
