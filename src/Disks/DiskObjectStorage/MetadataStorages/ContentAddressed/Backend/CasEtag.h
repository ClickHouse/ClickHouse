#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/defines.h>
#include <base/types.h>
#include <cstdint>

namespace DB::Cas
{

/// The per-dialect grammar a response value must meet to be an incarnation. Generation: canonical
/// positive decimal (no leading zero, not "0" -- zero is the dialect's absence sentinel). ETag:
/// non-empty, not "*" after trimming whitespace, no comma (a list matches any member). Emulated:
/// non-empty. `ObjectStorageBackend::isValidTokenValue` forwards here.
bool isIncarnationValue(Dialect dialect, const String & value);

/// One backend-observed incarnation of an object: the transport's own value naming that incarnation,
/// together with the backend and key it was observed against. "Etag" names the ROLE this value plays
/// -- whatever the backend hands back as the object's identity for a conditional write -- not the wire
/// field: it is a literal ETag on S3-compatible stores, a generation number on GCS's JSON dialect, and
/// a minted sequence value on the emulated/in-memory backends. Not default-constructible, not
/// constructible from a bare `String`, and minted ONLY by `CasRequests` -- a caller can hold one only
/// by way of an admitted read or write, so an `Etag` is always traceable to the request that produced
/// it.
class Etag
{
public:
    Etag() = delete;
    bool operator==(const Etag &) const = default;

    /// "etag:<value>" | "generation:<value>" | "emulated:<value>"
    String render() const;
    const String & key() const { return key_; }
    Dialect dialect() const { return dialect_; }
    uint64_t backendId() const { return backend_id_; }

private:
    friend class CasRequests;

    Etag(uint64_t backend_id, String key, Dialect dialect, String value)
        : backend_id_(backend_id), key_(std::move(key)), dialect_(dialect), value_(std::move(value))
    {
    }

    /// The transport's own text; CasRequests reads it to build the next conditional request.
    const String & value() const { return value_; }

    uint64_t backend_id_;
    String key_;
    Dialect dialect_;
    String value_;
};

inline String Etag::render() const
{
    switch (dialect_)
    {
        case Dialect::ETag: return "etag:" + value_;
        case Dialect::Generation: return "generation:" + value_;
        case Dialect::Emulated: return "emulated:" + value_;
    }
    UNREACHABLE();
}

/// An incarnation as recorded in a persisted manifest/ref: the dialect word and value, without any
/// live backend to check them against. Forward-only: a `PersistedEtag` is captured FROM a live
/// `Etag`, never the reverse -- a persisted record must never be trusted to mint a live one.
/// `matches` re-derives the same rendering the live incarnation would produce and compares it
/// textually, so the two representations can never drift apart.
struct PersistedEtag
{
    String dialect;    /// "etag" | "generation" | "emulated"
    String value;

    static PersistedEtag capture(const Etag & live);
    bool matches(const Etag & live) const;
};

inline PersistedEtag PersistedEtag::capture(const Etag & live)
{
    const String rendered = live.render();
    const auto colon = rendered.find(':');
    return PersistedEtag{rendered.substr(0, colon), rendered.substr(colon + 1)};
}

inline bool PersistedEtag::matches(const Etag & live) const
{
    return live.render() == dialect + ":" + value;
}

}
