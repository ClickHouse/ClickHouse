#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <optional>
#include <string>
#include <vector>

namespace DB::Cas
{

/// Provides the pool's plain-object surface: loose, non-content-addressed objects whose key is
/// chosen by the caller. This covers namespace files under `cas/ns/state/<life_id>/_files/` -- keyed
/// by the namespace LIFE, never by its bare name -- and mountpoint objects mirrored by path. The object
/// bodies are raw passthrough bytes; this component does not decode them as CAS metadata.
///
/// The component holds references to the shared `CasRequests` and `Layout` only. It owns no pool mutex
/// and has no pool back-reference, allowing `Pool` to retain thin forwarding methods with the same
/// external interface. The private helpers implement the shared head-plus-conditional-write and
/// head-plus-exact-delete protocols used by both object families, over an operation admitted fresh for
/// each call: every attempt inside it re-checks admission before touching the store, so a mount lease
/// lost mid-call is refused rather than written through.
class CasPlainObjects
{
public:
    CasPlainObjects(CasRequests & requests_, const Layout & layout_)
        : requests(requests_), layout(layout_)
    {
    }

    /// Stores the raw bytes under ONE LIFE's `_files/` prefix. Existing files are replaced
    /// conditionally using the object incarnation observed by the admitted operation; a storage failure
    /// or an exhausted conflict-retry bound is propagated as an exception.
    ///
    /// `life` is supplied by the caller and never re-derived here, so this surface issues no catalog
    /// request of its own. A stale writer therefore targets its own old incarnation's key and cannot
    /// write into a newer life's prefix.
    void putNamespaceFile(const NamespaceLifeId & life, const String & name, const String & bytes);

    /// Reads a namespace file of ONE LIFE without interpreting its body. Returns `nullopt` when the
    /// object is absent and propagates backend read failures. A stale reader may see stale bytes or
    /// absence, never a newer incarnation's data: its key names the life it was given.
    std::optional<String> getNamespaceFile(const NamespaceLifeId & life, const String & name);

    /// Enumerates the file names directly below ONE LIFE's `_files/` prefix. Fetches all paginated
    /// backend results, strips the prefix, and returns names in sorted order independent of backend
    /// listing order.
    std::vector<String> listNamespaceFiles(const NamespaceLifeId & life);

    /// Removes the current OBJECT incarnation of one of a life's files, if any (the object incarnation,
    /// not the namespace incarnation, which `life` fixes). A concurrent replacement is never removed
    /// accidentally: the underlying `removeCurrent` re-heads and retries against the new incarnation.
    void removeNamespaceFile(const NamespaceLifeId & life, const String & name);

    /// Stores raw bytes for a loose mountpoint file at the path-derived object key. The key is
    /// validated and constructed by `Layout`; this method applies the same conditional overwrite
    /// protocol as namespace files.
    void putMountpointObject(const String & key, const String & bytes);

    /// Reads a path-mirrored mountpoint object as raw bytes. Returns `nullopt` for an absent object
    /// and propagates backend read failures.
    std::optional<String> getMountpointObject(const String & key);

    /// Checks only object metadata, not the body. A directory at the path-derived key is therefore
    /// reported as absent, matching object-store semantics and avoiding a filesystem exception from
    /// attempting to read a directory as an object.
    bool mountpointObjectExists(const String & key);

    /// Removes the current path-mirrored mountpoint-object incarnation, if present, using
    /// `removeCurrent`'s re-head-and-retry so a concurrent rewrite remains intact.
    void removeMountpointObject(const String & key);

private:
    /// Creates or conditionally replaces one raw object. The write always sends `bytes` regardless of
    /// what is currently there, settling a refused precondition with a HEAD; only an ambiguous attempt
    /// reads the body, because only the bytes can prove it landed. Retries a lost precondition under
    /// the engine's own bound; an exhausted retry or a store refusal is propagated as an exception.
    void casPutObject(const String & full_key, const String & bytes);

    /// Reads one raw object by its complete backend key and returns `nullopt` when it is absent.
    std::optional<String> casGetObject(const String & full_key);

    /// Removes one raw object at its current incarnation. Absence is a successful no-op; a concurrent
    /// replacement is retried against the freshly observed incarnation, and an exhausted retry is
    /// propagated as an exception.
    void casRemoveObject(const String & full_key);

    CasRequests & requests;
    const Layout & layout;
};

}
