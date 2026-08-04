#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.h>
#include <functional>
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
/// The component holds references to the shared `Backend` and `Layout` only. It owns no pool mutex
/// and has no pool back-reference, allowing `Pool` to retain thin forwarding methods with the same
/// external interface. The private helpers implement the shared head-plus-conditional-write and
/// head-plus-exact-delete protocols used by both object families. A conditional outcome means that
/// the observed incarnation changed, so the helper re-reads the head and retries; the fixed bound
/// prevents an unexpected continuous conflict from becoming an unbounded operation and reports
/// `ABORTED` when it is reached.
///
/// Every durable write/delete on this surface is fence-generation-gated (rev.7 [C2]): `Pool` injects
/// two callbacks that reach its `mount_runtime` (declared AFTER this member, hence constructed
/// after it -- these callbacks capture `Pool` itself and are invoked only at runtime, post-
/// construction, exactly like `ref_ledger`'s callbacks in `CasPool.cpp`, so referencing a
/// not-yet-constructed sibling member through them is safe).
class CasPlainObjects
{
public:
    CasPlainObjects(
        Backend & backend_, const Layout & layout_,
        std::function<uint64_t()> fence_generation_fn_,
        std::function<void(uint64_t)> check_fence_or_throw_fn_)
        : backend(backend_), layout(layout_)
        , fence_generation_fn(std::move(fence_generation_fn_))
        , check_fence_or_throw_fn(std::move(check_fence_or_throw_fn_))
    {
    }

    /// Stores the raw bytes under ONE LIFE's `_files/` prefix. Existing files are replaced
    /// conditionally using the object incarnation observed by `Backend::head`; a storage failure or an
    /// exhausted conflict-retry bound is propagated as an exception.
    ///
    /// `life` is supplied by the caller and never re-derived here, so this surface issues no catalog
    /// request of its own. A stale writer therefore targets its own old incarnation's key and cannot
    /// write into a newer life's prefix.
    void putNamespaceFile(const NamespaceLifeId & life, const String & name, const String & bytes);

    /// Reads a namespace file of ONE LIFE without interpreting its body. Returns `nullopt` when the
    /// object is absent and propagates backend read failures. A stale reader may see stale bytes or
    /// `NotFound`, never a newer incarnation's data: its key names the life it was given.
    std::optional<String> getNamespaceFile(const NamespaceLifeId & life, const String & name);

    /// Enumerates the file names directly below ONE LIFE's `_files/` prefix. Fetches all paginated
    /// backend results, strips the prefix, and returns names in sorted order independent of backend
    /// listing order.
    std::vector<String> listNamespaceFiles(const NamespaceLifeId & life);

    /// Removes the current OBJECT incarnation of one of a life's files, if any (the object token, not
    /// the namespace incarnation, which `life` fixes). A concurrent replacement is never removed
    /// accidentally: the exact-delete helper re-reads and retries with the new token.
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

    /// Removes the current path-mirrored mountpoint-object incarnation, if present, using exact-token
    /// deletion so a concurrent rewrite remains intact.
    void removeMountpointObject(const String & key);

private:
    /// Creates or conditionally replaces one raw object. The method re-heads after a conditional
    /// conflict and throws `ABORTED` after the bounded retry loop cannot establish a stable token.
    /// Fence-generation-gated (rev.7 [C2]): captures the fence generation at admission for the call's
    /// whole retry loop; every iteration re-checks it immediately before its durable PUT.
    void casPutObject(const String & full_key, const String & bytes);

    /// Reads one raw object by its complete backend key and returns `nullopt` when it is absent. A read,
    /// not a durable-effect operation -- NOT fence-gated (rev.7 [C2] scopes the gate to durable writes).
    std::optional<String> casGetObject(const String & full_key);

    /// Removes one raw object by exact token. Absence is a successful no-op; a token mismatch causes
    /// a fresh head and retry, while a bounded retry failure throws `ABORTED`. Fence-generation-gated
    /// the same way as `casPutObject`.
    void casRemoveObject(const String & full_key);

    Backend & backend;
    const Layout & layout;

    /// ---- fence-generation admission (injected by `Pool`; see the class doc comment) ----
    std::function<uint64_t()> fence_generation_fn;
    std::function<void(uint64_t)> check_fence_or_throw_fn;
};

}
