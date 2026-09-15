#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPlainObjects.h>
#include <Common/Exception.h>
#include <algorithm>
#include <fmt/format.h>

namespace DB::Cas
{

void CasPlainObjects::casPutObject(const String & full_key, const String & bytes)
{
    /// SINGLE-APPENDER INVARIANT: `bytes` is frozen by the caller before this call (see the
    /// append-base note at `ContentAddressedTransaction::writeFile`'s Append branch); `decide` below
    /// always returns the same frozen bytes regardless of what it observes at the key. This is correct
    /// only while nothing concurrently appends to the same key -- a losing retry would overwrite the
    /// winner's bytes with a stale, pre-conflict payload (a lost update). Implement a real
    /// `casAppendObject` (deciding from the current body, not just presence) before adding any
    /// concurrent appender.
    CasOperation op = requests.admit();
    WriteResult result = op.readModifyWriteOnPresence(
        full_key,
        [&](const std::optional<Meta> &) -> std::optional<String> { return bytes; },
        Retry::standard());
    orThrow(std::move(result), fmt::format("object CAS write on '{}'", full_key));
}

std::optional<String> CasPlainObjects::casGetObject(const String & full_key)
{
    CasOperation op = requests.admit();
    std::optional<Object> result = op.read(full_key, Retry::standard());
    if (!result)
        return std::nullopt;
    return result->bytes;
}

void CasPlainObjects::casRemoveObject(const String & full_key)
{
    /// `removeCurrent` re-heads and retries against a concurrent replacement itself, and reports
    /// absence (its own no-op) the same way whether the key was never there or just vanished, so the
    /// result carries nothing this caller acts on differently.
    CasOperation op = requests.admit();
    op.removeCurrent(full_key, Retry::standard());
}

void CasPlainObjects::putNamespaceFile(const NamespaceLifeId & life, const String & name, const String & bytes)
{
    casPutObject(layout.namespaceFileKey(life, name), bytes);
}

std::optional<String> CasPlainObjects::getNamespaceFile(const NamespaceLifeId & life, const String & name)
{
    return casGetObject(layout.namespaceFileKey(life, name));
}

std::vector<String> CasPlainObjects::listNamespaceFiles(const NamespaceLifeId & life)
{
    const String prefix = layout.namespaceFilesPrefix(life);
    std::vector<String> names;
    CasOperation op = requests.admit();
    op.forEachListedKey(prefix, [&](const ListedKey & entry)
    {
        /// Strip the storage prefix so callers receive the bare flat file name.
        if (entry.key.starts_with(prefix))
            names.push_back(entry.key.substr(prefix.size()));
        return true;
    }, Retry::standard());
    /// Backends are not required to return pages in the same order, so make the public result
    /// deterministic instead of relying on `InMemoryBackend` ordering.
    std::sort(names.begin(), names.end());
    return names;
}

void CasPlainObjects::removeNamespaceFile(const NamespaceLifeId & life, const String & name)
{
    casRemoveObject(layout.namespaceFileKey(life, name));
}

void CasPlainObjects::putMountpointObject(const String & key, const String & bytes)
{
    casPutObject(layout.mountpointObjectKey(key), bytes);
}

std::optional<String> CasPlainObjects::getMountpointObject(const String & key)
{
    return casGetObject(layout.mountpointObjectKey(key));
}

bool CasPlainObjects::mountpointObjectExists(const String & key)
{
    /// Use metadata rather than a body GET because a path probe may resolve to a directory, such as
    /// the `store` pool subdirectory traversed by `system.remote_data_paths`. The local backend
    /// treats a directory as not an object, so this returns false instead of attempting a body read
    /// that would raise a filesystem exception for a directory.
    CasOperation op = requests.admit();
    return op.head(layout.mountpointObjectKey(key), Retry::standard()).has_value();
}

void CasPlainObjects::removeMountpointObject(const String & key)
{
    casRemoveObject(layout.mountpointObjectKey(key));
}

}
