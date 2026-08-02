#pragma once

#include <base/types.h>
#include <memory>
#include <unordered_map>

namespace DB
{

/// Which owner claimed each stream base name (filename minus extension) in one part directory, so a
/// column and a skip-index substream cannot share one base and therefore one marks file. Created per
/// part-write operation and shared by that directory's producers; a null pointer means no coordination.
struct StreamBaseManifest
{
    enum class Kind : uint8_t
    {
        Column,
        SkipIndex,
    };

    struct Owner
    {
        Kind kind;
        String name;
    };

    /// Throws INCORRECT_FILE_NAME when @base is already claimed by a different owner. Re-claiming
    /// with the identical owner is a no-op (peer data/marks files of one substream, Nested shared
    /// offsets, a packed substream that spills after its index was registered).
    void registerStreamBase(const String & base, Owner owner);

private:
    std::unordered_map<String, Owner> owners;
};

using StreamBaseManifestPtr = std::shared_ptr<StreamBaseManifest>;

}
