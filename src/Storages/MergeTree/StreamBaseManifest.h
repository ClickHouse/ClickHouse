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

    /// Throws INCORRECT_FILE_NAME when @base is claimed by owners of both kinds. A repeat claim of
    /// the same kind is a no-op: either one owner's peer files (data/marks of a substream, a packed
    /// substream that spills), or a same-kind direction owned elsewhere.
    void registerStreamBase(const String & base, Owner owner);

private:
    std::unordered_map<String, Owner> owners;
};

/// Not synchronised: the producers sharing one of these all belong to the single writer sequence of
/// one insert, merge or mutation, and a nested directory such as `<name>.proj` gets its own instance.
using StreamBaseManifestPtr = std::shared_ptr<StreamBaseManifest>;

}
