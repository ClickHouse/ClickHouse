#pragma once

#include <Common/HashTable/HashSet.h>
#include <base/types.h>

#include <optional>
#include <string>

namespace DB
{

/// Reading an archive entry by name gives the first entry with that name,
/// so a later entry with the same name is identified by its position (0-based, in enumeration order).
class ArchiveEntryPositions
{
public:
    /// Called for every entry of an archive in enumeration order.
    /// Returns the position of the entry if an earlier entry has the same name.
    std::optional<size_t> onEntry(const std::string & name);

    /// Called when an enumeration restarts at the first entry.
    void reset();

private:
    size_t position = 0;
    HashSet<UInt128, UInt128TrivialHash> name_hashes;
};

/// The cache or task key of an archive entry: `#<position>#<key>` for an entry with a position,
/// `#<key>` for another key starting with `#`, `key` otherwise. Different arguments never give the same key.
std::string getArchiveEntryKey(const std::string & key, std::optional<size_t> position);

}
