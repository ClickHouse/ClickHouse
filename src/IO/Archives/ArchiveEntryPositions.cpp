#include <IO/Archives/ArchiveEntryPositions.h>

#include <Common/SipHash.h>

#include <fmt/format.h>

namespace DB
{

std::optional<size_t> ArchiveEntryPositions::onEntry(const std::string & name)
{
    const size_t current_position = position++;

    HashSet<UInt128, UInt128TrivialHash>::LookupResult it = nullptr;
    bool inserted = false;
    name_hashes.emplace(sipHash128(name.data(), name.size()), it, inserted);

    if (inserted)
        return std::nullopt;
    return current_position;
}

void ArchiveEntryPositions::reset()
{
    position = 0;
    name_hashes = {};
}

std::string getArchiveEntryKey(const std::string & key, std::optional<size_t> position)
{
    if (position)
        return fmt::format("#{}#{}", *position, key);
    if (key.starts_with('#'))
        return "#" + key;
    return key;
}

}
