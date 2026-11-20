#include <IO/MMappedFileCache.h>
#include <Common/SipHash.h>

#include <algorithm>

namespace DB
{

UInt128 MMappedFileCache::hash(const String & path_to_file, size_t offset, ssize_t length)
{
    SipHash hash;
    hash.update(path_to_file.size());
    hash.update(path_to_file.data(), path_to_file.size());
    hash.update(offset);
    hash.update(length);
    return hash.get128();
}

void MMappedFileCache::removeEntriesUnderPaths(const Strings & prefixes)
{
    std::function<bool(const Key &, const MappedPtr &)> predicate = [&](const Key &, const MappedPtr & mapped)
    {
        const String file_name = mapped->getFileName();
        return std::ranges::any_of(prefixes, [&](const String & prefix) { return file_name.starts_with(prefix); });
    };
    Base::remove(predicate);
}

}
