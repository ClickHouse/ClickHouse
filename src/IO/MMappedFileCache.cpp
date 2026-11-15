#include <IO/MMappedFileCache.h>
#include <Common/SipHash.h>

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

}
