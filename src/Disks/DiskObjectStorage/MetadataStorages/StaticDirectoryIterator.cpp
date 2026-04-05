#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>

namespace DB
{

StaticDirectoryIterator::StaticDirectoryIterator(std::vector<std::filesystem::path> && dir_file_paths_)
    : dir_file_paths(std::move(dir_file_paths_))
    , iter(dir_file_paths.begin())
{
}

void StaticDirectoryIterator::next()
{
    ++iter;
}

bool StaticDirectoryIterator::isValid() const
{
    return iter != dir_file_paths.end();
}

std::string StaticDirectoryIterator::path() const
{
    return iter->string();
}

std::string StaticDirectoryIterator::name() const
{
    if (iter->filename().empty())
        return iter->parent_path().filename();

    return iter->filename();
}

}
