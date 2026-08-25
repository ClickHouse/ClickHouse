#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>

#include <string_view>

namespace DB
{

StaticDirectoryIterator::StaticDirectoryIterator(std::vector<std::string> && dir_file_paths_)
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
    return *iter;
}

std::string StaticDirectoryIterator::name() const
{
    /// These are logical metadata paths, not filesystem paths: `/` is their only separator, so the
    /// last component is taken in string space rather than through `std::filesystem::path`, which
    /// would re-encode a non-ASCII name through the active code page on Windows.
    std::string_view path_view = *iter;

    /// A trailing separator marks a directory; its name is the component before that separator.
    while (path_view.ends_with('/'))
        path_view.remove_suffix(1);

    const auto separator_pos = path_view.rfind('/');
    if (separator_pos != std::string_view::npos)
        path_view.remove_prefix(separator_pos + 1);

    return std::string(path_view);
}

}
