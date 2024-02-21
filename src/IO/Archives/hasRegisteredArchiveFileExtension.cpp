#include <IO/Archives/hasRegisteredArchiveFileExtension.h>


namespace DB
{

bool hasRegisteredArchiveFileExtension(const String & path)
{
    return path.ends_with(".zip") || path.ends_with(".zipx") || path.ends_with(".tar") || path.ends_with(".tar.gz")
        || path.ends_with(".tar.bz2") || path.ends_with(".tar.lzma") || path.ends_with(".tar.zst") || path.ends_with(".tzst")
        || path.ends_with(".tgz") || path.ends_with(".tar.xz");
}

}
