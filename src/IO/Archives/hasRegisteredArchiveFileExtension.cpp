#include <IO/Archives/hasRegisteredArchiveFileExtension.h>


namespace DB
{

bool hasRegisteredArchiveFileExtension(const String & path)
{
    return path.ends_with(".zip") || path.ends_with(".zipx");
}

}
