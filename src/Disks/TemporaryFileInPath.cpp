#include <Disks/TemporaryFileInPath.h>
#include <Common/filesystemHelpers.h>

namespace DB
{

TemporaryFileInPath::TemporaryFileInPath(const String & folder_path)
    : tmp_file(createTemporaryFile(folder_path))
{
    chassert(tmp_file);
}

String TemporaryFileInPath::getPath() const
{
    return tmp_file->path();
}

TemporaryFileInPath::~TemporaryFileInPath() = default;

}
