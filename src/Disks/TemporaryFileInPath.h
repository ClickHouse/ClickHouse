#pragma once

#include <Disks/TemporaryFileOnDisk.h>
#include <Common/filesystemHelpers.h>

namespace DB
{

/// Wrapper around Poco::TemporaryFile to implement ITemporaryFile.
class TemporaryFileInPath : public ITemporaryFile
{
public:
    explicit TemporaryFileInPath(const String & folder_path)
        : tmp_file(createTemporaryFile(folder_path))
    {
        chassert(tmp_file);
    }

    String getPath() const override { return tmp_file->path(); }

private:
    std::unique_ptr<TemporaryFile> tmp_file;
};



}
