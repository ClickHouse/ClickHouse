#pragma once

#include <Disks/TemporaryFileOnDisk.h>
#include <Poco/TemporaryFile.h>

namespace DB
{

/// Wrapper around Poco::TemporaryFile to implement ITemporaryFile.
class TemporaryFileInPath : public ITemporaryFile
{
public:
    explicit TemporaryFileInPath(const String & folder_path);
    String getPath() const override;

    ~TemporaryFileInPath() override;
private:
    std::unique_ptr<Poco::TemporaryFile> tmp_file;
};

}
