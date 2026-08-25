#pragma once

#include <Disks/DirectoryIterator.h>

#include <vector>
#include <string>

namespace DB
{

class StaticDirectoryIterator final : public IDirectoryIterator
{
public:
    explicit StaticDirectoryIterator(std::vector<std::string> && dir_file_paths_);

    void next() override;
    bool isValid() const override;
    std::string path() const override;
    std::string name() const override;

private:
    /// Logical metadata paths, kept as UTF-8 strings: `/` is their only separator.
    std::vector<std::string> dir_file_paths;
    std::vector<std::string>::iterator iter;
};

}
