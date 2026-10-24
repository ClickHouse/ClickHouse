#pragma once

#include <Disks/DirectoryIterator.h>

#include <vector>
#include <filesystem>
#include <string>

namespace DB
{

class StaticDirectoryIterator final : public IDirectoryIterator
{
public:
    explicit StaticDirectoryIterator(std::vector<std::filesystem::path> && dir_file_paths_);

    void next() override;
    [[nodiscard]] bool isValid() const override;
    [[nodiscard]] std::string path() const override;
    [[nodiscard]] std::string name() const override;

private:
    std::vector<std::filesystem::path> dir_file_paths;
    std::vector<std::filesystem::path>::iterator iter;
};

}
