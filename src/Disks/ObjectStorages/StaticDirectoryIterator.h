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
    explicit StaticDirectoryIterator(std::vector<std::filesystem::path> && dir_file_paths_)
        : dir_file_paths(std::move(dir_file_paths_))
        , iter(dir_file_paths.begin())
    {}

    void next() override { ++iter; }

    bool isValid() const override { return iter != dir_file_paths.end(); }

    std::string path() const override { return iter->string(); }

    std::string name() const override
    {
        if (iter->filename().empty())
            return iter->parent_path().filename();
        return iter->filename();
    }

private:
    std::vector<std::filesystem::path> dir_file_paths;
    std::vector<std::filesystem::path>::iterator iter;
};

}
