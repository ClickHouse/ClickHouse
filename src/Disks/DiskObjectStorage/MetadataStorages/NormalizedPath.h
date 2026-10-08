#pragma once

#include <filesystem>

namespace DB
{

/// Special class to disable implicit conversation from std::string
class NormalizedPath : public std::filesystem::path
{
public:
    NormalizedPath parent_path() const;
};

NormalizedPath normalizePath(std::string path);

}
