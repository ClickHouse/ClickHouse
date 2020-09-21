#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>

#include <filesystem>
#include <memory>
#include <string>
#include <sys/statvfs.h>
#include <Poco/TemporaryFile.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_STATVFS;
}

using TemporaryFile = Poco::TemporaryFile;

bool enoughSpaceInDirectory(const std::string & path, size_t data_size);
std::unique_ptr<TemporaryFile> createTemporaryFile(const std::string & path);

/// Returns mount point of filesystem where absolute_path (must exist) is located
std::filesystem::path getMountPoint(std::filesystem::path absolute_path);

/// Returns name of filesystem mounted to mount_point
#if !defined(__linux__)
[[noreturn]]
#endif
String getFilesystemName([[maybe_unused]] const String & mount_point);

inline struct statvfs getStatVFS(const String & path)
{
    struct statvfs fs;
    if (statvfs(path.c_str(), &fs) != 0)
        throwFromErrnoWithPath("Could not calculate available disk space (statvfs)", path, ErrorCodes::CANNOT_STATVFS);
    return fs;
}

}
