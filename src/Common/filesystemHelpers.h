#pragma once

#include <base/types.h>
#include <Common/Exception.h>

#include <filesystem>
#include <memory>
#include <string>
#include <sys/statvfs.h>
#include <Poco/TemporaryFile.h>


namespace DB
{

using TemporaryFile = Poco::TemporaryFile;

bool enoughSpaceInDirectory(const std::string & path, size_t data_size);
std::unique_ptr<TemporaryFile> createTemporaryFile(const std::string & path);

// Determine what block device is responsible for specified path
#if !defined(OS_LINUX)
[[noreturn]]
#endif
String getBlockDeviceId([[maybe_unused]] const String & path);

enum class BlockDeviceType
{
    UNKNOWN = 0, // we were unable to determine device type
    NONROT = 1, // not a rotational device (SSD, NVME, etc)
    ROT = 2 // rotational device (HDD)
};

// Try to determine block device type
#if !defined(OS_LINUX)
[[noreturn]]
#endif
BlockDeviceType getBlockDeviceType([[maybe_unused]] const String & device_id);

// Get size of read-ahead in bytes for specified block device
#if !defined(OS_LINUX)
[[noreturn]]
#endif
UInt64 getBlockDeviceReadAheadBytes([[maybe_unused]] const String & device_id);

/// Returns mount point of filesystem where absolute_path (must exist) is located
std::filesystem::path getMountPoint(std::filesystem::path absolute_path);

/// Returns name of filesystem mounted to mount_point
#if !defined(OS_LINUX)
[[noreturn]]
#endif
String getFilesystemName([[maybe_unused]] const String & mount_point);

struct statvfs getStatVFS(const String & path);

/// Returns true if path starts with prefix path
bool pathStartsWith(const std::filesystem::path & path, const std::filesystem::path & prefix_path);

/// Returns true if path starts with prefix path
bool pathStartsWith(const String & path, const String & prefix_path);

/// Same as pathStartsWith, but without canonization, i.e. allowed to check symlinks.
/// (Path is made absolute and normalized.)
bool fileOrSymlinkPathStartsWith(const String & path, const String & prefix_path);

}

namespace FS
{
bool createFile(const std::string & path);

bool exists(const std::string & path);
bool canRead(const std::string & path);
bool canWrite(const std::string & path);
bool canExecute(const std::string & path);

/// st_mtime
time_t getModificationTime(const std::string & path);
Poco::Timestamp getModificationTimestamp(const std::string & path);
void setModificationTime(const std::string & path, time_t time);
/// st_ctime
time_t getChangeTime(const std::string & path);
}
