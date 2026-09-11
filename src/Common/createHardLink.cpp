#include <Common/createHardLink.h>
#include <base/pathToString.h>
#if defined(OS_WINDOWS)
#include <Poco/UnWindows.h>
#endif
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <cerrno>
#include <unistd.h>
#include <sys/stat.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_STAT;
    extern const int CANNOT_LINK;
}

#if defined(OS_WINDOWS)
namespace
{

/// Windows `stat` reports `st_ino` as 0, so the POSIX "same inode?" test below cannot be used to
/// tell an already-correct hard link from a collision. The identity of a file there is the pair
/// (volume serial, file index), which is what `GetFileInformationByHandle` reports.
bool isSameFile(const String & left, const String & right)
{
    const auto identity = [](const String & path, BY_HANDLE_FILE_INFORMATION & info) -> bool
    {
        HANDLE handle = CreateFileW(
            pathFromString(path).c_str(),
            0, /// Querying metadata needs no access rights.
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            nullptr,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS, /// So that a directory can be opened too.
            nullptr);
        if (handle == INVALID_HANDLE_VALUE)
            return false;
        const bool ok = GetFileInformationByHandle(handle, &info);
        CloseHandle(handle);
        return ok;
    };

    BY_HANDLE_FILE_INFORMATION left_info{};
    BY_HANDLE_FILE_INFORMATION right_info{};
    if (!identity(left, left_info) || !identity(right, right_info))
        return false;

    return left_info.dwVolumeSerialNumber == right_info.dwVolumeSerialNumber
        && left_info.nFileIndexHigh == right_info.nFileIndexHigh
        && left_info.nFileIndexLow == right_info.nFileIndexLow;
}

}
#endif

void createHardLink(const String & source_path, const String & destination_path)
{
#if defined(OS_WINDOWS)
    if (!CreateHardLinkW(pathFromString(destination_path).c_str(), pathFromString(source_path).c_str(), nullptr))
    {
        const auto error = GetLastError();
        if (error == ERROR_ALREADY_EXISTS || error == ERROR_FILE_EXISTS)
        {
            /// Same meaning as the EEXIST branch below: an existing destination is only acceptable
            /// if it is already this very file.
            if (!isSameFile(source_path, destination_path))
                throw Exception(
                    ErrorCodes::CANNOT_LINK,
                    "Destination file {} already exists and is a different file",
                    destination_path);
        }
        else
        {
            throw Exception(
                ErrorCodes::CANNOT_LINK,
                "Cannot link {} to {} (CreateHardLink), error code: {}",
                source_path,
                destination_path,
                error);
        }
    }
#else
    if (0 != link(source_path.c_str(), destination_path.c_str()))
    {
        if (errno == EEXIST)
        {
            auto link_errno = errno;

            struct stat source_descr{};
            struct stat destination_descr{};

            if (0 != lstat(source_path.c_str(), &source_descr))
                ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, source_path, "Cannot stat {}", source_path);

            if (0 != lstat(destination_path.c_str(), &destination_descr))
                ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, destination_path, "Cannot stat {}", destination_path);

            if (source_descr.st_ino != destination_descr.st_ino)
                ErrnoException::throwFromPathWithErrno(
                    ErrorCodes::CANNOT_LINK,
                    destination_path,
                    link_errno,
                    "Destination file {} already exists and has a different inode",
                    destination_path);
        }
        else
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_LINK, destination_path, "Cannot link {} to {}", source_path, destination_path);
    }
#endif
}

}
