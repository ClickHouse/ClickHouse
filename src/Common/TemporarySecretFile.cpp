#include <Common/TemporarySecretFile.h>

#include <Common/ErrnoException.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

#include <filesystem>

#include <fcntl.h>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
}

TemporarySecretFile::TemporarySecretFile(const std::string & contents)
{
    /// `TMPDIR` if it is set, `/tmp` otherwise. A file created there with `O_EXCL` under a random
    /// name and mode 0600 is not readable by other users, and cannot be pre-created by them.
    const std::filesystem::path directory = std::filesystem::temp_directory_path();
    path = directory / ("clickhouse-tls-" + getRandomASCIIString(16));

    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd == -1)
    {
        const std::string failed_path = std::exchange(path, {});
        ErrnoException::throwFromPath(
            ErrorCodes::CANNOT_OPEN_FILE, failed_path, "Cannot create a temporary file {} for TLS credentials", failed_path);
    }

    size_t written = 0;
    while (written < contents.size())
    {
        ssize_t res = ::write(fd, contents.data() + written, contents.size() - written);
        if (res == -1)
        {
            if (errno == EINTR)
                continue;

            const int saved_errno = errno;
            ::close(fd);
            std::filesystem::remove(path);
            const std::string failed_path = std::exchange(path, {});
            ErrnoException::throwFromPathWithErrno(
                ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
                failed_path,
                saved_errno,
                "Cannot write TLS credentials to the temporary file {}",
                failed_path);
        }
        written += res;
    }

    ::close(fd);
}

TemporarySecretFile::~TemporarySecretFile()
{
    if (path.empty())
        return;

    std::error_code error_code;
    std::filesystem::remove(path, error_code);
    if (error_code)
        LOG_WARNING(getLogger("TemporarySecretFile"), "Cannot remove the temporary file {}: {}", path, error_code.message());
}

}
