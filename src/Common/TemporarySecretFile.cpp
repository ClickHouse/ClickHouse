#include <Common/TemporarySecretFile.h>

#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFile.h>

#include <filesystem>

#include <fcntl.h>


namespace DB
{

TemporarySecretFile::TemporarySecretFile(const std::string & contents)
{
    /// `TMPDIR` if it is set, `/tmp` otherwise. A file created there under a random name with
    /// `O_EXCL` and mode 0600 cannot be read or pre-created by other users.
    const std::filesystem::path directory = std::filesystem::temp_directory_path();
    path = directory / ("clickhouse-tls-" + getRandomASCIIString(16));

    try
    {
        WriteBufferFromFile out(
            path, contents.size(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, /* throttler_= */ {}, /* mode= */ 0600);
        out.write(contents.data(), contents.size());
        out.finalize();
    }
    catch (...)
    {
        std::error_code error_code;
        std::filesystem::remove(path, error_code);
        path.clear();
        throw;
    }
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
