#include "ServerUUIDFile.h"

#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>

#include <Poco/File.h>
#include <Poco/UUID.h>
#include <common/logger_useful.h>
#include <common/errnoToString.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/LimitReadBuffer.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_CLOSE_FILE;
extern const int CANNOT_TRUNCATE_FILE;
extern const int CANNOT_SEEK_THROUGH_FILE;
}


ServerUUIDFile::FillFunction ServerUUIDFile::write_server_uuid = [](WriteBuffer & out) {
    union
    {
        char bytes[16];
        struct
        {
            UInt64 a;
            UInt64 b;
        } words;
        __uint128_t uuid;
    } random;

    random.words.a = thread_local_rng(); //-V656
    random.words.b = thread_local_rng(); //-V656

    struct ServerUUID : Poco::UUID
    {
        ServerUUID(const char * bytes, Poco::UUID::Version version) : Poco::UUID(bytes, version) { }
    };

    auto server_uuid = ServerUUID(random.bytes, Poco::UUID::UUID_RANDOM).toString();

    out << server_uuid;
};


ServerUUIDFile::ServerUUIDFile(std::string path_, FillFunction fill_)
    : path(std::move(path_)), fill(std::move(fill_))
{
    /// If file already exists. NOTE Minor race condition.
    if (Poco::File(path).exists())
    {
        std::string contents;
        {
            ReadBufferFromFile in(path, 1024);
            LimitReadBuffer limit_in(in, 1024, false);
            readStringUntilEOF(contents, limit_in);
        }

        if (!contents.empty())
            LOG_INFO(&Poco::Logger::get("ServerUUIDFile"), "Server UUID file {} already exists - unclean restart. Contents:\n{}", path, contents);
        else
            LOG_INFO(&Poco::Logger::get("ServerUUIDFile"), "Server UUID file {} already exists and is empty - probably unclean hardware restart.", path);
    }

    fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0666);

    if (-1 == fd)
        throwFromErrnoWithPath("Cannot open file " + path, path, ErrorCodes::CANNOT_OPEN_FILE);

    try
    {
        int flock_ret = flock(fd, LOCK_EX | LOCK_NB);
        if (-1 == flock_ret)
        {
            if (errno == EWOULDBLOCK)
                throw Exception("Cannot lock file " + path + ". Another server instance in same directory is already running.", ErrorCodes::CANNOT_OPEN_FILE);
            else
                throwFromErrnoWithPath("Cannot lock file " + path, path, ErrorCodes::CANNOT_OPEN_FILE);
        }

        if (0 != ftruncate(fd, 0))
            throwFromErrnoWithPath("Cannot ftruncate " + path, path, ErrorCodes::CANNOT_TRUNCATE_FILE);

        if (0 != lseek(fd, 0, SEEK_SET))
            throwFromErrnoWithPath("Cannot lseek " + path, path, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        /// Write information about current server instance to the file.
        WriteBufferFromFileDescriptor out(fd, 1024);
        fill(out);
    }
    catch (...)
    {
        close(fd);
        throw;
    }
}


ServerUUIDFile::~ServerUUIDFile()
{
    if (0 != close(fd))
        LOG_ERROR(&Poco::Logger::get("ServerUUIDFile"), "Cannot close file {}, {}", path, errnoToString(ErrorCodes::CANNOT_CLOSE_FILE));

    if (0 != unlink(path.c_str()))
        LOG_ERROR(&Poco::Logger::get("ServerUUIDFile"), "Cannot unlink file {}, {}", path, errnoToString(ErrorCodes::CANNOT_CLOSE_FILE));
}

}
