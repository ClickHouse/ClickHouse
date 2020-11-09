#include "IDisk.h"
#include <IO/copyData.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

bool IDisk::isDirectoryEmpty(const String & path)
{
    return !iterateDirectory(path)->isValid();
}

void copyFile(IDisk & from_disk, const String & from_path, IDisk & to_disk, const String & to_path)
{
    LOG_DEBUG(&Poco::Logger::get("IDisk"), "Copying from {} {} to {} {}.", from_disk.getName(), from_path, to_disk.getName(), to_path);

    auto in = from_disk.readFile(from_path);
    auto out = to_disk.writeFile(to_path);
    copyData(*in, *out);
}

void IDisk::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    if (isFile(from_path))
    {
        DB::copyFile(*this, from_path, *to_disk, to_path + fileName(from_path));
    }
    else
    {
        Poco::Path path(from_path);
        const String & dir_name = path.directory(path.depth() - 1);
        const String dest = to_path + dir_name + "/";
        to_disk->createDirectories(dest);

        for (auto it = iterateDirectory(from_path); it->isValid(); it->next())
        {
            copy(it->path(), to_disk, dest);
        }
    }
}

void IDisk::truncateFile(const String &, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate operation is not implemented for disk of type {}", getType());
}

}
