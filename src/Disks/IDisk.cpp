#include "IDisk.h"
#include <IO/copyData.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include "future"
#include "Disks/DiskAsyncSupport.h"

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

/// Executes task synchronously in case when destination disk doesn't support async operations.
class NoAsync : public DiskAsyncSupport
{
public:
    NoAsync() = default;
    ~NoAsync() override = default;
    std::future<void> runAsync(std::function<void()> task) override
    {
        std::promise<void> promise;
        try
        {
            task();
            promise.set_value();
        }
        catch (...)
        {
            promise.set_exception(std::current_exception());
        }
        return promise.get_future();
    }
};

void copyFile(IDisk & from_disk, const String & from_path, IDisk & to_disk, const String & to_path)
{
    LOG_DEBUG(&Poco::Logger::get("IDisk"), "Copying from {} {} to {} {}.", from_disk.getName(), from_path, to_disk.getName(), to_path);

    auto in = from_disk.readFile(from_path);
    auto out = to_disk.writeFile(to_path);
    copyData(*in, *out);
}

std::future<void> asyncCopy(IDisk & from_disk, const String & from_path, IDisk & to_disk, const String & to_path, DiskAsyncSupport & async)
{
    if (from_disk.isFile(from_path))
    {
        return async.runAsync(
            [&from_disk, &from_path, &to_disk, &to_path]()
            {
                DB::copyFile(from_disk, from_path, to_disk, to_path + fileName(from_path));
            }
        );
    }
    else
    {
        Poco::Path path(from_path);
        const String & dir_name = path.directory(path.depth() - 1);
        const String dest = to_path + dir_name + "/";
        to_disk.createDirectories(dest);

        std::vector<std::future<void>> futures;
        std::promise<void> promise;

        for (auto it = from_disk.iterateDirectory(from_path); it->isValid(); it->next())
            futures.push_back(asyncCopy(from_disk, it->path(), to_disk, dest, async));

        for (auto & future : futures)
        {
            future.wait();
            try
            {
                future.get();
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        }

        return promise.get_future();
    }
}

void IDisk::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    std::future<void> future;
    if (auto async = std::dynamic_pointer_cast<DiskAsyncSupport>(to_disk))
    {
        future = asyncCopy(*this, from_path, *to_disk, to_path, *async);
    }
    else
    {
        auto no_async = std::make_unique<NoAsync>();
        future = asyncCopy(*this, from_path, *to_disk, to_path, *no_async);
    }

    future.wait();
    future.get();
}

void IDisk::truncateFile(const String &, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate operation is not implemented for disk of type {}", getType());
}

}
