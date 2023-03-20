#include "IDisk.h"
#include "Disks/Executor.h"
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <Common/setThreadName.h>

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
    LOG_DEBUG(&Poco::Logger::get("IDisk"), "Copying from {} (path: {}) {} to {} (path: {}) {}.",
              from_disk.getName(), from_disk.getPath(), from_path, to_disk.getName(), to_disk.getPath(), to_path);

    auto in = from_disk.readFile(from_path);
    auto out = to_disk.writeFile(to_path);
    copyData(*in, *out);
    out->finalize();
}


using ResultsCollector = std::vector<std::future<void>>;

void asyncCopy(IDisk & from_disk, String from_path, IDisk & to_disk, String to_path, Executor & exec, ResultsCollector & results)
{
    if (from_disk.isFile(from_path))
    {
        auto result = exec.execute(
            [&from_disk, from_path, &to_disk, to_path]()
            {
                setThreadName("DiskCopier");
                DB::copyFile(from_disk, from_path, to_disk, fs::path(to_path) / fileName(from_path));
            });

        results.push_back(std::move(result));
    }
    else
    {
        fs::path dir_name = fs::path(from_path).parent_path().filename();
        fs::path dest(fs::path(to_path) / dir_name);
        to_disk.createDirectories(dest);

        for (auto it = from_disk.iterateDirectory(from_path); it->isValid(); it->next())
            asyncCopy(from_disk, it->path(), to_disk, dest, exec, results);
    }
}

void IDisk::copyThroughBuffers(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    auto & exec = to_disk->getExecutor();
    ResultsCollector results;

    asyncCopy(*this, from_path, *to_disk, to_path, exec, results);

    for (auto & result : results)
        result.wait();
    for (auto & result : results)
        result.get();
}

void IDisk::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    copyThroughBuffers(from_path, to_disk, to_path);
}

void IDisk::truncateFile(const String &, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate operation is not implemented for disk of type {}", getType());
}

SyncGuardPtr IDisk::getDirectorySyncGuard(const String & /* path */) const
{
    return nullptr;
}

}
