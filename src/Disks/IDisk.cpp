#include "IDisk.h"
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Core/ServerUUID.h>
#include <Disks/FakeDiskTransaction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

bool IDisk::isDirectoryEmpty(const String & path) const
{
    return !iterateDirectory(path)->isValid();
}

void IDisk::copyFile(const String & from_file_path, IDisk & to_disk, const String & to_file_path, const WriteSettings & settings) /// NOLINT
{
    LOG_DEBUG(&Poco::Logger::get("IDisk"), "Copying from {} (path: {}) {} to {} (path: {}) {}.",
              getName(), getPath(), from_file_path, to_disk.getName(), to_disk.getPath(), to_file_path);

    auto in = readFile(from_file_path);
    auto out = to_disk.writeFile(to_file_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, settings);
    copyData(*in, *out);
    out->finalize();
}

DiskTransactionPtr IDisk::createTransaction()
{
    return std::make_shared<FakeDiskTransaction>(*this);
}

void IDisk::removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    for (const auto & file : files)
    {
        bool keep_file = keep_all_batch_data || file_names_remove_metadata_only.contains(fs::path(file.path).filename());
        if (file.if_exists)
            removeSharedFileIfExists(file.path, keep_file);
        else
            removeSharedFile(file.path, keep_file);
    }
}

std::unique_ptr<ReadBufferFromFileBase> IDisk::readEncryptedFile(const String &, const ReadSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "File encryption is not implemented for disk of type {}", getDataSourceDescription().type);
}

std::unique_ptr<WriteBufferFromFileBase> IDisk::writeEncryptedFile(const String &, size_t, WriteMode, const WriteSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "File encryption is not implemented for disk of type {}", getDataSourceDescription().type);
}

size_t IDisk::getEncryptedFileSize(const String &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "File encryption is not implemented for disk of type {}", getDataSourceDescription().type);
}

size_t IDisk::getEncryptedFileSize(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "File encryption is not implemented for disk of type {}", getDataSourceDescription().type);
}

UInt128 IDisk::getEncryptedFileIV(const String &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "File encryption is not implemented for disk of type {}", getDataSourceDescription().type);
}


using ResultsCollector = std::vector<std::future<void>>;

void asyncCopy(IDisk & from_disk, String from_path, IDisk & to_disk, String to_path, ThreadPool & pool, ResultsCollector & results, bool copy_root_dir, const WriteSettings & settings)
{
    if (from_disk.isFile(from_path))
    {
        auto promise = std::make_shared<std::promise<void>>();
        auto future = promise->get_future();

        pool.scheduleOrThrowOnError(
            [&from_disk, from_path, &to_disk, to_path, &settings, promise, thread_group = CurrentThread::getGroup()]()
            {
                try
                {
                    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););

                    if (thread_group)
                        CurrentThread::attachToGroup(thread_group);

                    from_disk.copyFile(from_path, to_disk, fs::path(to_path) / fileName(from_path), settings);
                    promise->set_value();
                }
                catch (...)
                {
                    promise->set_exception(std::current_exception());
                }
            });

        results.push_back(std::move(future));
    }
    else
    {
        fs::path dest(to_path);
        if (copy_root_dir)
        {
            fs::path dir_name = fs::path(from_path).parent_path().filename();
            dest /= dir_name;
            to_disk.createDirectories(dest);
        }

        for (auto it = from_disk.iterateDirectory(from_path); it->isValid(); it->next())
            asyncCopy(from_disk, it->path(), to_disk, dest, pool, results, true, settings);
    }
}

void IDisk::copyThroughBuffers(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path, bool copy_root_dir)
{
    ResultsCollector results;

    WriteSettings settings;
    /// Disable parallel write. We already copy in parallel.
    /// Avoid high memory usage. See test_s3_zero_copy_ttl/test.py::test_move_and_s3_memory_usage
    settings.s3_allow_parallel_part_upload = false;

    asyncCopy(*this, from_path, *to_disk, to_path, copying_thread_pool, results, copy_root_dir, settings);

    for (auto & result : results)
        result.wait();
    for (auto & result : results)
        result.get();   /// May rethrow an exception
}


void IDisk::copyDirectoryContent(const String & from_dir, const std::shared_ptr<IDisk> & to_disk, const String & to_dir)
{
    if (!to_disk->exists(to_dir))
        to_disk->createDirectories(to_dir);

    copyThroughBuffers(from_dir, to_disk, to_dir, /* copy_root_dir */ false);
}

void IDisk::truncateFile(const String &, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate operation is not implemented for disk of type {}", getDataSourceDescription().type);
}

SyncGuardPtr IDisk::getDirectorySyncGuard(const String & /* path */) const
{
    return nullptr;
}

void IDisk::startup(ContextPtr context, bool skip_access_check)
{
    if (!skip_access_check)
    {
        if (isReadOnly())
        {
            LOG_DEBUG(&Poco::Logger::get("IDisk"),
                "Skip access check for disk {} (read-only disk).",
                getName());
        }
        else
            checkAccess();
    }
    startupImpl(context);
}

void IDisk::checkAccess()
{
    DB::UUID server_uuid = DB::ServerUUID::get();
    if (server_uuid == DB::UUIDHelpers::Nil)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Server UUID is not initialized");
    const String path = fmt::format("clickhouse_access_check_{}", DB::toString(server_uuid));

    checkAccessImpl(path);
}

/// NOTE: should we mark the disk readonly if the write/unlink fails instead of throws?
void IDisk::checkAccessImpl(const String & path)
try
{
    const std::string_view payload("test", 4);

    /// write
    {
        auto file = writeFile(path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        try
        {
            file->write(payload.data(), payload.size());
            file->finalize();
        }
        catch (...)
        {
            /// Log current exception, because finalize() can throw a different exception.
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }

    /// read
    {
        auto file = readFile(path);
        String buf(payload.size(), '0');
        file->readStrict(buf.data(), buf.size());
        if (buf != payload)
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Content of {}::{} does not matches after read ({} vs {})", name, path, buf, payload);
        }
    }

    /// read with offset
    {
        auto file = readFile(path);
        auto offset = 2;
        String buf(payload.size() - offset, '0');
        file->seek(offset, 0);
        file->readStrict(buf.data(), buf.size());
        if (buf != payload.substr(offset))
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Content of {}::{} does not matches after read with offset ({} vs {})", name, path, buf, payload.substr(offset));
        }
    }

    /// remove
    removeFile(path);
}
catch (Exception & e)
{
    e.addMessage(fmt::format("While checking access for disk {}", name));
    throw;
}

void IDisk::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr /*context*/, const String & config_prefix, const DisksMap & /*map*/)
{
    copying_thread_pool.setMaxThreads(config.getInt(config_prefix + ".thread_pool_size", 16));
}

}
