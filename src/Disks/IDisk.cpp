#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <Core/Field.h>
#include <Core/ServerUUID.h>
#include <Disks/FakeDiskTransaction.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Storages/PartitionCommands.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

namespace CurrentMetrics
{
    extern const Metric IDiskCopierThreads;
    extern const Metric IDiskCopierThreadsActive;
    extern const Metric IDiskCopierThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

IDisk::IDisk(const String & name_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    : name(name_)
    , copying_thread_pool(std::make_unique<ThreadPool>(
          CurrentMetrics::IDiskCopierThreads,
          CurrentMetrics::IDiskCopierThreadsActive,
          CurrentMetrics::IDiskCopierThreadsScheduled,
          config.getUInt(config_prefix + ".thread_pool_size", 16)))
{
}

IDisk::IDisk(const String & name_)
    : name(name_)
    , copying_thread_pool(std::make_unique<ThreadPool>(
          CurrentMetrics::IDiskCopierThreads, CurrentMetrics::IDiskCopierThreadsActive, CurrentMetrics::IDiskCopierThreadsScheduled, 16))
{
}

IDisk::~IDisk() = default;

bool IDisk::isDirectoryEmpty(const String & path) const
{
    return !iterateDirectory(path)->isValid();
}

void IDisk::copyFile( /// NOLINT
    const String & from_file_path,
    IDisk & to_disk,
    const String & to_file_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const std::function<void()> & cancellation_hook
    )
{
    LOG_DEBUG(getLogger("IDisk"), "Copying from {} (path: {}) {} to {} (path: {}) {}.",
              getName(), getPath(), from_file_path, to_disk.getName(), to_disk.getPath(), to_file_path);

    auto in = readFile(from_file_path, read_settings);
    auto out = to_disk.writeFile(to_file_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, write_settings);
    copyData(*in, *out, cancellation_hook);
    out->finalize();
}

std::unique_ptr<ReadBufferFromFileBase> IDisk::readFileIfExists( /// NOLINT
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (existsFile(path))
        return readFile(path, settings, read_hint, file_size);
    else
        return {};
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

[[noreturn]] static void throwFileEncryptionNotImplemented(const DataSourceType & type)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "File encryption is not implemented for disk of type {}", type);
}

std::unique_ptr<ReadBufferFromFileBase> IDisk::readEncryptedFile(const String &, const ReadSettings &) const
{
    throwFileEncryptionNotImplemented(getDataSourceDescription().type);
}

std::unique_ptr<WriteBufferFromFileBase> IDisk::writeEncryptedFile(const String &, size_t, WriteMode, const WriteSettings &) const
{
    throwFileEncryptionNotImplemented(getDataSourceDescription().type);
}

size_t IDisk::getEncryptedFileSize(const String &) const
{
    throwFileEncryptionNotImplemented(getDataSourceDescription().type);
}

size_t IDisk::getEncryptedFileSize(size_t) const
{
    throwFileEncryptionNotImplemented(getDataSourceDescription().type);
}

UInt128 IDisk::getEncryptedFileIV(const String &) const
{
    throwFileEncryptionNotImplemented(getDataSourceDescription().type);
}

void asyncCopy(
    IDisk & from_disk,
    String from_path,
    IDisk & to_disk,
    String to_path,
    ThreadPoolCallbackRunnerLocal<void> & runner,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const std::function<void()> & cancellation_hook)
{
    if (from_disk.existsFile(from_path))
    {
        runner(
            [&from_disk, from_path, &to_disk, to_path, &read_settings, &write_settings, &cancellation_hook] {
                from_disk.copyFile(
                    from_path, to_disk, to_path, read_settings, write_settings, cancellation_hook);
            });
    }
    else /// Directory
    {
        fs::path dest(to_path);
        to_disk.createDirectories(dest);

        for (auto it = from_disk.iterateDirectory(from_path); it->isValid(); it->next())
            asyncCopy(from_disk, it->path(), to_disk, dest / it->name(), runner, read_settings, write_settings, cancellation_hook);
    }
}

void IDisk::copyThroughBuffers(
    const String & from_path,
    const std::shared_ptr<IDisk> & to_disk,
    const String & to_path,
    const ReadSettings & read_settings,
    WriteSettings write_settings,
    const std::function<void()> & cancellation_hook)
{
    ThreadPoolCallbackRunnerLocal<void> runner(*copying_thread_pool, "AsyncCopy");

    /// Disable parallel write. We already copy in parallel.
    /// Avoid high memory usage. See test_s3_zero_copy_ttl/test.py::test_move_and_s3_memory_usage
    write_settings.s3_allow_parallel_part_upload = false;
    write_settings.azure_allow_parallel_part_upload = false;

    asyncCopy(*this, from_path, *to_disk, to_path, runner, read_settings, write_settings, cancellation_hook);

    runner.waitForAllToFinishAndRethrowFirstError();
}


void IDisk::copyDirectoryContent(
    const String & from_dir,
    const std::shared_ptr<IDisk> & to_disk,
    const String & to_dir,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const std::function<void()> & cancellation_hook)
{
    copyThroughBuffers(from_dir, to_disk, to_dir, read_settings, write_settings, cancellation_hook);
}

[[noreturn]] inline void throwTruncateFileNotImplemented(const DataSourceType & type)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate operation is not implemented for disk of type {}", type);
}

void IDisk::truncateFile(const String &, size_t)
{
    throwTruncateFileNotImplemented(getDataSourceDescription().type);
}

bool IDisk::supportsPartitionCommand(const PartitionCommand & /*command*/) const
{
    return true;
}

SyncGuardPtr IDisk::getDirectorySyncGuard(const String & /* path */) const
{
    return nullptr;
}

void IDisk::startup(bool skip_access_check)
{
    if (!skip_access_check)
    {
        if (isReadOnly())
        {
            LOG_DEBUG(getLogger("IDisk"),
                "Skip access check for disk {} (read-only disk).",
                getName());
        }
        else
            checkAccess();
    }
    startupImpl();
}

void IDisk::checkAccess()
{
    DB::UUID server_uuid = DB::ServerUUID::get();
    if (server_uuid == DB::UUIDHelpers::Nil)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Server UUID is not initialized");
    const String path = fmt::format("clickhouse_access_check_{}", toString(server_uuid));

    checkAccessImpl(path);
}

/// NOTE: should we mark the disk readonly if the write/unlink fails instead of throws?
void IDisk::checkAccessImpl(const String & path)
try
{
    const std::string_view payload("test", 4);
    const auto read_settings = getReadSettings();

    /// write
    {
        auto file = writeFile(path, std::min<size_t>(DBMS_DEFAULT_BUFFER_SIZE, payload.size()), WriteMode::Rewrite);
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
        auto file = readFile(path, read_settings);
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
        auto file = readFile(path, read_settings);
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
    copying_thread_pool->setMaxThreads(config.getInt(config_prefix + ".thread_pool_size", 16));
}

[[noreturn]] static void throwRenameExchangeNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method `renameExchange()` not implemented for disk: {}", desc);
}

[[noreturn]] static void throwRenameExchangeIfSupportedNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method `renameExchangeIfSupported()` not implemented for disk: {}", desc);
}

[[noreturn]] static void throwRemoveDirectoryIfExistsNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method `removeDirectoryIfExists()` is not implemented for disk: {}", desc);
}

[[noreturn]] static void throwGetCacheLayersNamesNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method `getCacheLayersNames()` is not implemented for disk: {}", desc);
}

[[noreturn]] static void throwGetStorageObjectsNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method `getStorageObjects()` not implemented for disk: {}", desc);
}

[[noreturn]] static void throwIsSymlinkNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method isSymlink is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwIsSymlinkNoThrowNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method isSymlinkNothrow is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwCreateDirectorySymlinkNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method createDirectorySymlink is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwReadSymlinkNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method readSymlink is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwEquivalentNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method equivalent is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwEquivalentNoThrowNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method equivalent is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwGetMetadataStorageNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method getMetadataStorage is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwGetObjectStorageNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method getObjectStorage is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwGetS3StorageClientNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method getS3StorageClient is not implemented for disk type: {}", desc);
}

[[noreturn]] static void throwStatNotSupported() {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk does not support stat");
}

[[noreturn]] static void throwChmodNotSupported() {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk does not support chmod");
}

[[noreturn]] static void throwThereIsNoCache() {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no cache");
}

void IDisk::renameExchange(const std::string &, const std::string &)
{
    throwRenameExchangeNotImplemented(getDataSourceDescription().toString());
}

bool IDisk::renameExchangeIfSupported(const std::string &, const std::string &)
{
    throwRenameExchangeIfSupportedNotImplemented(getDataSourceDescription().toString());
}

void IDisk::removeDirectoryIfExists(const String &)
{
    throwRemoveDirectoryIfExistsNotImplemented(getDataSourceDescription().toString());
}

NameSet IDisk::getCacheLayersNames() const
{
    throwGetCacheLayersNamesNotImplemented(getDataSourceDescription().toString());
}

StoredObjects IDisk::getStorageObjects(const String &) const
{
    throwGetStorageObjectsNotImplemented(getDataSourceDescription().toString());
}

bool IDisk::isSymlink(const String &) const
{
    throwIsSymlinkNotImplemented(getDataSourceDescription().toString());
}

bool IDisk::isSymlinkNoThrow(const String &) const
{
    throwIsSymlinkNoThrowNotImplemented(getDataSourceDescription().toString());
}

void IDisk::createDirectorySymlink(const String &, const String &)
{
    throwCreateDirectorySymlinkNotImplemented(getDataSourceDescription().toString());
}

String IDisk::readSymlink(const fs::path &) const
{
    throwReadSymlinkNotImplemented(getDataSourceDescription().toString());
}

bool IDisk::equivalent(const String &, const String &) const
{
    throwEquivalentNotImplemented(getDataSourceDescription().toString());
}

bool IDisk::equivalentNoThrow(const String &, const String &) const
{
    throwEquivalentNoThrowNotImplemented(getDataSourceDescription().toString());
}

MetadataStoragePtr IDisk::getMetadataStorage()
{
    throwGetMetadataStorageNotImplemented(getDataSourceDescription().toString());
}

ObjectStoragePtr IDisk::getObjectStorage()
{
    throwGetObjectStorageNotImplemented(getDataSourceDescription().toString());
}

struct stat IDisk::stat(const String & /*path*/) const {
    throwStatNotSupported();
}

void IDisk::chmod(const String & /*path*/, mode_t /*mode*/) {
    throwChmodNotSupported();
}

[[noreturn]] static void throwCreateDiskObjectStorageNotImplemented(const std::string & desc) {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method createDiskObjectStorage is not implemented for disk type: {}", desc);
}

DiskObjectStoragePtr IDisk::createDiskObjectStorage()
{
    throwCreateDiskObjectStorageNotImplemented(getDataSourceDescription().toString());
}

const String & IDisk::getCacheName() const { throwThereIsNoCache(); }

#if USE_AWS_S3
    std::shared_ptr<const S3::Client> IDisk::getS3StorageClient() const
    {
        throwGetS3StorageClientNotImplemented(getDataSourceDescription().toString());
    }
#endif

}
