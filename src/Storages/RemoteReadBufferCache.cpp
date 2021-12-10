#include <fstream>
#include <memory>
#include <unistd.h>
#include <functional>
#include <Core/BackgroundSchedulePool.h>
#include <Poco/Logger.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <base/logger_useful.h>
#include <base/sleep.h>
#include <base/errnoToString.h>
#include <Common/SipHash.h>
#include <Common/hex.h>
#include <Common/Exception.h>
#include <Storages/RemoteReadBufferCache.h>
#include <IO/WriteHelpers.h>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

bool RemoteCacheController::loadInnerInformation(const fs::path & file_path)
{
    if (!fs::exists(file_path))
        return false;
    std::ifstream info_file(file_path);
    Poco::JSON::Parser info_parser;
    auto info_json = info_parser.parse(info_file).extract<Poco::JSON::Object::Ptr>();
    file_status = static_cast<LocalFileStatus>(info_json->get("file_status").convert<Int32>());
    metadata_class = info_json->get("metadata_class").convert<String>();
    info_file.close();
    return true;
}

std::shared_ptr<RemoteCacheController> RemoteCacheController::recover(const std::filesystem::path & local_path_)
{
    auto * log = &Poco::Logger::get("RemoteCacheController");

    if (!std::filesystem::exists(local_path_ / "data.bin"))
    {
        LOG_TRACE(log, "Invalid cached directory:{}", local_path_.string());
        return nullptr;
    }

    auto cache_controller = std::make_shared<RemoteCacheController>(nullptr, local_path_, 0);
    if (!cache_controller->loadInnerInformation(local_path_ / "info.txt")
            || cache_controller->file_status != DOWNLOADED)
    {
        LOG_INFO(log, "Recover cached file failed. local path:{}", local_path_.string());
        return nullptr;
    }

    cache_controller->file_metadata_ptr = RemoteFileMetadataFactory::instance().get(cache_controller->metadata_class);
    if (!cache_controller->file_metadata_ptr)
    {
        // do not load this invalid cached file and clear it. the clear action is in
        // RemoteReadBufferCache::recoverCachedFilesMetadata(), because deleting directories during iteration will
        // cause unexpected behaviors
        LOG_ERROR(log, "Cannot create the metadata class : {}. The cached file is invalid and will be remove. path:{}",
                cache_controller->metadata_class,
                local_path_.string());
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid metadata class:{}", cache_controller->metadata_class);
    }
    std::ifstream metadata_file(local_path_ / "metadata.txt");
    if (!cache_controller->file_metadata_ptr->fromString(std::string((std::istreambuf_iterator<char>(metadata_file)),
                    std::istreambuf_iterator<char>())))
    {
        LOG_ERROR(log, "Cannot load the metadata. The cached file is invalid and will be remove. path:{}",
                local_path_.string());
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid metadata file({}) for meta class {}", local_path_.string(), cache_controller->metadata_class);
    }

    cache_controller->current_offset = fs::file_size(local_path_ / "data.bin");

    RemoteReadBufferCache::instance().updateTotalSize(cache_controller->file_metadata_ptr->getFileSize());
    return cache_controller;
}

RemoteCacheController::RemoteCacheController(
    IRemoteFileMetadataPtr file_metadata_,
    const std::filesystem::path & local_path_,
    size_t cache_bytes_before_flush_)
    : file_metadata_ptr(file_metadata_)
    , local_path(local_path_)
    , valid(true)
    , local_cache_bytes_read_before_flush(cache_bytes_before_flush_)
    , current_offset(0)
{
    // on recover, file_metadata_ptr is null, but it will be allocated after loading from metadata.txt
    // when we allocate a whole new file cache ， file_metadata_ptr must not be null.
    if (file_metadata_ptr)
    {
        auto metadata_file_writer = std::make_unique<WriteBufferFromFile>((local_path_ / "metadata.txt").string());
        auto str_buf = file_metadata_ptr->toString();
        metadata_file_writer->write(str_buf.c_str(), str_buf.size());
        metadata_file_writer->close();
    }
}

RemoteReadBufferCacheError RemoteCacheController::waitMoreData(size_t start_offset_, size_t end_offset_)
{
    std::unique_lock lock{mutex};
    if (file_status == DOWNLOADED)
    {
        // finish reading
        if (start_offset_ >= current_offset)
        {
            lock.unlock();
            return RemoteReadBufferCacheError::END_OF_FILE;
        }
    }
    else // block until more data is ready
    {
        if (current_offset >= end_offset_)
        {
            lock.unlock();
            return RemoteReadBufferCacheError::OK;
        }
        else
            more_data_signal.wait(lock, [this, end_offset_] { return file_status == DOWNLOADED || current_offset >= end_offset_; });
    }
    lock.unlock();
    return RemoteReadBufferCacheError::OK;
}

bool RemoteCacheController::checkFileChanged(IRemoteFileMetadataPtr file_metadata_)
{
    return !(file_metadata_ptr->getVersion() == file_metadata_->getVersion());
}

void RemoteCacheController::startBackgroundDownload(std::unique_ptr<ReadBuffer> in_readbuffer_, BackgroundSchedulePool & thread_pool)
{
    data_file_writer = std::make_unique<WriteBufferFromFile>((fs::path(local_path) / "data.bin").string());
    flush(true);
    ReadBufferPtr in_readbuffer(in_readbuffer_.release());
    download_task_holder = thread_pool.createTask("download remote file",
            [this, in_readbuffer]{ backgroundDownload(in_readbuffer); });
    download_task_holder->activateAndSchedule();
}

void RemoteCacheController::backgroundDownload(ReadBufferPtr remote_read_buffer)
{
    file_status = DOWNLOADING;
    size_t before_unflush_bytes = 0;
    size_t total_bytes = 0;
    while (!remote_read_buffer->eof())
    {
        size_t bytes = remote_read_buffer->available();

        data_file_writer->write(remote_read_buffer->position(), bytes);
        remote_read_buffer->position() += bytes;
        total_bytes += bytes;
        before_unflush_bytes += bytes;
        if (before_unflush_bytes >= local_cache_bytes_read_before_flush)
        {
            std::unique_lock lock(mutex);
            current_offset += total_bytes;
            total_bytes = 0;
            flush();
            lock.unlock();
            more_data_signal.notify_all();
            before_unflush_bytes = 0;
        }
    }
    std::unique_lock lock(mutex);
    current_offset += total_bytes;
    file_status = DOWNLOADED;
    flush(true);
    data_file_writer.reset();
    lock.unlock();
    more_data_signal.notify_all();
    RemoteReadBufferCache::instance().updateTotalSize(file_metadata_ptr->getFileSize());
    LOG_TRACE(log, "Finish download into local path: {}, file metadata:{} ", local_path.string(), file_metadata_ptr->toString());
}

void RemoteCacheController::flush(bool need_flush_status)
{
    if (data_file_writer)
    {
        data_file_writer->sync();
    }
    if (need_flush_status)
    {
        auto file_writer = std::make_unique<WriteBufferFromFile>(local_path / "info.txt");
        Poco::JSON::Object jobj;
        jobj.set("file_status", static_cast<Int32>(file_status));
        jobj.set("metadata_class", metadata_class);
        std::stringstream buf; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        jobj.stringify(buf);
        file_writer->write(buf.str().c_str(), buf.str().size());
        file_writer->close();
    }
}

RemoteCacheController::~RemoteCacheController()
{
    if (download_task_holder)
        download_task_holder->deactivate();
}

void RemoteCacheController::close()
{
    // delete directory
    LOG_TRACE(log, "Removing the local cache. local path: {}", local_path.string());
    std::filesystem::remove_all(local_path);
}

std::unique_ptr<ReadBufferFromFileBase> RemoteCacheController::allocFile()
{
    ReadSettings settings;
    settings.local_fs_method = LocalFSReadMethod::read;
    auto file_buffer = createReadBufferFromFileBase((local_path / "data.bin").string(), settings);

    if (file_buffer)
    {
        std::lock_guard lock{mutex};
        opened_file_buffer_refs.insert(reinterpret_cast<uintptr_t>(file_buffer.get()));
    }
    return file_buffer;
}

void RemoteCacheController::deallocFile(std::unique_ptr<ReadBufferFromFileBase> file_buffer)
{
    if (!file_buffer)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Try to release a null file buffer for {}", local_path.string());
    }
    auto buffer_ref = reinterpret_cast<uintptr_t>(file_buffer.get());
    std::lock_guard lock{mutex};
    auto it = opened_file_buffer_refs.find(buffer_ref);
    if (it == opened_file_buffer_refs.end())
    {
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Try to deallocate file with invalid handler remote path: {}, local path: {}",
                file_metadata_ptr->getRemotePath(),
                local_path.string());
    }
    opened_file_buffer_refs.erase(it);
}

RemoteReadBuffer::RemoteReadBuffer(size_t buff_size) : BufferWithOwnMemory<SeekableReadBufferWithSize>(buff_size)
{
}

RemoteReadBuffer::~RemoteReadBuffer()
{
    if (file_cache_controller)
        file_cache_controller->deallocFile(std::move(file_buffer));
}

std::unique_ptr<ReadBuffer> RemoteReadBuffer::create(ContextPtr context, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> read_buffer)
{
    auto * log = &Poco::Logger::get("RemoteReadBuffer");
    size_t buff_size = DBMS_DEFAULT_BUFFER_SIZE;
    if (read_buffer)
        buff_size = read_buffer->internalBuffer().size();
    /*
     * in the new implement of ReadBufferFromHDFS, buffer size is 0.
     *
     * in the common case, we don't read bytes from readbuffer directly, so set buff_size = DBMS_DEFAULT_BUFFER_SIZE
     * is OK.
     *
     * we need be careful with the case  without local file reader.
     */
    if (buff_size == 0)
        buff_size = DBMS_DEFAULT_BUFFER_SIZE;

    auto remote_path = remote_file_metadata->getRemotePath();
    auto remote_read_buffer = std::make_unique<RemoteReadBuffer>(buff_size);
    RemoteReadBufferCacheError error;

    std::tie(remote_read_buffer->file_cache_controller, read_buffer, error) = RemoteReadBufferCache::instance().createReader(context, remote_file_metadata, read_buffer);
    if (remote_read_buffer->file_cache_controller == nullptr)
    {
        LOG_ERROR(log, "Failed to allocate local file for remote path: {}, reason: {}.", remote_path, error);
        // read_buffer is the input one.
        return read_buffer;
    }
    else
    {
        remote_read_buffer->file_buffer = remote_read_buffer->file_cache_controller->allocFile();
        if (!remote_read_buffer->file_buffer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Create file readbuffer failed. {}",
                    remote_read_buffer->file_cache_controller->getLocalPath().string());
    }
    remote_read_buffer->remote_file_size = remote_file_metadata->getFileSize();
    return remote_read_buffer;
}

bool RemoteReadBuffer::nextImpl()
{
    auto start_offset = file_buffer->getPosition();
    auto end_offset = start_offset + file_buffer->internalBuffer().size();
    file_cache_controller->waitMoreData(start_offset, end_offset);

    auto status = file_buffer->next();
    if (status)
        BufferBase::set(file_buffer->buffer().begin(),
                file_buffer->buffer().size(),
                file_buffer->offset());
    return status;
}

off_t RemoteReadBuffer::seek(off_t offset, int whence)
{
    if (!file_buffer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot call seek() in this buffer. It's a bug!");
    /*
     * Need to wait here. For example, the current file has been download at position X, but here we try to seek to
     * postition Y (Y > X), it would fail.
     */
    file_cache_controller->waitMoreData(offset, offset + file_buffer->internalBuffer().size());
    auto ret = file_buffer->seek(offset, whence);
    BufferBase::set(file_buffer->buffer().begin(),
            file_buffer->buffer().size(),
            file_buffer->offset());
    return ret;
}

off_t RemoteReadBuffer::getPosition()
{
    return file_buffer->getPosition();
}

RemoteReadBufferCache::RemoteReadBufferCache() = default;

RemoteReadBufferCache::~RemoteReadBufferCache() = default;

RemoteReadBufferCache & RemoteReadBufferCache::instance()
{
    static RemoteReadBufferCache instance;
    return instance;
}

void RemoteReadBufferCache::recoverCachedFilesMetadata(
    const fs::path & current_path,
    size_t current_depth,
    size_t max_depth)
{
    if (current_depth >= max_depth)
    {
        std::vector<fs::path> invalid_pathes;
        for (auto const & dir : fs::directory_iterator{current_path})
        {
            String path = dir.path();
            auto cache_controller = RemoteCacheController::recover(path);
            if (!cache_controller)
            {
                invalid_pathes.emplace_back(path);
                continue;
            }
            if (!lru_caches->set(path, cache_controller))
            {
                invalid_pathes.emplace_back(path);
            }
        }
        for (auto & path : invalid_pathes)
        {
            fs::remove_all(path);
        }
        return;
    }

    for (auto const & dir : fs::directory_iterator{current_path})
    {
        recoverCachedFilesMetadata(dir.path(), current_depth + 1, max_depth);
    }
}

void RemoteReadBufferCache::recoverTask()
{
    recoverCachedFilesMetadata(root_dir, 1, 2);
    initialized = true;
    LOG_INFO(log, "Recovered from directory:{}", root_dir);
}

void RemoteReadBufferCache::initOnce(
    ContextPtr context,
    const String & root_dir_, size_t limit_size_, size_t bytes_read_before_flush_)
{
    std::lock_guard lock(mutex);
    if (isInitialized())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot initialize RemoteReadBufferCache twice");
    }
    LOG_INFO(
        log, "Initializing local cache for remote data sources. Local cache root path: {}, cache size limit: {}", root_dir_, limit_size_);
    root_dir = root_dir_;
    local_cache_bytes_read_before_flush = bytes_read_before_flush_;
    lru_caches = std::make_unique<CacheType>(limit_size_);

    /// create if root_dir not exists
    if (!fs::exists(fs::path(root_dir)))
    {
        fs::create_directories(fs::path(root_dir));
    }

    recover_task_holder = context->getSchedulePool().createTask("recover local cache metadata for remote files", [this]{ recoverTask(); });
    recover_task_holder->activateAndSchedule();
}

String RemoteReadBufferCache::calculateLocalPath(IRemoteFileMetadataPtr metadata) const
{
    // add version into the full_path, and not block to read the new version
    String full_path = metadata->getName() + ":" + metadata->getRemotePath()
        + ":" + metadata->getVersion();
    UInt128 hashcode = sipHash128(full_path.c_str(), full_path.size());
    String hashcode_str = getHexUIntLowercase(hashcode);
    return fs::path(root_dir) / hashcode_str.substr(0, 3) / hashcode_str;
}

std::tuple<RemoteCacheControllerPtr, std::unique_ptr<ReadBuffer>, RemoteReadBufferCacheError>
RemoteReadBufferCache::createReader(ContextPtr context, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> & read_buffer)
{
    // If something is wrong on startup, rollback to read from the original ReadBuffer
    if (!isInitialized())
    {
        LOG_ERROR(log, "RemoteReadBufferCache has not been initialized");
        return {nullptr, std::move(read_buffer), RemoteReadBufferCacheError::NOT_INIT};
    }

    auto remote_path = remote_file_metadata->getRemotePath();
    const auto & last_modification_timestamp = remote_file_metadata->getLastModificationTimestamp();
    auto local_path = calculateLocalPath(remote_file_metadata);
    std::lock_guard lock(mutex);
    auto cache = lru_caches->get(local_path);
    if (cache)
    {
        // the remote file has been updated, need to redownload
        if (!cache->isValid() || cache->checkFileChanged(remote_file_metadata))
        {
            LOG_TRACE(
                log,
                "Remote file ({}) has been updated. Last saved modification time: {}, actual last modification time: {}",
                remote_path,
                std::to_string(cache->getLastModificationTimestamp()),
                std::to_string(last_modification_timestamp));
            cache->markInvalid();
        }
        else
        {
            return {cache, nullptr, RemoteReadBufferCacheError::OK};
        }
    }

    if (!fs::exists(local_path))
        fs::create_directories(local_path);

    // cache is not found or is invalid
    auto new_cache = std::make_shared<RemoteCacheController>(remote_file_metadata, local_path, local_cache_bytes_read_before_flush);
    if (!lru_caches->set(local_path, new_cache))
    {
        LOG_ERROR(log, "Insert the new cache failed. new file size:{}, current total size:{}",
                remote_file_metadata->getFileSize(),
                lru_caches->weight());
        return {nullptr, std::move(read_buffer), RemoteReadBufferCacheError::DISK_FULL};
    }
    new_cache->startBackgroundDownload(std::move(read_buffer), context->getSchedulePool());
    return {new_cache, nullptr, RemoteReadBufferCacheError::OK};
}

}
