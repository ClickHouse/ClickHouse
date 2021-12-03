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
#include <IO/RemoteReadBufferCache.h>
#include <IO/WriteHelpers.h>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_CREATE_DIRECTORY;
}

bool RemoteCacheController::loadInnerInformation(const fs::path & file_path)
{
    if (!fs::exists(file_path))
        return false;
    std::ifstream info_file(file_path);
    Poco::JSON::Parser info_parser;
    auto info_jobj = info_parser.parse(info_file).extract<Poco::JSON::Object::Ptr>();
    file_status = static_cast<LocalFileStatus>(info_jobj->get("file_status").convert<Int32>());
    meta_data_class = info_jobj->get("meta_data_class").convert<String>();
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
        LOG_INFO(log, "recover cached file failed. local path:{}", local_path_.string());
        return nullptr;
    }

    cache_controller->file_meta_data_ptr = RemoteFileMetaDataFactory::instance().create_class(cache_controller->meta_data_class);
    if (!cache_controller->file_meta_data_ptr)
    {
        // do not load this invalid cached file and clear it
        LOG_ERROR(log, "Cannot create the meta data class : {}. The cached file is invalid and will be remove. path:{}",
                cache_controller->meta_data_class,
                local_path_.string());
        fs::remove_all(local_path_);
        return nullptr;
    }
    std::ifstream meta_data_file(local_path_ / "meta_data.txt");
    if (!cache_controller->file_meta_data_ptr->fromString(std::string((std::istreambuf_iterator<char>(meta_data_file)),
                    std::istreambuf_iterator<char>())))
    {
        LOG_ERROR(log, "Cannot load the meta data. The cached file is invalid and will be remove. path:{}",
                local_path_.string());
        fs::remove_all(local_path_);
        return nullptr;
    }

    cache_controller->current_offset = fs::file_size(local_path_ / "data.bin");

    RemoteReadBufferCache::instance().updateTotalSize(cache_controller->file_meta_data_ptr->getFileSize());
    return cache_controller;
}

RemoteCacheController::RemoteCacheController(
    RemoteFileMetaDataBasePtr file_meta_data_,
    const std::filesystem::path & local_path_,
    size_t cache_bytes_before_flush_)
    : file_meta_data_ptr(file_meta_data_)
    , local_path(local_path_)
    , valid(true)
    , local_cache_bytes_read_before_flush(cache_bytes_before_flush_)
    , current_offset(0)
{
    if (file_meta_data_ptr)
    {
        std::ofstream meta_data_file(local_path_ / "meta_data.txt", std::ios::out);
        meta_data_file << file_meta_data_ptr->toString();
        meta_data_file.close();
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

bool RemoteCacheController::checkFileChanged(RemoteFileMetaDataBasePtr file_meta_data_)
{
    return !file_meta_data_ptr->equal(file_meta_data_);
}

void RemoteCacheController::startBackgroundDownload(std::shared_ptr<ReadBuffer> input_readbuffer, BackgroundSchedulePool & thread_pool)
{
    data_file_writer = std::make_unique<WriteBufferFromFile>((fs::path(local_path) / "data.bin").string());
    flush(true);
    download_task_holder = thread_pool.createTask("download remote file",
            [this,input_readbuffer]{ backgroundDownload(input_readbuffer); });
    download_task_holder->activateAndSchedule();
}

void RemoteCacheController::backgroundDownload(std::shared_ptr<ReadBuffer> remote_read_buffer)
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
    RemoteReadBufferCache::instance().updateTotalSize(file_meta_data_ptr->getFileSize());
    LOG_TRACE(log, "Finish download into local path: {}, file meta data:{} ", local_path.string(), file_meta_data_ptr->toString());
}

void RemoteCacheController::flush(bool need_flush_status)
{
    if (data_file_writer)
    {
        LOG_DEBUG(&Poco::Logger::get("RemoteCacheController"),"flush file. offset:{}, file:{}. total_size:{}", current_offset, local_path.string(), file_meta_data_ptr->getFileSize());
        data_file_writer->sync();
    }
    if (need_flush_status)
    {
        Poco::JSON::Object jobj;
        jobj.set("file_status", static_cast<Int32>(file_status));
        jobj.set("meta_data_class", meta_data_class);
        std::stringstream buf; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        jobj.stringify(buf);
        std::ofstream info_file(local_path / "info.txt");
        info_file << buf.str();
        info_file.close();
    }
}

RemoteCacheController::~RemoteCacheController() = default;

void RemoteCacheController::close()
{
    // delete directory
    LOG_TRACE(log, "Removing the local cache. local path: {}", local_path.string());
    std::filesystem::remove_all(local_path);
}

std::unique_ptr<ReadBufferFromFileBase> RemoteCacheController::allocFile()
{
    ReadSettings settings;
    settings.local_fs_prefetch = false;
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Try to release a null file buffer for ", local_path.string());
    }
    auto buffer_ref = reinterpret_cast<uintptr_t>(file_buffer.get());
    std::lock_guard lock{mutex};
    auto it = opened_file_buffer_refs.find(buffer_ref);
    if (it == opened_file_buffer_refs.end())
    {
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Try to deallocate file with invalid handler remote path: {}, local path: {}",
                file_meta_data_ptr->getRemotePath(),
                local_path.string());
    }
    opened_file_buffer_refs.erase(it);
}

// the size need be equal to the original buffer
RemoteReadBuffer::RemoteReadBuffer(size_t buff_size) : BufferWithOwnMemory<SeekableReadBufferWithSize>(buff_size)
{
}

RemoteReadBuffer::~RemoteReadBuffer()
{
    file_cache_controller->deallocFile(std::move(file_buffer));
}

std::unique_ptr<RemoteReadBuffer> RemoteReadBuffer::create(ContextPtr context, RemoteFileMetaDataBasePtr remote_file_meta_data, std::unique_ptr<ReadBuffer> read_buffer)
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

    auto remote_path = remote_file_meta_data->getRemotePath();
    auto remote_read_buffer = std::make_unique<RemoteReadBuffer>(buff_size);
    auto * raw_readbuffer_ptr = read_buffer.release();
    std::shared_ptr<ReadBuffer> shared_readbuffer_ptr(raw_readbuffer_ptr);
    RemoteReadBufferCacheError error;

    std::tie(remote_read_buffer->file_cache_controller, error) = RemoteReadBufferCache::instance().createReader(context, remote_file_meta_data, shared_readbuffer_ptr);
    if (remote_read_buffer->file_cache_controller == nullptr)
    {
        LOG_ERROR(log, "Failed to allocate local file for remote path: {}, reason: {}", remote_path, error);
        remote_read_buffer->original_read_buffer = shared_readbuffer_ptr;
    }
    else
    {
        remote_read_buffer->file_buffer = remote_read_buffer->file_cache_controller->allocFile();
        if (!remote_read_buffer->file_buffer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Create file readbuffer failed. {}",
                    remote_read_buffer->file_cache_controller->getLocalPath().string());
    }
    return remote_read_buffer;
}

bool RemoteReadBuffer::nextImpl()
{
    if (file_buffer)
    {
        auto start_offset = file_buffer->getPosition();
        auto end_offset = start_offset + file_buffer->internalBuffer().size();
        LOG_DEBUG(&Poco::Logger::get("RemoteReadBuffer"), "nextImpl. start:{}, end:{}, file:{}, total_size:{}, remote_path:{}",
                start_offset, end_offset, file_buffer->getFileName(), file_cache_controller->getFileMetaData()->getFileSize(),
                file_cache_controller->getFileMetaData()->getRemotePath());
        file_cache_controller->waitMoreData(start_offset, end_offset);

        auto status = file_buffer->next();
        if (status)
            BufferBase::set(file_buffer->buffer().begin(),
                    file_buffer->buffer().size(),
                    file_buffer->offset());
        return status;
    }
    else
    {
        // In the case we cannot use local cache, read from the original readbuffer directly
        if (!original_read_buffer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Original read buffer is not initialized. It's a bug");

        auto status = original_read_buffer->next();
        // We don't need to worry about the memory buffer allocated in RemoteReadBuffer, since it is owned by
        // BufferWithOwnMemory, BufferWithOwnMemory would release it.
        if (status)
            BufferBase::set(original_read_buffer->buffer().begin(), original_read_buffer->buffer().size(), original_read_buffer->offset());
        return status;
    }
}

off_t RemoteReadBuffer::seek(off_t offset, int whence)
{
    /*
     * Need to wait here. For example, the current file has been download at position X, but here we try to seek to
     * postition Y (Y > X), it would fail.
     */
    file_cache_controller->waitMoreData(offset, offset + file_buffer->internalBuffer().size());
    LOG_DEBUG(&Poco::Logger::get("RemoteReadBuffer"), "seek. offset:{}. file:{}, total_size:{}", offset, file_buffer->getFileName(),
            file_cache_controller->getFileMetaData()->getFileSize());
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

void RemoteReadBufferCache::recoverCachedFilesMetaData(
    const fs::path & current_path,
    size_t current_depth,
    size_t max_depth)
{
    if (current_depth >= max_depth)
    {
        for (auto const & dir : fs::directory_iterator{current_path})
        {
            String path = dir.path();
            auto cache_controller = RemoteCacheController::recover(path);
            if (!cache_controller)
                continue;
            auto & cell = caches[path];
            cell.cache_controller = cache_controller;
            cell.key_iterator = keys.insert(keys.end(), path);
        }
        return;
    }

    for (auto const & dir : fs::directory_iterator{current_path})
    {
        recoverCachedFilesMetaData(dir.path(), current_depth + 1, max_depth);
    }
}

void RemoteReadBufferCache::recoverTask()
{
    std::lock_guard lock(mutex);
    recoverCachedFilesMetaData(root_dir, 1, 2);
    initialized = true;
    LOG_TRACE(log, "Recovered from directory:{}", root_dir);
}

void RemoteReadBufferCache::initOnce(
    ContextPtr context,
    const String & root_dir_, size_t limit_size_, size_t bytes_read_before_flush_)
{
    LOG_INFO(
        log, "Initializing local cache for remote data sources. Local cache root path: {}, cache size limit: {}", root_dir_, limit_size_);
    root_dir = root_dir_;
    limit_size = limit_size_;
    local_cache_bytes_read_before_flush = bytes_read_before_flush_;

    /// create if root_dir not exists
    if (!fs::exists(fs::path(root_dir) / ""))
    {
        std::error_code ec;
        bool success = fs::create_directories(fs::path(root_dir) / "", ec);
        if (!success)
            throw Exception(
                ErrorCodes::CANNOT_CREATE_DIRECTORY, "Failed to create directories, error code:{} reason:{}", ec.value(), ec.message());
    }

    recover_task_holder = context->getSchedulePool().createTask("recover local cache meta data for remote files", [this]{ recoverTask(); });
    recover_task_holder->activateAndSchedule();
}

String RemoteReadBufferCache::calculateLocalPath(RemoteFileMetaDataBasePtr meta_data) const
{
    String full_path = meta_data->getSchema() + ":" + meta_data->getCluster() + ":" + meta_data->getRemotePath();
    UInt128 hashcode = sipHash128(full_path.c_str(), full_path.size());
    String hashcode_str = getHexUIntLowercase(hashcode);
    return fs::path(root_dir) / hashcode_str.substr(0, 3) / hashcode_str;
}

std::pair<RemoteCacheControllerPtr, RemoteReadBufferCacheError>
RemoteReadBufferCache::createReader(ContextPtr context, RemoteFileMetaDataBasePtr remote_file_meta_data, std::shared_ptr<ReadBuffer> & read_buffer)
{
    LOG_TRACE(log, "createReader. {}", remote_file_meta_data->toString());
    // If something is wrong on startup, rollback to read from the original ReadBuffer
    if (!isInitialized())
    {
        LOG_ERROR(log, "RemoteReadBufferCache has not been initialized");
        return {nullptr, RemoteReadBufferCacheError::NOT_INIT};
    }

    auto remote_path = remote_file_meta_data->getRemotePath();
    const auto & last_modification_timestamp = remote_file_meta_data->getLastModificationTimestamp();
    auto local_path = calculateLocalPath(remote_file_meta_data);
    std::lock_guard lock(mutex);
    auto cache_iter = caches.find(local_path);
    if (cache_iter != caches.end())
    {
        // if the file has been update on remote side, we need to redownload it
        if (cache_iter->second.cache_controller->checkFileChanged(remote_file_meta_data))
        {
            LOG_TRACE(
                log,
                "Remote file ({}) has been updated. Last saved modification time: {}, actual last modification time: {}",
                remote_path,
                std::to_string(cache_iter->second.cache_controller->getLastModificationTimestamp()),
                std::to_string(last_modification_timestamp));
            cache_iter->second.cache_controller->markInvalid();
        }
        else
        {
            // move the key to the list end
            keys.splice(keys.end(), keys, cache_iter->second.key_iterator);
            return { cache_iter->second.cache_controller, RemoteReadBufferCacheError::OK};
        }
    }

    auto clear_ret = clearLocalCache();
    cache_iter = caches.find(local_path);
    if (cache_iter != caches.end())
    {
        if (cache_iter->second.cache_controller->isValid())
        {
            keys.splice(keys.end(), keys, cache_iter->second.key_iterator);
            return {
                cache_iter->second.cache_controller,
                RemoteReadBufferCacheError::OK};
        }
        else
        {
            // maybe someone is holding this file
            LOG_INFO(log, "The remote file {} has been updated, but the previous readers do not finish reading.",
                    remote_path);
            return {nullptr, RemoteReadBufferCacheError::FILE_INVALID};
        }
    }

    // reach the disk capacity limit
    if (!clear_ret)
    {
        LOG_INFO(log, "Reached local cache capacity limit size ({})", limit_size);
        return {nullptr, RemoteReadBufferCacheError::DISK_FULL};
    }

    fs::create_directories(local_path);

    auto cache_controller
        = std::make_shared<RemoteCacheController>(remote_file_meta_data, local_path, local_cache_bytes_read_before_flush);
    cache_controller->startBackgroundDownload(read_buffer, context->getSchedulePool());
    CacheCell cache_cell;
    cache_cell.cache_controller = cache_controller;
    cache_cell.key_iterator = keys.insert(keys.end(), local_path);
    caches[local_path] = cache_cell;

    return {cache_controller, RemoteReadBufferCacheError::OK};
}

bool RemoteReadBufferCache::clearLocalCache()
{
    // clear closable cache from the list head
    for (auto it = keys.begin(); it != keys.end();)
    {
        auto cache_it = caches.find(*it);
        if (cache_it == caches.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Found no entry in local cache with key: {}", *it);

        auto cache_controller = cache_it->second.cache_controller;
        if (!cache_controller->isValid() && cache_controller->closable())
        {
            LOG_TRACE(log, "Clear invalid cache entry with key {} from local cache", *it);
            total_size
                = total_size > cache_it->second.cache_controller->size() ? total_size - cache_it->second.cache_controller->size() : 0;
            cache_controller->close();
            it = keys.erase(it);
            caches.erase(cache_it);
            continue;
        }

        // if enough disk space is release, just to iterate the remained caches and clear the invalid ones.
        if (total_size > limit_size && cache_controller->closable())
        {
            total_size = total_size > cache_controller->size() ? total_size - cache_controller->size() : 0;
            cache_controller->close();
            caches.erase(cache_it);
            it = keys.erase(it);
            LOG_TRACE(
                log,
                "clear local file {} for {}. key size:{}. next{}",
                cache_controller->getLocalPath().string(),
                cache_controller->getRemotePath(),
                keys.size(),
                *it);
        }
        else
            it++;
    }
    LOG_TRACE(log, "After clear local cache, keys size:{}, total_size:{}, limit size:{}", keys.size(), total_size, limit_size);
    return total_size < limit_size;
}

}
