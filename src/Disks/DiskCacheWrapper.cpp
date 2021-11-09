#include "DiskCacheWrapper.h"
#include <IO/copyData.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <Common/quoteString.h>
#include <condition_variable>

namespace DB
{
/**
 * Write buffer with possibility to set and invoke callback after 'finalize' call.
 */
class CompletionAwareWriteBuffer : public WriteBufferFromFileDecorator
{
public:
    CompletionAwareWriteBuffer(std::unique_ptr<WriteBufferFromFileBase> impl_, std::function<void()> completion_callback_)
        : WriteBufferFromFileDecorator(std::move(impl_)), completion_callback(completion_callback_) { }

    virtual ~CompletionAwareWriteBuffer() override
    {
        try
        {
            CompletionAwareWriteBuffer::finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void finalize() override
    {
        if (finalized)
            return;

        WriteBufferFromFileDecorator::finalize();

        completion_callback();
    }

private:
    const std::function<void()> completion_callback;
};

enum FileDownloadStatus
{
    NONE,
    DOWNLOADING,
    DOWNLOADED,
    ERROR
};

struct FileDownloadMetadata
{
    /// Thread waits on this condition if download process is in progress.
    std::condition_variable condition;
    FileDownloadStatus status = NONE;
};

DiskCacheWrapper::DiskCacheWrapper(
    std::shared_ptr<IDisk> delegate_, std::shared_ptr<DiskLocal> cache_disk_, std::function<bool(const String &)> cache_file_predicate_)
    : DiskDecorator(delegate_), cache_disk(cache_disk_), cache_file_predicate(cache_file_predicate_)
{
}

std::shared_ptr<FileDownloadMetadata> DiskCacheWrapper::acquireDownloadMetadata(const String & path) const
{
    std::unique_lock<std::mutex> lock{mutex};

    auto it = file_downloads.find(path);
    if (it != file_downloads.end() && !it->second.expired())
        return it->second.lock();

    std::shared_ptr<FileDownloadMetadata> metadata(
        new FileDownloadMetadata,
        [this, path] (FileDownloadMetadata * p)
        {
            std::unique_lock<std::mutex> erase_lock{mutex};
            file_downloads.erase(path);
            delete p;
        });

    file_downloads.emplace(path, metadata);

    return metadata;
}

std::unique_ptr<ReadBufferFromFileBase>
DiskCacheWrapper::readFile(
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> size) const
{
    if (!cache_file_predicate(path))
        return DiskDecorator::readFile(path, settings, size);

    LOG_DEBUG(log, "Read file {} from cache", backQuote(path));

    if (cache_disk->exists(path))
        return cache_disk->readFile(path, settings, size);

    auto metadata = acquireDownloadMetadata(path);

    {
        std::unique_lock<std::mutex> lock{mutex};

        if (metadata->status == NONE)
        {
            /// This thread will responsible for file downloading to cache.
            metadata->status = DOWNLOADING;
            LOG_DEBUG(log, "File {} doesn't exist in cache. Will download it", backQuote(path));
        }
        else if (metadata->status == DOWNLOADING)
        {
            LOG_DEBUG(log, "Waiting for file {} download to cache", backQuote(path));
            metadata->condition.wait(lock, [metadata] { return metadata->status == DOWNLOADED || metadata->status == ERROR; });
        }
    }

    if (metadata->status == DOWNLOADING)
    {
        FileDownloadStatus result_status = DOWNLOADED;

        if (!cache_disk->exists(path))
        {
            try
            {
                auto dir_path = directoryPath(path);
                if (!cache_disk->exists(dir_path))
                    cache_disk->createDirectories(dir_path);

                auto tmp_path = path + ".tmp";
                {
                    auto src_buffer = DiskDecorator::readFile(path, settings, size);
                    auto dst_buffer = cache_disk->writeFile(tmp_path, settings.local_fs_buffer_size, WriteMode::Rewrite);
                    copyData(*src_buffer, *dst_buffer);
                }
                cache_disk->moveFile(tmp_path, path);

                LOG_DEBUG(log, "File {} downloaded to cache", backQuote(path));
            }
            catch (...)
            {
                tryLogCurrentException("DiskCache", "Failed to download file + " + backQuote(path) + " to cache");
                result_status = ERROR;
            }
        }

        /// Notify all waiters that file download is finished.
        std::unique_lock<std::mutex> lock{mutex};

        metadata->status = result_status;
        lock.unlock();
        metadata->condition.notify_all();
    }

    if (metadata->status == DOWNLOADED)
        return cache_disk->readFile(path, settings, size);

    return DiskDecorator::readFile(path, settings, size);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskCacheWrapper::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    if (!cache_file_predicate(path))
        return DiskDecorator::writeFile(path, buf_size, mode);

    LOG_DEBUG(log, "Write file {} to cache", backQuote(path));

    auto dir_path = directoryPath(path);
    if (!cache_disk->exists(dir_path))
        cache_disk->createDirectories(dir_path);

    return std::make_unique<CompletionAwareWriteBuffer>(
        cache_disk->writeFile(path, buf_size, mode),
        [this, path, buf_size, mode]()
        {
            /// Copy file from cache to actual disk when cached buffer is finalized.
            auto src_buffer = cache_disk->readFile(path, ReadSettings(), /* size= */ {});
            auto dst_buffer = DiskDecorator::writeFile(path, buf_size, mode);
            copyData(*src_buffer, *dst_buffer);
            dst_buffer->finalize();
        });
}

void DiskCacheWrapper::clearDirectory(const String & path)
{
    if (cache_disk->exists(path))
        cache_disk->clearDirectory(path);
    DiskDecorator::clearDirectory(path);
}

void DiskCacheWrapper::moveDirectory(const String & from_path, const String & to_path)
{
    if (cache_disk->exists(from_path))
    {
        /// Destination directory may not be empty if previous directory move attempt was failed.
        if (cache_disk->exists(to_path) && cache_disk->isDirectory(to_path))
            cache_disk->clearDirectory(to_path);

        cache_disk->moveDirectory(from_path, to_path);
    }
    DiskDecorator::moveDirectory(from_path, to_path);
}

void DiskCacheWrapper::moveFile(const String & from_path, const String & to_path)
{
    if (cache_disk->exists(from_path))
    {
        auto dir_path = directoryPath(to_path);
        if (!cache_disk->exists(dir_path))
            cache_disk->createDirectories(dir_path);

        cache_disk->moveFile(from_path, to_path);
    }
    DiskDecorator::moveFile(from_path, to_path);
}

void DiskCacheWrapper::replaceFile(const String & from_path, const String & to_path)
{
    if (cache_disk->exists(from_path))
    {
        auto dir_path = directoryPath(to_path);
        if (!cache_disk->exists(dir_path))
            cache_disk->createDirectories(dir_path);

        cache_disk->replaceFile(from_path, to_path);
    }
    DiskDecorator::replaceFile(from_path, to_path);
}

void DiskCacheWrapper::removeFile(const String & path)
{
    cache_disk->removeFileIfExists(path);
    DiskDecorator::removeFile(path);
}

void DiskCacheWrapper::removeFileIfExists(const String & path)
{
    cache_disk->removeFileIfExists(path);
    DiskDecorator::removeFileIfExists(path);
}

void DiskCacheWrapper::removeDirectory(const String & path)
{
    if (cache_disk->exists(path))
        cache_disk->removeDirectory(path);
    DiskDecorator::removeDirectory(path);
}

void DiskCacheWrapper::removeRecursive(const String & path)
{
    if (cache_disk->exists(path))
        cache_disk->removeRecursive(path);
    DiskDecorator::removeRecursive(path);
}

void DiskCacheWrapper::removeSharedFile(const String & path, bool keep_s3)
{
    if (cache_disk->exists(path))
        cache_disk->removeSharedFile(path, keep_s3);
    DiskDecorator::removeSharedFile(path, keep_s3);
}

void DiskCacheWrapper::removeSharedRecursive(const String & path, bool keep_s3)
{
    if (cache_disk->exists(path))
        cache_disk->removeSharedRecursive(path, keep_s3);
    DiskDecorator::removeSharedRecursive(path, keep_s3);
}

void DiskCacheWrapper::createHardLink(const String & src_path, const String & dst_path)
{
    /// Don't create hardlinks for cache files to shadow directory as it just waste cache disk space.
    if (cache_disk->exists(src_path) && !dst_path.starts_with("shadow/"))
    {
        auto dir_path = directoryPath(dst_path);
        if (!cache_disk->exists(dir_path))
            cache_disk->createDirectories(dir_path);

        cache_disk->createHardLink(src_path, dst_path);
    }
    DiskDecorator::createHardLink(src_path, dst_path);
}

void DiskCacheWrapper::createDirectory(const String & path)
{
    cache_disk->createDirectory(path);
    DiskDecorator::createDirectory(path);
}

void DiskCacheWrapper::createDirectories(const String & path)
{
    cache_disk->createDirectories(path);
    DiskDecorator::createDirectories(path);
}

ReservationPtr DiskCacheWrapper::reserve(UInt64 bytes)
{
    auto ptr = DiskDecorator::reserve(bytes);
    if (ptr)
    {
        auto disk_ptr = std::static_pointer_cast<DiskCacheWrapper>(shared_from_this());
        return std::make_unique<ReservationDelegate>(std::move(ptr), disk_ptr);
    }
    return ptr;
}

}
