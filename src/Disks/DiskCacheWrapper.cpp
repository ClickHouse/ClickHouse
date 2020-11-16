#include "DiskCacheWrapper.h"
#include <IO/copyData.h>
#include <Common/quoteString.h>
#include <common/logger_useful.h>
#include <condition_variable>

namespace DB
{
/**
 * Write buffer with possibility to set and invoke callback when buffer is finalized.
 */
class CompletionAwareWriteBuffer : public WriteBufferFromFileBase
{
public:
    CompletionAwareWriteBuffer(std::unique_ptr<WriteBufferFromFileBase> impl_, std::function<void()> completion_callback_, size_t buf_size_)
        : WriteBufferFromFileBase(buf_size_, nullptr, 0), impl(std::move(impl_)), completion_callback(completion_callback_) { }

    ~CompletionAwareWriteBuffer() override
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

        next();
        impl->finalize();

        finalized = true;

        completion_callback();
    }

    void sync() override { impl->sync(); }

    std::string getFileName() const override { return impl->getFileName(); }

private:
    void nextImpl() override
    {
        impl->swap(*this);
        impl->next();
        impl->swap(*this);
    }

    /// Actual write buffer.
    std::unique_ptr<WriteBufferFromFileBase> impl;
    /// Callback is invoked when finalize is completed.
    const std::function<void()> completion_callback;
    bool finalized = false;
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
DiskCacheWrapper::readFile(const String & path, size_t buf_size, size_t estimated_size, size_t aio_threshold, size_t mmap_threshold) const
{
    if (!cache_file_predicate(path))
        return DiskDecorator::readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold);

    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Read file {} from cache", backQuote(path));

    if (cache_disk->exists(path))
        return cache_disk->readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold);

    auto metadata = acquireDownloadMetadata(path);

    {
        std::unique_lock<std::mutex> lock{mutex};

        if (metadata->status == NONE)
        {
            /// This thread will responsible for file downloading to cache.
            metadata->status = DOWNLOADING;
            LOG_DEBUG(&Poco::Logger::get("DiskS3"), "File {} doesn't exist in cache. Will download it", backQuote(path));
        }
        else if (metadata->status == DOWNLOADING)
        {
            LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Waiting for file {} download to cache", backQuote(path));
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
                auto dir_path = getDirectoryPath(path);
                if (!cache_disk->exists(dir_path))
                    cache_disk->createDirectories(dir_path);

                auto tmp_path = path + ".tmp";
                {
                    auto src_buffer = DiskDecorator::readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold);
                    auto dst_buffer = cache_disk->writeFile(tmp_path, buf_size, WriteMode::Rewrite, estimated_size, aio_threshold);
                    copyData(*src_buffer, *dst_buffer);
                }
                cache_disk->moveFile(tmp_path, path);

                LOG_DEBUG(&Poco::Logger::get("DiskS3"), "File {} downloaded to cache", backQuote(path));
            }
            catch (...)
            {
                tryLogCurrentException("DiskS3", "Failed to download file + " + backQuote(path) + " to cache");
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
        return cache_disk->readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold);

    return DiskDecorator::readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskCacheWrapper::writeFile(const String & path, size_t buf_size, WriteMode mode, size_t estimated_size, size_t aio_threshold)
{
    if (!cache_file_predicate(path))
        return DiskDecorator::writeFile(path, buf_size, mode, estimated_size, aio_threshold);

    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Write file {} to cache", backQuote(path));

    auto dir_path = getDirectoryPath(path);
    if (!cache_disk->exists(dir_path))
        cache_disk->createDirectories(dir_path);

    return std::make_unique<CompletionAwareWriteBuffer>(
        cache_disk->writeFile(path, buf_size, mode, estimated_size, aio_threshold),
        [this, path, buf_size, mode, estimated_size, aio_threshold]()
        {
            /// Copy file from cache to actual disk when cached buffer is finalized.
            auto src_buffer = cache_disk->readFile(path, buf_size, estimated_size, aio_threshold, 0);
            auto dst_buffer = DiskDecorator::writeFile(path, buf_size, mode, estimated_size, aio_threshold);
            copyData(*src_buffer, *dst_buffer);
            dst_buffer->finalize();
        },
        buf_size);
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
        cache_disk->moveDirectory(from_path, to_path);
    DiskDecorator::moveDirectory(from_path, to_path);
}

void DiskCacheWrapper::moveFile(const String & from_path, const String & to_path)
{
    if (cache_disk->exists(from_path))
    {
        auto dir_path = getDirectoryPath(to_path);
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
        auto dir_path = getDirectoryPath(to_path);
        if (!cache_disk->exists(dir_path))
            cache_disk->createDirectories(dir_path);

        cache_disk->replaceFile(from_path, to_path);
    }
    DiskDecorator::replaceFile(from_path, to_path);
}

void DiskCacheWrapper::copyFile(const String & from_path, const String & to_path)
{
    if (cache_disk->exists(from_path))
    {
        auto dir_path = getDirectoryPath(to_path);
        if (!cache_disk->exists(dir_path))
            cache_disk->createDirectories(dir_path);

        cache_disk->copyFile(from_path, to_path);
    }
    DiskDecorator::copyFile(from_path, to_path);
}

void DiskCacheWrapper::remove(const String & path)
{
    if (cache_disk->exists(path))
        cache_disk->remove(path);
    DiskDecorator::remove(path);
}

void DiskCacheWrapper::removeRecursive(const String & path)
{
    if (cache_disk->exists(path))
        cache_disk->removeRecursive(path);
    DiskDecorator::removeRecursive(path);
}

void DiskCacheWrapper::createHardLink(const String & src_path, const String & dst_path)
{
    if (cache_disk->exists(src_path))
    {
        auto dir_path = getDirectoryPath(dst_path);
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

inline String DiskCacheWrapper::getDirectoryPath(const String & path)
{
    return Poco::Path{path}.setFileName("").toString();
}

/// TODO: Current reservation mechanism leaks IDisk abstraction details.
/// This hack is needed to return proper disk pointer (wrapper instead of implementation) from reservation object.
class ReservationDelegate : public IReservation
{
public:
    ReservationDelegate(ReservationPtr delegate_, DiskPtr wrapper_) : delegate(std::move(delegate_)), wrapper(wrapper_) { }
    UInt64 getSize() const override { return delegate->getSize(); }
    DiskPtr getDisk(size_t) const override { return wrapper; }
    Disks getDisks() const override { return {wrapper}; }
    void update(UInt64 new_size) override { delegate->update(new_size); }

private:
    ReservationPtr delegate;
    DiskPtr wrapper;
};

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
