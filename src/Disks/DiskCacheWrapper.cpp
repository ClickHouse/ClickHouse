#include "DiskCacheWrapper.h"
#include <IO/copyData.h>
#include <Common/quoteString.h>
#include <common/logger_useful.h>


namespace DB
{
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

    std::shared_ptr<FileDownloadMetadata> metadata(new FileDownloadMetadata, [this, path](FileDownloadMetadata * p) {
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

    if (cache_disk->exists(path))
        return cache_disk->readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold);

    auto metadata = acquireDownloadMetadata(path);

    {
        std::unique_lock<std::mutex> lock{mutex};

        if (metadata->status == NONE)
        {
            /// This thread will responsible for file downloading to cache.
            metadata->status = DOWNLOADING;
            LOG_DEBUG(&Poco::Logger::get("DiskS3"), "File doesn't exist in cache. Will download it {}", backQuote(path));
        }
        else if (metadata->status == DOWNLOADING)
        {
            LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Waiting for file download to cache {}", backQuote(path));
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
                cache_disk->createDirectories(Poco::Path{path}.setFileName("").toString());

                auto src_buffer = DiskDecorator::readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold);
                auto dst_buffer = cache_disk->writeFile(path, buf_size, WriteMode::Rewrite, estimated_size, aio_threshold);
                copyData(*src_buffer, *dst_buffer);
                dst_buffer->finalize();
                dst_buffer->sync();

                LOG_DEBUG(&Poco::Logger::get("DiskS3"), "File downloaded to cache {}", backQuote(path));
            }
            catch (...)
            {
                tryLogCurrentException("DiskS3", "Failed to download file to cache " + backQuote(path));
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

CompletionAwareWriteBuffer::CompletionAwareWriteBuffer(
    std::unique_ptr<WriteBufferFromFileBase> impl_, std::function<void()> completion_callback_, size_t buf_size_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0), impl(std::move(impl_)), completion_callback(completion_callback_)
{
}

void CompletionAwareWriteBuffer::nextImpl()
{
    impl->swap(*this);
    impl->next();
    impl->swap(*this);
}

void CompletionAwareWriteBuffer::finalize()
{
    if (finalized)
        return;

    next();
    impl->finalize();

    finalized = true;

    completion_callback();
}

CompletionAwareWriteBuffer::~CompletionAwareWriteBuffer()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void CompletionAwareWriteBuffer::sync()
{
    impl->sync();
}

std::string CompletionAwareWriteBuffer::getFileName() const
{
    return impl->getFileName();
}

std::unique_ptr<WriteBufferFromFileBase>
DiskCacheWrapper::writeFile(const String & path, size_t buf_size, WriteMode mode, size_t estimated_size, size_t aio_threshold)
{
    if (!cache_file_predicate(path))
        return DiskDecorator::writeFile(path, buf_size, mode, estimated_size, aio_threshold);

    cache_disk->createDirectories(Poco::Path{path}.setFileName("").toString());

    return std::make_unique<CompletionAwareWriteBuffer>(
        cache_disk->writeFile(path, buf_size, mode, estimated_size, aio_threshold),
        [this, path, buf_size, mode, estimated_size, aio_threshold]() {
            /// Copy file from cache to actual disk when cached buffer is finalized.
            auto src_buffer = cache_disk->readFile(path, buf_size, estimated_size, aio_threshold, 0);
            auto dst_buffer = DiskDecorator::writeFile(path, buf_size, mode, estimated_size, aio_threshold);
            copyData(*src_buffer, *dst_buffer);
            dst_buffer->finalize();
            dst_buffer->sync();
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
        cache_disk->moveFile(from_path, to_path);
    DiskDecorator::moveFile(from_path, to_path);
}

void DiskCacheWrapper::replaceFile(const String & from_path, const String & to_path)
{
    if (cache_disk->exists(from_path))
        cache_disk->replaceFile(from_path, to_path);
    DiskDecorator::replaceFile(from_path, to_path);
}

void DiskCacheWrapper::copyFile(const String & from_path, const String & to_path)
{
    if (cache_disk->exists(from_path))
        cache_disk->copyFile(from_path, to_path);
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
        cache_disk->createHardLink(src_path, dst_path);
    DiskDecorator::createHardLink(src_path, dst_path);
}

}
