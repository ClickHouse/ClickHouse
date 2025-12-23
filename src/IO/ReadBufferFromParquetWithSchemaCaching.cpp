#include <IO/ReadBufferFromParquetWithSchemaCaching.h>
#include <optional>

namespace DB
{
/*
 * This class wraps around a concrete implementation of ReadBufferFromFileBase and adds
 * parquet cache key to it.  All IO operations are delegated to the wrapped implementation
 * buffer_impl.  We expect the passed in buffer implementation to be fully initialized.
 * The parquet metadata cache key is reset after use to prevent the key from
 * being used by another instance of ReadBufferFromParquetWithSchemaCaching
*/
ReadBufferFromParquetWithSchemaCaching::ReadBufferFromParquetWithSchemaCaching(
        std::unique_ptr<ReadBufferFromFileBase> impl_,
        std::optional<std::pair<String, String>> cache_key_)
        : ReadBufferFromFileBase(0, nullptr, 0, impl_->tryGetFileSize())
        , buffer_impl(std::move(impl_))
        , parquet_cache_key(cache_key_)
    {
    }
bool ReadBufferFromParquetWithSchemaCaching::nextImpl()
{
    bool result = buffer_impl->next();
    if (result)
       BufferBase::set(buffer_impl->buffer().begin(), buffer_impl->buffer().size(), buffer_impl->offset());
    return result;
}
off_t ReadBufferFromParquetWithSchemaCaching::seek(off_t off, int whence)
{ 
    off_t result = buffer_impl->seek(off, whence);
    BufferBase::set(buffer_impl->buffer().begin(), buffer_impl->buffer().size(), buffer_impl->offset());
    return result;
}
off_t ReadBufferFromParquetWithSchemaCaching::getPosition()
{
    return buffer_impl->getPosition();
}    
String ReadBufferFromParquetWithSchemaCaching::getFileName() const
{
    return buffer_impl->getFileName();
}
size_t ReadBufferFromParquetWithSchemaCaching::getFileOffsetOfBufferEnd() const
{
    return buffer_impl->getFileOffsetOfBufferEnd();
}
bool ReadBufferFromParquetWithSchemaCaching::isSeekCheap()
{
    return buffer_impl->isSeekCheap();
}
bool ReadBufferFromParquetWithSchemaCaching::isCached() const
{
    return buffer_impl->isCached();
}
std::optional<std::pair<String, String>> ReadBufferFromParquetWithSchemaCaching::consumeParquetCacheKey() 
    { 
        auto result = parquet_cache_key; 
        parquet_cache_key.reset(); 
        return result; 
    }
}
