#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <optional>

namespace DB
{
class ReadBufferFromParquetWithSchemaCaching : public ReadBufferFromFileBase
{
public:
    ReadBufferFromParquetWithSchemaCaching(
        std::unique_ptr<ReadBufferFromFileBase>,
        std::optional<std::pair<String, String>>);
    bool nextImpl() override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    String getFileName() const override;
    size_t getFileOffsetOfBufferEnd() const override;
    bool isSeekCheap() override;
    bool isCached() const override;
    std::optional<std::pair<String, String>> consumeParquetCacheKey();
private:
    std::unique_ptr<ReadBufferFromFileBase> buffer_impl;
    std::optional<std::pair<String, String>> parquet_cache_key;
};
}
