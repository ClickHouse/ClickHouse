#pragma once

#include "FileCache.h"

namespace DB
{

class MultiplelevelFileCacheWarpper : public IFileCache
{
public:
    MultiplelevelFileCacheWarpper(const String & cache_base_path_, const FileCacheSettings & cache_settings_, FileCachePtr cache_);

    void initialize() override;

    void remove(const Key & key) override;

    std::vector<String> tryGetCachePaths(const Key & key) override;

    FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size) override;

    FileSegmentsHolder get(const Key & key, size_t offset, size_t size) override;

    FileSegmentsHolder setDownloading(const Key & key, size_t offset, size_t size) override;

    FileSegments getSnapshot() const override;

    String dumpStructure(const Key & key) override;

    size_t getUsedCacheSize() const override;

    size_t getFileSegmentsNum() const override;

private:
    bool tryReserve(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock) override;

    void remove(Key key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

    bool isLastFileSegmentHolder(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

    void reduceSizeToDownloaded(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

private:
    FileCachePtr cache;
};

};
