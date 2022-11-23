#include <Disks/IO/FileCachePlaceholder.h>

namespace DB
{

void ISpacePlaceholder::reserveCapacity(size_t requested_capacity)
{
    chassert(used_space <= capacity);

    size_t remaining_space = capacity - used_space;
    /// TODO LOG_TEST
    LOG_DEBUG(&Poco::Logger::get("ISpacePlaceholder"), "reserving {} bytes (used_space: {}, capacity: {})", requested_capacity, used_space, capacity);

    if (requested_capacity <= remaining_space)
        return;

    size_t capacity_to_reserve = requested_capacity - remaining_space;
    reserveImpl(capacity_to_reserve);
    capacity += capacity_to_reserve;
}

void ISpacePlaceholder::setUsed(size_t size)
{
    /// TODO LOG_TEST
    LOG_DEBUG(&Poco::Logger::get("ISpacePlaceholder"), "using {} bytes ({} already used, {} capacity)", size, used_space, capacity);

    if (used_space + size > capacity)
    {
        LOG_WARNING(&Poco::Logger::get("ISpacePlaceholder"), "Used space is greater than capacity. It may lead to not enough space error");
        reserveCapacity(size);
    }

    used_space = used_space + size;

}

FileCachePlaceholder::FileCachePlaceholder(FileCache * cache, const String & name)
    : key_name(name)
    , file_cache(cache)
{
}


void FileCachePlaceholder::reserveImpl(size_t requested_size)
{
    String key = fmt::format("{}_{}", key_name, cache_writers.size());
    auto cache_writer = std::make_unique<FileSegmentRangeWriter>(file_cache,
                                                                 file_cache->hash(key),
                                                                 /* cache_log_ */ nullptr,
                                                                 /* query_id_ */ "",
                                                                 /* source_path_ */ key);

    while (requested_size > 0)
    {
        size_t current_offset = cache_writer->currentOffset();
        size_t written = cache_writer->tryWrite(nullptr, requested_size, current_offset, /* is_persistent */ false, /* strict */ false);
        if (written == 0)
        {
            cache_writer->finalize(/* clear */ true);
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE,
                "Cannot reserve space in file cache ({} bytes required, {} / {} bytes used, {} / {} elements used)",
                requested_size, file_cache->getUsedCacheSize(), file_cache->getTotalMaxSize(),
                file_cache->getFileSegmentsNum(), file_cache->getTotalMaxElements());
        }
        requested_size -= written;
    }

    cache_writers.push_back(std::move(cache_writer));
}

FileCachePlaceholder::~FileCachePlaceholder()
{
    try
    {
        for (auto & cache_writer : cache_writers)
        {
            cache_writer->finalize(/* clear */ true);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
