#include "DiskCache.h"
#include <Common/Exception.h>
#include <Common/hex.h>
#include <Common/SipHash.h>
#include <base/getThreadId.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int LOCAL_CACHE_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_DOESNT_EXIST;
}

DiskCacheLRUPolicy::CacheInProgressEntry::CacheInProgressEntry(size_t size_) :
    size(size_)
{
    status = FileDownloadStatus::DOWNLOADING;
}


DiskCacheLRUPolicy::DiskCacheLRUPolicy(size_t cache_size_limit_, size_t nodes_limit_) :
    cache_size_limit(cache_size_limit_), nodes_limit(nodes_limit_)
{
    log = &Poco::Logger::get("DiskCacheLRUPolicy");
}

void DiskCacheLRUPolicy::complete(const String & key, size_t offset, FileDownloadStatus status)
{
    uint64_t thread_id = getThreadId();
    if (!--read_thread_ids[thread_id])
        read_thread_ids.erase(thread_id);

    auto p = cache_in_progress.find(key);
    if (p == cache_in_progress.end())
    {
        LOG_WARNING(log, "Not in progress, Key {}, offset {}", key, offset);
        return;
    }

    auto q = p->second.find(offset);
    if (q == p->second.end())
    {
        LOG_WARNING(log, "Not in progress, Key {}, offset {}", key, offset);
        return;
    }

    LOG_TRACE(log, "Complete {}, offset {}", key, offset);

    if (q->second->size > reserved_size)
    { // Should never be here if code correct
        q->second->status = FileDownloadStatus::ERROR;
        q->second->condition.notify_all();
        p->second.erase(q);
        if (p->second.empty())
            cache_in_progress.erase(p);
        throw Exception("File downloading error with size more than reserverd", ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        reserved_size -= q->second->size;
        LOG_TRACE(log, "Unreserve {}, reserved {}", q->second->size, reserved_size);
    }

    q->second->status = status;
    q->second->condition.notify_all();
    p->second.erase(q);
    if (p->second.empty())
        cache_in_progress.erase(p);
}

void DiskCacheLRUPolicy::error(const String & key, size_t offset)
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    complete(key, offset, FileDownloadStatus::ERROR);
}

std::list<std::pair<String, size_t>> DiskCacheLRUPolicy::add(const String & key, size_t file_size, size_t offset, size_t size)
{
    std::unique_lock<std::mutex> lock(cache_mutex);

    if (size > cache_size_limit)
    { // Should never be here if code correct
        complete(key, offset, FileDownloadStatus::ERROR);
        throw Exception("Should not try to insert more than possibly cache size", ErrorCodes::LOGICAL_ERROR);
    }

    std::list<std::pair<String, size_t>> res = reserve_unsafe(size);

    auto p = cache_map.find(key);

    if (p != cache_map.end())
    {
        auto & parts = p->second.parts;
        auto entry_before = parts.lower_bound(offset);
        if (entry_before == parts.begin())
            entry_before = parts.end();
        else
            --entry_before;
        if (entry_before != parts.end() && entry_before->second->offset + entry_before->second->size > offset)
        {
            LOG_ERROR(log, "Key {} ({}+{}) collision with ({}+{})",
                key, offset, size, entry_before->second->offset, entry_before->second->size);
            complete(key, offset, FileDownloadStatus::ERROR);
            throw Exception("Could not insert already existing data, remove it first", ErrorCodes::LOGICAL_ERROR);
        }

        auto entry_after = parts.upper_bound(offset + size);
        if (entry_after != parts.end() && offset + size > entry_after->second->offset)
        {
            LOG_ERROR(log, "Key {} ({}+{}) collision with ({}+{})",
                key, offset, size, entry_after->second->offset, entry_after->second->size);
            complete(key, offset, FileDownloadStatus::ERROR);
            throw Exception("Could not insert already existing data, remove it first", ErrorCodes::LOGICAL_ERROR);
        }
    }

    CacheEntry new_entry(key, offset, size);
    cache_list.push_front(new_entry);
    if (!cache_map[key].size) /// File size was unknown before
        cache_map[key].size = file_size;
    else if (cache_map[key].size != file_size)
    {
        /// File on remote FS was changed, invalidate old cache records here
        /// This should not be in normal situation. Someone else changed file.
        LOG_WARNING(log, "Key {} : size changed from {} to {}. Old records for key invalidated.", key, cache_map[key].size, file_size);
        for (auto & entry : cache_map[key].parts)
            res.push_back(std::make_pair(key, entry.first));
        cache_map[key].parts.clear();

        cache_map[key].size = file_size;
    }
    cache_map[key].parts[offset] = cache_list.begin();
    cache_size += size;

    complete(key, offset, FileDownloadStatus::DOWNLOADED);

    LOG_TRACE(log, "Cache size after add {} ({}+{}) : {}", key, offset, size, cache_size);

    return res;
}

DiskCachePolicy::CachePartList DiskCacheLRUPolicy::remove(const String & key)
{
    CachePartList res;

    std::unique_lock<std::mutex> lock(cache_mutex);

    auto p = cache_map.find(key);
    if (p != cache_map.end())
    {
        for (auto & entry : p->second.parts)
        {
            res.push_back(CachePart(entry.second->offset, entry.second->size));
            cache_size -= entry.second->size;
            cache_list.erase(entry.second);
        }
        cache_map.erase(p);
    }

    return res;
}

void DiskCacheLRUPolicy::remove(const String & key, size_t offset, size_t)
{
    CachePartList res;

    std::unique_lock<std::mutex> lock(cache_mutex);

    auto p = cache_map.find(key);
    if (p != cache_map.end())
    {
        auto & parts = p->second.parts;
        auto entry = parts.find(offset);
        if (entry != parts.end())
        {
            cache_size -= entry->second->size;
            cache_list.erase(entry->second);
            parts.erase(entry);
        }
        if (parts.empty())
            cache_map.erase(p);
    }
}

std::list<std::pair<String, size_t>> DiskCacheLRUPolicy::reserve(size_t size)
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    return reserve_unsafe(size);
}

std::list<std::pair<String, size_t>> DiskCacheLRUPolicy::reserve_unsafe(size_t size)
{
    std::list<std::pair<String, size_t>> keys_to_remove;

    if (size + reserved_size > cache_size_limit)
    { // TODO: do something here
        LOG_ERROR(log, "Try to reserve {} + {}, limit {}", size, reserved_size, cache_size_limit);
        return keys_to_remove;
        //throw Exception("Should not try to reserve more than possibly cache size", ErrorCodes::LOGICAL_ERROR);
    }

    auto last = cache_list.end();
    LOG_TRACE(log, "Cache size before cleanup: {}", cache_size);
    while (cache_size > cache_size_limit - size || cache_map.size() >= nodes_limit)
    {
        if (last == cache_list.begin())
        { // Should never be here if code correct
            throw Exception("Should not be here, non-zero cache_size with empty cache list", ErrorCodes::LOGICAL_ERROR);
        }

        --last;

        if (last->size > cache_size)
        { // Should never be here if code correct
            throw Exception("Should not be here, cache_size less than one cache element", ErrorCodes::LOGICAL_ERROR);
        }

        keys_to_remove.push_back(std::make_pair(last->key, last->offset));
        cache_map[last->key].parts.erase(last->offset);
        if (cache_map[last->key].parts.empty())
            cache_map.erase(last->key);
        cache_size -= last->size;
    }
    LOG_TRACE(log, "Cache size after cleanup: {}", cache_size);

    cache_list.erase(last, cache_list.end());

    reserved_size += size;

    LOG_TRACE(log, "Reserve {}, total {}", size, reserved_size);

    return keys_to_remove;
}

DiskCachePolicy::CachePart DiskCacheLRUPolicy::find(const String & key, size_t offset, size_t size)
{
    CachePart res(offset, size);
    res.type = DiskCachePolicy::CachePart::CachePartType::ABSENT;

    uint64_t thread_id = getThreadId();

    static const int retries_max = 25; /// TODO - settings? tuning?

    int retry = 0;

    std::unique_lock<std::mutex> lock(cache_mutex);

    LOG_TRACE(log, "Key {}, try to find in cache {}+{}", key, offset, size);

    while (retry < retries_max)
    {
        auto p = cache_map.find(key);
        if (p != cache_map.end() && p->second.parts.empty())
        { // should never be here
            LOG_ERROR(log, "Key {}, empty part list", key);
            cache_map.erase(p);
        }
        else if (p != cache_map.end())
        {
            if (p->second.size && offset >= p->second.size)
            {
                LOG_TRACE(log, "Key {}, try get after file end, {} >= {}", key, res.offset, p->second.size);
                res.type = DiskCachePolicy::CachePart::CachePartType::EMPTY;
                return res;
            }

            auto & parts = p->second.parts;
            auto entry = parts.lower_bound(offset);

            if (entry != parts.end())
            {
                LOG_TRACE(log, "Key {}, entry {}+{}", key, entry->second->offset, entry->second->size);
            }

            if (entry != parts.end() && entry->second->offset == offset)
            { // found cached part with same offset
                res.size = entry->second->size;
                res.type = DiskCachePolicy::CachePart::CachePartType::CACHED;
                LOG_TRACE(log, "Key {} in cache, read {}+{}", key, res.offset, res.size);
            }
            else if (entry != parts.end() && entry == parts.begin())
            { // need to download part from offset to first exists in cache
                res.size = entry->second->offset - offset;
                LOG_TRACE(log, "Key {} not in cache, download {}+{}", key, res.offset, res.size);
            }
            else
            { // try previous cached part
                size_t next_offset = entry != parts.end() ? entry->second->offset : 0;
                --entry;
                LOG_TRACE(log, "Key {}, prev entry {}+{}", key, entry->second->offset, entry->second->size);
                if (entry->second->offset + entry->second->size > offset)
                { // found cached part with lower offset
                    res.offset = entry->second->offset;
                    res.size = entry->second->size;
                    res.type = DiskCachePolicy::CachePart::CachePartType::CACHED;
                    LOG_TRACE(log, "Key {} in cache, read {}+{}", key, res.offset, res.size);
                }
                else
                { // need to download
                    res.size = next_offset ? next_offset - offset : 0;
                    LOG_TRACE(log, "Key {} not in cache, download {}+{}", key, res.offset, res.size);
                }
            }
        }
        else
        {
            LOG_TRACE(log, "Key {} completely not in cache, download {}+{}", key, offset, size);
        }

        if (res.type != DiskCachePolicy::CachePart::CachePartType::ABSENT)
            break;

        ++retry;

        {
            auto cp = cache_in_progress.find(key);
            if (cp == cache_in_progress.end())
            {
                LOG_TRACE(log, "Key {}, not found in progress completely", key);
                break;
            }
            auto cq = cp->second.lower_bound(res.offset);

            if (cq == cp->second.end() || (cq->first > res.offset && cq != cp->second.begin()))
                --cq;

            bool waited = false;

            while (cq != cp->second.end())
            {
                if ((!res.size && (!cq->second->size || cq->first + cq->second->size > res.offset)) /// Unknown size - check all from offset to end
                        || (cq->first <= res.offset && (!cq->second->size || cq->first + cq->second->size > res.offset)) // downloaded part covered begin of requested part
                        || (cq->first > res.offset && cq->first <= res.offset + res.size)
                    )
                {
                    if (cq->second->status == FileDownloadStatus::DOWNLOADING)
                    {
                        if (read_thread_ids.count(thread_id))
                        { /// Workaroud for avoiding deadlocks when current thread already downloading some object
                            LOG_TRACE(log, "Skip cache usage to avoid deadlock for key {}", key);
                            res.type = DiskCachePolicy::CachePart::CachePartType::ABSENT_NO_CACHE;
                            return res;
                        }

                        waited = true;
                        size_t waited_offset = cq->first;
                        size_t waited_size = cq->second->size;
                        LOG_TRACE(log, "Key {}, found downloading part {}+{}, retry {}", key, waited_offset, waited_size, retry);
                        cq->second->condition.wait(lock);
                        LOG_TRACE(log, "Key {}, part {}+{} downloaded", key, waited_offset, waited_size);
                        break;
                    }
                    else
                    {
                        LOG_TRACE(log, "Key {}, found completed part {}+{} in progress list", key, cq->first, cq->second->size);
                    }
                }
                else
                {
                    LOG_TRACE(log, "Key {}, skip non-covered part {}+{} in progress list", key, cq->first, cq->second->size);
                }

                ++cq;
            }

            if (!waited)
            {
                LOG_TRACE(log, "Key {}, not found in progress for {}+{}", key, offset, size);
                break;
            }

            if (retry >= retries_max)
            {
                LOG_WARNING(log, "Can't get stable cache info for {}", key);
                res.type = DiskCachePolicy::CachePart::CachePartType::ABSENT_NO_CACHE;
                return res;
            }
        }
    }

    if (res.type == DiskCachePolicy::CachePart::CachePartType::ABSENT)
    {
        ++read_thread_ids[thread_id];
        cache_in_progress[key][offset] = std::make_shared<CacheInProgressEntry>(res.size);
    }

    return res;
}

void DiskCacheLRUPolicy::read(const String & key, size_t offset, size_t size)
{
    std::unique_lock<std::mutex> lock(cache_mutex);

    auto p = cache_map.find(key);
    if (p == cache_map.end())
        return;

    auto entry = p->second.parts.find(offset);
    if (entry == p->second.parts.end())
        return;

    if (entry->second->size != size)
        return;

    cache_list.splice(cache_list.begin(), cache_list, entry->second);
}


DiskCache::DiskCache(std::shared_ptr<DiskLocal> cache_disk_, std::shared_ptr<DiskCachePolicy> cache_policy_) :
    cache_disk(cache_disk_), cache_policy(cache_policy_)
{

}

class CacheableReadBufferDecorator : public ReadBuffer
{
public:
    CacheableReadBufferDecorator(std::unique_ptr<ReadBuffer> &from_,
        std::shared_ptr<DiskLocal> cache_disk_, std::shared_ptr<DiskCachePolicy> cache_policy_,
        const String & cache_key_, const String & cache_path_, size_t file_size_,
        size_t part_offset_, size_t expected_size_) :
        ReadBuffer(nullptr, 0), from(std::move(from_)), cache_disk(std::move(cache_disk_)),
        cache_policy(std::move(cache_policy_)), cache_key(cache_key_), cache_path(cache_path_),
        cache_file_size(file_size_),
        part_offset(part_offset_), expected_size(expected_size_)
    {
        String name = "CacheableReadBufferDecorator " + std::to_string(reinterpret_cast<size_t>(this));
        log = &Poco::Logger::get(name);

        LOG_TRACE(log, "Create for {} ({}+{}) of {}", cache_key, part_offset, expected_size, cache_file_size);

        auto dir_path = directoryPath(cache_path);
        if (!cache_disk->exists(dir_path))
            cache_disk->createDirectories(dir_path);
        
        cache = cache_disk->writeFile(cache_path + ".tmp", 1024, DB::WriteMode::Rewrite);
    }

    ~CacheableReadBufferDecorator() override
    {
        try
        {
            finalize();
        }
        catch (...)
        {
            LOG_ERROR(log, "Can't put object in cache. Error: {}", getCurrentExceptionMessage(false));
            clean();
        }
    }

    void set(BufferBase::Position ptr, size_t size, size_t offset) override
    {
        LOG_TRACE(log, "set(ptr, {}, {})", size, offset);
        return from->BufferBase::set(ptr, size, offset);
    }
    BufferBase::Buffer & internalBuffer() override
    {
        LOG_TRACE(log, "internalBuffer()");
        return from->internalBuffer();
    }
    BufferBase::Buffer & buffer() override
    {
        LOG_TRACE(log, "buffer()");
        return from->buffer();
    }
    BufferBase::Position & position() override
    {
        LOG_TRACE(log, "position()");
        return from->position();
    }
    size_t offset() const override
    {
        LOG_TRACE(log, "offset()");
        return from->offset();
    }
    size_t available() const override
    {
        LOG_TRACE(log, "available()");
        return from->available();
    }
    void swap(BufferBase & other) override
    {
        LOG_TRACE(log, "swap(other)");
        from->swap(other);
    }
    size_t count() const override
    {
        LOG_TRACE(log, "count()");
        return from->count();
    }
    bool hasPendingData() const override
    {
        LOG_TRACE(log, "hasPendingData()");
        bool res = from->hasPendingData();
        LOG_TRACE(log, "hasPendingData() -> {}", res);
        return res;
    }
    bool isPadded() const override
    {
        LOG_TRACE(log, "isPadded()");
        return from->isPadded();
    }

    void set(BufferBase::Position ptr, size_t size) override
    {
        LOG_TRACE(log, "internalBuffer()");
        from->set(ptr, size);
    }
    bool next() override
    {
        LOG_TRACE(log, "next()");
        if (!from->next())
        {
            LOG_TRACE(log, "next() -> false");
            return false;
        }
        auto buffer = from->buffer();
        LOG_TRACE(log, "Write to cache {} bytes", buffer.size());
        cache->write(buffer.begin(), buffer.size());
        cache_part_size += buffer.size();
        LOG_TRACE(log, "next() -> true");
        return true;
    }
    void nextIfAtEnd() override
    {
        LOG_TRACE(log, "nextIfAtEnd()");
        from->nextIfAtEnd();
    }
    bool eof() override
    {
        LOG_TRACE(log, "eof()");
        bool res = from->eof();
        LOG_TRACE(log, "eof() -> {}", res);
        return res;
    }
    void ignore(size_t n) override
    {
        LOG_TRACE(log, "ignore({})", n);
        from->ignore(n);
    }
    size_t tryIgnore(size_t n) override
    {
        LOG_TRACE(log, "tryIgnore({})", n);
        return from->tryIgnore(n);
    }
    void ignoreAll() override
    {
        LOG_TRACE(log, "internalBuffer()");
        from->ignoreAll();
    }
    bool peek(char & c) override
    {
        LOG_TRACE(log, "peek(c)");
        return from->peek(c);
    }
    bool read(char & c) override
    {
        LOG_TRACE(log, "read(c)");
        return from->read(c);
    }
    void readStrict(char & c) override
    {
        LOG_TRACE(log, "readStrict(c)");
        return from->readStrict(c);
    }
    size_t read(char * to, size_t n) override
    {
        LOG_TRACE(log, "read(to, {})", n);
        return from->read(to, n);
    }
    void readStrict(char * to, size_t n) override
    {
        LOG_TRACE(log, "readStrict(to, {})", n);
        from->readStrict(to, n);
    }
    void prefetch() override
    {
        LOG_TRACE(log, "prefetch()");
        return from->prefetch();
    }

private:
    void finalize()
    {
        cache->finalize();
        cache.reset(nullptr);
        
        if (cache_part_size > expected_size)
        {
            LOG_ERROR(log, "Unexpected part size, got {}, expected {}", cache_part_size, expected_size);
            throw Exception("Unexpected part size", ErrorCodes::LOCAL_CACHE_ERROR);
        }
        
        cache_disk->moveFile(cache_path + ".tmp", cache_path);
        LOG_TRACE(log, "Add key {}: {}+{} of {}", cache_key, part_offset, cache_part_size, cache_file_size);
        auto to_remove = cache_policy->add(cache_key, cache_file_size, part_offset, cache_part_size);
        for (auto & entry_to_remove : to_remove)
        {
            String cache_path_ = DiskCache::getCacheBasePath(entry_to_remove.first) + "_" + std::to_string(entry_to_remove.second);
            LOG_TRACE(log, "Remove cached file {}", cache_path_);
            cache_disk->removeFileIfExists(cache_path_);
        }
    }

    void clean()
    {
        cache.reset(nullptr);
        cache_policy->error(cache_key, part_offset);
        cache_disk->removeFileIfExists(cache_path + ".tmp");
    }

    std::unique_ptr<ReadBuffer> from;
    std::unique_ptr<WriteBufferFromFileBase> cache;
    std::shared_ptr<DiskLocal> cache_disk;
    std::shared_ptr<DiskCachePolicy> cache_policy;
    String cache_key;
    String cache_path;
    // total size on remote FS
    size_t cache_file_size = 0;
    size_t part_offset = 0;
    size_t expected_size = 0;

    size_t cache_part_size = 0;

    Poco::Logger * log = nullptr;
};


class CacheableMultipartReadBufferDecorator : public ReadBuffer
{
public:
    CacheableMultipartReadBufferDecorator(std::shared_ptr<DiskLocal> cache_disk_, std::shared_ptr<DiskCachePolicy> cache_policy_,
        const String & cache_key_, const String & cache_base_path_,
        size_t offset_, size_t size_,
        std::shared_ptr<DiskCacheDownloader> downloader_) :
        ReadBuffer(nullptr, 0), cache_disk(std::move(cache_disk_)),
        cache_policy(std::move(cache_policy_)), cache_key(cache_key_), cache_base_path(cache_base_path_),
        current_offset(offset_), end_offset(size_ ? offset_ + size_ : size_),
        downloader(std::move(downloader_))
    {
        log = &Poco::Logger::get("CacheableMultipartReadBufferDecorator");
        ensureFrom();
    }

    void set(BufferBase::Position, size_t size, size_t offset) override
    {
        LOG_TRACE(log, "set(ptr, {}, {})", size, offset);
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    BufferBase::Buffer & internalBuffer() override
    {
        LOG_TRACE(log, "internalBuffer()");
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    BufferBase::Buffer & buffer() override
    {
        LOG_TRACE(log, "buffer()");
        return from->buffer();
    }

    BufferBase::Position & position() override
    {
        LOG_TRACE(log, "position()");
        return from->position();
    }

    size_t offset() const override
    {
        LOG_TRACE(log, "offset()");
        return from->offset();
    }

    size_t available() const override
    {
        LOG_TRACE(log, "available()");
        return from->available();
    }

    void swap(BufferBase &) override
    {
        LOG_TRACE(log, "swap(other)");
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t count() const override
    {
        LOG_TRACE(log, "count()");
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool hasPendingData() const override
    {
        LOG_TRACE(log, "hasPendingData()");
        bool res = from->hasPendingData();
        LOG_TRACE(log, "hasPendingData() -> {}", res);
        return res;
    }

    bool isPadded() const override
    {
        LOG_TRACE(log, "isPadded()");
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void set(BufferBase::Position, size_t size) override
    {
        LOG_TRACE(log, "set(pos, {})", size);
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool next() override
    {
        LOG_TRACE(log, "next()");
        bool res = from->next();
        if (!res)
        {
            from.reset(nullptr);
            ensureFrom();
            if (from)
                res = from->next();
        }
        LOG_TRACE(log, "next() -> {}", res);
        return res;
    }

    void nextIfAtEnd() override
    {
        LOG_TRACE(log, "nextIfAtEnd()");
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool eof() override
    {
        LOG_TRACE(log, "eof()");
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void ignore(size_t n) override
    {
        LOG_TRACE(log, "ignore({})", n);
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t tryIgnore(size_t n) override
    {
        LOG_TRACE(log, "tryIgnore({})", n);
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void ignoreAll() override
    {
        LOG_TRACE(log, "internalBuffer()");
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool peek(char & c) override
    {
        LOG_TRACE(log, "peek(c)");
        return from->peek(c);
    }

    bool read(char & c) override
    {
        LOG_TRACE(log, "read(c)");
        return from->read(c);
    }

    void readStrict(char & c) override
    {
        LOG_TRACE(log, "readStrict(c)");
        return from->readStrict(c);
    }

    size_t read(char * to, size_t n) override
    {
        LOG_TRACE(log, "read(to, {})", n);
        return from->read(to, n);
    }

    void readStrict(char * to, size_t n) override
    {
        LOG_TRACE(log, "readStrict(to, {})", n);
        from->readStrict(to, n);
    }

    void prefetch() override
    {
        LOG_TRACE(log, "prefetch()");
        from->prefetch();
    }

private:
    void ensureFrom()
    {
        if (!from)
        {
            LOG_TRACE(log, "ensureFrom(init)");

            auto part = cache_policy->find(cache_key, current_offset, end_offset ? end_offset - current_offset : 0);

            if (part.type == DiskCachePolicy::CachePart::CachePartType::EMPTY)
                return;

            if (part.type == DiskCachePolicy::CachePart::CachePartType::CACHED)
            {
                String cache_path = cache_base_path + "_" + std::to_string(part.offset);
                LOG_TRACE(log, "ensureFrom try to read from cache: {}", cache_path);
                cache_policy->read(cache_key, part.offset, part.size);
                try
                {
                    auto from_seekable = cache_disk->readFile(cache_path, ReadSettings(), 0);
                    if (current_offset > part.offset)
                        from_seekable->seek(current_offset - part.offset, SEEK_SET);
                    from = std::move(from_seekable);
                    current_offset = part.offset + part.size;
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
                    {
                        LOG_WARNING(log, "Cached file {} not found, try to download", cache_path);
                        cache_policy->remove(cache_key, part.offset, part.size);
                        part.type = DiskCachePolicy::CachePart::CachePartType::ABSENT;
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            if (part.type == DiskCachePolicy::CachePart::CachePartType::ABSENT)
            {
                LOG_TRACE(log, "ensureFrom download from s3 {}-{}", part.offset, part.size);
                auto to_remove = cache_policy->reserve(part.size);
                for (auto & entry_to_remove : to_remove)
                {
                    String cache_path = DiskCache::getCacheBasePath(entry_to_remove.first) + "_" + std::to_string(entry_to_remove.second);
                    LOG_TRACE(log, "Remove cached file {}", cache_path);
                    cache_disk->removeFileIfExists(cache_path);
                }
                auto s3from = downloader->get(part.offset, part.size);
                String cache_path = cache_base_path + "_" + std::to_string(part.offset);
                from = std::unique_ptr<ReadBuffer>{ new CacheableReadBufferDecorator(s3from.stream,
                    cache_disk, cache_policy, cache_key, cache_path,
                    s3from.file_size,
                    part.offset, s3from.expected_size) };
                current_offset = part.offset + s3from.expected_size;
            }

            if (part.type == DiskCachePolicy::CachePart::CachePartType::ABSENT_NO_CACHE)
            {
                LOG_TRACE(log, "ensureFrom download from s3 {}-{} without caching", part.offset, part.size);
                auto s3from = downloader->get(part.offset, part.size);
                from = std::move(s3from.stream);
                current_offset = part.offset + s3from.expected_size;
            }
        }
    }

    std::shared_ptr<DiskLocal> cache_disk;
    std::shared_ptr<DiskCachePolicy> cache_policy;
    String cache_key;
    String cache_base_path;
    size_t current_offset = 0;
    size_t end_offset = 0;
    std::shared_ptr<DiskCacheDownloader> downloader;

    std::unique_ptr<ReadBuffer> from;
    std::unique_ptr<WriteBuffer> cache;

    Poco::Logger * log = nullptr;
};


std::unique_ptr<ReadBuffer> DiskCache::find(const String & path, size_t offset, size_t size, std::shared_ptr<DiskCacheDownloader> downloader)
{
    // TODO: find with other offset
    // TODO: mutithreading
    String cache_key = getKey(path);

    String cache_path = getCacheBasePath(cache_key);

    return std::unique_ptr<ReadBuffer>{ new CacheableMultipartReadBufferDecorator(cache_disk, cache_policy, 
        cache_key, cache_path, offset, size, downloader) };
}

void DiskCache::remove(const String & path)
{
    // TODO: mutithreading
    String cache_key = getKey(path);
    auto to_remove = cache_policy->remove(cache_key);

    String cache_base_path = getCacheBasePath(cache_key);
    for (auto & entry_to_remove : to_remove)
    {
        String cache_path = cache_base_path + "_" + std::to_string(entry_to_remove.offset);
        LOG_TRACE(&Poco::Logger::get("DiskCache"), "Remove cached file {}", cache_path);
        cache_disk->removeFileIfExists(cache_path);
    }
}

String DiskCache::getKey(const String & path)
{
    union
    {
        char bytes[16];
        UInt128 i128;
    } hash;
    sipHash128(path.data(), path.size(), hash.bytes);
    String key = getHexUIntLowercase(hash.i128);
    return key;
}

String DiskCache::getCacheBasePath(const String & key)
{
    // TODO: use std::path 
    String path = key.substr(0, 3);
    path += "/";
    path += key;
    //path += "_" + std::to_string(offset);
    return path;
}

}
