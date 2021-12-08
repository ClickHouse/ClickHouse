#include "DiskCache.h"
#include <Common/Exception.h>
#include <Common/hex.h>
#include <Common/SipHash.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <base/getThreadId.h>

namespace ProfileEvents
{
    extern const Event DiskCacheRequestsIn;
    extern const Event DiskCacheRequestsOut;
    extern const Event DiskCacheBytesTotal;
    extern const Event DiskCacheBytesCached;
    extern const Event DiskCacheBytesRemote;
    extern const Event DiskCacheBytesRemoteSkipCache;
    extern const Event S3ReadBytes;
}

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
        throw Exception("File downloading error with size more than reserved", ErrorCodes::LOGICAL_ERROR);
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

std::list<std::pair<String, size_t>> DiskCacheLRUPolicy::add(const String & key, size_t file_size,
    size_t offset, size_t size, bool restore)
{
    std::unique_lock<std::mutex> lock(cache_mutex);

    std::list<std::pair<String, size_t>> res = reserve_unsafe(size, true);

    if (size > cache_size_limit)
    { // Should never be here if code correct
        if (!restore)
        {
            complete(key, offset, FileDownloadStatus::ERROR);
            throw Exception("Should not try to insert more than possibly cache size", ErrorCodes::LOGICAL_ERROR);
        }

        return res;
    }

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
            if (restore)
            { /// incorrect cache on disk
                res.push_back(std::make_pair(key, offset));
                return res;
            }
            else
            {
                complete(key, offset, FileDownloadStatus::ERROR);
                throw Exception("Could not insert already existing data, remove it first", ErrorCodes::LOGICAL_ERROR);
            }
        }

        auto entry_after = parts.upper_bound(offset + size);
        if (entry_after != parts.end() && offset + size > entry_after->second->offset)
        {
            LOG_ERROR(log, "Key {} ({}+{}) collision with ({}+{})",
                key, offset, size, entry_after->second->offset, entry_after->second->size);
            if (restore)
            { /// incorrect cache on disk
                res.push_back(std::make_pair(key, offset));
                return res;
            }
            else
            {
                complete(key, offset, FileDownloadStatus::ERROR);
                throw Exception("Could not insert already existing data, remove it first", ErrorCodes::LOGICAL_ERROR);
            }
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

    if (!restore)
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

std::list<std::pair<String, size_t>> DiskCacheLRUPolicy::reserve_unsafe(size_t size, bool free_only)
{
    std::list<std::pair<String, size_t>> keys_to_remove;

    if (size + reserved_size > cache_size_limit)
    {
        LOG_INFO(log, "Try to reserve {} + {}, limit {}", size, reserved_size, cache_size_limit);
        return keys_to_remove;
    }

    auto last = cache_list.end();
    LOG_TRACE(log, "Cache size before cleanup: {}", cache_size);
    while (cache_size > cache_size_limit - size || cache_map.size() >= nodes_limit)
    {
        if (last == cache_list.begin())
        { // Should never be here if code correct
            throw Exception("Non-zero cache size with empty cache list", ErrorCodes::LOGICAL_ERROR);
        }

        --last;

        if (last->size > cache_size)
        { // Should never be here if code correct
            throw Exception("Cache size less than one cache element", ErrorCodes::LOGICAL_ERROR);
        }

        keys_to_remove.push_back(std::make_pair(last->key, last->offset));
        cache_map[last->key].parts.erase(last->offset);
        if (cache_map[last->key].parts.empty())
            cache_map.erase(last->key);
        cache_size -= last->size;
    }
    LOG_TRACE(log, "Cache size after cleanup: {}", cache_size);

    cache_list.erase(last, cache_list.end());

    if (!free_only)
        reserved_size += size;

    LOG_TRACE(log, "New reserved {}, total reserved {}", size, reserved_size);

    return keys_to_remove;
}

DiskCachePolicy::CachePart DiskCacheLRUPolicy::find(const String & key, size_t offset, size_t size)
{
    CachePart res(offset, size);
    res.type = DiskCachePolicy::CachePart::CachePartType::ABSENT;

    uint64_t thread_id = getThreadId();

    static const int retries_max = 3; /// TODO - settings? tuning?

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
                    if (!res.size || (size && res.size > size))
                        res.size = size;
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
                        if (cq->first > res.offset)
                        {
                            res.size = cq->first - res.offset;
                            LOG_TRACE(log, "Download less size for key {} ({}+{}) to avoid downloading collision", key, res.offset, res.size);
                            break;
                        }

                        if (read_thread_ids.count(thread_id))
                        { /// Workaround for avoiding deadlocks when current thread already downloading some object
                            /// TODO: this case needs optimization
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
    reload();
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
        log = &Poco::Logger::get("CacheableReadBufferDecorator");

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
        from->BufferBase::set(ptr, size, offset);
    }

    BufferBase::Buffer & internalBuffer() override
    {
        return from->internalBuffer();
    }

    BufferBase::Buffer & buffer() override
    {
        return from->buffer();
    }
    BufferBase::Position & position() override
    {
        return from->position();
    }

    size_t offset() const override
    {
        return from->offset();
    }

    size_t available() const override
    {
        return from->available();
    }

    void swap(BufferBase & other) override
    {
        from->swap(other);
    }

    size_t count() const override
    {
        return from->count();
    }

    bool hasPendingData() const override
    {
        return from->hasPendingData();
    }

    bool isPadded() const override
    {
        return from->isPadded();
    }

    void set(BufferBase::Position ptr, size_t size) override
    {
        from->set(ptr, size);
    }

    void setReadUntilPosition(size_t position) override
    {
        from->setReadUntilPosition(position);
    }

    void setReadUntilEnd() override
    {
        from->setReadUntilEnd();
    }

    bool next() override
    {
        if (!from->next())
        {
            return false;
        }
        auto buffer = from->buffer();
        LOG_TRACE(log, "Write to cache {} bytes", buffer.size());
        cache->write(buffer.begin(), buffer.size());
        cache_part_size += buffer.size();
        return true;
    }

    void nextIfAtEnd() override
    {
        from->nextIfAtEnd();
    }

    bool eof() override
    {
        return from->eof();
    }

    void ignore(size_t n) override
    {
        from->ignore(n);
    }

    size_t tryIgnore(size_t n) override
    {
        return from->tryIgnore(n);
    }

    void ignoreAll() override
    {
        from->ignoreAll();
    }

    bool peek(char & c) override
    {
        return from->peek(c);
    }

    bool read(char & c) override
    {
        return from->read(c);
    }

    void readStrict(char & c) override
    {
        from->readStrict(c);
    }

    size_t read(char * to, size_t n) override
    {
        return from->read(to, n);
    }

    void readStrict(char * to, size_t n) override
    {
        from->readStrict(to, n);
    }

    void prefetch() override
    {
        from->prefetch();
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
        auto to_remove = cache_policy->add(cache_key, cache_file_size, part_offset, cache_part_size, false);
        for (auto & entry_to_remove : to_remove)
        {
            String remove_cache_path = DiskCache::getCacheBasePath(entry_to_remove.first) + "_" + std::to_string(entry_to_remove.second);
            LOG_TRACE(log, "Remove cached file {}", remove_cache_path);
            cache_disk->removeFileIfExists(remove_cache_path);
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
        ProfileEvents::increment(ProfileEvents::DiskCacheRequestsIn, 1);
        log = &Poco::Logger::get("CacheableMultipartReadBufferDecorator");
    }

    void set(BufferBase::Position ptr, size_t size, size_t offset) override
    {
        external_buffer_ptr = ptr;
        external_buffer_size = size;
        external_buffer_offset = offset;

        if (from)
            from->set(ptr, size, offset);
    }

    BufferBase::Buffer & internalBuffer() override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    BufferBase::Buffer & buffer() override
    {
        ensureFrom();
        return from->buffer();
    }

    BufferBase::Position & position() override
    {
        ensureFrom();
        return from->position();
    }

    size_t offset() const override
    {
        if (!from)
            throw Exception("Call method before buffer creation", ErrorCodes::LOGICAL_ERROR);
        return from->offset();
    }

    size_t available() const override
    {
        if (!from)
            throw Exception("Call method before buffer creation", ErrorCodes::LOGICAL_ERROR);
        return from->available();
    }

    void swap(BufferBase &) override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t count() const override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool hasPendingData() const override
    {
        if (!from)
            throw Exception("Call method before buffer creation", ErrorCodes::LOGICAL_ERROR);
        return from->hasPendingData();
    }

    bool isPadded() const override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void set(BufferBase::Position ptr, size_t size) override
    {
        external_buffer_ptr = ptr;
        external_buffer_size = size;
        external_buffer_offset = 0;

        if (from)
            from->set(ptr, size);
    }

    void setReadUntilPosition(size_t position) override
    {
        if (from)
            throw Exception("Can't set end position after start usage", ErrorCodes::LOGICAL_ERROR);
        end_offset = position;
    }

    void setReadUntilEnd() override
    {
        if (from)
            throw Exception("Can't set end position after start usage", ErrorCodes::LOGICAL_ERROR);
        end_offset = 0;
    }

    bool next() override
    {
        bool res = false;
        if (from)
            res = from->next();
        if (!res)
        {
            from.reset(nullptr);
            ensureFrom();
            if (from)
                res = from->next();
        }

        if (res)
        {
            auto buffer = from->buffer();
            size_t size = buffer.size();
            ProfileEvents::increment(ProfileEvents::DiskCacheBytesTotal, size);

            switch (current_type)
            {
                case DiskCachePolicy::CachePart::CachePartType::CACHED:
                    ProfileEvents::increment(ProfileEvents::DiskCacheBytesCached, size);
                    break;
                case DiskCachePolicy::CachePart::CachePartType::ABSENT:
                    ProfileEvents::increment(ProfileEvents::DiskCacheBytesRemote, size);
                    ProfileEvents::increment(ProfileEvents::S3ReadBytes, size);
                    break;
                case DiskCachePolicy::CachePart::CachePartType::ABSENT_NO_CACHE:
                    ProfileEvents::increment(ProfileEvents::DiskCacheBytesRemote, size);
                    ProfileEvents::increment(ProfileEvents::DiskCacheBytesRemoteSkipCache, size);
                    ProfileEvents::increment(ProfileEvents::S3ReadBytes, size);
                    break;
                default:
                    break;
            }
        }

        return res;
    }

    void nextIfAtEnd() override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool eof() override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void ignore(size_t) override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t tryIgnore(size_t) override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void ignoreAll() override
    {
        throw Exception("Method not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool peek(char & c) override
    {
        ensureFrom();
        return from->peek(c);
    }

    bool read(char & c) override
    {
        ensureFrom();
        return from->read(c);
    }

    void readStrict(char & c) override
    {
        ensureFrom();
        from->readStrict(c);
    }

    size_t read(char * to, size_t n) override
    {
        ensureFrom();
        return from->read(to, n);
    }

    void readStrict(char * to, size_t n) override
    {
        ensureFrom();
        from->readStrict(to, n);
    }

    void prefetch() override
    {
        ensureFrom();
        from->prefetch();
    }

private:
    void ensureFrom()
    {
        int retries = 3;

        while (!from)
        {
            size_t max_size = end_offset ? end_offset - current_offset : 0;
            if (external_buffer_ptr && (!max_size || max_size > external_buffer_size))
                max_size = external_buffer_size;
            DiskCachePolicy::CachePart part(current_offset, max_size);
            if (retries > 0)
                part = cache_policy->find(cache_key, current_offset, max_size);
            else
            {   // Max retry attempts reached
                // Came here when part present in index but absent on disk, several times
                LOG_WARNING(log, "ensureFrom attempts max retries for key {}", cache_key);
                part.type = DiskCachePolicy::CachePart::CachePartType::ABSENT_NO_CACHE;
            }

            if (part.type == DiskCachePolicy::CachePart::CachePartType::EMPTY)
            {
                from = std::make_unique<ReadBufferFromEmptyFile>();
                return;
            }

            if (part.type == DiskCachePolicy::CachePart::CachePartType::CACHED)
            {
                String cache_path = cache_base_path + "_" + std::to_string(part.offset);
                LOG_TRACE(log, "ensureFrom try to read key {} from cache {}+{}", cache_path, part.offset, part.size);
                cache_policy->read(cache_key, part.offset, part.size);
                try
                {
                    size_t required_size = part.size;
                    if (max_size && (part.offset + part.size > current_offset + max_size))
                        required_size = current_offset + max_size - part.offset;
                    auto from_seekable = cache_disk->readFile(cache_path, ReadSettings(), required_size);
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
                        --retries;
                        continue; // retry
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            if (part.type == DiskCachePolicy::CachePart::CachePartType::ABSENT)
            {
                LOG_TRACE(log, "ensureFrom download key {} from s3 {}+{}", cache_key, part.offset, part.size);
                auto to_remove = cache_policy->reserve(part.size);
                for (auto & entry_to_remove : to_remove)
                {
                    String remove_cache_path = DiskCache::getCacheBasePath(entry_to_remove.first) + "_" + std::to_string(entry_to_remove.second);
                    LOG_TRACE(log, "Remove cached file {}", remove_cache_path);
                    cache_disk->removeFileIfExists(remove_cache_path);
                }

                ProfileEvents::increment(ProfileEvents::DiskCacheRequestsOut, 1);
                auto s3from = downloader->get(part.offset, part.size);
                if (s3from.stream)
                {
                    String cache_path = cache_base_path + "_" + std::to_string(part.offset);
                    from = std::unique_ptr<ReadBuffer>{ new CacheableReadBufferDecorator(s3from.stream,
                        cache_disk, cache_policy, cache_key, cache_path,
                        s3from.file_size,
                        part.offset, s3from.expected_size) };
                    current_offset = part.offset + s3from.expected_size;
                }
                else
                {
                    throw Exception("Can't download remote file", ErrorCodes::LOCAL_CACHE_ERROR);
                }
            }

            if (part.type == DiskCachePolicy::CachePart::CachePartType::ABSENT_NO_CACHE)
            {
                LOG_TRACE(log, "ensureFrom download key {} from s3 {}+{} without caching", cache_key, part.offset, part.size);
                ProfileEvents::increment(ProfileEvents::DiskCacheRequestsOut, 1);
                auto s3from = downloader->get(part.offset, part.size);
                if (s3from.stream)
                {
                    from = std::move(s3from.stream);
                    current_offset = part.offset + s3from.expected_size;
                }
                else
                {
                    throw Exception("Can't download remote file", ErrorCodes::LOCAL_CACHE_ERROR);
                }
            }

            if (external_buffer_ptr)
            {
                LOG_TRACE(log, "Set size {}, offset {}", external_buffer_size, external_buffer_offset);
                from->set(external_buffer_ptr, external_buffer_size, external_buffer_offset);
                LOG_TRACE(log, "Change offset from {} to {}", external_buffer_offset, external_buffer_offset + std::min(part.size, external_buffer_size));
                external_buffer_offset += std::min(part.size, external_buffer_size);
            }

            current_type = part.type;
            break;
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

    DiskCachePolicy::CachePart::CachePartType current_type = DiskCachePolicy::CachePart::CachePartType::EMPTY;

    BufferBase::Position external_buffer_ptr = nullptr;
    size_t external_buffer_size = 0;
    size_t external_buffer_offset = 0;
};


std::unique_ptr<ReadBuffer> DiskCache::find(const String & path, size_t offset, size_t size, std::shared_ptr<DiskCacheDownloader> downloader)
{
    String cache_key = getKey(path);

    String cache_path = getCacheBasePath(cache_key);

    return std::unique_ptr<ReadBuffer>{ new CacheableMultipartReadBufferDecorator(cache_disk, cache_policy,
        cache_key, cache_path, offset, size, downloader) };
}

void DiskCache::remove(const String & path)
{
    String cache_key = getKey(path);
    auto to_remove = cache_policy->remove(cache_key);

    String cache_base_path = getCacheBasePath(cache_key);
    for (auto & entry_to_remove : to_remove)
    {
        String remove_cache_path = cache_base_path + "_" + std::to_string(entry_to_remove.offset);
        LOG_TRACE(&Poco::Logger::get("DiskCache"), "Remove cached file {}", remove_cache_path);
        cache_disk->removeFileIfExists(remove_cache_path);
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
    return fs::path(key.substr(0, 3)) / key;
}

void DiskCache::reload()
{
    String root = cache_disk->getPath();

    if (!cache_disk->exists(root))
        return;

    auto & log = Poco::Logger::get("DiskCache");

    LOG_TRACE(&log, "Reload DiskCache from {}", root);

    for (auto it = cache_disk->iterateDirectory(root); it->isValid(); it->next())
    {
        if (cache_disk->isDirectory(it->path()))
        {
            std::unordered_set<String> files_to_remove;

            LOG_TRACE(&log, "Reload DiskCache folder {}", it->path());
            for (auto itf = cache_disk->iterateDirectory(it->path()); itf->isValid(); itf->next())
            {
                if (!cache_disk->isFile(itf->path()))
                    continue;

                String name = itf->name();
                if (files_to_remove.count(itf->path()))
                    continue;

                /// name should be 'KEY(lower case hex, 32 symbols)_OFFSET(dec, 1+ symbols)'

                auto delimiter = name.find('_');
                if (delimiter != 32)
                {
                    LOG_WARNING(&log, "Wrong file name in disk cache: {}", name);
                    continue;
                }

                String key = name.substr(0, delimiter);
                String off = name.substr(delimiter + 1);

                if (key.find_first_not_of("0123456789abcdef") != std::string::npos
                        || off.empty() || off.find_first_not_of("0123456789") != std::string::npos)
                {
                    LOG_WARNING(&log, "Wrong file name in disk cache: {}", name);
                    continue;
                }

                LOG_TRACE(&log, "Reload DiskCache file {}", itf->path());

                size_t offset = std::stoull(off);
                size_t size = cache_disk->getFileSize(itf->path());

                size_t cache_file_size = 0;

                auto to_remove = cache_policy->add(key, cache_file_size, offset, size, true);
                for (auto & entry_to_remove : to_remove)
                {
                    String remove_cache_path = DiskCache::getCacheBasePath(entry_to_remove.first) + "_" + std::to_string(entry_to_remove.second);
                    files_to_remove.insert(remove_cache_path);
                }
            }

            for (const auto & file_to_remove : files_to_remove)
            {
                LOG_TRACE(&log, "Remove cached file {}", file_to_remove);
                cache_disk->removeFileIfExists(file_to_remove);
            }
        }
    }
}

}
