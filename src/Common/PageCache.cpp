#include "PageCache.h"

#include <unistd.h>
#include <sys/mman.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <base/hex.h>
#include <base/errnoToString.h>
#include <base/getPageSize.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

namespace ProfileEvents
{
    extern const Event PageCacheChunkMisses;
    extern const Event PageCacheChunkShared;
    extern const Event PageCacheChunkDataHits;
    extern const Event PageCacheChunkDataPartialHits;
    extern const Event PageCacheChunkDataMisses;
    extern const Event PageCacheBytesUnpinnedRoundedToPages;
    extern const Event PageCacheBytesUnpinnedRoundedToHugePages;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int INVALID_SETTING_VALUE;
    extern const int FILE_DOESNT_EXIST;
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunknown-warning-option"
#pragma clang diagnostic ignored "-Wreadability-make-member-function-const"

PinnedPageChunk::PinnedPageChunk(PinnedPageChunk && c) noexcept
    : cache(std::exchange(c.cache, nullptr)), chunk(std::exchange(c.chunk, nullptr)) {}

PinnedPageChunk & PinnedPageChunk::operator=(PinnedPageChunk && c) noexcept
{
    if (cache)
        cache->removeRef(chunk);
    cache = std::exchange(c.cache, nullptr);
    chunk = std::exchange(c.chunk, nullptr);
    return *this;
}

PinnedPageChunk::~PinnedPageChunk() noexcept
{
    if (cache)
        cache->removeRef(chunk);
}

PinnedPageChunk::PinnedPageChunk(PageCache * cache_, PageChunk * chunk_) noexcept : cache(cache_), chunk(chunk_) {}

const PageChunk * PinnedPageChunk::getChunk() const { return chunk; }

bool PinnedPageChunk::markPagePopulated(size_t page_idx)
{
    bool r = chunk->pages_populated.set(page_idx);
    return r;
}

void PinnedPageChunk::markPrefixPopulated(size_t bytes)
{
    for (size_t i = 0; i < (bytes + chunk->page_size - 1) / chunk->page_size; ++i)
        markPagePopulated(i);
}

bool PinnedPageChunk::isPrefixPopulated(size_t bytes) const
{
    for (size_t i = 0; i < (bytes + chunk->page_size - 1) / chunk->page_size; ++i)
        if (!chunk->pages_populated.get(i))
            return false;
    return true;
}

AtomicBitSet::AtomicBitSet() = default;

void AtomicBitSet::init(size_t nn)
{
    n = nn;
    v = std::make_unique<std::atomic<UInt8>[]>((n + 7) / 8);
}

bool AtomicBitSet::get(size_t i) const
{
    return (v[i / 8] & (1 << (i % 8))) != 0;
}

bool AtomicBitSet::any() const
{
    for (size_t i = 0; i < (n + 7) / 8; ++i)
        if (v[i])
            return true;
    return false;
}

bool AtomicBitSet::set(size_t i) const
{
    UInt8 prev = v[i / 8].fetch_or(1 << (i % 8));
    return (prev & (1 << (i % 8))) == 0;
}

bool AtomicBitSet::set(size_t i, bool val) const
{
    if (val)
        return set(i);
    return unset(i);
}

bool AtomicBitSet::unset(size_t i) const
{
    UInt8 prev = v[i / 8].fetch_and(~(1 << (i % 8)));
    return (prev & (1 << (i % 8))) != 0;
}

void AtomicBitSet::unsetAll() const
{
    for (size_t i = 0; i < (n + 7) / 8; ++i)
        v[i].store(0, std::memory_order_relaxed);
}

PageCache::PageCache(size_t bytes_per_chunk, size_t bytes_per_mmap, size_t bytes_total, bool use_madv_free_, bool use_huge_pages_)
    : bytes_per_page(getPageSize())
    , use_madv_free(use_madv_free_)
    , use_huge_pages(use_huge_pages_)
    , rng(randomSeed())
{
    if (bytes_per_chunk == 0 || bytes_per_mmap == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Userspace page cache chunk size and mmap size can't be zero.");

    if (use_huge_pages)
    {
        use_huge_pages = false;
        bool print_warning = false;
#ifdef OS_LINUX
        try
        {
            ReadBufferFromFile in("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size");
            size_t huge_page_size;
            readIntText(huge_page_size, in);

            if (huge_page_size == 0 || huge_page_size % bytes_per_page != 0)
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Invalid huge page size reported by the OS: {}", huge_page_size);

            /// THP can be configured to be 2 MiB or 1 GiB in size. 1 GiB is way too big for us.
            if (huge_page_size <= (16 << 20))
            {
                pages_per_big_page = huge_page_size / bytes_per_page;
                use_huge_pages = true;
            }
            else
            {
                LOG_WARNING(&Poco::Logger::get("PageCache"), "The OS huge page size is too large for our purposes: {} KiB. Using regular pages. Userspace page cache will be relatively slow.", huge_page_size);
            }
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
                throw;
            print_warning = true;
        }
#else
        print_warning = true;
#endif
        if (print_warning)
            LOG_WARNING(&Poco::Logger::get("PageCache"), "The OS doesn't support transparent huge pages. Userspace page cache will be relatively slow.");
    }

    pages_per_chunk = ((bytes_per_chunk - 1) / (bytes_per_page * pages_per_big_page) + 1) * pages_per_big_page;
    chunks_per_mmap_target = (bytes_per_mmap - 1) / (bytes_per_page * pages_per_chunk) + 1;
    max_mmaps = (bytes_total - 1) / (bytes_per_page * pages_per_chunk * chunks_per_mmap_target) + 1;
}

PageCache::~PageCache()
{
    chassert(getPinnedSize() == 0);
}

size_t PageCache::pageSize() const { return bytes_per_page; }
size_t PageCache::chunkSize() const { return bytes_per_page * pages_per_chunk; }
size_t PageCache::maxChunks() const { return chunks_per_mmap_target * max_mmaps; }

size_t PageCache::getPinnedSize() const
{
    std::lock_guard lock(global_mutex);
    return (total_chunks - lru.size()) * bytes_per_page * pages_per_chunk;
}

PageCache::MemoryStats PageCache::getResidentSetSize() const
{
    MemoryStats stats;

#ifdef OS_LINUX
    if (use_madv_free)
    {
        std::unordered_set<UInt64> cache_mmap_addrs;
        {
            std::lock_guard lock(global_mutex);

            /// Don't spend time on reading smaps if page cache is not used.
            if (mmaps.empty())
                return stats;

            for (const auto & m : mmaps)
                cache_mmap_addrs.insert(reinterpret_cast<UInt64>(m.ptr));
        }

        ReadBufferFromFile in("/proc/self/smaps");

        /// Parse the smaps contents, which is text consisting of entries like this:
        ///
        /// 117ba4a00000-117be4a00000 rw-p 00000000 00:00 0
        /// Size:            1048576 kB
        /// KernelPageSize:        4 kB
        /// MMUPageSize:           4 kB
        /// Rss:              539516 kB
        /// Pss:              539516 kB
        /// ...

        auto read_token = [&]
        {
            String res;
            while (!in.eof())
            {
                char c = *in.position();
                if (c == '\n' || c == '\t' || c == ' ' || c == '-')
                    break;
                res += c;
                ++in.position();
            }
            return res;
        };

        auto skip_whitespace = [&]
        {
            while (!in.eof())
            {
                char c = *in.position();
                if (c != ' ' && c != '\t')
                    break;
                ++in.position();
            }
        };

        bool current_range_is_cache = false;
        size_t total_rss = 0;
        size_t total_lazy_free = 0;
        while (!in.eof())
        {
            String s = read_token();
            if (!in.eof() && *in.position() == '-')
            {
                if (s.size() < 16)
                    s.insert(0, 16 - s.size(), '0');
                UInt64 addr = unhexUInt<UInt64>(s.c_str());
                current_range_is_cache = cache_mmap_addrs.contains(addr);
            }
            else if (s == "Rss:" || s == "LazyFree:")
            {
                skip_whitespace();
                size_t val;
                readIntText(val, in);
                skip_whitespace();
                String unit = read_token();
                if (unit != "kB")
                    throw Exception(ErrorCodes::SYSTEM_ERROR, "Unexpected units in /proc/self/smaps: {}", unit);
                size_t bytes = val * 1024;

                if (s == "Rss:")
                {
                    total_rss += bytes;
                    if (current_range_is_cache)
                        stats.page_cache_rss += bytes;
                }
                else
                    total_lazy_free += bytes;
            }
            skipToNextLineOrEOF(in);
        }
        stats.unreclaimable_rss = total_rss - std::min(total_lazy_free, total_rss);

        return stats;
    }
#endif

    std::lock_guard lock(global_mutex);
    stats.page_cache_rss = bytes_per_page * pages_per_chunk * total_chunks;
    return stats;
}

PinnedPageChunk PageCache::getOrSet(PageCacheKey key, bool detached_if_missing, bool inject_eviction)
{
    PageChunk * chunk;
    /// Make sure we increment exactly one of the counters about the fate of a chunk lookup.
    bool incremented_profile_events = false;

    {
        std::lock_guard lock(global_mutex);

        auto * it = chunk_by_key.find(key);
        if (it == chunk_by_key.end())
        {
            chunk = getFreeChunk();
            chassert(!chunk->key.has_value());

            if (!detached_if_missing)
            {
                chunk->key = key;
                chunk_by_key.insert({key, chunk});
            }

            ProfileEvents::increment(ProfileEvents::PageCacheChunkMisses);
            incremented_profile_events = true;
        }
        else
        {
            chunk = it->getMapped();
            size_t prev_pin_count = chunk->pin_count.fetch_add(1);

            if (prev_pin_count == 0)
            {
                /// Not eligible for LRU eviction while pinned.
                chassert(chunk->is_linked());
                lru.erase(lru.iterator_to(*chunk));

                if (detached_if_missing)
                {
                    /// Peek the first page to see if it's evicted.
                    /// (Why not use the full probing procedure instead, restoreChunkFromLimbo()?
                    ///  Right here we can't do it because of how the two mutexes are organized.
                    ///  And we want to do the check+detach before unlocking global_mutex, because
                    ///  otherwise we may detach a chunk pinned by someone else, which may be unexpected
                    ///  for that someone else. Or maybe the latter is fine, dropCache() already does it.)
                    if (chunk->pages_populated.get(0) && reinterpret_cast<volatile std::atomic<char>*>(chunk->data)->load(std::memory_order_relaxed) == 0)
                        evictChunk(chunk);
                }

                if (inject_eviction && chunk->key.has_value() && rng() % 10 == 0)
                {
                    /// Simulate eviction of the chunk or some of its pages.
                    if (rng() % 2 == 0)
                        evictChunk(chunk);
                    else
                        for (size_t i = 0; i < 20; ++i)
                            chunk->pages_populated.unset(rng() % (chunk->size / chunk->page_size));
                }
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::PageCacheChunkShared);
                incremented_profile_events = true;
            }
        }
    }

    {
        std::lock_guard chunk_lock(chunk->chunk_mutex);

        if (chunk->pages_state == PageChunkState::Limbo)
        {
            auto [pages_restored, pages_evicted] = restoreChunkFromLimbo(chunk, chunk_lock);
            chunk->pages_state = PageChunkState::Stable;

            if (!incremented_profile_events)
            {
                if (pages_evicted == 0)
                    ProfileEvents::increment(ProfileEvents::PageCacheChunkDataHits);
                else if (pages_evicted < pages_restored)
                    ProfileEvents::increment(ProfileEvents::PageCacheChunkDataPartialHits);
                else
                    ProfileEvents::increment(ProfileEvents::PageCacheChunkDataMisses);
            }
        }
    }

    return PinnedPageChunk(this, chunk);
}

void PageCache::removeRef(PageChunk * chunk) noexcept
{
    /// Fast path if this is not the last reference.
    size_t prev_pin_count = chunk->pin_count.load();
    if (prev_pin_count > 1 && chunk->pin_count.compare_exchange_strong(prev_pin_count, prev_pin_count - 1))
        return;

    {
        std::lock_guard lock(global_mutex);

        prev_pin_count = chunk->pin_count.fetch_sub(1);
        if (prev_pin_count > 1)
            return;

        chassert(!chunk->is_linked());
        if (chunk->key.has_value())
            lru.push_back(*chunk);
        else
            /// Unpinning detached chunk. We'd rather reuse it soon, so put it at the front.
            lru.push_front(*chunk);
    }

    {
        std::lock_guard chunk_lock(chunk->chunk_mutex);

        /// Need to be extra careful here because we unlocked global_mutex above, so other
        /// getOrSet()/removeRef() calls could have happened during this brief period.
        if (use_madv_free && chunk->pages_state == PageChunkState::Stable && chunk->pin_count.load() == 0)
        {
            sendChunkToLimbo(chunk, chunk_lock);
            chunk->pages_state = PageChunkState::Limbo;
        }
    }
}

static void logUnexpectedSyscallError(std::string name)
{
    std::string message = fmt::format("{} failed: {}", name, errnoToString());
    LOG_WARNING(&Poco::Logger::get("PageCache"), "{}", message);
#if defined(DEBUG_OR_SANITIZER_BUILD)
    volatile bool true_ = true;
    if (true_) // suppress warning about missing [[noreturn]]
        abortOnFailedAssertion(message);
#endif
}

void PageCache::sendChunkToLimbo(PageChunk * chunk [[maybe_unused]], std::lock_guard<std::mutex> & /* chunk_mutex */) const noexcept
{
#ifdef MADV_FREE // if we're not on a very old version of Linux
    chassert(chunk->size == bytes_per_page * pages_per_chunk);
    size_t populated_pages = 0;
    size_t populated_big_pages = 0;
    for (size_t big_page_idx = 0; big_page_idx < pages_per_chunk / pages_per_big_page; ++big_page_idx)
    {
        bool big_page_populated = false;
        for (size_t sub_idx = 0; sub_idx < pages_per_big_page; ++sub_idx)
        {
            size_t idx = big_page_idx * pages_per_big_page + sub_idx;
            if (!chunk->pages_populated.get(idx))
                continue;
            big_page_populated = true;
            populated_pages += 1;

            auto & byte = reinterpret_cast<volatile std::atomic<char> &>(chunk->data[idx * bytes_per_page]);
            chunk->first_bit_of_each_page.set(idx, (byte.load(std::memory_order_relaxed) & 1) != 0);
            byte.fetch_or(1, std::memory_order_relaxed);
        }
        if (big_page_populated)
            populated_big_pages += 1;
    }
    int r = madvise(chunk->data, chunk->size, MADV_FREE);
    if (r != 0)
        logUnexpectedSyscallError("madvise(MADV_FREE)");

    ProfileEvents::increment(ProfileEvents::PageCacheBytesUnpinnedRoundedToPages, bytes_per_page * populated_pages);
    ProfileEvents::increment(ProfileEvents::PageCacheBytesUnpinnedRoundedToHugePages, bytes_per_page * pages_per_big_page * populated_big_pages);
#endif
}

std::pair<size_t, size_t> PageCache::restoreChunkFromLimbo(PageChunk * chunk, std::lock_guard<std::mutex> & /* chunk_mutex */) const noexcept
{
    static_assert(sizeof(std::atomic<char>) == 1, "char is not atomic?");
    // Make sure our strategic memory reads/writes are not reordered or optimized out.
    auto * data = reinterpret_cast<volatile std::atomic<char> *>(chunk->data);
    size_t pages_restored = 0;
    size_t pages_evicted = 0;
    for (size_t idx = 0; idx < chunk->size / bytes_per_page; ++idx)
    {
        if (!chunk->pages_populated.get(idx))
            continue;

        /// After MADV_FREE, it's guaranteed that:
        ///  * writing to the page makes it non-freeable again (reading doesn't),
        ///  * after the write, the page contents are either fully intact or fully zero-filled,
        ///  * even before the write, reads return either intact data (if the page wasn't freed) or zeroes (if it was, and the read page-faulted).
        /// (And when doing the write there's no way to tell whether it page-faulted or not, AFAICT; that would make our life much easier!)
        ///
        /// With that in mind, we do the following dance to bring the page back from the MADV_FREE limbo:
        ///  0. [in advance] Before doing MADV_FREE, make sure the page's first byte is not zero.
        ///     We do it by setting the lowest bit of the first byte to 1, after saving the original value of that bit into a bitset.
        ///  1. Read the second byte.
        ///  2. Write the second byte back. This makes the page non-freeable.
        ///  3. Read the first byte.
        ///    3a. If it's zero, the page was freed.
        ///        Set the second byte to 0, to keep the buffer zero-filled if the page was freed
        ///        between steps 1 and 2.
        ///    3b. If it's nonzero, the page is intact.
        ///        Restore the lowest bit of the first byte to the saved original value from the bitset.

        char second_byte = data[idx * bytes_per_page + 1].load(std::memory_order_relaxed);
        data[idx * bytes_per_page + 1].store(second_byte, std::memory_order_relaxed);

        char first_byte = data[idx * bytes_per_page].load(std::memory_order_relaxed);
        if (first_byte == 0)
        {
            pages_evicted += 1;
            data[idx * bytes_per_page + 1].store(0, std::memory_order_relaxed);
            chunk->pages_populated.unset(idx);
        }
        else
        {
            pages_restored += 1;
            chassert(first_byte & 1);
            if (!chunk->first_bit_of_each_page.get(idx))
                data[idx * bytes_per_page].fetch_and(~1, std::memory_order_relaxed);
        }
    }
    return {pages_restored, pages_evicted};
}

PageChunk * PageCache::getFreeChunk()
{
    if (lru.empty() || (mmaps.size() < max_mmaps && lru.front().key.has_value()))
        addMmap();
    if (lru.empty())
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "All chunks in the entire page cache ({:.3} GiB) are pinned.",
            bytes_per_page * pages_per_chunk * total_chunks * 1. / (1l << 30));

    PageChunk * chunk = &lru.front();
    lru.erase(lru.iterator_to(*chunk));

    size_t prev_pin_count = chunk->pin_count.fetch_add(1);
    chassert(prev_pin_count == 0);

    evictChunk(chunk);

    return chunk;
}

void PageCache::evictChunk(PageChunk * chunk)
{
    if (chunk->key.has_value())
    {
        size_t erased = chunk_by_key.erase(chunk->key.value());
        chassert(erased);
        chunk->key.reset();
    }

    chunk->state.reset();

    /// This is tricky. We're not holding the chunk_mutex, so another thread might be running
    /// sendChunkToLimbo() or even restoreChunkFromLimbo() on this chunk right now.
    ///
    /// Nevertheless, it's correct and sufficient to clear pages_populated here because sendChunkToLimbo()
    /// and restoreChunkFromLimbo() only touch pages_populated (only unsetting the bits),
    /// first_bit_of_each_page, and the data; and we don't care about first_bit_of_each_page and the data.
    ///
    /// This is precarious, but I don't have better ideas. Note that this clearing (or something else)
    /// must be done before unlocking the global_mutex because otherwise another call to getOrSet() might
    /// return this chunk before we clear it.
    chunk->pages_populated.unsetAll();
}

void PageCache::addMmap()
{
    /// ASLR by hand.
    void * address_hint = reinterpret_cast<void *>(std::uniform_int_distribution<size_t>(0x100000000000UL, 0x700000000000UL)(rng));

    mmaps.emplace_back(bytes_per_page, pages_per_chunk, pages_per_big_page, chunks_per_mmap_target, address_hint, use_huge_pages);

    size_t num_chunks = mmaps.back().num_chunks;
    total_chunks += num_chunks;
    for (size_t i = 0; i < num_chunks; ++i)
        /// Link in reverse order, so they get assigned in increasing order. Not important, just seems nice.
        lru.push_front(mmaps.back().chunks[num_chunks - 1 - i]);
}

void PageCache::dropCache()
{
    std::lock_guard lock(global_mutex);

    /// Detach and free unpinned chunks.
    bool logged_error = false;
    for (PageChunk & chunk : lru)
    {
        evictChunk(&chunk);

        if (use_madv_free)
        {
            /// This might happen in parallel with sendChunkToLimbo() or restoreChunkFromLimbo(), but it's ok.
            int r = madvise(chunk.data, chunk.size, MADV_DONTNEED);
            if (r != 0 && !logged_error)
            {
                logUnexpectedSyscallError("madvise(MADV_DONTNEED)");
                logged_error = true;
            }
        }
    }

    /// Detach pinned chunks.
    for (auto [key, chunk] : chunk_by_key)
    {
        chassert(chunk->key == key);
        chassert(chunk->pin_count > 0); // otherwise it would have been evicted above
        chunk->key.reset();
    }
    chunk_by_key.clear();
}

PageCache::Mmap::Mmap(size_t bytes_per_page_, size_t pages_per_chunk_, size_t pages_per_big_page_, size_t num_chunks_, void * address_hint, bool use_huge_pages_)
{
    num_chunks = num_chunks_;
    size = bytes_per_page_ * pages_per_chunk_ * num_chunks;

    size_t alignment = bytes_per_page_ * pages_per_big_page_;
    address_hint = reinterpret_cast<void*>(reinterpret_cast<UInt64>(address_hint) / alignment * alignment);

    auto temp_chunks = std::make_unique<PageChunk[]>(num_chunks);

    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef OS_LINUX
    flags |= MAP_NORESERVE;
#endif
    ptr = mmap(address_hint, size, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (MAP_FAILED == ptr)
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, fmt::format("Cannot mmap {}.", ReadableSize(size)));
    if (reinterpret_cast<UInt64>(ptr) % bytes_per_page_ != 0)
    {
        munmap(ptr, size);
        throw Exception(ErrorCodes::SYSTEM_ERROR, "mmap returned unaligned address: {}", ptr);
    }

    void * chunks_start = ptr;

#ifdef OS_LINUX
    if (madvise(ptr, size, MADV_DONTDUMP) != 0)
        logUnexpectedSyscallError("madvise(MADV_DONTDUMP)");
    if (madvise(ptr, size, MADV_DONTFORK) != 0)
        logUnexpectedSyscallError("madvise(MADV_DONTFORK)");

    if (use_huge_pages_)
    {
        if (reinterpret_cast<UInt64>(ptr) % alignment != 0)
        {
            LOG_DEBUG(&Poco::Logger::get("PageCache"), "mmap() returned address not aligned on huge page boundary.");
            chunks_start = reinterpret_cast<void*>((reinterpret_cast<UInt64>(ptr) / alignment + 1) * alignment);
            chassert(reinterpret_cast<UInt64>(chunks_start) % alignment == 0);
            num_chunks -= 1;
        }

        if (madvise(ptr, size, MADV_HUGEPAGE) != 0)
            LOG_WARNING(&Poco::Logger::get("PageCache"),
                "madvise(MADV_HUGEPAGE) failed: {}. Userspace page cache will be relatively slow.", errnoToString());
    }
#else
    (void)use_huge_pages_;
#endif

    chunks = std::move(temp_chunks);
    for (size_t i = 0; i < num_chunks; ++i)
    {
        PageChunk * chunk = &chunks[i];
        chunk->data = reinterpret_cast<char *>(chunks_start) + bytes_per_page_ * pages_per_chunk_ * i;
        chunk->size = bytes_per_page_ * pages_per_chunk_;
        chunk->page_size = bytes_per_page_;
        chunk->big_page_size = bytes_per_page_ * pages_per_big_page_;
        chunk->pages_populated.init(pages_per_chunk_);
        chunk->first_bit_of_each_page.init(pages_per_chunk_);
    }
}

PageCache::Mmap::Mmap(Mmap && m) noexcept : ptr(std::exchange(m.ptr, nullptr)), size(std::exchange(m.size, 0)), chunks(std::move(m.chunks)), num_chunks(std::exchange(m.num_chunks, 0)) {}

PageCache::Mmap::~Mmap() noexcept
{
    if (ptr && 0 != munmap(ptr, size))
        logUnexpectedSyscallError("munmap");
}

void FileChunkState::reset() {}

PageCacheKey FileChunkAddress::hash() const
{
    SipHash hash(offset);
    hash.update(path.data(), path.size());
    if (!file_version.empty())
    {
        hash.update("\0", 1);
        hash.update(file_version.data(), file_version.size());
    }
    return hash.get128();
}

std::string FileChunkAddress::toString() const
{
    return fmt::format("{}:{}{}{}", path, offset, file_version.empty() ? "" : ":", file_version);
}

#pragma clang diagnostic pop

}
