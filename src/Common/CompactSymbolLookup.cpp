#include <Common/CompactSymbolLookup.h>
#include <Common/CurrentMetrics.h>

#include <new>
#include <stdexcept>

namespace CurrentMetrics
{
    extern const Metric SymbolNameCacheEntries;
    extern const Metric SymbolNameCacheBytes;
}

namespace DB
{

namespace
{

constexpr uint32_t no_index = std::numeric_limits<uint32_t>::max();

void updateMetric(CurrentMetrics::Metric metric, std::atomic<size_t> & reported, size_t value) noexcept
{
    if (reported.load(std::memory_order_relaxed) == value)
        return;
    size_t previous = reported.exchange(value, std::memory_order_relaxed);
    CurrentMetrics::add(metric, static_cast<CurrentMetrics::Value>(value) - static_cast<CurrentMetrics::Value>(previous));
}

}

CompactSymbolTable::CompactSymbolTable(std::string_view data)
    : reader(data)
    , name_cache(name_cache_minimum_capacity, reader.nameCount())
{
}

CompactSymbolTable::~CompactSymbolTable()
{
    CurrentMetrics::sub(CurrentMetrics::SymbolNameCacheEntries, reported_entries.load(std::memory_order_relaxed));
    CurrentMetrics::sub(CurrentMetrics::SymbolNameCacheBytes, reported_bytes.load(std::memory_order_relaxed));
}

void CompactSymbolTable::updateNameCacheMetrics() const noexcept
{
    updateMetric(CurrentMetrics::SymbolNameCacheEntries, reported_entries, name_cache.size());
    updateMetric(CurrentMetrics::SymbolNameCacheBytes, reported_bytes, name_cache.bytesAllocated());
}

ZstdDCtxPtr createDecompressionContext()
{
    ZstdDCtxPtr context(ZSTD_createDCtx());
    if (!context)
        throw std::bad_alloc();
    return context;
}

std::string_view decodeCompactName(
    const CompactSymbols::Reader & reader,
    uint32_t granule_index,
    uint32_t entry_index,
    ZSTD_DCtx * decompression_context,
    std::span<char> granule_buffer,
    std::optional<CompactSymbols::NameGranuleDecoder> & decoder,
    std::string & name_buffer,
    uint32_t & cached_granule_index,
    uint32_t & cached_entry_index)
{
    if (granule_index != cached_granule_index)
    {
        decoder.emplace(reader.decodeNameGranule(granule_index, decompression_context, granule_buffer));
        cached_granule_index = granule_index;
        cached_entry_index = no_index;
        name_buffer.clear();
    }
    else if (cached_entry_index != no_index && cached_entry_index > entry_index)
    {
        decoder->reset();
        cached_entry_index = no_index;
        name_buffer.clear();
    }

    if (cached_entry_index == entry_index)
        return std::string_view(name_buffer.data(), name_buffer.size());

    uint32_t next_entry_index = cached_entry_index == no_index ? 0 : cached_entry_index + 1;
    while (next_entry_index <= entry_index)
    {
        if (!decoder->next(name_buffer))
            return std::string_view("");
        cached_entry_index = next_entry_index;
        ++next_entry_index;
    }
    return std::string_view(name_buffer.data(), name_buffer.size());
}

void CompactNameWorkspace::prepare(size_t maximum_name_size, size_t maximum_granule_size)
{
    prepareNameBuffer(maximum_name_size);
    prepareDecoder(maximum_granule_size);
}

void CompactNameWorkspace::prepareNameBuffer(size_t maximum_name_size)
{
    if (name_buffer.capacity() < maximum_name_size)
    {
        invalidate();
        name_buffer.reserve(maximum_name_size);
    }
}

void CompactNameWorkspace::prepareDecoder(size_t maximum_granule_size)
{
    if (!decompression_context)
        decompression_context = createDecompressionContext();
    if (granule_buffer_size < maximum_granule_size)
    {
        invalidate();
        granule_buffer.reset();
        granule_buffer_size = 0;
        /// Not zero-filled, so that only the pages of the granules actually decoded become resident.
        granule_buffer = std::make_unique_for_overwrite<char[]>(maximum_granule_size);
        granule_buffer_size = maximum_granule_size;
    }
}

void CompactNameWorkspace::invalidate()
{
    granule_index = no_index;
    entry_index = no_index;
    buffered_name_index = no_index;
    decoder.reset();
    name_buffer.clear();
}

bool CompactNameWorkspace::tryGetCachedName(SymbolNameCache & cache, uint32_t name_index, UInt64 now_seconds, size_t maximum_name_size)
{
    bool hit = false;
    size_t previous_size = name_buffer.size();
    name_buffer.resize_and_overwrite(maximum_name_size, [&](char * buffer, size_t size) noexcept
    {
        /// The buffer has room for the terminating NUL after `size` characters. On a miss the
        /// previous name is kept, because the decoder continues from it.
        std::optional<size_t> name_size = cache.tryGet(name_index, now_seconds, buffer, size + 1);
        hit = name_size.has_value();
        return hit ? *name_size : previous_size;
    });
    if (!hit)
        return false;

    /// The decoder continues from the name in `name_buffer`, which is gone: restart it in its granule.
    if (decoder)
        decoder->reset();
    entry_index = no_index;
    buffered_name_index = name_index;
    return true;
}

std::string_view CompactNameWorkspace::getName(
    const CompactSymbolTable & table,
    uint32_t source_id_,
    uint32_t name_index,
    UInt64 now_seconds,
    size_t maximum_name_size,
    size_t maximum_granule_size)
{
    prepareNameBuffer(maximum_name_size);
    if (source_id_ != source_id)
    {
        invalidate();
        source_id = source_id_;
    }

    /// The same name again, e.g. `getSymbolName` after `getSymbolNameCString`: it is not a cache miss.
    if (buffered_name_index == name_index)
        return name_buffer;

    if (tryGetCachedName(table.name_cache, name_index, now_seconds, maximum_name_size))
    {
        if (!name_cache_read_only)
        {
            table.name_cache.maintain(now_seconds);
            table.updateNameCacheMetrics();
        }
        return name_buffer;
    }

    prepareDecoder(maximum_granule_size);
    std::string_view name;
    try
    {
        name = decodeCompactName(
            table.reader,
            name_index / CompactSymbols::names_per_granule,
            name_index % CompactSymbols::names_per_granule,
            decompression_context.get(),
            {granule_buffer.get(), granule_buffer_size},
            decoder,
            name_buffer,
            granule_index,
            entry_index);
    }
    catch (const std::runtime_error &)
    {
        name = {};
    }

    if (name.empty())
    {
        invalidate();
        return name_buffer;
    }
    buffered_name_index = name_index;

    if (!name_cache_read_only)
    {
        table.name_cache.add(name_index, name_buffer, now_seconds);
        table.name_cache.maintain(now_seconds);
        table.updateNameCacheMetrics();
    }
    return name_buffer;
}

}
