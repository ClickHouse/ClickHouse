#pragma once

#include <Common/CompactSymbols.h>
#include <Common/SymbolNameCache.h>
#include <IO/ZstdContext.h>
#include <base/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace DB
{

/// The compact symbol table of one object and the cache of the names decoded from it.
class CompactSymbolTable
{
public:
    /// The table is allocated on the first insert with this capacity and grows when a quarter of it is in use,
    /// so about two thousand names in use fit without growing.
    static constexpr size_t name_cache_minimum_capacity = 8192;

    /// Lookups cannot report errors, so the reader rejects a corrupted name payload up front and the section stays absent.
    explicit CompactSymbolTable(std::string_view data);
    ~CompactSymbolTable();

    CompactSymbolTable(const CompactSymbolTable &) = delete;
    CompactSymbolTable & operator=(const CompactSymbolTable &) = delete;

    /// Adds to the metrics the change of the cache since the previous call, which may run concurrently.
    void updateNameCacheMetrics() const noexcept;

    CompactSymbols::Reader reader;
    /// Lookups through a const table write to the cache.
    mutable SymbolNameCache name_cache;

private:
    mutable std::atomic<size_t> reported_entries{0};
    mutable std::atomic<size_t> reported_bytes{0};
};

ZstdDCtxPtr createDecompressionContext();

/// Decodes the name `entry_index` of the granule `granule_index` into `name_buffer`, continuing from the
/// granule and entry the previous call left in `cached_granule_index` and `cached_entry_index`.
std::string_view decodeCompactName(
    const CompactSymbols::Reader & reader,
    uint32_t granule_index,
    uint32_t entry_index,
    ZSTD_DCtx * decompression_context,
    std::span<char> granule_buffer,
    std::optional<CompactSymbols::NameGranuleDecoder> & decoder,
    std::string & name_buffer,
    uint32_t & cached_granule_index,
    uint32_t & cached_entry_index);

/// Per-thread state of compact name lookups: the name buffer, which holds the returned name and is
/// reserved for the longest name, and the decoder state (decompression context and granule buffer),
/// which is created on the first miss and sized for the largest granule.
/// The caller accounts the allocations of `prepare` and `getName`.
class CompactNameWorkspace
{
public:
    /// Allocates everything later lookups need. Lookups also prepare what they need.
    void prepare(size_t maximum_name_size, size_t maximum_granule_size);

    /// After this, lookups only read the name caches and never insert into or maintain them, so that
    /// a prepared workspace does not allocate. For the thread that symbolizes the stack of a thread
    /// that got a fatal signal.
    void setNameCacheReadOnly() { name_cache_read_only = true; }

    /// The name `name_index` of `table`, which is identified by `source_id`. NUL-terminated, valid
    /// until the next call; empty if it cannot be decoded. The maximum sizes must cover every table
    /// used with this workspace.
    std::string_view getName(
        const CompactSymbolTable & table,
        uint32_t source_id,
        uint32_t name_index,
        UInt64 now_seconds,
        size_t maximum_name_size,
        size_t maximum_granule_size);

private:
    void prepareNameBuffer(size_t maximum_name_size);
    void prepareDecoder(size_t maximum_granule_size);
    void invalidate();
    bool tryGetCachedName(SymbolNameCache & cache, uint32_t name_index, UInt64 now_seconds, size_t maximum_name_size);

    std::string name_buffer;
    ZstdDCtxPtr decompression_context;
    std::unique_ptr<char[]> granule_buffer;
    size_t granule_buffer_size = 0;
    std::optional<CompactSymbols::NameGranuleDecoder> decoder;
    uint32_t source_id = std::numeric_limits<uint32_t>::max();
    /// The granule in `granule_buffer` and the entry of it in `name_buffer`, if `decoder` is right after it.
    uint32_t granule_index = std::numeric_limits<uint32_t>::max();
    uint32_t entry_index = std::numeric_limits<uint32_t>::max();
    /// The name in `name_buffer`, however it got there.
    uint32_t buffered_name_index = std::numeric_limits<uint32_t>::max();
    bool name_cache_read_only = false;
};

}
