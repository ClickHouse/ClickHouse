#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <zstd.h>

namespace DB::CompactSymbols
{

inline constexpr char section_name[] = ".clickhouse.symbols";
inline constexpr uint32_t format_version = 1;
inline constexpr uint32_t names_per_granule = 4096;
inline constexpr int compression_level = 19;

struct Symbol
{
    uint64_t address;
    uint64_t size;
    std::string_view name;
};

struct AddressEntry
{
    uint64_t address;
    uint64_t size;
    uint32_t name_index;
};

std::vector<char> encode(std::span<const Symbol> symbols);

class NameGranuleDecoder
{
public:
    bool next(std::string & name);
    void reset();

private:
    friend class Reader;
    NameGranuleDecoder(std::span<const char> data_, size_t name_count_);

    const char * begin;
    const char * position;
    const char * end;
    size_t name_count;
    size_t remaining_names;
    bool first_name;
};

class Reader
{
public:
    explicit Reader(std::string_view data_);

    uint64_t nameCount() const { return name_count; }
    uint64_t addressCount() const { return address_count; }
    uint32_t granuleCount() const { return granule_count; }

    std::vector<AddressEntry> decodeAddresses() const;
    size_t maximumNameGranuleSize() const;
    NameGranuleDecoder decodeNameGranule(uint32_t granule_index, ZSTD_DCtx * decompression_context, std::span<char> destination) const;

private:
    std::string_view compressedNameGranule(uint32_t granule_index) const;
    size_t nameGranuleContentSize(uint32_t granule_index) const;

    std::string_view data;
    uint64_t name_count;
    uint64_t address_count;
    uint32_t granule_count;
    uint64_t marks_offset;
    uint64_t marks_size;
    uint64_t names_offset;
    uint64_t names_size;
    uint64_t addresses_offset;
    uint64_t addresses_size;
};

}
