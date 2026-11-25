#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <zstd.h>

/** Symbol names and addresses of an executable, stored in its `.clickhouse.symbols` section.
  *
  * All integers are little-endian. Layout:
  * - header: magic, version, header size, name count, address count, names per granule, granule count,
  *   and offset and size of each part below (64-bit, offsets from the start of the section);
  * - dictionary: a zstd dictionary shared by the name granules, possibly empty;
  * - marks: the 32-bit end offset of each name granule in the names part;
  * - names: unique names, sorted, split into granules of `names_per_granule`, each front-coded
  *   (varint shared prefix size, varint suffix size, suffix) and compressed into a zstd frame
  *   with content size and checksum, against the dictionary when it is present;
  * - addresses: one zstd frame of (varint address delta, varint size, varint name index) sorted by address.
  */
namespace DB::CompactSymbols
{

inline constexpr char section_name[] = ".clickhouse.symbols";
inline constexpr uint32_t format_version = 2;
inline constexpr uint32_t names_per_granule = 128;
inline constexpr int compression_level = 19;

/// The dictionary is trained if and only if the front-coded name granules have at least
/// `dictionary_training_ratio` times the dictionary capacity bytes, the training set size zstd recommends.
/// Otherwise the dictionary is empty and the granules are plain frames.
inline constexpr size_t default_dictionary_capacity = 112640;
inline constexpr size_t dictionary_training_ratio = 100;

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

/// The same symbols and capacity always produce the same bytes.
std::vector<char> encode(std::span<const Symbol> symbols, size_t dictionary_capacity = default_dictionary_capacity);

class NameGranuleDecoder
{
public:
    bool next(std::string & name);
    void reset();
    /// The size of the granule after decompression.
    [[nodiscard]] size_t decodedSize() const { return static_cast<size_t>(end - begin); }

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
    /// Validates the whole section, decoding every name granule, so that a corrupted name payload
    /// is rejected before the table is used instead of silently producing wrong names on lookups.
    explicit Reader(std::string_view data_);

    [[nodiscard]] uint64_t nameCount() const { return name_count; }
    [[nodiscard]] uint64_t addressCount() const { return address_count; }
    [[nodiscard]] uint32_t granuleCount() const { return granule_count; }
    [[nodiscard]] uint64_t dictionarySize() const { return dictionary_size; }
    [[nodiscard]] size_t maximumNameGranuleSize() const { return maximum_name_granule_size; }
    [[nodiscard]] size_t maximumNameSize() const { return maximum_name_size; }

    [[nodiscard]] std::vector<AddressEntry> decodeAddresses() const;

    /// Does not allocate when `destination` holds `maximumNameGranuleSize` bytes and `next` is given
    /// a string with capacity for `maximumNameSize` characters.
    NameGranuleDecoder decodeNameGranule(uint32_t granule_index, ZSTD_DCtx * decompression_context, std::span<char> destination) const;

private:
    [[nodiscard]] std::string_view compressedNameGranule(uint32_t granule_index) const;
    [[nodiscard]] size_t nameGranuleContentSize(uint32_t granule_index) const;
    void validateNames();

    std::string_view data;
    uint64_t name_count = 0;
    uint64_t address_count = 0;
    uint32_t granule_count = 0;
    uint64_t dictionary_offset = 0;
    uint64_t dictionary_size = 0;
    uint64_t marks_offset = 0;
    uint64_t marks_size = 0;
    uint64_t names_offset = 0;
    uint64_t names_size = 0;
    uint64_t addresses_offset = 0;
    uint64_t addresses_size = 0;

    /// Storage of `dictionary`, which references the dictionary bytes in `data`.
    std::unique_ptr<uint64_t[]> dictionary_workspace;
    const ZSTD_DDict * dictionary = nullptr;
    unsigned dictionary_id = 0;

    size_t maximum_name_granule_size = 0;
    size_t maximum_name_size = 0;
};

}
