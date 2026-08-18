#include <Common/CompactSymbols.h>

#include <algorithm>
#include <array>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <zstd.h>

namespace DB::CompactSymbols
{
namespace
{

constexpr std::array<char, 8> magic{'C', 'H', 'S', 'Y', 'M', 'S', '\0', '\0'};
constexpr size_t header_size = 88;
constexpr size_t mark_size = 16;
constexpr size_t minimum_address_entry_size = 3;
constexpr size_t maximum_var_uint_size = (static_cast<size_t>(std::numeric_limits<uint64_t>::digits) + 6) / 7;
constexpr size_t maximum_address_entry_size = 3 * maximum_var_uint_size;
constexpr size_t minimum_name_entry_size = 2;

/// Front-coded name granules normally expand by about 15-30x. This format cap leaves over two
/// orders of magnitude of margin while bounding allocations derived from untrusted frame headers.
constexpr size_t max_expansion_ratio = 1024;

template <typename T>
void appendLittleEndian(std::vector<char> & output, T value)
{
    static_assert(std::is_unsigned_v<T>);
    for (size_t byte = 0; byte < sizeof(T); ++byte)
        output.push_back(static_cast<char>(value >> (byte * 8)));
}

template <typename T>
T readLittleEndian(std::string_view input, size_t offset)
{
    static_assert(std::is_unsigned_v<T>);
    if (offset > input.size() || sizeof(T) > input.size() - offset)
        throw std::runtime_error("Compact symbols field is out of bounds");

    T value = 0;
    for (size_t byte = 0; byte < sizeof(T); ++byte)
        value |= static_cast<T>(static_cast<unsigned char>(input[offset + byte])) << (byte * 8);
    return value;
}

bool rangeWithin(uint64_t offset, uint64_t size, uint64_t total)
{
    return offset <= total && size <= total - offset;
}

void appendVarUInt(std::vector<char> & output, uint64_t value)
{
    while (value >= 0x80)
    {
        output.push_back(static_cast<char>((value & 0x7f) | 0x80));
        value >>= 7;
    }
    output.push_back(static_cast<char>(value));
}

uint64_t readVarUInt(const char *& position, const char * end)
{
    uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 7)
    {
        if (position == end)
            throw std::runtime_error("Truncated compact symbols varint");

        uint8_t byte = static_cast<uint8_t>(*position++);
        if (shift == 63 && (byte & 0xfe))
            throw std::runtime_error("Overflowing compact symbols varint");

        value |= static_cast<uint64_t>(byte & 0x7f) << shift;
        if (!(byte & 0x80))
            return value;
    }
    throw std::runtime_error("Overflowing compact symbols varint");
}

std::vector<char> compress(std::span<const char> input)
{
    std::vector<char> output(ZSTD_compressBound(input.size()));
    size_t compressed_size = ZSTD_compress(output.data(), output.size(), input.data(), input.size(), compression_level);
    if (ZSTD_isError(compressed_size))
        throw std::runtime_error(std::string("Cannot compress compact symbols: ") + ZSTD_getErrorName(compressed_size));
    output.resize(compressed_size);
    return output;
}

size_t maximumFrameContentSize(size_t compressed_size)
{
    if (compressed_size > std::numeric_limits<size_t>::max() / max_expansion_ratio)
        return std::numeric_limits<size_t>::max();
    return compressed_size * max_expansion_ratio;
}

size_t frameContentSize(std::string_view input)
{
    uint64_t decompressed_size = ZSTD_getFrameContentSize(input.data(), input.size());
    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR)
        throw std::runtime_error("Invalid zstd frame in compact symbols");
    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN)
        throw std::runtime_error("Compact symbols zstd frame has no content size");
    if (!std::in_range<size_t>(decompressed_size))
        throw std::runtime_error("Compact symbols zstd frame is too large");
    size_t result = static_cast<size_t>(decompressed_size);
    if (result > maximumFrameContentSize(input.size()))
        throw std::runtime_error("Compact symbols zstd frame exceeds the maximum expansion ratio");
    return result;
}

std::vector<char> decompress(std::string_view input, size_t expected_size)
{
    std::vector<char> output(expected_size);
    size_t result = ZSTD_decompress(output.data(), output.size(), input.data(), input.size());
    if (ZSTD_isError(result))
        throw std::runtime_error(std::string("Cannot decompress compact symbols: ") + ZSTD_getErrorName(result));
    if (result != output.size())
        throw std::runtime_error("Compact symbols zstd frame size mismatch");
    return output;
}

size_t commonPrefixSize(std::string_view lhs, std::string_view rhs)
{
    size_t limit = std::min(lhs.size(), rhs.size());
    size_t size = 0;
    while (size < limit && lhs[size] == rhs[size])
        ++size;
    return size;
}

}

NameGranuleDecoder::NameGranuleDecoder(std::span<const char> data_, size_t name_count_)
    : begin(data_.empty() ? "" : data_.data())
    , position(begin)
    , end(begin + data_.size())
    , name_count(name_count_)
    , remaining_names(name_count_)
    , first_name(true)
{
}

bool NameGranuleDecoder::next(std::string & name)
{
    if (remaining_names == 0)
        return false;

    uint64_t shared = readVarUInt(position, end);
    uint64_t suffix_size = readVarUInt(position, end);
    if ((first_name && shared != 0) || shared > name.size())
        throw std::runtime_error("Invalid compact symbol shared name prefix");
    if (suffix_size > static_cast<uint64_t>(end - position))
        throw std::runtime_error("Truncated compact symbol name suffix");

    /// Front coding guarantees that the shared part is already at the start of `name`.
    /// Keep it in place and append only the suffix instead of materializing preceding names.
    name.resize(static_cast<size_t>(shared));
    name.append(position, static_cast<size_t>(suffix_size));
    position += static_cast<size_t>(suffix_size);
    if (name.empty())
        throw std::runtime_error("Empty compact symbol name");

    first_name = false;
    --remaining_names;
    if (remaining_names == 0 && position != end)
        throw std::runtime_error("Trailing data in compact symbol names granule");
    return true;
}

void NameGranuleDecoder::reset()
{
    position = begin;
    remaining_names = name_count;
    first_name = true;
}

std::vector<char> encode(std::span<const Symbol> symbols)
{
    std::vector<std::string_view> names;
    names.reserve(symbols.size());
    for (const auto & symbol : symbols)
    {
        if (symbol.name.empty())
            throw std::runtime_error("Cannot encode an empty compact symbol name");
        names.push_back(symbol.name);
    }

    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());

    if (names.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("Too many names for compact symbols format v1");

    const size_t granule_count_size = (names.size() + names_per_granule - 1) / names_per_granule;
    if (granule_count_size > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("Too many name granules for compact symbols format v1");
    const auto granule_count = static_cast<uint32_t>(granule_count_size);

    std::vector<char> marks;
    marks.reserve(granule_count_size * mark_size);
    std::vector<char> names_blob;

    for (uint32_t granule = 0; granule < granule_count; ++granule)
    {
        size_t begin = static_cast<size_t>(granule) * names_per_granule;
        size_t end = std::min(begin + names_per_granule, names.size());
        std::vector<char> uncompressed;
        std::string_view previous;

        for (size_t name_index = begin; name_index < end; ++name_index)
        {
            std::string_view name = names[name_index];
            size_t shared = name_index == begin ? 0 : commonPrefixSize(previous, name);
            appendVarUInt(uncompressed, shared);
            appendVarUInt(uncompressed, name.size() - shared);
            uncompressed.insert(uncompressed.end(), name.begin() + shared, name.end());
            previous = name;
        }

        std::vector<char> compressed = compress(uncompressed);
        appendLittleEndian<uint64_t>(marks, names_blob.size());
        appendLittleEndian<uint64_t>(marks, compressed.size());
        names_blob.insert(names_blob.end(), compressed.begin(), compressed.end());
    }

    std::vector<size_t> address_order(symbols.size());
    std::iota(address_order.begin(), address_order.end(), 0);
    std::stable_sort(
        address_order.begin(), address_order.end(), [&](size_t lhs, size_t rhs) { return symbols[lhs].address < symbols[rhs].address; });

    std::vector<char> addresses;
    uint64_t previous_address = 0;
    for (size_t symbol_index : address_order)
    {
        const auto & symbol = symbols[symbol_index];
        auto name_it = std::lower_bound(names.begin(), names.end(), symbol.name);
        if (name_it == names.end() || *name_it != symbol.name)
            throw std::runtime_error("Compact symbol name index is missing");

        appendVarUInt(addresses, symbol.address - previous_address);
        appendVarUInt(addresses, symbol.size);
        appendVarUInt(addresses, static_cast<uint64_t>(name_it - names.begin()));
        previous_address = symbol.address;
    }
    std::vector<char> compressed_addresses = compress(addresses);

    const uint64_t marks_offset = header_size;
    const uint64_t names_offset = marks_offset + marks.size();
    const uint64_t addresses_offset = names_offset + names_blob.size();

    std::vector<char> output;
    output.reserve(header_size + marks.size() + names_blob.size() + compressed_addresses.size());
    output.insert(output.end(), magic.begin(), magic.end());
    appendLittleEndian<uint32_t>(output, format_version);
    appendLittleEndian<uint32_t>(output, header_size);
    appendLittleEndian<uint64_t>(output, names.size());
    appendLittleEndian<uint64_t>(output, symbols.size());
    appendLittleEndian<uint32_t>(output, names_per_granule);
    appendLittleEndian<uint32_t>(output, granule_count);
    appendLittleEndian<uint64_t>(output, marks_offset);
    appendLittleEndian<uint64_t>(output, marks.size());
    appendLittleEndian<uint64_t>(output, names_offset);
    appendLittleEndian<uint64_t>(output, names_blob.size());
    appendLittleEndian<uint64_t>(output, addresses_offset);
    appendLittleEndian<uint64_t>(output, compressed_addresses.size());

    if (output.size() != header_size)
        throw std::runtime_error("Compact symbols header size mismatch");

    output.insert(output.end(), marks.begin(), marks.end());
    output.insert(output.end(), names_blob.begin(), names_blob.end());
    output.insert(output.end(), compressed_addresses.begin(), compressed_addresses.end());
    return output;
}

Reader::Reader(std::string_view data_)
    : data(data_)
    , name_count(0)
    , address_count(0)
    , granule_count(0)
    , marks_offset(0)
    , marks_size(0)
    , names_offset(0)
    , names_size(0)
    , addresses_offset(0)
    , addresses_size(0)
{
    if (data.size() < header_size || !std::equal(magic.begin(), magic.end(), data.begin()))
        throw std::runtime_error("Invalid compact symbols magic");
    if (readLittleEndian<uint32_t>(data, 8) != format_version)
        throw std::runtime_error("Unsupported compact symbols version");
    if (readLittleEndian<uint32_t>(data, 12) != header_size)
        throw std::runtime_error("Invalid compact symbols header size");

    name_count = readLittleEndian<uint64_t>(data, 16);
    address_count = readLittleEndian<uint64_t>(data, 24);
    if (readLittleEndian<uint32_t>(data, 32) != names_per_granule)
        throw std::runtime_error("Invalid compact symbols name granule size");
    granule_count = readLittleEndian<uint32_t>(data, 36);
    marks_offset = readLittleEndian<uint64_t>(data, 40);
    marks_size = readLittleEndian<uint64_t>(data, 48);
    names_offset = readLittleEndian<uint64_t>(data, 56);
    names_size = readLittleEndian<uint64_t>(data, 64);
    addresses_offset = readLittleEndian<uint64_t>(data, 72);
    addresses_size = readLittleEndian<uint64_t>(data, 80);

    if (name_count > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("Too many names for compact symbols format v1");
    uint64_t expected_granules = name_count / names_per_granule + (name_count % names_per_granule != 0);
    if (expected_granules != granule_count)
        throw std::runtime_error("Compact symbols granule count mismatch");
    if (marks_size != static_cast<uint64_t>(granule_count) * mark_size)
        throw std::runtime_error("Compact symbols marks size mismatch");
    if (!rangeWithin(marks_offset, marks_size, data.size()) || !rangeWithin(names_offset, names_size, data.size())
        || !rangeWithin(addresses_offset, addresses_size, data.size()))
        throw std::runtime_error("Compact symbols part is out of bounds");
    if (marks_offset < header_size || names_offset < marks_offset + marks_size || addresses_offset < names_offset + names_size)
        throw std::runtime_error("Compact symbols parts overlap");
    if (address_count > maximumFrameContentSize(static_cast<size_t>(addresses_size)) / minimum_address_entry_size)
        throw std::runtime_error("Compact symbol address count exceeds the compressed stream");

    uint64_t expected_names_offset = 0;
    for (uint32_t granule = 0; granule < granule_count; ++granule)
    {
        uint64_t mark_offset = marks_offset + static_cast<uint64_t>(granule) * mark_size;
        uint64_t compressed_offset = readLittleEndian<uint64_t>(data, mark_offset);
        uint64_t compressed_size = readLittleEndian<uint64_t>(data, mark_offset + sizeof(uint64_t));
        if (compressed_offset != expected_names_offset || !rangeWithin(compressed_offset, compressed_size, names_size))
            throw std::runtime_error("Invalid compact symbols name mark");
        expected_names_offset += compressed_size;
    }
    if (expected_names_offset != names_size)
        throw std::runtime_error("Compact symbols names blob size mismatch");
}

std::vector<AddressEntry> Reader::decodeAddresses() const
{
    std::string_view compressed = data.substr(addresses_offset, addresses_size);
    size_t decoded_size = frameContentSize(compressed);
    if (address_count > decoded_size / minimum_address_entry_size)
        throw std::runtime_error("Compact symbol address count exceeds the decoded stream");
    size_t minimum_entry_count = decoded_size / maximum_address_entry_size + (decoded_size % maximum_address_entry_size != 0);
    if (address_count < minimum_entry_count)
        throw std::runtime_error("Compact symbol decoded address stream exceeds the address count");

    std::vector<char> decoded = decompress(compressed, decoded_size);
    const char * position = decoded.empty() ? "" : decoded.data();
    const char * end = position + decoded.size();

    std::vector<AddressEntry> result;
    result.reserve(static_cast<size_t>(address_count));
    uint64_t address = 0;
    for (uint64_t index = 0; index < address_count; ++index)
    {
        uint64_t delta = readVarUInt(position, end);
        if (delta > std::numeric_limits<uint64_t>::max() - address)
            throw std::runtime_error("Compact symbol address overflows");
        address += delta;
        uint64_t size = readVarUInt(position, end);
        uint64_t name_index = readVarUInt(position, end);
        if (name_index >= name_count || name_index > std::numeric_limits<uint32_t>::max())
            throw std::runtime_error("Compact symbol name index is out of bounds");
        result.push_back({address, size, static_cast<uint32_t>(name_index)});
    }
    if (position != end)
        throw std::runtime_error("Trailing data in compact symbol addresses");
    return result;
}

std::string_view Reader::compressedNameGranule(uint32_t granule_index) const
{
    if (granule_index >= granule_count)
        throw std::runtime_error("Compact symbol name granule is out of bounds");

    uint64_t mark_offset = marks_offset + static_cast<uint64_t>(granule_index) * mark_size;
    uint64_t compressed_offset = readLittleEndian<uint64_t>(data, mark_offset);
    uint64_t compressed_size = readLittleEndian<uint64_t>(data, mark_offset + sizeof(uint64_t));
    return data.substr(names_offset + compressed_offset, compressed_size);
}

size_t Reader::nameGranuleContentSize(uint32_t granule_index) const
{
    size_t granule_size = frameContentSize(compressedNameGranule(granule_index));
    uint64_t first_name_index = static_cast<uint64_t>(granule_index) * names_per_granule;
    size_t count = static_cast<size_t>(std::min<uint64_t>(names_per_granule, name_count - first_name_index));
    if (count > granule_size / minimum_name_entry_size)
        throw std::runtime_error("Compact symbol name count exceeds the decoded granule");
    return granule_size;
}

size_t Reader::maximumNameGranuleSize() const
{
    size_t result = 0;
    for (uint32_t granule_index = 0; granule_index < granule_count; ++granule_index)
    {
        size_t granule_size = nameGranuleContentSize(granule_index);
        result = std::max(result, granule_size);
    }
    return result;
}

NameGranuleDecoder Reader::decodeNameGranule(uint32_t granule_index, ZSTD_DCtx * decompression_context, std::span<char> destination) const
{
    if (!decompression_context)
        throw std::runtime_error("Compact symbol decompression context is missing");

    std::string_view compressed = compressedNameGranule(granule_index);
    size_t expected_size = nameGranuleContentSize(granule_index);
    if (destination.size() < expected_size)
        throw std::runtime_error("Compact symbol name granule destination is too small");

    size_t decoded_size
        = ZSTD_decompressDCtx(decompression_context, destination.data(), destination.size(), compressed.data(), compressed.size());
    if (ZSTD_isError(decoded_size))
        throw std::runtime_error(std::string("Cannot decompress compact symbols: ") + ZSTD_getErrorName(decoded_size));
    if (decoded_size != expected_size)
        throw std::runtime_error("Compact symbols zstd frame size mismatch");

    uint64_t first_name_index = static_cast<uint64_t>(granule_index) * names_per_granule;
    size_t count = static_cast<size_t>(std::min<uint64_t>(names_per_granule, name_count - first_name_index));
    return NameGranuleDecoder(std::span<const char>(destination.data(), decoded_size), count);
}

}
