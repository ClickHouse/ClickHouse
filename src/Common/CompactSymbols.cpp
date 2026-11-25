#include <Common/CompactSymbols.h>
#include <base/sanitizer_defs.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <new>
#include <numeric>
#include <stdexcept>
#include <type_traits>
#include <utility>

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zdict.h>

namespace DB::CompactSymbols
{
namespace
{

constexpr std::array<char, 8> magic{'C', 'H', 'S', 'Y', 'M', 'S', '\0', '\0'};
constexpr size_t header_size = 104;
constexpr size_t mark_size = sizeof(uint32_t);
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

void checkZstd(size_t result, const char * action)
{
    if (ZSTD_isError(result))
        throw std::runtime_error(std::string("Cannot ") + action + " compact symbols: " + ZSTD_getErrorName(result));
}

/// Compresses frames with content size and checksum, against a dictionary if one is given.
class Compressor
{
public:
    explicit Compressor(std::span<const char> dictionary)
        : context(ZSTD_createCCtx(), &ZSTD_freeCCtx)
        , compression_dictionary(nullptr, &ZSTD_freeCDict)
    {
        if (!context)
            throw std::bad_alloc();
        checkZstd(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_compressionLevel, compression_level), "configure compression of");
        checkZstd(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_checksumFlag, 1), "configure compression of");
        if (!dictionary.empty())
        {
            compression_dictionary.reset(ZSTD_createCDict(dictionary.data(), dictionary.size(), compression_level));
            if (!compression_dictionary)
                throw std::runtime_error("Cannot create the compression dictionary of compact symbols");
            checkZstd(ZSTD_CCtx_refCDict(context.get(), compression_dictionary.get()), "configure compression of");
        }
    }

    std::vector<char> compress(std::span<const char> input)
    {
        std::vector<char> output(ZSTD_compressBound(input.size()));
        size_t compressed_size = ZSTD_compress2(context.get(), output.data(), output.size(), input.data(), input.size());
        checkZstd(compressed_size, "compress");
        output.resize(compressed_size);
        return output;
    }

private:
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context;
    std::unique_ptr<ZSTD_CDict, decltype(&ZSTD_freeCDict)> compression_dictionary;
};

std::vector<char> trainDictionary(std::span<const char> granules, std::span<const size_t> granule_sizes, size_t dictionary_capacity)
{
    if (granule_sizes.size() > std::numeric_limits<unsigned>::max())
        throw std::runtime_error("Too many name granules to train the compact symbols dictionary");

    /// `ZDICT_trainFromBuffer` trains on the first three quarters of the samples and tunes on the rest.
    /// Taking the granules by their index modulo 4 spreads both sets over the whole sorted name space.
    constexpr size_t interleave = 4;
    std::vector<size_t> granule_offsets(granule_sizes.size());
    std::exclusive_scan(granule_sizes.begin(), granule_sizes.end(), granule_offsets.begin(), size_t{0});
    std::vector<char> samples;
    samples.reserve(granules.size());
    std::vector<size_t> sample_sizes;
    sample_sizes.reserve(granule_sizes.size());
    for (size_t phase = 0; phase < interleave; ++phase)
    {
        for (size_t granule = phase; granule < granule_sizes.size(); granule += interleave)
        {
            const char * begin = granules.data() + granule_offsets[granule];
            samples.insert(samples.end(), begin, begin + granule_sizes[granule]);
            sample_sizes.push_back(granule_sizes[granule]);
        }
    }

    std::vector<char> dictionary(dictionary_capacity);
    size_t dictionary_size = ZDICT_trainFromBuffer(
        dictionary.data(), dictionary.size(), samples.data(), sample_sizes.data(), static_cast<unsigned>(sample_sizes.size()));
    if (ZDICT_isError(dictionary_size))
        throw std::runtime_error(std::string("Cannot train the compact symbols dictionary: ") + ZDICT_getErrorName(dictionary_size));
    dictionary.resize(dictionary_size);
    if (ZSTD_getDictID_fromDict(dictionary.data(), dictionary.size()) == 0)
        throw std::runtime_error("Trained compact symbols dictionary has no dictionary id");
    return dictionary;
}

size_t maximumFrameContentSize(size_t compressed_size)
{
    if (compressed_size > std::numeric_limits<size_t>::max() / max_expansion_ratio)
        return std::numeric_limits<size_t>::max();
    return compressed_size * max_expansion_ratio;
}

/// `ZSTD_CONTENTSIZE_ERROR` is defined as `(0ULL - 2)`, which wraps around on purpose.
NO_SANITIZE_UNSIGNED_OVERFLOW
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

/// A part must be exactly one frame as the encoder writes it: `ZSTD_decompress` would also decode
/// frames appended to it, and frames of the legacy formats, which `ZSTD_getFrameHeader` rejects.
void checkSingleFrame(std::string_view input)
{
    ZSTD_frameHeader header;
    size_t header_result = ZSTD_getFrameHeader(&header, input.data(), input.size());
    checkZstd(header_result, "parse");
    if (header_result != 0 || header.frameType != ZSTD_frame)
        throw std::runtime_error("Compact symbols part is not a zstd frame");
    if (!header.checksumFlag)
        throw std::runtime_error("Compact symbols zstd frame has no checksum");

    size_t frame_size = ZSTD_findFrameCompressedSize(input.data(), input.size());
    checkZstd(frame_size, "parse");
    if (frame_size != input.size())
        throw std::runtime_error("Compact symbols part is not a single zstd frame");
}

std::vector<char> decompress(std::string_view input, size_t expected_size)
{
    std::vector<char> output(expected_size);
    size_t result = ZSTD_decompress(output.data(), output.size(), input.data(), input.size());
    checkZstd(result, "decompress");
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

std::vector<char> encode(std::span<const Symbol> symbols, size_t dictionary_capacity)
{
    if (dictionary_capacity > std::numeric_limits<size_t>::max() / dictionary_training_ratio)
        throw std::runtime_error("Compact symbols dictionary capacity is too large");

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
        throw std::runtime_error("Too many names for compact symbols");

    const size_t granule_count_size = (names.size() + names_per_granule - 1) / names_per_granule;
    if (granule_count_size > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("Too many name granules for compact symbols");
    const auto granule_count = static_cast<uint32_t>(granule_count_size);

    std::vector<char> granules;
    std::vector<size_t> granule_sizes;
    granule_sizes.reserve(granule_count_size);
    for (uint32_t granule = 0; granule < granule_count; ++granule)
    {
        size_t begin = static_cast<size_t>(granule) * names_per_granule;
        size_t end = std::min(begin + names_per_granule, names.size());
        size_t granule_begin = granules.size();
        std::string_view previous;

        for (size_t name_index = begin; name_index < end; ++name_index)
        {
            std::string_view name = names[name_index];
            size_t shared = name_index == begin ? 0 : commonPrefixSize(previous, name);
            appendVarUInt(granules, shared);
            appendVarUInt(granules, name.size() - shared);
            granules.insert(granules.end(), name.begin() + shared, name.end());
            previous = name;
        }
        granule_sizes.push_back(granules.size() - granule_begin);
    }

    std::vector<char> dictionary;
    if (granules.size() >= dictionary_capacity * dictionary_training_ratio)
        dictionary = trainDictionary(granules, granule_sizes, dictionary_capacity);

    Compressor names_compressor(dictionary);
    std::vector<char> marks;
    marks.reserve(granule_count_size * mark_size);
    std::vector<char> names_blob;
    size_t granule_begin = 0;
    for (size_t granule_size : granule_sizes)
    {
        std::vector<char> compressed = names_compressor.compress(std::span<const char>(granules.data() + granule_begin, granule_size));
        granule_begin += granule_size;
        names_blob.insert(names_blob.end(), compressed.begin(), compressed.end());
        if (names_blob.size() > std::numeric_limits<uint32_t>::max())
            throw std::runtime_error("Compressed compact symbol names do not fit 32-bit marks");
        appendLittleEndian<uint32_t>(marks, static_cast<uint32_t>(names_blob.size()));
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
    std::vector<char> compressed_addresses = Compressor({}).compress(addresses);

    const uint64_t dictionary_offset = header_size;
    const uint64_t marks_offset = dictionary_offset + dictionary.size();
    const uint64_t names_offset = marks_offset + marks.size();
    const uint64_t addresses_offset = names_offset + names_blob.size();

    std::vector<char> output;
    output.reserve(header_size + dictionary.size() + marks.size() + names_blob.size() + compressed_addresses.size());
    output.insert(output.end(), magic.begin(), magic.end());
    appendLittleEndian<uint32_t>(output, format_version);
    appendLittleEndian<uint32_t>(output, header_size);
    appendLittleEndian<uint64_t>(output, names.size());
    appendLittleEndian<uint64_t>(output, symbols.size());
    appendLittleEndian<uint32_t>(output, names_per_granule);
    appendLittleEndian<uint32_t>(output, granule_count);
    appendLittleEndian<uint64_t>(output, dictionary_offset);
    appendLittleEndian<uint64_t>(output, dictionary.size());
    appendLittleEndian<uint64_t>(output, marks_offset);
    appendLittleEndian<uint64_t>(output, marks.size());
    appendLittleEndian<uint64_t>(output, names_offset);
    appendLittleEndian<uint64_t>(output, names_blob.size());
    appendLittleEndian<uint64_t>(output, addresses_offset);
    appendLittleEndian<uint64_t>(output, compressed_addresses.size());

    if (output.size() != header_size)
        throw std::runtime_error("Compact symbols header size mismatch");

    output.insert(output.end(), dictionary.begin(), dictionary.end());
    output.insert(output.end(), marks.begin(), marks.end());
    output.insert(output.end(), names_blob.begin(), names_blob.end());
    output.insert(output.end(), compressed_addresses.begin(), compressed_addresses.end());
    return output;
}

Reader::Reader(std::string_view data_)
    : data(data_)
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
    dictionary_offset = readLittleEndian<uint64_t>(data, 40);
    dictionary_size = readLittleEndian<uint64_t>(data, 48);
    marks_offset = readLittleEndian<uint64_t>(data, 56);
    marks_size = readLittleEndian<uint64_t>(data, 64);
    names_offset = readLittleEndian<uint64_t>(data, 72);
    names_size = readLittleEndian<uint64_t>(data, 80);
    addresses_offset = readLittleEndian<uint64_t>(data, 88);
    addresses_size = readLittleEndian<uint64_t>(data, 96);

    if (name_count > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("Too many names for compact symbols");
    uint64_t expected_granules = name_count / names_per_granule + (name_count % names_per_granule != 0);
    if (expected_granules != granule_count)
        throw std::runtime_error("Compact symbols granule count mismatch");
    if (marks_size != static_cast<uint64_t>(granule_count) * mark_size)
        throw std::runtime_error("Compact symbols marks size mismatch");
    if (!rangeWithin(dictionary_offset, dictionary_size, data.size()) || !rangeWithin(marks_offset, marks_size, data.size())
        || !rangeWithin(names_offset, names_size, data.size()) || !rangeWithin(addresses_offset, addresses_size, data.size()))
        throw std::runtime_error("Compact symbols part is out of bounds");
    if (dictionary_offset < header_size || marks_offset < dictionary_offset + dictionary_size || names_offset < marks_offset + marks_size
        || addresses_offset < names_offset + names_size)
        throw std::runtime_error("Compact symbols parts overlap");
    if (address_count > maximumFrameContentSize(static_cast<size_t>(addresses_size)) / minimum_address_entry_size)
        throw std::runtime_error("Compact symbol address count exceeds the compressed stream");

    uint64_t granule_begin = 0;
    for (uint32_t granule = 0; granule < granule_count; ++granule)
    {
        uint64_t granule_end = readLittleEndian<uint32_t>(data, marks_offset + static_cast<uint64_t>(granule) * mark_size);
        if (granule_end <= granule_begin || granule_end > names_size)
            throw std::runtime_error("Invalid compact symbols name mark");
        granule_begin = granule_end;
    }
    if (granule_begin != names_size)
        throw std::runtime_error("Compact symbols names blob size mismatch");

    if (dictionary_size != 0)
    {
        std::string_view dictionary_data = data.substr(dictionary_offset, dictionary_size);
        dictionary_id = ZSTD_getDictID_fromDict(dictionary_data.data(), dictionary_data.size());
        if (dictionary_id == 0)
            throw std::runtime_error("Invalid compact symbols dictionary header");

        size_t workspace_size = ZSTD_estimateDDictSize(dictionary_data.size(), ZSTD_dlm_byRef);
        size_t workspace_words = (workspace_size + sizeof(uint64_t) - 1) / sizeof(uint64_t);
        dictionary_workspace = std::make_unique_for_overwrite<uint64_t[]>(workspace_words);
        dictionary = ZSTD_initStaticDDict(
            dictionary_workspace.get(),
            workspace_words * sizeof(uint64_t),
            dictionary_data.data(),
            dictionary_data.size(),
            ZSTD_dlm_byRef,
            ZSTD_dct_fullDict);
        if (!dictionary)
            throw std::runtime_error("Invalid compact symbols dictionary");
    }

    for (uint32_t granule_index = 0; granule_index < granule_count; ++granule_index)
    {
        std::string_view compressed = compressedNameGranule(granule_index);
        checkSingleFrame(compressed);
        if (ZSTD_getDictID_fromFrame(compressed.data(), compressed.size()) != dictionary_id)
            throw std::runtime_error("Compact symbol name granule does not match the dictionary");
        maximum_name_granule_size = std::max(maximum_name_granule_size, nameGranuleContentSize(granule_index));
    }

    validateNames();
}

std::vector<AddressEntry> Reader::decodeAddresses() const
{
    std::string_view compressed = data.substr(addresses_offset, addresses_size);
    checkSingleFrame(compressed);
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

    uint64_t begin = granule_index == 0
        ? 0
        : readLittleEndian<uint32_t>(data, marks_offset + static_cast<uint64_t>(granule_index - 1) * mark_size);
    uint64_t end = readLittleEndian<uint32_t>(data, marks_offset + static_cast<uint64_t>(granule_index) * mark_size);
    return data.substr(names_offset + begin, end - begin);
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

void Reader::validateNames()
{
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> decompression_context(ZSTD_createDCtx(), &ZSTD_freeDCtx);
    if (!decompression_context)
        throw std::bad_alloc();

    std::vector<char> granule_buffer(maximum_name_granule_size);
    std::string name;

    uint64_t decoded_names = 0;
    for (uint32_t granule_index = 0; granule_index < granule_count; ++granule_index)
    {
        auto decoder = decodeNameGranule(granule_index, decompression_context.get(), granule_buffer);
        name.clear();
        while (decoder.next(name))
        {
            ++decoded_names;
            maximum_name_size = std::max(maximum_name_size, name.size());
        }
    }

    if (decoded_names != name_count)
        throw std::runtime_error("Compact symbol name count mismatch");
}

NameGranuleDecoder Reader::decodeNameGranule(uint32_t granule_index, ZSTD_DCtx * decompression_context, std::span<char> destination) const
{
    if (!decompression_context)
        throw std::runtime_error("Compact symbol decompression context is missing");

    std::string_view compressed = compressedNameGranule(granule_index);
    size_t expected_size = nameGranuleContentSize(granule_index);
    if (destination.size() < expected_size)
        throw std::runtime_error("Compact symbol name granule destination is too small");

    /// With a null dictionary this decodes a plain frame; it never uses a dictionary loaded into the context.
    size_t decoded_size = ZSTD_decompress_usingDDict(
        decompression_context, destination.data(), destination.size(), compressed.data(), compressed.size(), dictionary);
    checkZstd(decoded_size, "decompress");
    if (decoded_size != expected_size)
        throw std::runtime_error("Compact symbols zstd frame size mismatch");

    uint64_t first_name_index = static_cast<uint64_t>(granule_index) * names_per_granule;
    size_t count = static_cast<size_t>(std::min<uint64_t>(names_per_granule, name_count - first_name_index));
    return NameGranuleDecoder(std::span<const char>(destination.data(), decoded_size), count);
}

}
