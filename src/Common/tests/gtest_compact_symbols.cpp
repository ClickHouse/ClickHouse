#include <IO/ZstdContext.h>
#include <Common/CompactSymbols.h>
#include <base/defines.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace DB::CompactSymbols
{
namespace
{

constexpr size_t granule_count_field = 36;
constexpr size_t version_field = 8;
constexpr size_t dictionary_offset_field = 40;
constexpr size_t dictionary_size_field = 48;
constexpr size_t marks_offset_field = 56;
constexpr size_t names_offset_field = 72;
constexpr size_t names_size_field = 80;
constexpr size_t addresses_offset_field = 88;
constexpr size_t addresses_size_field = 96;

/// A step of a linear congruential generator: the multiplication and the addition are modulo 2^64.
NO_SANITIZE_UNSIGNED_OVERFLOW uint64_t nextRandom(uint64_t state)
{
    return state * 6364136223846793005ULL + 1442695040888963407ULL;
}

struct DecodedSymbol
{
    uint64_t address;
    uint64_t size;
    std::string name;

    bool operator==(const DecodedSymbol &) const = default;
};

template <typename T>
T readField(const std::vector<char> & data, size_t offset)
{
    if (offset > data.size() || sizeof(T) > data.size() - offset)
        throw std::runtime_error("Compact symbols test field is out of bounds");

    T value = 0;
    for (size_t byte = 0; byte < sizeof(T); ++byte)
        value |= static_cast<T>(static_cast<unsigned char>(data[offset + byte])) << (byte * 8);
    return value;
}

template <typename T>
void writeField(std::vector<char> & data, size_t offset, T value)
{
    if (offset > data.size() || sizeof(T) > data.size() - offset)
        throw std::runtime_error("Compact symbols test field is out of bounds");

    for (size_t byte = 0; byte < sizeof(T); ++byte)
        data[offset + byte] = static_cast<char>(value >> (byte * 8));
}

std::string_view nameGranuleFrame(const std::vector<char> & data, uint32_t granule)
{
    size_t marks_offset = readField<uint64_t>(data, marks_offset_field);
    size_t names_offset = readField<uint64_t>(data, names_offset_field);
    size_t begin = granule == 0 ? 0 : readField<uint32_t>(data, marks_offset + (granule - 1) * sizeof(uint32_t));
    size_t end = readField<uint32_t>(data, marks_offset + granule * sizeof(uint32_t));
    return std::string_view(data.data() + names_offset + begin, end - begin);
}

/// `value + delta` for a delta that may be negative.
template <typename T>
T shifted(T value, int64_t delta)
{
    return static_cast<T>(static_cast<int64_t>(value) + delta);
}

/// Splices `frame` in place of a name granule and shifts the marks and the parts after it.
void replaceNameGranule(std::vector<char> & data, uint32_t granule, std::string_view frame)
{
    size_t marks_offset = readField<uint64_t>(data, marks_offset_field);
    size_t names_offset = readField<uint64_t>(data, names_offset_field);
    uint32_t granule_count = readField<uint32_t>(data, granule_count_field);
    std::string_view old_frame = nameGranuleFrame(data, granule);
    size_t begin = old_frame.data() - data.data();
    int64_t delta = static_cast<int64_t>(frame.size()) - static_cast<int64_t>(old_frame.size());

    data.erase(data.begin() + begin, data.begin() + begin + old_frame.size());
    data.insert(data.begin() + begin, frame.begin(), frame.end());
    for (uint32_t index = granule; index < granule_count; ++index)
    {
        size_t mark = marks_offset + index * sizeof(uint32_t);
        writeField<uint32_t>(data, mark, shifted(readField<uint32_t>(data, mark), delta));
    }
    writeField<uint64_t>(data, names_size_field, shifted(readField<uint64_t>(data, names_size_field), delta));
    writeField<uint64_t>(data, addresses_offset_field, shifted(readField<uint64_t>(data, addresses_offset_field), delta));
    if (readField<uint64_t>(data, names_offset_field) != names_offset)
        throw std::runtime_error("Compact symbols test moved the names part");
}

std::string compressFrame(std::string_view payload, std::string_view dictionary, bool checksum = true)
{
    ZstdCCtxPtr context(ZSTD_createCCtx());
    if (!context)
        throw std::bad_alloc();
    if (ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_compressionLevel, compression_level))
        || ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_checksumFlag, checksum ? 1 : 0))
        || ZSTD_isError(ZSTD_CCtx_loadDictionary(context.get(), dictionary.data(), dictionary.size())))
        throw std::runtime_error("Compact symbols test cannot configure compression");
    std::string frame(ZSTD_compressBound(payload.size()), '\0');
    size_t size = ZSTD_compress2(context.get(), frame.data(), frame.size(), payload.data(), payload.size());
    if (ZSTD_isError(size))
        throw std::runtime_error(std::string("Compact symbols test cannot compress: ") + ZSTD_getErrorName(size));
    frame.resize(size);
    return frame;
}

std::string decompressFrame(std::string_view frame, std::string_view dictionary)
{
    ZstdDCtxPtr context(ZSTD_createDCtx());
    if (!context)
        throw std::bad_alloc();
    /// zstd defines these as `0ULL - 1` and `0ULL - 2`; as constants they are computed at compile time.
    static constexpr auto content_size_unknown = ZSTD_CONTENTSIZE_UNKNOWN;
    static constexpr auto content_size_error = ZSTD_CONTENTSIZE_ERROR;
    const auto content_size = ZSTD_getFrameContentSize(frame.data(), frame.size());
    if (content_size == content_size_error || content_size == content_size_unknown)
        throw std::runtime_error("Compact symbols test frame has no content size");
    std::string payload(content_size, '\0');
    size_t size = ZSTD_decompress_usingDict(
        context.get(), payload.data(), payload.size(), frame.data(), frame.size(), dictionary.data(), dictionary.size());
    if (ZSTD_isError(size) || size != payload.size())
        throw std::runtime_error("Compact symbols test cannot decompress");
    return payload;
}

std::string_view dictionaryOf(const std::vector<char> & data)
{
    size_t offset = readField<uint64_t>(data, dictionary_offset_field);
    size_t size = readField<uint64_t>(data, dictionary_size_field);
    return std::string_view(data.data() + offset, size);
}

/// The same frame with an 8-byte content size field advertising 2^60 bytes. The rest of the frame,
/// including its blocks and checksum, is unchanged, so it stays a structurally valid single frame.
std::string withHugeContentSize(std::string_view frame)
{
    constexpr uint64_t advertised_size = uint64_t{1} << 60;
    constexpr size_t descriptor_offset = 4;
    if (frame.size() <= descriptor_offset)
        throw std::runtime_error("Compact symbols test frame is too small");

    auto descriptor = static_cast<uint8_t>(frame[descriptor_offset]);
    bool single_segment = descriptor & 0x20;
    constexpr std::array<size_t, 4> dictionary_id_sizes{0, 1, 2, 4};
    std::array<size_t, 4> content_size_sizes{single_segment ? size_t{1} : size_t{0}, 2, 4, 8};
    size_t kept_size = (single_segment ? 0 : 1) + dictionary_id_sizes[descriptor & 3];
    size_t header_size = descriptor_offset + 1 + kept_size + content_size_sizes[descriptor >> 6];

    std::string result(frame.substr(0, descriptor_offset));
    result += static_cast<char>((descriptor & 0x3f) | 0xc0);
    result += frame.substr(descriptor_offset + 1, kept_size);
    for (size_t byte = 0; byte < sizeof(advertised_size); ++byte)
        result += static_cast<char>(advertised_size >> (byte * 8));
    result += frame.substr(header_size);

    if (ZSTD_getFrameContentSize(result.data(), result.size()) != advertised_size
        || ZSTD_findFrameCompressedSize(result.data(), result.size()) != result.size())
        throw std::runtime_error("Compact symbols test cannot enlarge the content size of a frame");
    return result;
}

void replaceAddressesFrame(std::vector<char> & data, std::string_view frame)
{
    size_t addresses_offset = readField<uint64_t>(data, addresses_offset_field);
    if (addresses_offset + readField<uint64_t>(data, addresses_size_field) != data.size())
        throw std::runtime_error("Compact symbols test expects the addresses part at the end");
    data.resize(addresses_offset);
    data.insert(data.end(), frame.begin(), frame.end());
    writeField<uint64_t>(data, addresses_size_field, frame.size());
}

std::string_view addressesFrame(const std::vector<char> & data)
{
    return std::string_view(data.data() + readField<uint64_t>(data, addresses_offset_field), readField<uint64_t>(data, addresses_size_field));
}

template <typename F>
void expectExpansionBoundRejection(F && function)
{
    try
    {
        function();
        ADD_FAILURE() << "Expected the expansion bound to reject the frame";
    }
    catch (const std::runtime_error & e)
    {
        EXPECT_NE(std::string_view(e.what()).find("maximum expansion ratio"), std::string_view::npos) << e.what();
    }
}

std::vector<std::string> decodeNames(const Reader & reader, std::vector<size_t> * granule_sizes = nullptr)
{
    ZstdDCtxPtr decompression_context(ZSTD_createDCtx());
    if (!decompression_context)
        throw std::bad_alloc();

    std::vector<char> granule_buffer(reader.maximumNameGranuleSize());
    std::string name_buffer;
    name_buffer.reserve(reader.maximumNameSize());

    std::vector<std::string> names;
    for (uint32_t granule = 0; granule < reader.granuleCount(); ++granule)
    {
        size_t names_before_granule = names.size();
        auto decoder = reader.decodeNameGranule(granule, decompression_context.get(), granule_buffer);
        while (decoder.next(name_buffer))
            names.push_back(name_buffer);
        if (granule_sizes)
            granule_sizes->push_back(names.size() - names_before_granule);
    }
    return names;
}

std::vector<DecodedSymbol> decodeSymbols(const Reader & reader)
{
    auto names = decodeNames(reader);
    auto addresses = reader.decodeAddresses();
    std::vector<DecodedSymbol> result;
    result.reserve(addresses.size());
    for (const auto & address : addresses)
        result.push_back({address.address, address.size, names.at(address.name_index)});
    return result;
}

/// Small enough to train on a small input. The format behaves the same for any capacity.
constexpr size_t test_dictionary_capacity = 4096;

/// Names whose front-coded granules are just large enough to train a dictionary of `test_dictionary_capacity`.
struct LargeInput
{
    LargeInput()
    {
        static constexpr std::array<std::string_view, 16> words{
            "Aggregate", "Column", "Merge", "Tree", "Function", "Reader", "Buffer", "Context",
            "Storage", "Settings", "Pipeline", "Processor", "Execute", "Transform", "Serialize", "Hash"};

        uint64_t state = 1;
        size_t front_coded_size = 0;
        constexpr size_t training_size = test_dictionary_capacity * dictionary_training_ratio;
        while (front_coded_size < training_size + training_size / 10)
        {
            /// A random token after the namespace keeps the prefix shared by sorted neighbours short.
            state = nextRandom(state);
            std::string name = "_ZN2DB" + std::to_string(state >> 32);
            for (size_t word = 0; word < 8; ++word)
            {
                state = nextRandom(state);
                std::string_view chosen = words[(state >> 59) & 15];
                name += std::to_string(chosen.size());
                name += chosen;
            }
            name += "Ev";
            constexpr size_t maximum_expected_shared_prefix = 16;
            front_coded_size += name.size() - maximum_expected_shared_prefix + 2;
            names.push_back(std::move(name));
        }

        symbols.reserve(names.size());
        for (size_t index = 0; index < names.size(); ++index)
            symbols.push_back({0x1000 + index * 16, 16, names[index]});
        encoded = encode(symbols, test_dictionary_capacity);
    }

    std::vector<std::string> names;
    std::vector<Symbol> symbols;
    std::vector<char> encoded;
};

const LargeInput & largeInput()
{
    static const LargeInput input;
    return input;
}

std::vector<char> encodeSingleName()
{
    std::vector<Symbol> symbols{{0x1234, 17, "only_symbol"}};
    return encode(symbols);
}

TEST(CompactSymbols, Empty)
{
    std::vector<Symbol> symbols;
    auto encoded = encode(symbols);
    Reader reader(std::string_view(encoded.data(), encoded.size()));

    EXPECT_EQ(reader.nameCount(), 0);
    EXPECT_EQ(reader.addressCount(), 0);
    EXPECT_EQ(reader.granuleCount(), 0);
    EXPECT_EQ(reader.dictionarySize(), 0);
    EXPECT_EQ(reader.maximumNameGranuleSize(), 0);
    EXPECT_EQ(reader.maximumNameSize(), 0);
    EXPECT_TRUE(reader.decodeAddresses().empty());
    EXPECT_TRUE(decodeNames(reader).empty());
}

TEST(CompactSymbols, AddressAndNameRoundTrip)
{
    std::vector<Symbol> symbols{
        {0x120, 7, "prefix_gamma"},
        {0x100, 3, "prefix_alpha"},
        {0x120, 11, "prefix_beta"},
        {0x105, 5, "prefix_alpha"},
    };
    auto encoded = encode(symbols);
    Reader reader(std::string_view(encoded.data(), encoded.size()));

    EXPECT_EQ(reader.nameCount(), 3);
    EXPECT_EQ(reader.addressCount(), 4);
    EXPECT_EQ(reader.dictionarySize(), 0);
    EXPECT_EQ(reader.maximumNameSize(), std::string_view("prefix_alpha").size());
    EXPECT_EQ(
        decodeSymbols(reader),
        (std::vector<DecodedSymbol>{
            {0x100, 3, "prefix_alpha"},
            {0x105, 5, "prefix_alpha"},
            {0x120, 7, "prefix_gamma"},
            {0x120, 11, "prefix_beta"},
        }));
}

TEST(CompactSymbols, SingleName)
{
    auto encoded = encodeSingleName();
    Reader reader(std::string_view(encoded.data(), encoded.size()));

    EXPECT_EQ(reader.granuleCount(), 1);
    EXPECT_EQ(decodeNames(reader), (std::vector<std::string>{"only_symbol"}));
    EXPECT_EQ(decodeSymbols(reader), (std::vector<DecodedSymbol>{{0x1234, 17, "only_symbol"}}));
}

TEST(CompactSymbols, NameGranuleBoundaries)
{
    std::vector<std::string> storage;
    storage.reserve(names_per_granule + 1);
    for (uint32_t index = 0; index < names_per_granule + 1; ++index)
        storage.push_back("long_common_prefix_for_symbol_" + std::to_string(100000 + index));

    std::vector<Symbol> symbols;
    symbols.reserve(storage.size());
    for (uint32_t index = 0; index < storage.size(); ++index)
        symbols.push_back({0x1000 + index * 4, 4, storage[index]});

    auto encoded = encode(symbols);
    Reader reader(std::string_view(encoded.data(), encoded.size()));
    std::vector<size_t> granule_sizes;
    auto decoded_names = decodeNames(reader, &granule_sizes);
    std::vector<std::string> expected = storage;
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(reader.granuleCount(), 2);
    EXPECT_EQ(granule_sizes, (std::vector<size_t>{names_per_granule, 1}));
    EXPECT_EQ(decoded_names, expected);
}

TEST(CompactSymbols, DictionaryRoundTripAndDeterminism)
{
    const auto & input = largeInput();
    Reader reader(std::string_view(input.encoded.data(), input.encoded.size()));

    EXPECT_GT(reader.dictionarySize(), 0);
    EXPECT_LE(reader.dictionarySize(), test_dictionary_capacity);
    std::vector<std::string> expected = input.names;
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(decodeNames(reader), expected);
    size_t maximum_name_size = 0;
    for (const auto & name : input.names)
        maximum_name_size = std::max(maximum_name_size, name.size());
    EXPECT_EQ(reader.maximumNameSize(), maximum_name_size);

    auto addresses = reader.decodeAddresses();
    ASSERT_EQ(addresses.size(), input.symbols.size());
    for (size_t index = 0; index < addresses.size(); ++index)
    {
        EXPECT_EQ(addresses[index].address, input.symbols[index].address);
        EXPECT_EQ(expected.at(addresses[index].name_index), input.symbols[index].name);
    }

    EXPECT_EQ(encode(input.symbols, test_dictionary_capacity), input.encoded);
}

TEST(CompactSymbols, RejectsUnsupportedVersion)
{
    auto encoded = encodeSingleName();
    for (uint32_t version : {format_version - 1, format_version + 1})
    {
        auto patched = encoded;
        writeField<uint32_t>(patched, version_field, version);
        EXPECT_THROW((void)Reader(std::string_view(patched.data(), patched.size())), std::runtime_error);
    }
}

TEST(CompactSymbols, RejectsCorruptedDictionary)
{
    const auto & encoded = largeInput().encoded;
    size_t dictionary_offset = readField<uint64_t>(encoded, dictionary_offset_field);
    size_t dictionary_size = readField<uint64_t>(encoded, dictionary_size_field);

    /// The magic, the entropy tables right after the dictionary id, and the tail of the content.
    const std::array<std::pair<size_t, size_t>, 3> corruptions{{{0, 1}, {8, 64}, {dictionary_size - dictionary_size / 4, dictionary_size / 4}}};
    for (auto [begin, size] : corruptions)
    {
        auto corrupted = encoded;
        for (size_t index = begin; index < begin + size; ++index)
            corrupted[dictionary_offset + index] ^= static_cast<char>(0x5a);
        EXPECT_THROW((void)Reader(std::string_view(corrupted.data(), corrupted.size())), std::runtime_error) << begin;
    }
}

TEST(CompactSymbols, RejectsFrameNotMatchingDictionary)
{
    const auto & with_dictionary = largeInput().encoded;
    std::string_view dictionary = dictionaryOf(with_dictionary);
    std::string payload = decompressFrame(nameGranuleFrame(with_dictionary, 0), dictionary);

    {
        auto patched = with_dictionary;
        replaceNameGranule(patched, 0, compressFrame(payload, {}));
        EXPECT_THROW((void)Reader(std::string_view(patched.data(), patched.size())), std::runtime_error);
    }

    {
        std::string other_dictionary(dictionary);
        other_dictionary[4] ^= 1;
        auto patched = with_dictionary;
        replaceNameGranule(patched, 0, compressFrame(payload, other_dictionary));
        EXPECT_THROW((void)Reader(std::string_view(patched.data(), patched.size())), std::runtime_error);
    }

    {
        auto without_dictionary = encodeSingleName();
        std::string single_payload = decompressFrame(nameGranuleFrame(without_dictionary, 0), {});
        replaceNameGranule(without_dictionary, 0, compressFrame(single_payload, {}));
        EXPECT_NO_THROW((void)Reader(std::string_view(without_dictionary.data(), without_dictionary.size())));
        replaceNameGranule(without_dictionary, 0, compressFrame(single_payload, {}, /* checksum = */ false));
        EXPECT_THROW((void)Reader(std::string_view(without_dictionary.data(), without_dictionary.size())), std::runtime_error);
        replaceNameGranule(without_dictionary, 0, compressFrame(single_payload, dictionary));
        EXPECT_THROW((void)Reader(std::string_view(without_dictionary.data(), without_dictionary.size())), std::runtime_error);
    }
}

TEST(CompactSymbols, RejectsHugeAdvertisedAddressFrameSize)
{
    auto encoded = encodeSingleName();
    replaceAddressesFrame(encoded, withHugeContentSize(addressesFrame(encoded)));

    Reader reader(std::string_view(encoded.data(), encoded.size()));
    expectExpansionBoundRejection([&] { (void)reader.decodeAddresses(); });
}

TEST(CompactSymbols, RejectsHugeAdvertisedNameGranuleSize)
{
    auto encoded = encodeSingleName();
    replaceNameGranule(encoded, 0, withHugeContentSize(nameGranuleFrame(encoded, 0)));

    expectExpansionBoundRejection([&] { (void)Reader(std::string_view(encoded.data(), encoded.size())); });
}

TEST(CompactSymbols, RejectsInvalidNamePayload)
{
    auto encoded = encodeSingleName();
    EXPECT_NO_THROW((void)Reader(std::string_view(encoded.data(), encoded.size())));

    /// A well-formed frame whose front-coded payload is invalid: the first name must not share a prefix.
    replaceNameGranule(encoded, 0, compressFrame(std::string("\x01\x0b") + "only_symbol", {}));
    EXPECT_THROW((void)Reader(std::string_view(encoded.data(), encoded.size())), std::runtime_error);
}

}
}
