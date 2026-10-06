#include <IO/ZstdContext.h>
#include <Common/CompactSymbols.h>

#include <algorithm>
#include <array>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

namespace DB::CompactSymbols
{
namespace
{

struct DecodedSymbol
{
    uint64_t address;
    uint64_t size;
    std::string name;

    bool operator==(const DecodedSymbol &) const = default;
};

uint64_t readLittleEndian64(const std::vector<char> & data, size_t offset)
{
    if (offset > data.size() || sizeof(uint64_t) > data.size() - offset)
        throw std::runtime_error("Compact symbols test field is out of bounds");

    uint64_t value = 0;
    for (size_t byte = 0; byte < sizeof(uint64_t); ++byte)
        value |= static_cast<uint64_t>(static_cast<unsigned char>(data[offset + byte])) << (byte * 8);
    return value;
}

void advertiseHugeFrameContentSize(std::vector<char> & data, size_t frame_offset)
{
    constexpr uint64_t advertised_size = uint64_t{1} << 60;
    constexpr std::array<char, 5> frame_header{
        static_cast<char>(0x28),
        static_cast<char>(0xb5),
        static_cast<char>(0x2f),
        static_cast<char>(0xfd),
        static_cast<char>(0xe0),
    };
    constexpr size_t content_size_offset = frame_header.size();
    constexpr size_t frame_header_size = content_size_offset + sizeof(advertised_size);
    if (frame_offset > data.size() || frame_header_size > data.size() - frame_offset)
        throw std::runtime_error("Compact symbols test frame is too small");

    std::copy(frame_header.begin(), frame_header.end(), data.data() + frame_offset);
    for (size_t byte = 0; byte < sizeof(advertised_size); ++byte)
        data[frame_offset + content_size_offset + byte] = static_cast<char>(advertised_size >> (byte * 8));

    if (ZSTD_getFrameContentSize(data.data() + frame_offset, data.size() - frame_offset) != advertised_size)
        throw std::runtime_error("Compact symbols test frame does not advertise the expected size");
}

std::vector<std::string> decodeNames(const Reader & reader, std::vector<size_t> * granule_sizes = nullptr)
{
    ZstdDCtxPtr decompression_context(ZSTD_createDCtx());
    if (!decompression_context)
        throw std::bad_alloc();

    size_t maximum_granule_size = reader.maximumNameGranuleSize();
    std::vector<char> granule_buffer(maximum_granule_size);
    std::string name_buffer;
    name_buffer.reserve(maximum_granule_size);

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

TEST(CompactSymbols, Empty)
{
    std::vector<Symbol> symbols;
    auto encoded = encode(symbols);
    Reader reader(std::string_view(encoded.data(), encoded.size()));

    EXPECT_EQ(reader.nameCount(), 0);
    EXPECT_EQ(reader.addressCount(), 0);
    EXPECT_EQ(reader.granuleCount(), 0);
    EXPECT_EQ(reader.maximumNameGranuleSize(), 0);
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
    std::vector<Symbol> symbols{{0x1234, 17, "only_symbol"}};
    auto encoded = encode(symbols);
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

TEST(CompactSymbols, RejectsHugeAdvertisedAddressFrameSize)
{
    std::vector<Symbol> symbols{{0x1234, 17, "only_symbol"}};
    auto encoded = encode(symbols);
    constexpr size_t addresses_offset_field = 72;
    size_t addresses_offset = static_cast<size_t>(readLittleEndian64(encoded, addresses_offset_field));
    advertiseHugeFrameContentSize(encoded, addresses_offset);

    Reader reader(std::string_view(encoded.data(), encoded.size()));
    EXPECT_THROW(reader.decodeAddresses(), std::runtime_error);
}

TEST(CompactSymbols, RejectsHugeAdvertisedNameGranuleSize)
{
    std::vector<Symbol> symbols{{0x1234, 17, "only_symbol"}};
    auto encoded = encode(symbols);
    constexpr size_t names_offset_field = 56;
    size_t names_offset = static_cast<size_t>(readLittleEndian64(encoded, names_offset_field));
    advertiseHugeFrameContentSize(encoded, names_offset);

    Reader reader(std::string_view(encoded.data(), encoded.size()));
    EXPECT_THROW(reader.maximumNameGranuleSize(), std::runtime_error);
}

}
}
