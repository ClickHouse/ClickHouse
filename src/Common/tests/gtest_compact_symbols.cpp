#include <Common/CompactSymbols.h>
#include <IO/ZstdContext.h>

#include <algorithm>
#include <new>
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

}
}
