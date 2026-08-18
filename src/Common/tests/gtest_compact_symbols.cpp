#include <Common/CompactSymbols.h>

#include <algorithm>
#include <iterator>
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

std::vector<std::string> decodeNames(const Reader & reader)
{
    std::vector<std::string> names;
    for (uint32_t granule = 0; granule < reader.granuleCount(); ++granule)
    {
        auto decoded = reader.decodeNameGranule(granule);
        names.insert(names.end(), std::make_move_iterator(decoded.begin()), std::make_move_iterator(decoded.end()));
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
    auto decoded_names = decodeNames(reader);
    std::vector<std::string> expected = storage;
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(reader.granuleCount(), 2);
    EXPECT_EQ(reader.decodeNameGranule(0).size(), names_per_granule);
    EXPECT_EQ(reader.decodeNameGranule(1).size(), 1);
    EXPECT_EQ(decoded_names, expected);
}

}
}
