#include <gtest/gtest.h>

#include <Interpreters/ITokenizer.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <Common/PODArray.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <string>
#include <vector>

using namespace DB;

namespace
{

using Occurrences = std::vector<std::pair<UInt32, UInt32>>;

/// `forEachToken` reads past the end of the document, so its buffer must be padded.
struct PaddedDocument
{
    explicit PaddedDocument(std::string_view text) : buffer(text), length(text.size()) { buffer.resize(length + 16); }
    std::string_view view() const { return {buffer.data(), length}; }

    std::string buffer;
    size_t length;
};

/// `addToken` hashes through StringHashTable, which reads past both ends of the key.
PaddedPODArray<UInt8> paddedToken(std::string_view text)
{
    PaddedPODArray<UInt8> token(text.size());
    memcpy(token.data(), text.data(), text.size());
    return token;
}

std::map<std::string, Occurrences> collectPositions(MergeTreeIndexTextGranuleBuilder & builder)
{
    std::map<std::string, Occurrences> result;

    builder.tokens_map.forEachValue([&](const auto & key, auto & mapped)
    {
        const auto * positions = mapped.getPositions();
        if (!positions)
            return;

        auto & occurrences = result[std::string(static_cast<std::string_view>(key))];
        for (const auto & entry : positions->getEntries())
            for (UInt32 bit = 0; bit < RoaringishEntry::BITMAP_BITS; ++bit)
                if (entry.bitmap & (1U << bit))
                    occurrences.emplace_back(entry.doc_id, entry.group * RoaringishEntry::BITMAP_BITS + bit);
    });

    for (auto & [_, occurrences] : result)
        std::sort(occurrences.begin(), occurrences.end());

    return result;
}

MergeTreeIndexTextParams paramsWithPositions()
{
    MergeTreeIndexTextParams params;
    params.positions = 1;
    return params;
}

}

/// A row built from several documents (the elements of an Array column) keeps one position sequence.
TEST(MergeTreeIndexText, TokenPositionsAreContinuousWithinRow)
{
    SplitByNonAlphaTokenizer tokenizer;
    auto codec = PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::Bitpacking);
    MergeTreeIndexTextGranuleBuilder builder(paramsWithPositions(), &tokenizer, codec.get());
    const auto context = builder.buildContext();

    PaddedDocument first("quick brown");
    PaddedDocument second("fox jumps");
    builder.addDocument(first.view(), context);
    builder.addDocument(second.view(), context);
    builder.incrementCurrentRow();

    PaddedDocument third("brown fox");
    builder.addDocument(third.view(), context);
    builder.incrementCurrentRow();

    auto positions = collectPositions(builder);
    EXPECT_EQ(positions["quick"], (Occurrences{{0, 0}}));
    EXPECT_EQ(positions["brown"], (Occurrences{{0, 1}, {1, 0}}));
    EXPECT_EQ(positions["fox"], (Occurrences{{0, 2}, {1, 1}}));
    EXPECT_EQ(positions["jumps"], (Occurrences{{0, 3}}));
}

/// Documents and verbatim tokens advance the same per-row sequence.
TEST(MergeTreeIndexText, TokenPositionsOfDocumentsAndTokens)
{
    SplitByNonAlphaTokenizer tokenizer;
    auto codec = PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::Bitpacking);
    MergeTreeIndexTextGranuleBuilder builder(paramsWithPositions(), &tokenizer, codec.get());
    const auto context = builder.buildContext();

    PaddedDocument document("quick brown");
    auto token = paddedToken("fox");

    builder.addDocument(document.view(), context);
    builder.addToken({reinterpret_cast<const char *>(token.data()), token.size()}, context);
    builder.incrementCurrentRow();

    auto positions = collectPositions(builder);
    EXPECT_EQ(positions["quick"], (Occurrences{{0, 0}}));
    EXPECT_EQ(positions["brown"], (Occurrences{{0, 1}}));
    EXPECT_EQ(positions["fox"], (Occurrences{{0, 2}}));
}
