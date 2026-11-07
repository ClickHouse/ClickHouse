#include <gtest/gtest.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/ITokenizer.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>

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
        auto * positions = mapped.getPositions();
        if (!positions)
            return;

        /// The tests add few rows, so all row ids are still in the raw buffer.
        const auto & large = mapped.getLarge();
        EXPECT_EQ(large.encoder, nullptr);
        const auto & rows = large.values;

        WriteBufferFromOwnString out;
        positions->finalize(out);
        const std::string blob = out.str();

        PaddedPODArray<UInt32> doc_offsets;
        PaddedPODArray<UInt32> doc_positions;
        TextIndexBlockedPositionsCodec::DecodeScratch scratch;
        ReadBufferFromMemory in(blob.data(), blob.size());
        TextIndexBlockedPositionsCodec::decodeAll(in, rows.size(), blob.size(), doc_offsets, doc_positions, scratch);

        auto & occurrences = result[std::string(static_cast<std::string_view>(key))];
        for (size_t rank = 0; rank < rows.size(); ++rank)
            for (size_t i = doc_offsets[rank]; i < doc_offsets[rank + 1]; ++i)
                occurrences.emplace_back(rows[rank], doc_positions[i]);
    });

    for (auto & [_, occurrences] : result)
        std::sort(occurrences.begin(), occurrences.end());

    return result;
}

ColumnPtr lowCardinalityArrayColumn(const Array & elements, size_t rows)
{
    auto column = DataTypeFactory::instance().get("Array(LowCardinality(String))")->createColumn();
    for (size_t i = 0; i < rows; ++i)
        column->insert(elements);
    return column;
}

void expectQuickBrownFoxRows(const std::map<std::string, Occurrences> & positions, size_t rows)
{
    Occurrences quick;
    Occurrences brown;
    Occurrences fox;
    for (UInt32 row = 0; row < rows; ++row)
    {
        quick.emplace_back(row, 0);
        brown.emplace_back(row, 1);
        fox.emplace_back(row, 2);
    }
    EXPECT_EQ(positions.at("quick"), quick);
    EXPECT_EQ(positions.at("brown"), brown);
    EXPECT_EQ(positions.at("fox"), fox);
}

MergeTreeIndexTextParams paramsWithPositions()
{
    MergeTreeIndexTextParams params;
    params.enable_positions = true;
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

/// 32 elements over 3 dictionary values enable the cache of tokenized dictionary values.
TEST(MergeTreeIndexText, TokenPositionsOfLowCardinalityArrayWithCache)
{
    SplitByNonAlphaTokenizer tokenizer;
    auto codec = PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::Bitpacking);
    MergeTreeIndexTextGranuleBuilder builder(paramsWithPositions(), &tokenizer, codec.get());

    constexpr size_t rows = 16;
    builder.addDocumentsFromLowCardinality(lowCardinalityArrayColumn(Array{"quick brown", "fox"}, rows), 0, rows, builder.buildContext());

    expectQuickBrownFoxRows(collectPositions(builder), rows);
}

TEST(MergeTreeIndexText, TokenPositionsOfLowCardinalityArrayWithoutCache)
{
    SplitByNonAlphaTokenizer tokenizer;
    auto codec = PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::Bitpacking);
    MergeTreeIndexTextGranuleBuilder builder(paramsWithPositions(), &tokenizer, codec.get());

    constexpr size_t rows = 2;
    builder.addDocumentsFromLowCardinality(lowCardinalityArrayColumn(Array{"quick brown", "fox"}, rows), 0, rows, builder.buildContext());

    expectQuickBrownFoxRows(collectPositions(builder), rows);
}
