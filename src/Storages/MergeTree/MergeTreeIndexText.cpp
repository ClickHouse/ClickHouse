#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include "DataTypes/Serializations/SerializationNumber.h"
#include "DataTypes/Serializations/SerializationString.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DictionaryBlock::DictionaryBlock(ColumnPtr tokens_, ColumnPtr marks_) : tokens(tokens_), marks(marks_)
{
}

bool DictionaryBlock::empty() const
{
    chassert(tokens && marks && tokens->size() == marks->size());
    return tokens->empty();
}

MergeTreeIndexGranuleText::MergeTreeIndexGranuleText()
{
}

void MergeTreeIndexGranuleText::serializeBinary(WriteBuffer & ostr) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Serialization of MergeTreeIndexGranuleText is not implemented");
}

void MergeTreeIndexGranuleText::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
}

MergeTreeIndexGranuleTextWritable::MergeTreeIndexGranuleTextWritable(
    BloomFilter bloom_filter_,
    std::vector<StringRef> tokens_,
    std::vector<roaring::Roaring> posting_lists_,
    std::unique_ptr<Arena> arena_)
    : bloom_filter(bloom_filter_), tokens(tokens_), posting_lists(posting_lists_), arena(std::move(arena_))
{
}

UInt64 getCompressedMark(const MarkInCompressedFile & begin_mark, const MarkInCompressedFile & current_mark)
{
    UInt32 offset_in_compressed_file = current_mark.offset_in_compressed_file - begin_mark.offset_in_compressed_file;
    UInt32 offset_in_decompressed_block = static_cast<UInt32>(current_mark.offset_in_decompressed_block);
    return (static_cast<UInt64>(offset_in_compressed_file) << 32) | offset_in_decompressed_block;
}

template <typename Stream>
ColumnPtr serializePostings(const std::vector<roaring::Roaring> & postings, Stream & stream)
{
    auto begin_mark = stream.getCurrentMark();

    auto marks_column = ColumnUInt64::create(postings.size());
    auto & marks_data = marks_column->getData();
    marks_data.reserve(postings.size());

    SerializationNumber<UInt32> serialization;
    auto postings_columns = ColumnUInt32::create();
    auto & postings_data = postings_columns->getData();

    for (const auto & posting_list : postings)
    {
        auto current_mark = stream.getCurrentMark();
        marks_data.emplace_back(getCompressedMark(begin_mark, current_mark));

        postings_data.resize(posting_list.cardinality());
        posting_list.toUint32Array(postings_data.data());
        serialization.serializeBinaryBulk(*postings_columns, stream.compressed_hashing, 0, posting_list.cardinality());
    }

    return marks_column;
}

template <typename Stream>
DictionaryBlock serializeDictionary(const std::vector<StringRef> & tokens, Stream & stream, size_t block_size)
{
    auto begin_mark = stream.getCurrentMark();

    auto marks_column = ColumnUInt64::create(tokens.size());
    auto & marks_data = marks_column->getData();
    marks_data.reserve(tokens.size() / block_size);

    SerializationString serialization;
    auto sparse_index = ColumnString::create();
    auto & sparse_index_str = assert_cast<ColumnString &>(*sparse_index);
    sparse_index_str.reserve(tokens.size() / block_size);

    for (size_t i = 0; i < tokens.size(); ++i)
    {
        if (i % block_size == 0)
        {
            auto current_mark = stream.getCurrentMark();
            marks_data.emplace_back(getCompressedMark(begin_mark, current_mark));
            sparse_index_str.insertData(tokens[i].data, tokens[i].size);
        }

        writeVarUInt(tokens[i].size, stream.compressed_hashing);
        stream.compressed_hashing.write(tokens[i].data, tokens[i].size);
    }

    return DictionaryBlock(std::move(sparse_index), std::move(marks_column));
}

template <typename Stream>
void serializeBloomFilter(const BloomFilter & bloom_filter, Stream & stream)
{
}

template <typename Stream>
void serializeSparseIndex(const DictionaryBlock & sparse_index, Stream & stream)
{
}

void MergeTreeIndexGranuleTextWritable::serializeBinary(WriteBuffer & ostr) const
{
    UNUSED(ostr);

    using Stream = MergeTreeDataPartWriterOnDisk::Stream<false>;

    Stream * index_stream = nullptr;
    Stream * dictionary_stream = nullptr;
    Stream * postings_stream = nullptr;

    auto marks_to_postings = serializePostings(posting_lists, *postings_stream);
    auto sparse_index_block = serializeDictionary(tokens, *dictionary_stream, 256);
    serializeBloomFilter(bloom_filter, *index_stream);
    serializeSparseIndex(sparse_index_block, *index_stream);
}

void MergeTreeIndexGranuleTextWritable::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deserialization of MergeTreeIndexGranuleTextWritable is not implemented");
}

MergeTreeIndexTextGranuleBuilder::MergeTreeIndexTextGranuleBuilder(const BloomFilterParameters & bloom_filter_params_, TokenExtractorPtr token_extractor_)
    : bloom_filter(bloom_filter_params_), token_extractor(token_extractor_), arena(std::make_unique<Arena>())
{
}

void MergeTreeIndexTextGranuleBuilder::addDocument(StringRef document)
{
    for (const auto & token : token_extractor->getTokensView(document.data, document.size))
    {
        bool inserted;
        TermsMap::LookupResult it;
        terms_map.emplace(token, it, inserted);

        if (inserted)
        {
            arena->insert(token.data(), token.size());
            bloom_filter.add(token.data(), token.size());
            it->getMapped() = &posting_lists.emplace_back();
        }

        it->getMapped()->add(current_row);
    }

    ++current_row;
}

std::unique_ptr<MergeTreeIndexGranuleTextWritable> MergeTreeIndexTextGranuleBuilder::build()
{
    std::vector<std::pair<StringRef, roaring::Roaring *>> sorted_values;
    sorted_values.reserve(terms_map.size());

    for (auto it = terms_map.begin(); it != terms_map.end(); ++it)
        sorted_values.emplace_back(it->getKey(), it->getMapped());

    std::sort(sorted_values.begin(), sorted_values.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

    std::vector<StringRef> sorted_terms;
    std::vector<roaring::Roaring> sorted_posting_lists;

    sorted_terms.reserve(sorted_values.size());
    sorted_posting_lists.reserve(sorted_values.size());

    for (const auto & [token, posting_list] : sorted_values)
    {
        sorted_terms.emplace_back(token);
        sorted_posting_lists.emplace_back(std::move(*posting_list));
    }

    return std::make_unique<MergeTreeIndexGranuleTextWritable>(
        std::move(bloom_filter),
        std::move(sorted_terms),
        std::move(sorted_posting_lists),
        std::move(arena));
}

MergeTreeIndexAggregatorText::MergeTreeIndexAggregatorText(String index_column_name_, BloomFilterParameters bloom_filter_params_, TokenExtractorPtr token_extractor_)
    : index_column_name(std::move(index_column_name_)), bloom_filter_params(std::move(bloom_filter_params_)), token_extractor(token_extractor_), granule_builder(bloom_filter_params)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorText::getGranuleAndReset()
{
    auto granule = granule_builder->build();
    granule_builder.emplace(bloom_filter_params, token_extractor);
    return granule;
}

void MergeTreeIndexAggregatorText::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (rows_read == 0)
        return;

    size_t current_position = *pos;
    const auto & index_column = block.getByName(index_column_name).column;

    for (size_t i = 0; i < rows_read; ++i)
    {
        auto ref = index_column->getDataAt(current_position + i);
        granule_builder->addDocument(ref);
    }

    *pos += rows_read;
}

}
