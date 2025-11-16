#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <Interpreters/BloomFilterHash.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/HashTable/HashSet.h>
#include <Common/logger_useful.h>
#include <base/range.h>
#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event TextIndexReadDictionaryBlocks;
    extern const Event TextIndexReadSparseIndexBlocks;
    extern const Event TextIndexBloomFilterTrueNegatives;
    extern const Event TextIndexBloomFilterTruePositives;
    extern const Event TextIndexBloomFilterFalsePositives;
    extern const Event TextIndexReadGranulesMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int CORRUPTED_DATA;
}

static size_t getBloomFilterSizeInBytes(size_t bits_per_row, size_t num_tokens)
{
    static constexpr size_t atom_size = 8;
    return std::max(1UL, (bits_per_row * num_tokens + atom_size - 1) / atom_size);
}

static constexpr UInt64 MAX_CARDINALITY_FOR_RAW_POSTINGS = 16;
static constexpr UInt64 DEFAULT_NGRAM_SIZE = 3;
static constexpr UInt64 DEFAULT_DICTIONARY_BLOCK_SIZE = 128;
static constexpr UInt64 DEFAULT_MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS = 16;
/// 0.1 may seem quite high. The motivation of it is to minimize the size of the bloom filter.
/// Rate of 0.1 gives 5 bits per token. 0.05 gives 7 bits; 0.025 - 8 bits.
static constexpr double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.1; /// 10%

enum class TokensSerializationFormat : UInt64
{
    RawStrings = 0,
};

UInt32 TokenPostingsInfo::getCardinality() const
{
    return std::visit([]<typename T>(const T & arg) -> UInt32
    {
        if constexpr (std::is_same_v<T, PostingList>)
            return arg.cardinality();
        else
            return arg.cardinality;
    }, postings);
}

bool DictionaryBlockBase::empty() const
{
    return !tokens || tokens->empty();
}

size_t DictionaryBlockBase::size() const
{
    return tokens ? tokens->size() : 0;
}

size_t DictionaryBlockBase::lowerBound(const StringRef & token) const
{
    auto range = collections::range(0, tokens->size());

    auto it = std::lower_bound(range.begin(), range.end(), token, [this](size_t lhs_idx, const StringRef & rhs_ref)
    {
        return assert_cast<const ColumnString &>(*tokens).getDataAt(lhs_idx) < rhs_ref;
    });

    return it - range.begin();
}

size_t DictionaryBlockBase::upperBound(const StringRef & token) const
{
    auto range = collections::range(0, tokens->size());

    auto it = std::upper_bound(range.begin(), range.end(), token, [this](const StringRef & lhs_ref, size_t rhs_idx)
    {
        return lhs_ref < assert_cast<const ColumnString &>(*tokens).getDataAt(rhs_idx);
    });

    return it - range.begin();
}

std::optional<size_t> DictionaryBlockBase::binarySearch(const StringRef & token) const
{
    size_t idx = lowerBound(token);
    const auto & tokens_str = assert_cast<const ColumnString &>(*tokens);

    if (idx == tokens_str.size() || token != tokens_str.getDataAt(idx))
        return std::nullopt;

    return idx;
}

DictionarySparseIndex::DictionarySparseIndex(ColumnPtr tokens_, ColumnPtr offsets_in_file_)
    : DictionaryBlockBase(std::move(tokens_)), offsets_in_file(std::move(offsets_in_file_))
{
}

UInt64 DictionarySparseIndex::getOffsetInFile(size_t idx) const
{
    return assert_cast<const ColumnUInt64 &>(*offsets_in_file).getData()[idx];
}

DictionaryBlock::DictionaryBlock(ColumnPtr tokens_, std::vector<TokenPostingsInfo> token_infos_)
    : DictionaryBlockBase(std::move(tokens_))
    , token_infos(std::move(token_infos_))
{
}

void PostingsSerialization::serialize(UInt64 header, PostingList && postings, WriteBuffer & ostr)
{
    if (header & Flags::RawPostings)
    {
        for (const auto row_id : postings)
            writeVarUInt(row_id, ostr);
    }
    else
    {
        postings.runOptimize();
        size_t num_bytes = postings.getSizeInBytes();
        writeVarUInt(num_bytes, ostr);

        std::vector<char> memory(num_bytes);
        postings.write(memory.data());
        ostr.write(memory.data(), num_bytes);
    }
}

PostingList PostingsSerialization::deserialize(UInt64 header, UInt32 cardinality, ReadBuffer & istr)
{
    if (header & Flags::RawPostings)
    {
        std::vector<UInt32> values(cardinality);
        for (size_t i = 0; i < cardinality; ++i)
            readVarUInt(values[i], istr);

        PostingList postings;
        postings.addMany(cardinality, values.data());
        return postings;
    }

    size_t num_bytes;
    readVarUInt(num_bytes, istr);

    /// If the posting list is completely in the buffer, avoid copying.
    if (istr.position() && istr.position() + num_bytes <= istr.buffer().end())
    {
        return PostingList::read(istr.position());
    }

    std::vector<char> buf(num_bytes);
    istr.readStrict(buf.data(), num_bytes);
    return PostingList::read(buf.data());
}

MergeTreeIndexGranuleText::MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_)
    : params(std::move(params_))
    , bloom_filter(params.bloom_filter_bits_per_row, params.bloom_filter_num_hashes, 0)
{
}

void MergeTreeIndexGranuleText::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Serialization of MergeTreeIndexGranuleText is not implemented");
}

void MergeTreeIndexGranuleText::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be deserialized with 3 streams: index, dictionary, postings");
}

void MergeTreeIndexGranuleText::deserializeBloomFilter(ReadBuffer & istr)
{
    readVarUInt(num_tokens, istr);

    size_t bytes_size = getBloomFilterSizeInBytes(params.bloom_filter_bits_per_row, num_tokens);
    bloom_filter.resize(bytes_size);
    istr.readStrict(reinterpret_cast<char *>(bloom_filter.getFilter().data()), bytes_size);
}

static ColumnPtr deserializeTokens(ReadBuffer & istr)
{
    size_t num_tokens;
    readVarUInt(num_tokens, istr);

    auto tokens_column = ColumnString::create();
    tokens_column->reserve(num_tokens);

    SerializationString serialization_string;
    serialization_string.deserializeBinaryBulk(*tokens_column, istr, 0, num_tokens, 0.0);

    return tokens_column;
}

/// TODO: add cache for dictionary blocks
static DictionaryBlock deserializeDictionaryBlock(ReadBuffer & istr)
{
    ProfileEvents::increment(ProfileEvents::TextIndexReadDictionaryBlocks);

    UInt64 tokens_format;
    readVarUInt(tokens_format, istr);

    if (tokens_format != static_cast<UInt64>(TokensSerializationFormat::RawStrings))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown tokens serialization format ({}) in dictionary block", tokens_format);

    auto tokens_column = deserializeTokens(istr);
    size_t num_tokens = tokens_column->size();

    std::vector<TokenPostingsInfo> token_infos;
    token_infos.reserve(num_tokens);

    for (size_t i = 0; i < num_tokens; ++i)
    {
        UInt64 header;
        UInt32 cardinality;

        readVarUInt(header, istr);
        readVarUInt(cardinality, istr);

        if (header & PostingsSerialization::EmbeddedPostings)
        {
            auto postings = PostingsSerialization::deserialize(header, cardinality, istr);
            token_infos.emplace_back(std::move(postings));
        }
        else
        {
            UInt64 offset_in_file;
            readVarUInt(offset_in_file, istr);
            token_infos.emplace_back(TokenPostingsInfo::FuturePostings{header, cardinality, offset_in_file});
        }
    }

    return DictionaryBlock(std::move(tokens_column), std::move(token_infos));
}

/// TODO: add cache for dictionary sparse index
void MergeTreeIndexGranuleText::deserializeSparseIndex(ReadBuffer & istr)
{
    ProfileEvents::increment(ProfileEvents::TextIndexReadSparseIndexBlocks);

    sparse_index.tokens = deserializeTokens(istr);
    size_t num_index_tokens = sparse_index.tokens->size();

    auto offsets_in_file = ColumnUInt64::create();
    SerializationNumber<UInt64> serialization_number;
    serialization_number.deserializeBinaryBulk(*offsets_in_file, istr, 0, num_index_tokens, 0.0);
    sparse_index.offsets_in_file = std::move(offsets_in_file);
}

void MergeTreeIndexGranuleText::deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReadGranulesMicroseconds);

    auto * index_stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    auto * dictionary_stream = streams.at(MergeTreeIndexSubstream::Type::TextIndexDictionary);

    if (!index_stream || !dictionary_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be deserialized with 3 streams: index, dictionary, postings. One of the streams is missing");

    deserializeBloomFilter(*index_stream->getDataBuffer());
    deserializeSparseIndex(*index_stream->getDataBuffer());

    analyzeBloomFilter(*state.condition);
    analyzeDictionary(*dictionary_stream, state);
}

void MergeTreeIndexGranuleText::analyzeBloomFilter(const IMergeTreeIndexCondition & condition)
{
    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(condition);
    const auto & search_tokens = condition_text.getAllSearchTokens();
    auto global_search_mode = condition_text.getGlobalSearchMode();

    if (condition_text.useBloomFilter())
    {
        for (const auto & token : search_tokens)
        {
            if (bloom_filter.find(token.data(), token.size()))
            {
                /// Create empty postings info, it will be filled during the dictionary analysis.
                remaining_tokens.emplace(token, TokenPostingsInfo{});
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::TextIndexBloomFilterTrueNegatives);

                if (global_search_mode == TextSearchMode::All)
                {
                    remaining_tokens.clear();
                    return;
                }
            }
        }
    }
    else
    {
        for (const auto & token : search_tokens)
            remaining_tokens.emplace(token, TokenPostingsInfo{});
    }
}

void MergeTreeIndexGranuleText::analyzeDictionary(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state)
{
    if (remaining_tokens.empty())
        return;

    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    auto global_search_mode = condition_text.getGlobalSearchMode();
    std::map<size_t, std::vector<StringRef>> block_to_tokens;

    for (const auto & [token, _] : remaining_tokens)
    {
        size_t idx = sparse_index.upperBound(token);

        if (idx != 0)
            --idx;

        block_to_tokens[idx].emplace_back(token);
    }

    auto * data_buffer = stream.getDataBuffer();
    auto * compressed_buffer = stream.getCompressedDataBuffer();

    for (const auto & [block_idx, tokens] : block_to_tokens)
    {
        UInt64 offset_in_file = sparse_index.getOffsetInFile(block_idx);
        compressed_buffer->seek(offset_in_file, 0);
        auto dictionary_block = deserializeDictionaryBlock(*data_buffer);

        for (const auto & token : tokens)
        {
            auto it = remaining_tokens.find(token);
            chassert(it != remaining_tokens.end());
            auto token_idx = dictionary_block.binarySearch(token);

            if (token_idx.has_value())
            {
                ProfileEvents::increment(ProfileEvents::TextIndexBloomFilterTruePositives);
                it->second = dictionary_block.token_infos.at(token_idx.value());
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::TextIndexBloomFilterFalsePositives);

                if (global_search_mode == TextSearchMode::All)
                {
                    remaining_tokens.clear();
                    return;
                }

                remaining_tokens.erase(it);
            }
        }
    }
}

size_t MergeTreeIndexGranuleText::memoryUsageBytes() const
{
    return sizeof(*this)
        + bloom_filter.getFilterSizeBytes()
        + sparse_index.tokens->allocatedBytes()
        + sparse_index.offsets_in_file->allocatedBytes()
        + remaining_tokens.capacity() * sizeof(*remaining_tokens.begin());
}

bool MergeTreeIndexGranuleText::hasAnyTokenFromQuery(const TextSearchQuery & query) const
{
    for (const auto & token : query.tokens)
    {
        if (remaining_tokens.contains(token))
            return true;
    }
    return query.tokens.empty();
}

bool MergeTreeIndexGranuleText::hasAllTokensFromQuery(const TextSearchQuery & query) const
{
    for (const auto & token : query.tokens)
    {
        if (!remaining_tokens.contains(token))
            return false;
    }
    return true;
}

void MergeTreeIndexGranuleText::resetAfterAnalysis()
{
    /// Reset data that is not needed after the analysis.
    /// Keep only remaining tokens with postings lists.
    bloom_filter = BloomFilter(1, 1, 0);
    sparse_index = DictionarySparseIndex();
}

MergeTreeIndexGranuleTextWritable::MergeTreeIndexGranuleTextWritable(
    MergeTreeIndexTextParams params_,
    BloomFilter && bloom_filter_,
    SortedTokensAndPostings && tokens_and_postings_,
    TokenToPostingsMap && tokens_map_,
    std::list<PostingList> && posting_lists_,
    std::unique_ptr<Arena> && arena_)
    : params(std::move(params_))
    , bloom_filter(std::move(bloom_filter_))
    , tokens_and_postings(std::move(tokens_and_postings_))
    , tokens_map(std::move(tokens_map_))
    , posting_lists(std::move(posting_lists_))
    , arena(std::move(arena_))
{
}

template <typename Stream>
DictionarySparseIndex serializeTokensAndPostings(
    const SortedTokensAndPostings & tokens_and_postings,
    Stream & dictionary_stream,
    Stream & postings_stream,
    size_t block_size,
    size_t max_cardinality_for_embedded_postings)
{
    size_t num_tokens = tokens_and_postings.size();
    size_t num_blocks = (num_tokens + block_size - 1) / block_size;

    auto sparse_index_tokens = ColumnString::create();
    auto & sparse_index_str = assert_cast<ColumnString &>(*sparse_index_tokens);
    sparse_index_str.reserve(num_blocks);

    auto sparse_index_offsets = ColumnUInt64::create();
    auto & sparse_index_offsets_data = sparse_index_offsets->getData();
    sparse_index_offsets_data.reserve(num_blocks);

    UInt64 tokens_format = static_cast<UInt64>(TokensSerializationFormat::RawStrings);

    for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx)
    {
        size_t block_begin = block_idx * block_size;
        size_t block_end = std::min(block_begin + block_size, num_tokens);

        /// Start a new compressed block because the dictionary blocks
        /// are usually read with random reads and it is more efficient
        /// to decompress only the needed data.
        dictionary_stream.compressed_hashing.next();
        auto current_mark = dictionary_stream.getCurrentMark();
        chassert(current_mark.offset_in_decompressed_block == 0);

        const auto & first_token = tokens_and_postings[block_begin].first;
        sparse_index_offsets_data.emplace_back(current_mark.offset_in_compressed_file);
        sparse_index_str.insertData(first_token.data, first_token.size);

        size_t num_tokens_in_block = block_end - block_begin;
        writeVarUInt(tokens_format, dictionary_stream.compressed_hashing);
        writeVarUInt(num_tokens_in_block, dictionary_stream.compressed_hashing);

        for (size_t i = block_begin; i < block_end; ++i)
        {
            /// Write tokens the same as in SerializationString::serializeBinaryBulk
            /// to be able to read them later with SerializationString::deserializeBinaryBulk.
            const auto & token = tokens_and_postings[i].first;
            writeVarUInt(token.size, dictionary_stream.compressed_hashing);
            dictionary_stream.compressed_hashing.write(token.data, token.size);
        }

        for (size_t i = block_begin; i < block_end; ++i)
        {
            auto & postings = *tokens_and_postings[i].second;
            UInt32 cardinality = postings.cardinality();

            UInt64 header = 0;
            bool raw_postings = cardinality <= MAX_CARDINALITY_FOR_RAW_POSTINGS;
            bool embedded_postings = cardinality <= max_cardinality_for_embedded_postings;

            if (raw_postings)
                header |= PostingsSerialization::RawPostings;

            if (embedded_postings)
                header |= PostingsSerialization::EmbeddedPostings;

            writeVarUInt(header, dictionary_stream.compressed_hashing);
            writeVarUInt(cardinality, dictionary_stream.compressed_hashing);

            if (embedded_postings)
            {
                PostingsSerialization::serialize(header, std::move(postings), dictionary_stream.compressed_hashing);
            }
            else
            {
                /// Start a new compressed block because of the same reason as above for dictionary block.
                postings_stream.compressed_hashing.next();
                auto postings_mark = postings_stream.getCurrentMark();
                chassert(postings_mark.offset_in_decompressed_block == 0);
                UInt64 offset_in_file = postings_mark.offset_in_compressed_file;

                writeVarUInt(offset_in_file, dictionary_stream.compressed_hashing);
                PostingsSerialization::serialize(header, std::move(postings), postings_stream.compressed_hashing);
            }
        }
    }

    return DictionarySparseIndex(std::move(sparse_index_tokens), std::move(sparse_index_offsets));
}

template <typename Stream>
void serializeBloomFilter(size_t num_tokens, const BloomFilter & bloom_filter, Stream & stream)
{
    writeVarUInt(num_tokens, stream.compressed_hashing);
    const char * filter_data = reinterpret_cast<const char *>(bloom_filter.getFilter().data());
    stream.compressed_hashing.write(filter_data, bloom_filter.getFilterSizeBytes());
}

template <typename Stream>
void serializeSparseIndex(const DictionarySparseIndex & sparse_index, Stream & stream)
{
    chassert(sparse_index.tokens->size() == sparse_index.offsets_in_file->size());

    SerializationString serialization_string;
    SerializationNumber<UInt64> serialization_number;

    writeVarUInt(sparse_index.tokens->size(), stream.compressed_hashing);
    serialization_string.serializeBinaryBulk(*sparse_index.tokens, stream.compressed_hashing, 0, sparse_index.tokens->size());
    serialization_number.serializeBinaryBulk(*sparse_index.offsets_in_file, stream.compressed_hashing, 0, sparse_index.offsets_in_file->size());
}

void MergeTreeIndexGranuleTextWritable::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be serialized with 3 streams: index, dictionary, postings");
}

void MergeTreeIndexGranuleTextWritable::serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const
{
    auto * index_stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    auto * dictionary_stream = streams.at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto * postings_stream = streams.at(MergeTreeIndexSubstream::Type::TextIndexPostings);

    if (!index_stream || !dictionary_stream || !postings_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be serialized with 3 streams: index, dictionary, postings. One of the streams is missing");

    auto sparse_index_block = serializeTokensAndPostings(
        tokens_and_postings,
        *dictionary_stream,
        *postings_stream,
        params.dictionary_block_size,
        params.max_cardinality_for_embedded_postings);

    serializeBloomFilter(tokens_and_postings.size(), bloom_filter, *index_stream);
    serializeSparseIndex(sparse_index_block, *index_stream);
}

void MergeTreeIndexGranuleTextWritable::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deserialization of MergeTreeIndexGranuleTextWritable is not implemented");
}

MergeTreeIndexTextGranuleBuilder::MergeTreeIndexTextGranuleBuilder(MergeTreeIndexTextParams params_, TokenExtractorPtr token_extractor_)
    : params(std::move(params_)), token_extractor(token_extractor_), arena(std::make_unique<Arena>())
{
}

void MergeTreeIndexTextGranuleBuilder::addDocument(StringRef document)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;
    size_t length = document.size;

    while (cur < length && token_extractor->nextInStringPadded(document.data, length, &cur, &token_start, &token_len))
    {
        bool inserted;
        TokenToPostingsMap::LookupResult it;

        ArenaKeyHolder key_holder{StringRef(document.data + token_start, token_len), *arena};
        tokens_map.emplace(key_holder, it, inserted);
        auto & [posting_list, bulk_context] = it->getMapped();

        if (inserted)
        {
            posting_list = &posting_lists.emplace_back();
            bulk_context = roaring::BulkContext();
        }

        /// Use addBulk to optimize consecutive insertions into the posting list.
        posting_list->addBulk(bulk_context, current_row);
    }

    ++current_row;
}

std::unique_ptr<MergeTreeIndexGranuleTextWritable> MergeTreeIndexTextGranuleBuilder::build()
{
    SortedTokensAndPostings sorted_values;
    sorted_values.reserve(tokens_map.size());

    size_t bloom_filter_bytes = getBloomFilterSizeInBytes(params.bloom_filter_bits_per_row, tokens_map.size());
    BloomFilter bloom_filter(bloom_filter_bytes, params.bloom_filter_num_hashes, 0);

    tokens_map.forEachValue([&](const auto & key, auto & mapped)
    {
        sorted_values.emplace_back(key, mapped.first);
        bloom_filter.add(key.data, key.size);
    });

    std::ranges::sort(sorted_values, [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    return std::make_unique<MergeTreeIndexGranuleTextWritable>(
        params,
        std::move(bloom_filter),
        std::move(sorted_values),
        std::move(tokens_map),
        std::move(posting_lists),
        std::move(arena));
}

void MergeTreeIndexTextGranuleBuilder::reset()
{
    current_row = 0;
    tokens_map = {};
    posting_lists.clear();
    arena = std::make_unique<Arena>();
}

MergeTreeIndexAggregatorText::MergeTreeIndexAggregatorText(
    String index_column_name_,
    MergeTreeIndexTextParams params_,
    TokenExtractorPtr token_extractor_)
    : index_column_name(std::move(index_column_name_))
    , params(std::move(params_))
    , token_extractor(token_extractor_)
    , granule_builder(params, token_extractor_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorText::getGranuleAndReset()
{
    auto granule = granule_builder.build();
    granule_builder.reset();
    return granule;
}

void MergeTreeIndexAggregatorText::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos, block.rows());
    }

    size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    size_t current_position = *pos;
    const auto & index_column = block.getByName(index_column_name).column;

    for (size_t i = 0; i < rows_read; ++i)
    {
        auto ref = index_column->getDataAt(current_position + i);
        granule_builder.addDocument(ref);
    }

    *pos += rows_read;
}

MergeTreeIndexText::MergeTreeIndexText(
    const IndexDescription & index_,
    MergeTreeIndexTextParams params_,
    std::unique_ptr<ITokenExtractor> token_extractor_)
    : IMergeTreeIndex(index_)
    , params(std::move(params_))
    , token_extractor(std::move(token_extractor_))
{
}

MergeTreeIndexSubstreams MergeTreeIndexText::getSubstreams() const
{
    return
    {
        {MergeTreeIndexSubstream::Type::Regular, "", ".idx"},
        {MergeTreeIndexSubstream::Type::TextIndexDictionary, ".dct", ".idx"},
        {MergeTreeIndexSubstream::Type::TextIndexPostings, ".pst", ".idx"}
    };
}

MergeTreeIndexFormat MergeTreeIndexText::getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix) const
{
    if (checksums.files.contains(path_prefix + ".idx"))
        return {1, getSubstreams()};
    return {0, {}};
}

MergeTreeIndexGranulePtr MergeTreeIndexText::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleText>(params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexText::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorText>(index.column_names[0], params, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexText::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionText>(predicate, context, index.sample_block, token_extractor.get());
}

static const String ARGUMENT_TOKENIZER = "tokenizer";
static const String ARGUMENT_NGRAM_SIZE = "ngram_size";
static const String ARGUMENT_SEPARATORS = "separators";
static const String ARGUMENT_DICTIONARY_BLOCK_SIZE = "dictionary_block_size";
static const String ARGUMENT_MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS = "max_cardinality_for_embedded_postings";
static const String ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE = "bloom_filter_false_positive_rate";

namespace
{

template <typename Type>
std::optional<Type> extractOption(std::unordered_map<String, Field> & options, const String & option)
{
    if (auto it = options.find(option); it != options.end())
    {
        Field value = std::move(it->second);
        Field::Types::Which expected_type = Field::TypeToEnum<NearestFieldType<Type>>::value;

        if (value.getType() != expected_type)
        {
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Text index argument '{}' expected to be {}, but got {}",
                option, fieldTypeToString(expected_type), value.getTypeName());
        }

        options.erase(it);
        return value.safeGet<Type>();
    }
    return {};
}

std::optional<std::vector<String>> extractOptionAsStringArray(std::unordered_map<String, Field> & options, const String & option)
{
    auto array = extractOption<Array>(options, option);
    if (array.has_value())
    {
        std::vector<String> values;
        for (const auto & entry : array.value())
            values.emplace_back(entry.template safeGet<String>());

        return values;
    }
    return {};
}

std::unordered_map<String, Field> convertArgumentsToOptionsMap(const FieldVector & arguments)
{
    std::unordered_map<String, Field> options;
    for (const Field & argument : arguments)
    {
        if (argument.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Arguments of text index must be key-value pair (identifier = literal)");

        Tuple tuple = argument.template safeGet<Tuple>();
        String key = tuple[0].safeGet<String>();

        if (options.contains(key))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Text index '{}' argument is specified more than once", key);

        options[key] = tuple[1];
    }
    return options;
}

}

MergeTreeIndexPtr textIndexCreator(const IndexDescription & index)
{
    std::unordered_map<String, Field> options = convertArgumentsToOptionsMap(index.arguments);

    String tokenizer = extractOption<String>(options, ARGUMENT_TOKENIZER).value();

    std::unique_ptr<ITokenExtractor> token_extractor;

    if (tokenizer == DefaultTokenExtractor::getExternalName())
    {
        token_extractor = std::make_unique<DefaultTokenExtractor>();
    }
    else if (tokenizer == NgramTokenExtractor::getExternalName())
    {
        auto ngram_size = extractOption<UInt64>(options, ARGUMENT_NGRAM_SIZE);
        token_extractor = std::make_unique<NgramTokenExtractor>(ngram_size.value_or(DEFAULT_NGRAM_SIZE));
    }
    else if (tokenizer == SplitTokenExtractor::getExternalName())
    {
        auto separators = extractOptionAsStringArray(options, ARGUMENT_SEPARATORS).value_or(std::vector<String>{" "});
        token_extractor = std::make_unique<SplitTokenExtractor>(separators);
    }
    else if (tokenizer == NoOpTokenExtractor::getExternalName())
    {
        token_extractor = std::make_unique<NoOpTokenExtractor>();
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tokenizer {} not supported", tokenizer);
    }

    UInt64 dictionary_block_size = extractOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE).value_or(DEFAULT_DICTIONARY_BLOCK_SIZE);
    UInt64 max_cardinality_for_embedded_postings = extractOption<UInt64>(options, ARGUMENT_MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS).value_or(DEFAULT_MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS);
    double bloom_filter_false_positive_rate = extractOption<double>(options, ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE).value_or(DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE);

    const auto [bits_per_rows, num_hashes] = BloomFilterHash::calculationBestPractices(bloom_filter_false_positive_rate);
    MergeTreeIndexTextParams params{dictionary_block_size, max_cardinality_for_embedded_postings, bits_per_rows, num_hashes};

    if (!options.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unexpected text index arguments: {}", fmt::join(std::views::keys(options), ", "));

    return std::make_shared<MergeTreeIndexText>(index, params, std::move(token_extractor));
}

void textIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    std::unordered_map<String, Field> options = convertArgumentsToOptionsMap(index.arguments);

    /// Check that tokenizer is present and supported
    std::optional<String> tokenizer = extractOption<String>(options, ARGUMENT_TOKENIZER);
    if (!tokenizer)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Text index must have an '{}' argument", ARGUMENT_TOKENIZER);

    const bool is_supported_tokenizer = (tokenizer.value() == DefaultTokenExtractor::getExternalName()
                                      || tokenizer.value() == NgramTokenExtractor::getExternalName()
                                      || tokenizer.value() == SplitTokenExtractor::getExternalName()
                                      || tokenizer.value() == NoOpTokenExtractor::getExternalName());
    if (!is_supported_tokenizer)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Text index argument '{}' supports only 'default', 'ngram', 'split', and 'no_op', but got {}",
            ARGUMENT_TOKENIZER,
            tokenizer.value());
    }

    std::optional<UInt64> ngram_size;
    if (tokenizer.value() == NgramTokenExtractor::getExternalName())
    {
        ngram_size = extractOption<UInt64>(options, ARGUMENT_NGRAM_SIZE);
        if (ngram_size.has_value() && (*ngram_size < 2 || *ngram_size > 8))
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Text index argument '{}' must be between 2 and 8, but got {}",
                ARGUMENT_NGRAM_SIZE,
                *ngram_size);
    }
    else if (tokenizer.value() == SplitTokenExtractor::getExternalName())
    {
        std::optional<DB::FieldVector> separators = extractOption<Array>(options, ARGUMENT_SEPARATORS);
        if (separators.has_value())
        {
            for (const auto & separator : separators.value())
            {
                if (separator.getType() != Field::Types::String)
                    throw Exception(
                        ErrorCodes::INCORRECT_QUERY,
                        "Element of text index argument '{}' expected to be String, but got {}",
                        ARGUMENT_SEPARATORS,
                        separator.getTypeName());
            }
        }
    }

    double bloom_filter_false_positive_rate = extractOption<double>(options, ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE).value_or(DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE);

    if (!std::isfinite(bloom_filter_false_positive_rate) || bloom_filter_false_positive_rate <= 0.0 || bloom_filter_false_positive_rate >= 1.0)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Text index argument '{}' must be between 0.0 and 1.0, but got {}",
            ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE,
            bloom_filter_false_positive_rate);
    }

    UInt64 dictionary_block_size = extractOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE).value_or(DEFAULT_DICTIONARY_BLOCK_SIZE);
    if (dictionary_block_size == 0)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Text index argument '{}' must be greater than 0, but got {}", ARGUMENT_DICTIONARY_BLOCK_SIZE, dictionary_block_size);

    /// No validation for max_cardinality_for_embedded_postings.
    extractOption<UInt64>(options, ARGUMENT_MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS);

    if (!options.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unexpected text index arguments: {}", fmt::join(std::views::keys(options), ", "));

    /// Check that the index is created on a single column
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Text index must be created on a single column");

    WhichDataType data_type(index.data_types[0]);
    if (data_type.isLowCardinality())
    {
        /// TODO Consider removing support for LowCardinality. The index exists for high-cardinality cases.
        const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index.data_types[0]);
        data_type = WhichDataType(low_cardinality.getDictionaryType());
    }

    if (!data_type.isString() && !data_type.isFixedString())
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Text index must be created on columns of type `String`, `FixedString`, `LowCardinality(String)`, `LowCardinality(FixedString)`");
    }
}

}
