#include <Storages/MergeTree/MergeTreeIndexText.h>
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
#include <IO/BitHelpers.h>
#include <base/range.h>

namespace ProfileEvents
{
    extern const Event TextIndexReadDictionaryBlocks;
    extern const Event TextIndexReadDictionarySparseIndexBlocks;
    extern const Event TextIndexBloomFilterHits;
    extern const Event TextIndexBloomFilterMisses;
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
}

static size_t getBloomFilterSizeInBytes(size_t bits_per_row, size_t num_tokens)
{
    static constexpr size_t atom_size = 8;
    return std::max(1UL, (bits_per_row * num_tokens + atom_size - 1) / atom_size);
}

static constexpr UInt8 EMBEDDED_POSTINGS_FLAG = static_cast<UInt8>(1 << 6);

CompressedPostings::CompressedPostings(UInt8 delta_bits_, UInt32 cardinality_)
    : delta_bits(delta_bits_), cardinality(cardinality_)
{
    if (hasRawDeltas())
    {
        deltas_buffer.resize(num_cells_for_raw_deltas, 0);
    }
    else
    {
        size_t num_cells = getNumCellsForBitsPacked64(cardinality, delta_bits);
        deltas_buffer.resize(num_cells, 0);
    }
}

CompressedPostings::CompressedPostings(const roaring::Roaring & postings)
{
    cardinality = postings.cardinality();
    std::vector<UInt32> raw_deltas(cardinality);

    if (cardinality == 0)
        return;

    auto it = postings.begin();
    min_delta = *it;
    UInt32 prev = *it;
    UInt32 max_delta = 0;

    for (size_t i = 0; i < cardinality; ++i, ++it)
    {
        UInt32 delta = *it - prev;
        max_delta = std::max(max_delta, delta);
        raw_deltas[i] = delta;
        prev = *it;
    }

    delta_bits = max_delta ? bitScanReverse(max_delta) + 1 : 0;

    if (cardinality <= max_raw_deltas)
    {
        deltas_buffer.resize(num_cells_for_raw_deltas, 0);
        memcpy(deltas_buffer.data(), raw_deltas.data(), sizeof(UInt32) * cardinality);
        return;
    }

    size_t num_cells = getNumCellsForBitsPacked64(cardinality, delta_bits);
    deltas_buffer.resize(num_cells, 0);

    for (size_t i = 0; i < cardinality; ++i)
        writeBitsPacked64(deltas_buffer.data(), i * delta_bits, raw_deltas[i]);
}

UInt32 CompressedPostings::getDelta(size_t idx) const
{
    if (hasRawDeltas())
    {
        return reinterpret_cast<const UInt32 *>(deltas_buffer.data())[idx];
    }

    return readBitsPacked64(deltas_buffer.data(), idx * delta_bits, delta_bits);
}

void CompressedPostings::serialize(WriteBuffer & ostr) const
{
    writeVarUInt(min_delta, ostr);

    if (hasRawDeltas())
    {
        const auto * raw_deltas = reinterpret_cast<const UInt32 *>(deltas_buffer.data());
        for (size_t i = 1; i < cardinality; ++i)
            writeVarUInt(raw_deltas[i], ostr);
    }
    else
    {
        ostr.write(reinterpret_cast<const char *>(deltas_buffer.data()), sizeof(UInt64) * deltas_buffer.size());
    }
}

void CompressedPostings::deserialize(ReadBuffer & istr)
{
    readVarUInt(min_delta, istr);

    if (hasRawDeltas())
    {
        auto * raw_deltas = reinterpret_cast<UInt32 *>(deltas_buffer.data());
        for (size_t i = 1; i < cardinality; ++i)
            readVarUInt(raw_deltas[i], istr);
    }
    else
    {
        istr.readStrict(reinterpret_cast<char *>(deltas_buffer.data()), sizeof(UInt64) * deltas_buffer.size());
    }
}

UInt32 TokenInfo::getCardinality() const
{
    return std::visit([](const auto & arg) { return arg.getCardinality(); }, postings);
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

DictionaryBlock::DictionaryBlock(ColumnPtr tokens_, std::vector<TokenInfo> token_infos_)
    : DictionaryBlockBase(std::move(tokens_))
    , token_infos(std::move(token_infos_))
{
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
    readVarUInt(bloom_filter_elements, istr);

    size_t bytes_size = getBloomFilterSizeInBytes(params.bloom_filter_bits_per_row, bloom_filter_elements);
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

    auto tokens_column = deserializeTokens(istr);
    size_t num_tokens = tokens_column->size();

    std::vector<TokenInfo> token_infos;
    token_infos.reserve(num_tokens);

    for (size_t i = 0; i < num_tokens; ++i)
    {
        UInt32 cardinality;
        UInt8 delta_bits;

        readVarUInt(cardinality, istr);
        readVarUInt(delta_bits, istr);

        if (delta_bits & EMBEDDED_POSTINGS_FLAG)
        {
            delta_bits &= ~EMBEDDED_POSTINGS_FLAG;
            CompressedPostings postings(delta_bits, cardinality);
            postings.deserialize(istr);
            token_infos.emplace_back(std::move(postings));
        }
        else
        {
            UInt64 offset_in_file;
            readVarUInt(offset_in_file, istr);
            token_infos.emplace_back(TokenInfo::FuturePostings{cardinality, delta_bits, offset_in_file});
        }
    }

    return DictionaryBlock(std::move(tokens_column), std::move(token_infos));
}

/// TODO: add cache for dictionary sparse index
void MergeTreeIndexGranuleText::deserializeSparseIndex(ReadBuffer & istr)
{
    ProfileEvents::increment(ProfileEvents::TextIndexReadDictionarySparseIndexBlocks);

    sparse_index.tokens = deserializeTokens(istr);
    size_t num_index_tokens = sparse_index.tokens->size();

    auto offsets_in_file = ColumnUInt64::create();
    SerializationNumber<UInt64> serialization_number;
    serialization_number.deserializeBinaryBulk(*offsets_in_file, istr, 0, num_index_tokens, 0.0);
    sparse_index.offsets_in_file = std::move(offsets_in_file);
}

void MergeTreeIndexGranuleText::deserializeBinaryWithMultipleStreams(IndexInputStreams & streams, IndexDeserializationState & state)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReadGranulesMicroseconds);

    auto * index_stream = streams.at(IndexSubstream::Type::Regular);
    auto * dictionary_stream = streams.at(IndexSubstream::Type::TextIndexDictionary);

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
                remaining_tokens.emplace(token, TokenInfo{});
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::TextIndexBloomFilterHits);

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
            remaining_tokens.emplace(token, TokenInfo{});
    }
}

void MergeTreeIndexGranuleText::analyzeDictionary(IndexReaderStream & stream, IndexDeserializationState & state)
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
                ProfileEvents::increment(ProfileEvents::TextIndexBloomFilterMisses);
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
    bloom_filter = BloomFilter(1, 1, 0);
    sparse_index = DictionarySparseIndex();
}

MergeTreeIndexGranuleTextWritable::MergeTreeIndexGranuleTextWritable(
    MergeTreeIndexTextParams params_,
    BloomFilter bloom_filter_,
    SortedTokensAndPostings tokens_and_postings_,
    Holder holder_)
    : params(std::move(params_))
    , bloom_filter(std::move(bloom_filter_))
    , tokens_and_postings(std::move(tokens_and_postings_))
    , holder(std::move(holder_))
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

    for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx)
    {
        size_t block_begin = block_idx * block_size;
        size_t block_end = std::min(block_begin + block_size, num_tokens);

        dictionary_stream.compressed_hashing.next();
        auto current_mark = dictionary_stream.getCurrentMark();
        chassert(current_mark.offset_in_decompressed_block == 0);

        const auto & first_token = tokens_and_postings[block_begin].first;
        sparse_index_offsets_data.emplace_back(current_mark.offset_in_compressed_file);
        sparse_index_str.insertData(first_token.data, first_token.size);

        size_t num_tokens_in_block = block_end - block_begin;
        writeVarUInt(num_tokens_in_block, dictionary_stream.compressed_hashing);

        for (size_t i = block_begin; i < block_end; ++i)
        {
            const auto & token = tokens_and_postings[i].first;
            writeVarUInt(token.size, dictionary_stream.compressed_hashing);
            dictionary_stream.compressed_hashing.write(token.data, token.size);
        }

        for (size_t i = block_begin; i < block_end; ++i)
        {
            const auto & postings = *tokens_and_postings[i].second;
            CompressedPostings compressed_postings(postings);

            UInt32 cardinality = compressed_postings.getCardinality();
            UInt8 delta_bits = compressed_postings.getDeltaBits();

            bool embedded_postings = cardinality <= max_cardinality_for_embedded_postings;
            if (embedded_postings)
                delta_bits |= EMBEDDED_POSTINGS_FLAG;

            writeVarUInt(cardinality, dictionary_stream.compressed_hashing);
            writeVarUInt(delta_bits, dictionary_stream.compressed_hashing);

            if (embedded_postings)
            {
                compressed_postings.serialize(dictionary_stream.compressed_hashing);
            }
            else
            {
                postings_stream.compressed_hashing.next();
                auto postings_mark = postings_stream.getCurrentMark();
                UInt64 offset_in_file = postings_mark.offset_in_compressed_file + postings_mark.offset_in_decompressed_block;

                writeVarUInt(offset_in_file, dictionary_stream.compressed_hashing);
                compressed_postings.serialize(postings_stream.compressed_hashing);
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

void MergeTreeIndexGranuleTextWritable::serializeBinaryWithMultipleStreams(IndexOutputStreams & streams) const
{
    auto * index_stream = streams.at(IndexSubstream::Type::Regular);
    auto * dictionary_stream = streams.at(IndexSubstream::Type::TextIndexDictionary);
    auto * postings_stream = streams.at(IndexSubstream::Type::TextIndexPostings);

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
        TokensMap::LookupResult it;

        ArenaKeyHolder key_holder{StringRef(document.data + token_start, token_len), *arena};
        tokens_map.emplace(key_holder, it, inserted);

        if (inserted)
        {
            it->getMapped() = &posting_lists.emplace_back();
        }

        it->getMapped()->add(current_row);
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
        sorted_values.emplace_back(key, mapped);
        bloom_filter.add(key.data, key.size);
    });

    std::sort(sorted_values.begin(), sorted_values.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    using Holder = MergeTreeIndexGranuleTextWritable::Holder;
    Holder holder{std::move(tokens_map), std::move(arena), std::move(posting_lists)};

    return std::make_unique<MergeTreeIndexGranuleTextWritable>(
        params,
        std::move(bloom_filter),
        std::move(sorted_values),
        std::move(holder));
}

MergeTreeIndexAggregatorText::MergeTreeIndexAggregatorText(
    String index_column_name_,
    MergeTreeIndexTextParams params_,
    TokenExtractorPtr token_extractor_)
    : index_column_name(std::move(index_column_name_))
    , params(std::move(params_))
    , token_extractor(token_extractor_)
    , granule_builder(MergeTreeIndexTextGranuleBuilder(params, token_extractor))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorText::getGranuleAndReset()
{
    auto granule = granule_builder->build();
    granule_builder.emplace(params, token_extractor);
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

MergeTreeIndexText::MergeTreeIndexText(
    const IndexDescription & index_,
    MergeTreeIndexTextParams params_,
    std::unique_ptr<ITokenExtractor> token_extractor_)
    : IMergeTreeIndex(index_)
    , params(std::move(params_))
    , token_extractor(std::move(token_extractor_))
{
}

IndexSubstreams MergeTreeIndexText::getSubstreams() const
{
    return
    {
        {IndexSubstream::Type::Regular, "", ".idx"},
        {IndexSubstream::Type::TextIndexDictionary, ".dct", ".idx"},
        {IndexSubstream::Type::TextIndexPostings, ".pst", ".idx"}
    };
}

MergeTreeIndexFormat MergeTreeIndexText::getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & path_prefix) const
{
    if (data_part_storage.existsFile(path_prefix + ".idx"))
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
std::optional<Type> getOption(const std::unordered_map<String, Field> & options, const String & option)
{
    if (auto it = options.find(option); it != options.end())
    {
        const Field & value = it->second;
        Field::Types::Which expected_type = Field::TypeToEnum<NearestFieldType<Type>>::value;

        if (value.getType() != expected_type)
        {
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Text index argument '{}' expected to be {}, but got {}",
                option, fieldTypeToString(expected_type), value.getTypeName());
        }

        return value.safeGet<Type>();
    }
    return {};
}

template <typename... Args>
std::optional<std::vector<String>> getOptionAsStringArray(Args &&... args)
{
    auto array = getOption<Array>(std::forward<Args>(args)...);
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

    String tokenizer = getOption<String>(options, ARGUMENT_TOKENIZER).value();

    std::unique_ptr<ITokenExtractor> token_extractor;

    if (tokenizer == DefaultTokenExtractor::getExternalName())
    {
        token_extractor = std::make_unique<DefaultTokenExtractor>();
    }
    else if (tokenizer == NgramTokenExtractor::getExternalName())
    {
        auto ngram_size = getOption<UInt64>(options, ARGUMENT_NGRAM_SIZE);
        token_extractor = std::make_unique<NgramTokenExtractor>(ngram_size.value_or(DEFAULT_NGRAM_SIZE));
    }
    else if (tokenizer == SplitTokenExtractor::getExternalName())
    {
        auto separators = getOptionAsStringArray(options, ARGUMENT_SEPARATORS).value_or(std::vector<String>{" "});
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

    UInt64 dictionary_block_size = getOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE).value_or(128);
    UInt64 max_cardinality_for_embedded_postings = getOption<UInt64>(options, ARGUMENT_MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS).value_or(16);
    double bloom_filter_false_positive_rate = getOption<double>(options, ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE).value_or(0.1);

    const auto [bits_per_rows, num_hashes] = BloomFilterHash::calculationBestPractices(bloom_filter_false_positive_rate);
    MergeTreeIndexTextParams params{dictionary_block_size, max_cardinality_for_embedded_postings, bits_per_rows, num_hashes};

    return std::make_shared<MergeTreeIndexText>(index, params, std::move(token_extractor));
}

void textIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    std::unordered_map<String, Field> options = convertArgumentsToOptionsMap(index.arguments);

    /// Check that tokenizer is present and supported
    std::optional<String> tokenizer = getOption<String>(options, ARGUMENT_TOKENIZER);
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
        ngram_size = getOption<UInt64>(options, ARGUMENT_NGRAM_SIZE);
        if (ngram_size.has_value() && (*ngram_size < 2 || *ngram_size > 8))
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Text index argument '{}' must be between 2 and 8, but got {}",
                ARGUMENT_NGRAM_SIZE,
                *ngram_size);
    }
    else if (tokenizer.value() == SplitTokenExtractor::getExternalName())
    {
        std::optional<DB::FieldVector> separators = getOption<Array>(options, ARGUMENT_SEPARATORS);
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

    double bloom_filter_false_positive_rate = getOption<double>(options, ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE).value_or(0.025);

    if (!std::isfinite(bloom_filter_false_positive_rate) || bloom_filter_false_positive_rate <= 0.0 || bloom_filter_false_positive_rate >= 1.0)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Text index argument '{}' must be between 0.0 and 1.0, but got {}",
            ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE,
            bloom_filter_false_positive_rate);
    }

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
