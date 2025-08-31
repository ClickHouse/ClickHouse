#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <Interpreters/BloomFilterHash.h>
#include <Common/logger_useful.h>
#include "base/range.h"

namespace ProfileEvents
{
    extern const Event TextIndexReadDictionaryBlocks;
    extern const Event TextIndexSkipDictionaryBlocks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
}

namespace
{

size_t getBloomFilterSizeInBytes(size_t bits_per_row, size_t total_rows)
{
    static constexpr size_t atom_size = 8;
    return (bits_per_row * total_rows + atom_size - 1) / atom_size;
}

UInt64 packMark(const MarkInCompressedFile & begin_mark, const MarkInCompressedFile & current_mark)
{
    UInt32 offset_in_compressed_file = current_mark.offset_in_compressed_file - begin_mark.offset_in_compressed_file;
    UInt32 offset_in_decompressed_block = static_cast<UInt32>(current_mark.offset_in_decompressed_block);
    return (static_cast<UInt64>(offset_in_compressed_file) << 32) | offset_in_decompressed_block;
}

MarkInCompressedFile unpackMark(const MarkInCompressedFile & begin_mark, UInt64 packed_mark)
{
    UInt32 offset_in_compressed_file = static_cast<UInt32>(packed_mark >> 32);
    UInt32 offset_in_decompressed_block = static_cast<UInt32>(packed_mark & 0xFFFFFFFF);
    return {begin_mark.offset_in_compressed_file + offset_in_compressed_file, offset_in_decompressed_block};
}

}

DictionaryBlock::DictionaryBlock(ColumnPtr tokens_, ColumnPtr marks_)
    : tokens(std::move(tokens_)), marks(std::move(marks_))
{
}

bool DictionaryBlock::empty() const
{
    return !tokens || tokens->empty();
}

size_t DictionaryBlock::size() const
{
    return tokens ? tokens->size() : 0;
}

UInt64 DictionaryBlock::getPackedMark(size_t idx) const
{
    return assert_cast<const ColumnUInt64 &>(*marks).getData()[idx];
}

size_t DictionaryBlock::lowerBound(const StringRef & token) const
{
    auto range = collections::range(0, tokens->size());

    auto it = std::lower_bound(range.begin(), range.end(), token, [this](size_t lhs_idx, const StringRef & rhs_ref)
    {
        return assert_cast<const ColumnString &>(*tokens).getDataAt(lhs_idx) < rhs_ref;
    });

    return it - range.begin();
}

size_t DictionaryBlock::upperBound(const StringRef & token) const
{
    auto range = collections::range(0, tokens->size());

    auto it = std::upper_bound(range.begin(), range.end(), token, [this](const StringRef & lhs_ref, size_t rhs_idx)
    {
        return lhs_ref < assert_cast<const ColumnString &>(*tokens).getDataAt(rhs_idx);
    });

    return it - range.begin();
}


std::optional<size_t> DictionaryBlock::binarySearch(const StringRef & token) const
{
    size_t idx = lowerBound(token);
    const auto & tokens_str = assert_cast<const ColumnString &>(*tokens);

    if (idx == tokens->size() || token != tokens_str.getDataAt(idx))
        return std::nullopt;

    return idx;
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
    readVarUInt(total_rows, istr);

    size_t bytes_size = getBloomFilterSizeInBytes(params.bloom_filter_bits_per_row, total_rows);
    bloom_filter.resize(bytes_size);
    istr.readStrict(reinterpret_cast<char *>(bloom_filter.getFilter().data()), bytes_size);
}

/// TODO: add cache for dictionary blocks
static DictionaryBlock deserializeDictionaryBlock(ReadBuffer & istr, bool skip)
{
    size_t tokens_size;
    readVarUInt(tokens_size, istr);

    auto tokens_column = ColumnString::create();
    auto marks_column = ColumnUInt64::create();

    tokens_column->reserve(tokens_size);
    marks_column->reserve(tokens_size);

    SerializationString serialization_string;
    SerializationNumber<UInt64> serialization_number;

    if (skip)
    {
        ProfileEvents::increment(ProfileEvents::TextIndexSkipDictionaryBlocks);
        serialization_string.deserializeBinaryBulk(*tokens_column, istr, tokens_size, 0, 0.0);
        serialization_number.deserializeBinaryBulk(*marks_column, istr, tokens_size, 0, 0.0);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::TextIndexReadDictionaryBlocks);
        serialization_string.deserializeBinaryBulk(*tokens_column, istr, 0, tokens_size, 0.0);
        serialization_number.deserializeBinaryBulk(*marks_column, istr, 0, tokens_size, 0.0);
    }

    return DictionaryBlock(std::move(tokens_column), std::move(marks_column));
}

void MergeTreeIndexGranuleText::deserializeBinaryWithMultipleStreams(IndexInputStreams & streams, IndexDeserializationState & state)
{
    auto * index_stream = streams.at(IndexSubstream::Type::Regular);
    auto * dictionary_stream = streams.at(IndexSubstream::Type::TextIndexDictionary);

    if (!index_stream || !dictionary_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be deserialized with 3 streams: index, dictionary, postings. One of the streams is missing");

    deserializeBloomFilter(*index_stream->getDataBuffer());
    analyzeBloomFilter(*state.condition);

    bool skip_index = remaining_tokens.empty();
    sparse_index = deserializeDictionaryBlock(*index_stream->getDataBuffer(), skip_index);
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
                remaining_tokens.emplace(token, MarkInCompressedFile{0, 0});
            }
            else if (global_search_mode == TextSearchMode::All)
            {
                remaining_tokens.clear();
                return;
            }
        }
    }
    else
    {
        for (const auto & token : search_tokens)
            remaining_tokens.emplace(token, MarkInCompressedFile{0, 0});
    }
}

void MergeTreeIndexGranuleText::analyzeDictionary(IndexReaderStream & stream, IndexDeserializationState & state)
{
    if (remaining_tokens.empty())
        return;

    auto begin_dict_mark = state.current_marks.at(IndexSubstream::Type::TextIndexDictionary);
    auto begin_postings_mark = state.current_marks.at(IndexSubstream::Type::TextIndexPostings);

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

    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    auto global_search_mode = condition_text.getGlobalSearchMode();

    for (const auto & [block_idx, tokens] : block_to_tokens)
    {
        UInt64 packed_dict_mark = sparse_index.getPackedMark(block_idx);
        MarkInCompressedFile dict_mark = unpackMark(begin_dict_mark, packed_dict_mark);

        compressed_buffer->seek(dict_mark.offset_in_compressed_file, dict_mark.offset_in_decompressed_block);
        auto dictionary_block = deserializeDictionaryBlock(*data_buffer, false);

        for (const auto & token : tokens)
        {
            auto it = remaining_tokens.find(token);
            chassert(it != remaining_tokens.end());

            auto token_idx = dictionary_block.binarySearch(token);

            if (token_idx.has_value())
            {
                UInt64 packed_posting_mark = dictionary_block.getPackedMark(token_idx.value());
                it->second = unpackMark(begin_postings_mark, packed_posting_mark);
            }
            else if (global_search_mode == TextSearchMode::Any)
            {
                remaining_tokens.erase(it);
            }
            else
            {
                remaining_tokens.clear();
                return;
            }
        }
    }
}

bool MergeTreeIndexGranuleText::hasAllTokensFromQuery(const GinQueryString & query) const
{
    for (const auto & token : query.getTokens())
    {
        if (remaining_tokens.find(token) == remaining_tokens.end())
            return false;
    }
    return true;
}

void MergeTreeIndexGranuleText::resetAfterAnalysis(bool may_be_true)
{
    bloom_filter = BloomFilter(1, 1, 0);
    sparse_index = DictionaryBlock();

    if (!may_be_true)
        remaining_tokens.clear();
}

MergeTreeIndexGranuleTextWritable::MergeTreeIndexGranuleTextWritable(
    size_t dictionary_block_size_,
    size_t total_rows_,
    BloomFilter bloom_filter_,
    std::vector<StringRef> tokens_,
    std::vector<PostingList> posting_lists_,
    std::unique_ptr<Arena> arena_)
    : dictionary_block_size(dictionary_block_size_)
    , total_rows(total_rows_)
    , bloom_filter(std::move(bloom_filter_))
    , tokens(std::move(tokens_))
    , posting_lists(std::move(posting_lists_))
    , arena(std::move(arena_))
{
}

template <typename Stream>
ColumnPtr serializePostings(const std::vector<PostingList> & postings, Stream & stream, const IndexSerializationState & state)
{
    auto begin_mark = state.current_marks.at(IndexSubstream::Type::TextIndexPostings);

    auto marks_column = ColumnUInt64::create();
    auto & marks_data = marks_column->getData();
    marks_data.reserve(postings.size());

    SerializationNumber<UInt32> serialization;
    auto postings_column = ColumnUInt32::create();
    auto & postings_data = postings_column->getData();

    for (const auto & posting_list : postings)
    {
        auto current_mark = stream.getCurrentMark();
        marks_data.emplace_back(packMark(begin_mark, current_mark));

        postings_data.clear();
        postings_data.resize(posting_list.cardinality() + 1);
        postings_data[0] = posting_list.cardinality();
        posting_list.toUint32Array(postings_data.data() + 1);

        serialization.serializeBinaryBulk(*postings_column, stream.compressed_hashing, 0, postings_data.size());
    }

    return marks_column;
}

template <typename Stream>
DictionaryBlock serializeDictionary(const std::vector<StringRef> & tokens, const IColumn & marks_to_postings, Stream & stream, const IndexSerializationState & state, size_t block_size)
{
    chassert(tokens.size() == marks_to_postings.size());

    auto begin_mark = state.current_marks.at(IndexSubstream::Type::TextIndexDictionary);
    size_t num_blocks = (tokens.size() + block_size - 1) / block_size;
    SerializationNumber<UInt64> serialization_number;

    auto sparse_index_tokens = ColumnString::create();
    auto & sparse_index_str = assert_cast<ColumnString &>(*sparse_index_tokens);
    sparse_index_str.reserve(num_blocks);

    auto sparse_index_marks = ColumnUInt64::create();
    auto & sparse_index_marks_data = sparse_index_marks->getData();
    sparse_index_marks_data.reserve(num_blocks);

    for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx)
    {
        size_t block_begin = block_idx * block_size;
        size_t block_end = std::min(block_begin + block_size, tokens.size());

        auto current_mark = stream.getCurrentMark();
        sparse_index_marks_data.emplace_back(packMark(begin_mark, current_mark));
        sparse_index_str.insertData(tokens[block_begin].data, tokens[block_begin].size);

        size_t num_tokens = block_end - block_begin;
        writeVarUInt(num_tokens, stream.compressed_hashing);

        for (size_t i = block_begin; i < block_end; ++i)
        {
            writeVarUInt(tokens[i].size, stream.compressed_hashing);
            stream.compressed_hashing.write(tokens[i].data, tokens[i].size);
        }

        serialization_number.serializeBinaryBulk(marks_to_postings, stream.compressed_hashing, block_begin, num_tokens);
    }

    return DictionaryBlock(std::move(sparse_index_tokens), std::move(sparse_index_marks));
}

template <typename Stream>
void serializeBloomFilter(size_t total_rows, const BloomFilter & bloom_filter, Stream & stream)
{
    writeVarUInt(total_rows, stream.compressed_hashing);
    const char * filter_data = reinterpret_cast<const char *>(bloom_filter.getFilter().data());
    stream.compressed_hashing.write(filter_data, bloom_filter.getFilterSizeBytes());
}

template <typename Stream>
void serializeSparseIndex(const DictionaryBlock & sparse_index, Stream & stream)
{
    chassert(sparse_index.tokens->size() == sparse_index.marks->size());

    SerializationString serialization_string;
    SerializationNumber<UInt64> serialization_number;

    writeVarUInt(sparse_index.tokens->size(), stream.compressed_hashing);
    serialization_string.serializeBinaryBulk(*sparse_index.tokens, stream.compressed_hashing, 0, sparse_index.tokens->size());
    serialization_number.serializeBinaryBulk(*sparse_index.marks, stream.compressed_hashing, 0, sparse_index.marks->size());
}

void MergeTreeIndexGranuleTextWritable::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be serialized with 3 streams: index, dictionary, postings");
}

void MergeTreeIndexGranuleTextWritable::serializeBinaryWithMultipleStreams(IndexOutputStreams & streams, IndexSerializationState & state) const
{
    auto * index_stream = streams.at(IndexSubstream::Type::Regular);
    auto * dictionary_stream = streams.at(IndexSubstream::Type::TextIndexDictionary);
    auto * postings_stream = streams.at(IndexSubstream::Type::TextIndexPostings);

    if (!index_stream || !dictionary_stream || !postings_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be serialized with 3 streams: index, dictionary, postings. One of the streams is missing");

    auto marks_to_postings = serializePostings(posting_lists, *postings_stream, state);
    auto sparse_index_block = serializeDictionary(tokens, *marks_to_postings, *dictionary_stream, state, dictionary_block_size);

    serializeBloomFilter(total_rows, bloom_filter, *index_stream);
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
    for (const auto & token : token_extractor->getTokensView(document.data, document.size))
    {
        bool inserted;
        TokensMap::LookupResult it;

        ArenaKeyHolder key_holder{StringRef(token.data(), token.length()), *arena};
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
    size_t total_rows = current_row;
    std::vector<std::pair<StringRef, PostingList *>> sorted_values;
    sorted_values.reserve(tokens_map.size());

    for (auto it = tokens_map.begin(); it != tokens_map.end(); ++it)
        sorted_values.emplace_back(it->getKey(), it->getMapped());

    std::sort(sorted_values.begin(), sorted_values.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    std::vector<StringRef> sorted_tokens;
    std::vector<PostingList> sorted_posting_lists;

    sorted_tokens.reserve(sorted_values.size());
    sorted_posting_lists.reserve(sorted_values.size());

    size_t bloom_filter_bytes = getBloomFilterSizeInBytes(params.bloom_filter_bits_per_row, total_rows);
    BloomFilter bloom_filter(bloom_filter_bytes, params.bloom_filter_num_hashes, 0);

    for (auto & [token, posting_list] : sorted_values)
    {
        bloom_filter.add(token.data, token.size);
        sorted_tokens.emplace_back(token);
        sorted_posting_lists.emplace_back(std::move(*posting_list));
    }

    return std::make_unique<MergeTreeIndexGranuleTextWritable>(
        params.dictionary_block_size,
        total_rows,
        std::move(bloom_filter),
        std::move(sorted_tokens),
        std::move(sorted_posting_lists),
        std::move(arena));
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

    UInt64 dictionary_block_size = getOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE).value_or(256);
    double bloom_filter_false_positive_rate = getOption<double>(options, ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE).value_or(0.025);

    const auto [bits_per_rows, num_hashes] = BloomFilterHash::calculationBestPractices(bloom_filter_false_positive_rate);
    MergeTreeIndexTextParams params{dictionary_block_size, bits_per_rows, num_hashes};

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
