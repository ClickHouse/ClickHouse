#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <Interpreters/BloomFilterHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
}

DictionaryBlock::DictionaryBlock(ColumnPtr tokens_, ColumnPtr marks_) : tokens(tokens_), marks(marks_)
{
}

bool DictionaryBlock::empty() const
{
    return !tokens || tokens->empty();
}

MergeTreeIndexGranuleText::MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_)
    : params(std::move(params_))
{
}

void MergeTreeIndexGranuleText::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Serialization of MergeTreeIndexGranuleText is not implemented");
}

void MergeTreeIndexGranuleText::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    UNUSED(istr);
    UNUSED(version);
}

MergeTreeIndexGranuleTextWritable::MergeTreeIndexGranuleTextWritable(
    size_t total_rows_,
    BloomFilter bloom_filter_,
    std::vector<StringRef> tokens_,
    std::vector<roaring::Roaring> posting_lists_,
    std::unique_ptr<Arena> arena_)
    : total_rows(total_rows_), bloom_filter(bloom_filter_), tokens(tokens_), posting_lists(posting_lists_), arena(std::move(arena_))
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

        postings_data.resize(posting_list.cardinality() + 1);
        postings_data[0] = posting_list.cardinality();

        posting_list.toUint32Array(postings_data.data() + 1);
        serialization.serializeBinaryBulk(*postings_columns, stream.compressed_hashing, 0, posting_list.cardinality() + 1);
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
void serializeBloomFilter(size_t total_rows, const BloomFilter & bloom_filter, Stream & stream)
{
    writeVarUInt(total_rows, stream.compressed_hashing);
    const char * filter_data = reinterpret_cast<const char *>(bloom_filter.getFilter().data());
    stream.compressed_hashing.write(filter_data, bloom_filter.getFilterSizeBytes());
}

template <typename Stream>
void serializeSparseIndex(const DictionaryBlock & sparse_index, Stream & stream)
{
    SerializationString serialization_string;
    SerializationNumber<UInt32> serialization_number;

    serialization_string.serializeBinaryBulk(*sparse_index.tokens, stream.compressed_hashing, 0, sparse_index.tokens->size());
    serialization_number.serializeBinaryBulk(*sparse_index.marks, stream.compressed_hashing, 0, sparse_index.marks->size());
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
        tokens_map.emplace(StringRef(token.data(), token.length()), it, inserted);

        if (inserted)
        {
            arena->insert(token.data(), token.size());
            it->getMapped() = &posting_lists.emplace_back();
        }

        it->getMapped()->add(current_row);
    }

    ++current_row;
}

std::unique_ptr<MergeTreeIndexGranuleTextWritable> MergeTreeIndexTextGranuleBuilder::build()
{
    size_t total_rows = current_row;
    std::vector<std::pair<StringRef, roaring::Roaring *>> sorted_values;
    sorted_values.reserve(tokens_map.size());

    for (auto it = tokens_map.begin(); it != tokens_map.end(); ++it)
        sorted_values.emplace_back(it->getKey(), it->getMapped());

    std::sort(sorted_values.begin(), sorted_values.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    std::vector<StringRef> sorted_tokens;
    std::vector<roaring::Roaring> sorted_posting_lists;

    sorted_tokens.reserve(sorted_values.size());
    sorted_posting_lists.reserve(sorted_values.size());

    static constexpr size_t atom_size = 8;
    size_t bloom_filter_bytes = (params.bloom_filter_bits_per_row * total_rows + atom_size - 1) / atom_size;
    BloomFilter bloom_filter(bloom_filter_bytes, params.bloom_filter_num_hashes, 0);

    for (const auto & [token, posting_list] : sorted_values)
    {
        bloom_filter.add(token.data, token.size);
        sorted_tokens.emplace_back(token);
        sorted_posting_lists.emplace_back(std::move(*posting_list));
    }

    return std::make_unique<MergeTreeIndexGranuleTextWritable>(
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
    // return std::make_shared<MergeTreeIndexConditionText>(predicate, context, index.sample_block, bloom_filter_params, token_extractor);
    UNUSED(predicate);
    UNUSED(context);
    return nullptr;
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
    double bloom_filter_false_positive_rate = getOption<double>(options, ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE).value_or(0.01);

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
