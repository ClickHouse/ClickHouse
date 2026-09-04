#include <Core/SettingsEnums.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/HashTable/HashSet.h>
#include <Common/Logger.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostprocessor.h>
#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/SmallFloat.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <base/arithmeticOverflow.h>
#include <base/range.h>
#include <base/types.h>
#include <fmt/ranges.h>

#include <limits>
#include <numeric>

namespace ProfileEvents
{
    extern const Event TextIndexReadDictionaryBlocks;
    extern const Event TextIndexReadSparseIndexBlocks;
    extern const Event TextIndexReadGranulesMicroseconds;
    extern const Event TextIndexReadPostings;
    extern const Event TextIndexTokensCacheHits;
    extern const Event TextIndexTokensCacheMisses;
    extern const Event TextIndexTokensCacheNegativeHits;
    extern const Event TextIndexTokensCacheNegativeMisses;
    extern const Event TextIndexDiscardPatternScan;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int CORRUPTED_DATA;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsNonZeroUInt64 text_index_dictionary_block_size;
    extern const MergeTreeSettingsBool text_index_dictionary_block_frontcoding_compression;
    extern const MergeTreeSettingsNonZeroUInt64 text_index_posting_list_block_size;
    extern const MergeTreeSettingsTextIndexPostingListCodec text_index_posting_list_codec;
    extern const MergeTreeSettingsMergeTreeTextIndexSerializationVersion text_index_serialization_version;
    extern const MergeTreeSettingsBool allow_experimental_text_index_phrase_search;
    extern const MergeTreeSettingsBool allow_experimental_text_index_scoring;
}

namespace Setting
{
    extern const SettingsUInt64 text_index_like_max_postings_to_read;
    extern const SettingsFloat text_index_hint_max_selectivity;
    extern const SettingsBool use_text_index_negative_tokens_cache;
}

/// The enum values are written verbatim into the text index header and must remain stable.
static_assert(static_cast<UInt64>(MergeTreeTextIndexSerializationVersion::V0_Initial) == 0);
static_assert(static_cast<UInt64>(MergeTreeTextIndexSerializationVersion::V1_WithCodec) == 1);
static_assert(static_cast<UInt64>(MergeTreeTextIndexSerializationVersion::V2_WithPositions) == 2);
static_assert(static_cast<UInt64>(MergeTreeTextIndexSerializationVersion::V3_WithScoring) == 3);

/// Kept as a fixed default rather than a MergeTree setting: a mutable table-level default would let
/// an index's positions value change after parts exist, mixing positional and non-positional parts
/// within one index.
static constexpr bool DEFAULT_POSITIONS = false;

DictionaryBlock::DictionaryBlock(ColumnPtr tokens_, std::vector<TokenPostingsInfo> token_infos_, UInt64 tokens_format_)
    : tokens(std::move(tokens_))
    , token_infos(std::move(token_infos_))
    , tokens_format(tokens_format_)
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

DictionarySparseIndex::DictionarySparseIndex(ColumnPtr tokens_, ColumnPtr offsets_in_file_)
    : tokens(std::move(tokens_)), offsets_in_file(std::move(offsets_in_file_))
{
}

size_t DictionarySparseIndex::size() const
{
    if (const auto * tokens_column = std::get_if<ColumnPtr>(&tokens))
        return *tokens_column ? (*tokens_column)->size() : 0;

    return std::get<BitPackedStringArray>(tokens).size();
}

size_t DictionarySparseIndex::upperBound(std::string_view token) const
{
    auto range = collections::range(0, size());

    auto it = std::upper_bound(range.begin(), range.end(), token, [this](std::string_view lhs, size_t rhs_idx)
    {
        return lhs < getToken(rhs_idx);
    });

    return it - range.begin();
}

std::string_view DictionarySparseIndex::getToken(size_t idx) const
{
    if (const auto * tokens_column = std::get_if<ColumnPtr>(&tokens))
        return assert_cast<const ColumnString &>(**tokens_column).getDataAt(idx);

    return std::get<BitPackedStringArray>(tokens).get(idx);
}

UInt64 DictionarySparseIndex::getOffsetInFile(size_t idx) const
{
    if (const auto * offsets_column = std::get_if<ColumnPtr>(&offsets_in_file))
        return assert_cast<const ColumnUInt64 &>(**offsets_column).getData()[idx];

    return std::get<BitPackedUInt64Array>(offsets_in_file).get(idx);
}

ColumnPtr DictionarySparseIndex::getTokensColumn() const
{
    const auto * tokens_column = std::get_if<ColumnPtr>(&tokens);
    if (!tokens_column || !*tokens_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tokens in sparse index of text index must not be bit-packed here");

    return *tokens_column;
}

ColumnPtr DictionarySparseIndex::getOffsetsColumn() const
{
    const auto * offsets_column = std::get_if<ColumnPtr>(&offsets_in_file);
    if (!offsets_column || !*offsets_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Offsets in sparse index of text index must not be bit-packed here");

    return *offsets_column;
}

size_t DictionarySparseIndex::memoryUsageBytes() const
{
    size_t tokens_bytes = 0;
    size_t offsets_bytes = 0;

    if (const auto * tokens_column = std::get_if<ColumnPtr>(&tokens))
        tokens_bytes = (*tokens_column) ? (*tokens_column)->allocatedBytes() : 0;
    else
        tokens_bytes = std::get<BitPackedStringArray>(tokens).allocatedBytes();

    if (const auto * offsets_column = std::get_if<ColumnPtr>(&offsets_in_file))
        offsets_bytes = (*offsets_column) ? (*offsets_column)->allocatedBytes() : 0;
    else
        offsets_bytes = std::get<BitPackedUInt64Array>(offsets_in_file).allocatedBytes();

    return sizeof(*this) + tokens_bytes + offsets_bytes;
}

void DictionarySparseIndex::optimize()
{
    if (const auto * tokens_column = std::get_if<ColumnPtr>(&tokens); tokens_column && *tokens_column)
    {
        const auto & tokens_string = assert_cast<const ColumnString &>(**tokens_column);
        tokens = BitPackedStringArray(tokens_string.getChars(), tokens_string.getOffsets());
    }

    if (const auto * offsets_column = std::get_if<ColumnPtr>(&offsets_in_file); offsets_column && *offsets_column)
    {
        const auto & offsets_data = assert_cast<const ColumnUInt64 &>(**offsets_column).getData();
        offsets_in_file = BitPackedUInt64Array(std::span(offsets_data.begin(), offsets_data.end()));
    }
}

PostingsSerialization::PostingsSerialization(PostingListCodecPtr posting_list_codec_, MergeTreeTextIndexSerializationVersion serialization_version_)
    : posting_list_codec(std::move(posting_list_codec_))
    , serialization_version(serialization_version_)
    , raw_postings_buffer(MAX_CARDINALITY_FOR_RAW_POSTINGS)
{
    chassert(posting_list_codec);
}

const IPostingListCodec & PostingsSerialization::resolveCodec(UInt64 header)
{
    if (!posting_list_codec)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "No posting list codec is configured");

    /// An uncompressed posting list is always a plain serialized roaring bitmap.
    if (!(header & IsCompressed))
    {
        static const PostingListCodecNone codec_none;
        return codec_none;
    }

    if (serialization_version < MergeTreeTextIndexSerializationVersion::V1_WithCodec)
    {
        /// Pre-V1_WithCodec parts don't persist the codec type, but Bitpacking was the only
        /// compression codec at the time, so an IsCompressed posting list must be Bitpacking.
        if (posting_list_codec->getType() == IPostingListCodec::Type::None)
            posting_list_codec = PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::Bitpacking);
    }

    if (posting_list_codec->getType() == IPostingListCodec::Type::None)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Posting list header marks compressed data but configured codec is None");
    }

    return *posting_list_codec;
}

/// Raw posting lists are never compressed, so the flags are mutually exclusive.
static void checkPostingListFlags(UInt64 header)
{
    using Flags = PostingsSerialization::Flags;

    if ((header & Flags::RawPostings) && (header & Flags::IsCompressed))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Posting list header marks the data as both raw and compressed");
}

static void skipVarUInts(ReadBuffer & istr, size_t count)
{
    for (size_t i = 0; i < count; ++i)
        ignoreVarUInt(istr);
}

template <typename Container>
static void readVarUInts(ReadBuffer & istr, size_t count, Container & values)
{
    values.resize(count);

    for (size_t i = 0; i < count; ++i)
        readVarUInt(values[i], istr);
}

/// Small posting lists are stored as raw VarUInt-encoded row ids, followed by
/// one VarUInt-encoded `(tf - 1)` per row id if the posting list stores term frequencies.
static void deserializeRawPostings(
    ReadBuffer & istr,
    UInt64 header,
    UInt64 cardinality,
    PaddedPODArray<UInt32> & row_ids,
    PaddedPODArray<UInt32> * term_frequencies)
{
    readVarUInts(istr, cardinality, row_ids);
    const bool has_term_frequencies = (header & PostingsSerialization::HasTermFrequencies) != 0;

    if (!has_term_frequencies)
    {
        if (term_frequencies)
            term_frequencies->resize_fill(cardinality, 1u);
        return;
    }

    if (!term_frequencies)
    {
        skipVarUInts(istr, cardinality);
        return;
    }

    readVarUInts(istr, cardinality, *term_frequencies);
    for (auto & tf : *term_frequencies)
        tf += 1;
}

void PostingsSerialization::deserializeToBitmap(ReadBuffer & istr, UInt64 header, UInt64 cardinality, PostingList & postings, PaddedPODArray<UInt32> * term_frequencies)
{
    checkPostingListFlags(header);

    /// Only the codec can decode straight into a bitmap; raw postings and postings
    /// with requested term frequencies go through a plain array of row ids.
    if (!(header & RawPostings) && !term_frequencies)
    {
        bool has_term_frequencies = (header & HasTermFrequencies) != 0;
        resolveCodec(header).decode(istr, postings, has_term_frequencies, raw_data_buffer);
        return;
    }

    deserializeToArray(istr, header, cardinality, raw_postings_buffer, term_frequencies);
    postings.addMany(raw_postings_buffer.size(), raw_postings_buffer.data());
}

void PostingsSerialization::deserializeToArray(ReadBuffer & istr, UInt64 header, UInt64 cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> * term_frequencies)
{
    checkPostingListFlags(header);

    /// The decoded values always overwrite the previous contents of the output arrays.
    row_ids.clear();
    if (term_frequencies)
        term_frequencies->clear();

    if (header & RawPostings)
    {
        deserializeRawPostings(istr, header, cardinality, row_ids, term_frequencies);
        return;
    }

    const bool has_term_frequencies = (header & HasTermFrequencies) != 0;
    const auto & codec = resolveCodec(header);

    if (term_frequencies && has_term_frequencies)
    {
        /// The codec decodes both the row ids and the exact per-row term frequencies.
        codec.decodeWithTermFrequencies(istr, row_ids, *term_frequencies, raw_data_buffer);
    }
    else
    {
        codec.decode(istr, row_ids, has_term_frequencies, raw_data_buffer);

        /// A posting list without stored term frequencies implies `tf == 1` for every row.
        if (term_frequencies)
            term_frequencies->resize_fill(row_ids.size(), 1u);
    }
}


bool RowsRange::intersects(const RowsRange & other) const
{
    return (begin <= other.begin && other.begin <= end) || (other.begin <= begin && begin <= other.end);
}

std::optional<RowsRange> RowsRange::intersectWith(const RowsRange & other) const
{
    if (!intersects(other))
        return std::nullopt;

    return RowsRange(std::max(begin, other.begin), std::min(end, other.end));
}

RowsRange RowsRange::unionWith(const RowsRange & other) const
{
    return RowsRange(std::min(begin, other.begin), std::max(end, other.end));
}

std::vector<size_t> TokenPostingsInfo::getBlocksToRead(const RowsRange & range) const
{
    std::vector<size_t> blocks;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        if (ranges[i].intersects(range))
            blocks.emplace_back(i);
    }
    return blocks;
}

size_t TokenPostingsInfo::bytesAllocated() const
{
    return sizeof(TokenPostingsInfo)
        + offsets.capacity() * sizeof(UInt64)
        + ranges.capacity() * sizeof(RowsRange)
        + (embedded_postings.capacity() > MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS ? embedded_postings.capacity() * sizeof(UInt32) : 0);
}

MergeTreeIndexGranuleText::MergeTreeIndexGranuleText(MergeTreeIndexTextParams params_)
    : params(std::move(params_))
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

namespace
{

ColumnPtr deserializeTokensRaw(ReadBuffer & istr, size_t num_tokens)
{
    auto tokens_column = ColumnString::create();
    tokens_column->reserve(num_tokens);

    auto serialization_string = SerializationString::create();
    serialization_string->deserializeBinaryBulk(*tokens_column, istr, num_tokens, 0.0);

    return tokens_column;
}

ColumnPtr deserializeTokensFrontCoding(ReadBuffer & istr, size_t num_tokens)
{
    auto tokens_column = ColumnString::create();

    if (num_tokens != 0)
    {
        tokens_column->reserve(num_tokens);
        ColumnString::Chars & data = tokens_column->getChars();
        ColumnString::Offsets & offsets = tokens_column->getOffsets();

        /// Avoiding calling resize in a loop improves the performance.
        /// The average length of words in English language is 4.7 characters, rounded up to the next power of 2.
        data.resize(roundUpToPowerOfTwoOrZero(num_tokens * 8));

        size_t offset = 0;

        /// Read the first token
        {
            UInt64 first_token_size = 0;
            readVarUInt(first_token_size, istr);
            /// Prevent a corrupt or malicious .dct file from allocating huge amounts of memory
            if (first_token_size > SerializationString::MAX_STRING_SIZE)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: first token size ({}) exceeds the maximum ({})", first_token_size, SerializationString::MAX_STRING_SIZE);
            offset += first_token_size;
            if (offset > data.size())
                data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset, data.size() * 2)));
            istr.readStrict(reinterpret_cast<char *>(data.data()), first_token_size);
            offsets.push_back(offset);
        }

        size_t previous_token_offset = 0;
        for (size_t i = 1; i < num_tokens; ++i)
        {
            const UInt64 data_offset = offset;

            UInt64 lcp = 0;
            readVarUInt(lcp, istr);
            UInt64 data_size = 0;
            readVarUInt(data_size, istr);

            /// Reject a corrupted or malicious `.dct`: an out-of-range `lcp` or an overflowing `lcp + data_size` would wrap `offset`, skip the resize, and cause an out-of-bounds write below.
            const UInt64 previous_token_size = data_offset - previous_token_offset;
            if (lcp > previous_token_size)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Corrupted text index dictionary: front-coding longest common prefix ({}) exceeds the previous token size ({})",
                    lcp, previous_token_size);

            UInt64 token_size = 0;
            UInt64 next_offset = 0;
            if (common::addOverflow(lcp, data_size, token_size) || common::addOverflow<UInt64>(offset, token_size, next_offset))
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Corrupted text index dictionary: front-coding token size overflows (lcp = {}, data_size = {})",
                    lcp, data_size);

            if (token_size > SerializationString::MAX_STRING_SIZE)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: front-coding token size ({}) exceeds the maximum ({})", token_size, SerializationString::MAX_STRING_SIZE);

            offset = next_offset;

            if (offset > data.size())
                data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset, data.size() * 2)));

            std::memcpy(&data[data_offset], &data[previous_token_offset], lcp);
            istr.readStrict(reinterpret_cast<char *>(&data[data_offset + lcp]), data_size);

            offsets.push_back(offset);
            previous_token_offset = data_offset;
        }

        data.resize_exact(offset);
    }

    return tokens_column;
}

}

MergeTreeIndexGranuleText::~MergeTreeIndexGranuleText() = default;

void MergeTreeIndexGranuleText::deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReadGranulesMicroseconds);
    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);

    auto * index_stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    auto * dictionary_stream = streams.at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto * postings_stream = streams.at(MergeTreeIndexSubstream::Type::TextIndexPostings);

    if (!index_stream || !dictionary_stream || !postings_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be deserialized with 3 streams: index, dictionary, postings. One of the streams is missing");

    if (index_id_for_caches.empty())
    {
        const auto & part_storage = *state.part_info.getDataPartStorage();
        index_id_for_caches = fmt::format("{}:{}:{}", part_storage.getDiskName(), part_storage.getFullPath(), state.index.getFileName());
    }

    is_empty = false;
    analyzer = std::make_unique<TextIndexAnalyzer>(condition_text);
    scoring_enabled = condition_text.isScoringEnabled();

    /// Push the row ranges still readable after the analysis of the primary key and prior skip indexes into the analyzer.
    if (state.readable_ranges)
    {
        const auto & index_granularity = state.part_info.getIndexGranularity();
        std::vector<RowsRange> readable_row_ranges;
        readable_row_ranges.reserve(state.readable_ranges->size());

        for (const auto & range : *state.readable_ranges)
        {
            size_t row_begin = index_granularity.getMarkStartingRow(range.begin);
            size_t row_end = index_granularity.getMarkStartingRow(range.end);

            if (row_begin < row_end)
                readable_row_ranges.emplace_back(row_begin, row_end - 1);
        }

        analyzer->setReadableRows(std::move(readable_row_ranges));
    }

    auto text_index_header = loadHeader(*index_stream, state);
    auto postings_codec = PostingListCodecFactory::createPostingListCodec(text_index_header->codec_type);
    auto postings_serialization = PostingsSerialization(std::move(postings_codec), text_index_header->version);
    serialization_version = text_index_header->version;
    positions_codec = text_index_header->positions_codec;
    scoring_stats = text_index_header->scoring_stats;

    analyzeDictionaryForTokens(text_index_header->sparse_index, *dictionary_stream, state);
    analyzeDictionaryForPatterns(text_index_header->sparse_index, *dictionary_stream, state);
    if (state.text_index_read_postings)
        analyzePostings(postings_serialization, *postings_stream, state);

    const auto & settings = condition_text.getContext()->getSettingsRef();
    analyzer->analyzeCardinalitiesAndBypassHints(static_cast<double>(settings[Setting::text_index_hint_max_selectivity]), state.part_info.getRowCount());

    /// Capture the codec after the analysis — for pre-V1_WithCodec parts the
    /// codec may have been lazily installed while decoding an IsCompressed posting list.
    postings_codec_type = postings_serialization.getPostingListCodec()->getType();
}

void MergeTreeIndexGranuleText::analyzeDictionaryForTokens(
    const DictionarySparseIndex & sparse_index,
    MergeTreeIndexReaderStream & dictionary_stream,
    MergeTreeIndexDeserializationState & state)
{
    if (sparse_index.empty())
        return;

    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    auto cardinalities_cache = condition_text.cardinalitiesCache();
    auto tokens_to_read = fillTokensFromCache(state);

    if (tokens_to_read.empty() || analyzer->alwaysFalse())
    {
        cardinalities_cache->update(analyzer->getAllTokenInfos(), analyzer->getMissingTokens(), state.part_info.getRowCount());
        return;
    }

    auto tokens_cache = condition_text.tokensCache();
    const bool use_negative_tokens_cache = condition_text.getContext()->getSettingsRef()[Setting::use_text_index_negative_tokens_cache];
    cardinalities_cache->sortTokens(tokens_to_read);

    LOG_TEST(getLogger("MergeTreeIndexGranuleText"), "Reading tokens {} from part {}", toString(tokens_to_read), state.part_info.getDataPartStorage()->getFullPath());

    /// Collect blocks ids in the same order as tokens are sorted by cardinality.
    std::vector<size_t> blocks_ids_to_read;
    std::unordered_map<size_t, std::vector<std::string_view>> block_id_to_tokens;

    for (const auto & token : tokens_to_read)
    {
        size_t idx = sparse_index.upperBound(token);
        if (idx != 0)
            --idx;

        auto [it, inserted] = block_id_to_tokens.try_emplace(idx);
        if (inserted)
            blocks_ids_to_read.emplace_back(idx);

        it->second.emplace_back(token);
    }

    for (const auto & block_idx : blocks_ids_to_read)
    {
        auto & needed_tokens = block_id_to_tokens[block_idx];

        std::erase_if(needed_tokens, [&](const auto & token)
        {
            return !analyzer->isTokenNeeded(token);
        });

        if (needed_tokens.empty())
            continue;

        /// Seek to the dictionary block and deserialize tokens.
        UInt64 offset_in_file = sparse_index.getOffsetInFile(block_idx);
        dictionary_stream.seekToMark({offset_in_file, 0});
        auto * data_buffer = dictionary_stream.getDataBuffer();

        ProfileEvents::increment(ProfileEvents::TextIndexReadDictionaryBlocks);
        auto tokens_column = TextIndexSerialization::deserializeTokens(*data_buffer).first;
        const auto & block_tokens = assert_cast<const ColumnString &>(*tokens_column);
        auto [matched_indices, missing_tokens] = matchTokens(block_tokens, std::move(needed_tokens));

        for (const auto & token : missing_tokens)
        {
            if (use_negative_tokens_cache)
            {
                tokens_cache->setNotFound(TextIndexTokensCache::hash(index_id_for_caches, token));
                ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheNegativeMisses);
            }

            analyzer->addMissingToken(token);
        }

        if (analyzer->alwaysFalse())
        {
            cardinalities_cache->update(analyzer->getAllTokenInfos(), analyzer->getMissingTokens(), state.part_info.getRowCount());
            return;
        }

        /// Deserialize only the token infos for matched tokens.
        auto infos = TextIndexSerialization::deserializeTokenInfos(*data_buffer, block_tokens.size(), matched_indices);

        for (size_t i = 0; i < matched_indices.size(); ++i)
        {
            String token(block_tokens.getDataAt(matched_indices[i]));
            auto token_hash = TextIndexTokensCache::hash(index_id_for_caches, token);
            tokens_cache->set(token_hash, infos[i]);
            analyzer->addTokenInfo(token, infos[i]);
        }

        if (analyzer->alwaysFalse())
        {
            cardinalities_cache->update(analyzer->getAllTokenInfos(), analyzer->getMissingTokens(), state.part_info.getRowCount());
            return;
        }
    }

    cardinalities_cache->update(analyzer->getAllTokenInfos(), analyzer->getMissingTokens(), state.part_info.getRowCount());
}

void MergeTreeIndexGranuleText::analyzeDictionaryForPatterns(
    const DictionarySparseIndex & sparse_index,
    MergeTreeIndexReaderStream & dictionary_stream,
    MergeTreeIndexDeserializationState & state)
{
    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    if (!condition_text.hasSearchPatterns())
        return;

    if (sparse_index.empty())
        return;

    const size_t max_postings_to_read = condition_text.getContext()->getSettingsRef()[Setting::text_index_like_max_postings_to_read];

    size_t postings_to_read = 0;
    std::vector<size_t> matched_indices;
    for (size_t block_idx = 0; block_idx < sparse_index.size(); ++block_idx)
    {
        /// TODO(ahmadov): Include the byte size of token infos into dictionary block to avoid multi-seek.
        UInt64 offset_in_file = sparse_index.getOffsetInFile(block_idx);
        dictionary_stream.seekToMark({offset_in_file, 0});
        auto * data_buffer = dictionary_stream.getDataBuffer();

        auto tokens_column = TextIndexSerialization::deserializeTokens(*data_buffer).first;
        const auto & block_tokens = assert_cast<const ColumnString &>(*tokens_column);
        size_t num_tokens = block_tokens.size();

        matched_indices.clear();

        for (size_t token_idx = 0; token_idx < num_tokens; ++token_idx)
        {
            const auto & token = block_tokens.getDataAt(token_idx);
            if (analyzer->addTokenToPatterns(token))
                matched_indices.emplace_back(token_idx);
        }

        if (matched_indices.empty())
            continue;

        /// Deserialize only the token infos for matched tokens.
        auto infos = TextIndexSerialization::deserializeTokenInfos(*data_buffer, num_tokens, matched_indices);

        for (size_t i = 0; i < matched_indices.size(); ++i)
        {
            String token(block_tokens.getDataAt(matched_indices[i]));
            postings_to_read += !(infos[i]->header & PostingsSerialization::Flags::EmbeddedPostings);
            analyzer->addTokenInfo(token, infos[i]);
        }

        if (postings_to_read > max_postings_to_read)
        {
            /// Too many large-posting tokens matched.
            /// Not all dictionary blocks were scanned, so the set of matched pattern tokens is incomplete.
            analyzer->bypassPatternQueries();
            ProfileEvents::increment(ProfileEvents::TextIndexDiscardPatternScan);
            return;
        }
    }
}

std::vector<String> MergeTreeIndexGranuleText::fillTokensFromCache(MergeTreeIndexDeserializationState & state)
{
    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    const auto & all_search_tokens = condition_text.getAllSearchTokens();
    auto tokens_cache = condition_text.tokensCache();
    const bool use_negative_tokens_cache
        = condition_text.getContext()->getSettingsRef()[Setting::use_text_index_negative_tokens_cache];

    std::vector<TextIndexTokensCache::Key> keys;
    keys.reserve(all_search_tokens.size());

    for (const auto & token : all_search_tokens)
        keys.emplace_back(TextIndexTokensCache::hash(index_id_for_caches, token));

    auto cached_infos = tokens_cache->getMany(keys);
    std::vector<String> tokens_to_read;

    for (size_t i = 0; i < all_search_tokens.size(); ++i)
    {
        if (cached_infos[i])
        {
            if (TextIndexTokensCache::isNotFound(cached_infos[i]))
            {
                if (use_negative_tokens_cache)
                {
                    analyzer->addMissingToken(all_search_tokens[i]);
                    ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheNegativeHits);
                    continue;
                }
            }
            else
            {
                analyzer->addTokenInfo(all_search_tokens[i], cached_infos[i]);
                ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheHits);
                continue;
            }
        }

        tokens_to_read.emplace_back(all_search_tokens[i]);
        ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheMisses);
    }

    return tokens_to_read;
}

std::pair<std::vector<size_t>, NameSet> MergeTreeIndexGranuleText::matchTokens(const ColumnString & all_tokens, std::vector<std::string_view> needed_tokens)
{
    NameSet missing_tokens;
    std::vector<size_t> matched_indices;
    matched_indices.reserve(needed_tokens.size());

    size_t num_tokens = all_tokens.size();
    auto idx_range = collections::range(0, num_tokens);
    auto it_begin = idx_range.begin();

    /// Sort tokens lexicographically for correct binary search in the dictionary.
    std::sort(needed_tokens.begin(), needed_tokens.end());

    for (const auto & token : needed_tokens)
    {
        /// Use binary search to find indices of needed tokens in the block.
        auto it = std::lower_bound(it_begin, idx_range.end(), token, [&all_tokens](size_t lhs_idx, std::string_view rhs_ref)
        {
            return all_tokens.getDataAt(lhs_idx) < rhs_ref;
        });

        it_begin = it;
        size_t idx_in_block = it - idx_range.begin();

        if (idx_in_block < num_tokens && all_tokens.getDataAt(idx_in_block) == token)
            matched_indices.emplace_back(idx_in_block);
        else
            missing_tokens.insert(String(token));
    }

    return {std::move(matched_indices), std::move(missing_tokens)};
}

std::shared_ptr<TextIndexHeader> MergeTreeIndexGranuleText::loadHeader(MergeTreeIndexReaderStream & header_stream, MergeTreeIndexDeserializationState & state)
{
    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);

    const auto load_header = [&]
    {
        header_stream.seekToStart();
        auto loaded_header = std::make_shared<TextIndexHeader>(TextIndexSerialization::deserializeHeader(*header_stream.getDataBuffer()));

        /// Optimize the memory usage of the sparse index only if the header is put into the global cache.
        if (condition_text.useGlobalHeaderCache())
            loaded_header->sparse_index.optimize();

        return loaded_header;
    };

    auto header_hash = TextIndexHeaderCache::hash(index_id_for_caches);
    return condition_text.headerCache()->getOrSet(header_hash, load_header);
}

MergeTreeIndexGranuleText::PostingsBlock MergeTreeIndexGranuleText::readPostingsBlock(
    MergeTreeIndexReaderStream & stream,
    MergeTreeIndexDeserializationState & state,
    const TokenPostingsInfo & token_info,
    size_t block_idx,
    PostingsSerialization & postings_serialization,
    const String & index_id_for_caches,
    bool with_scoring)
{
    auto * data_buffer = stream.getDataBuffer();
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*state.condition);
    auto & postings_cache = *condition_text.postingsCache();

    UInt64 offset_in_file = token_info.offsets[block_idx];
    auto postings_key = TextIndexPostingsCache::hash(index_id_for_caches, offset_in_file, static_cast<UInt8>(TextIndexPostingsCacheKind::Roaring));

    if (!with_scoring)
    {
        const auto load_postings = [&]
        {
            ProfileEvents::increment(ProfileEvents::TextIndexReadPostings);
            stream.seekToMark({offset_in_file, 0});
            auto postings = std::make_shared<PostingList>();
            postings_serialization.deserializeToBitmap(*data_buffer, token_info.header, token_info.cardinality, *postings, nullptr);
            return std::make_shared<TextIndexPostingsCacheCell>(std::move(postings));
        };

        auto cell = postings_cache.getOrSet(postings_key, load_postings);
        return {.postings = std::get<PostingListPtr>(cell->value), .scoring = {}};
    }

    /// Scoring also needs the postings as a flat sorted array of row ids with their term frequencies.
    auto scoring_key = TextIndexPostingsCache::hash(index_id_for_caches, offset_in_file, static_cast<UInt8>(TextIndexPostingsCacheKind::ScoringPostings));

    const auto load_scoring_postings = [&]
    {
        ProfileEvents::increment(ProfileEvents::TextIndexReadPostings);
        stream.seekToMark({offset_in_file, 0});

        /// For a posting list without stored term frequencies the array is filled with ones.
        auto scoring = std::make_shared<ScoringPostings>();
        postings_serialization.deserializeToArray(*data_buffer, token_info.header, token_info.cardinality, scoring->row_ids, &scoring->term_frequencies);
        scoring->calculateMaxTermFrequency();

        return std::make_shared<TextIndexPostingsCacheCell>(ScoringPostingsPtr(std::move(scoring)));
    };

    auto scoring_cell = postings_cache.getOrSet(scoring_key, load_scoring_postings);
    const auto & scoring_postings = std::get<ScoringPostingsPtr>(scoring_cell->value);

    /// The bitmap for the match stage is built from the flat array, without re-reading the stream.
    const auto load_postings_from_array = [&]
    {
        auto postings = std::make_shared<PostingList>();
        postings->addMany(scoring_postings->row_ids.size(), scoring_postings->row_ids.data());
        return std::make_shared<TextIndexPostingsCacheCell>(std::move(postings));
    };

    auto postings_cell = postings_cache.getOrSet(postings_key, load_postings_from_array);
    return {.postings = std::get<PostingListPtr>(postings_cell->value), .scoring = scoring_postings};
}

ScoringPostingsPtr MergeTreeIndexGranuleText::getScoringPostings(UInt64 offset_in_file) const
{
    auto it = scoring_postings_by_offset.find(offset_in_file);
    return it != scoring_postings_by_offset.end() ? it->second : nullptr;
}

void MergeTreeIndexGranuleText::analyzePostings(PostingsSerialization & postings_serialization, MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state)
{
    if (analyzer->alwaysFalse())
        return;

    using enum PostingsSerialization::Flags;
    const auto & token_infos = analyzer->getAllTokenInfos();

    std::vector<std::pair<std::string_view, TokenPostingsInfoPtr>> tokens_to_read;
    tokens_to_read.reserve(token_infos.size());

    for (const auto & [token, token_info] : token_infos)
    {
        if (token_info->offsets.size() == 1 && analyzer->isTokenNeeded(token) && !analyzer->hasReadPostings(token))
            tokens_to_read.emplace_back(token, token_info);
    }

    /// Sort tokens by cardinality to read the most rare ones first.
    std::ranges::sort(tokens_to_read, [](const auto & lhs, const auto & rhs)
    {
        return lhs.second->cardinality < rhs.second->cardinality;
    });

    for (const auto & [token, token_info] : tokens_to_read)
    {
        /// Check one more time, because query with this token may have been
        /// discarded by the analyzer after reading postings for previous tokens.
        if (analyzer->isTokenNeeded(token))
        {
            auto block = readPostingsBlock(stream, state, *token_info, 0, postings_serialization, index_id_for_caches, scoring_enabled);

            /// Keep the flat postings for the BM25 scoring cursors of the query.
            if (block.scoring)
                scoring_postings_by_offset.emplace(token_info->offsets[0], std::move(block.scoring));

            analyzer->addPostings(token, *block.postings);
        }

        if (analyzer->alwaysFalse())
            break;
    }
}

size_t MergeTreeIndexGranuleText::memoryUsageBytes() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method memoryUsageBytes is not implemented for MergeTreeIndexGranuleText");
}

MergeTreeIndexGranuleTextWritable::MergeTreeIndexGranuleTextWritable(
    MergeTreeIndexTextParams params_,
    IPostingListCodec::Type posting_list_codec_type_,
    TokenToPostingsBuilderMap && tokens_map_,
    std::unique_ptr<Arena> && arena_,
    SortedTokens && sorted_tokens_,
    PaddedPODArray<UInt8> && doc_lengths_,
    UInt64 num_docs_,
    UInt64 sum_doc_length_)
    : params(std::move(params_))
    , posting_list_codec_type(posting_list_codec_type_)
    , tokens_map(std::move(tokens_map_))
    , arena(std::move(arena_))
    , sorted_tokens(std::move(sorted_tokens_))
    , logger(getLogger("TextIndexGranuleWriter"))
    , doc_lengths(std::move(doc_lengths_))
    , num_docs(num_docs_)
    , sum_doc_length(sum_doc_length_)
{
}

namespace
{

size_t computeCommonPrefixLength(const std::string_view lhs, const std::string_view rhs)
{
    size_t common_prefix_length = 0;
    size_t max_length = std::min(lhs.size(), rhs.size());

    while (common_prefix_length < max_length && lhs[common_prefix_length] == rhs[common_prefix_length])
        ++common_prefix_length;

    return common_prefix_length;
}

template <typename TokenGetter>
void serializeTokensRaw(
    const TokenGetter & token_getter,
    WriteBuffer & ostr,
    size_t block_begin,
    size_t block_end)
{
    /// Write tokens the same as in SerializationString::serializeBinaryBulk
    /// to be able to read them later with SerializationString::deserializeBinaryBulk.

    for (size_t i = block_begin; i < block_end; ++i)
    {
        auto current_token = token_getter(i);
        TextIndexSerialization::checkTokenSize(current_token.size());
        writeVarUInt(current_token.size(), ostr);
        ostr.write(current_token.data(), current_token.size());
    }
}

/*
 * The front coding implementation is based on the idea from following papers.
 * 1. https://doi.org/10.1109/Innovate-Data.2017.9
 * 2. https://doi.org/10.1145/3448016.345279
 */
template <typename TokenGetter>
void serializeTokensFrontCoding(
    const TokenGetter & token_getter,
    WriteBuffer & ostr,
    size_t block_begin,
    size_t block_end)
{
    const auto & first_token = token_getter(block_begin);
    TextIndexSerialization::checkTokenSize(first_token.size());
    writeVarUInt(first_token.size(), ostr);
    ostr.write(first_token.data(), first_token.size());

    std::string_view previous_token = first_token;
    for (size_t i = block_begin + 1; i < block_end; ++i)
    {
        auto current_token = token_getter(i);
        TextIndexSerialization::checkTokenSize(current_token.size());
        auto lcp = computeCommonPrefixLength(previous_token, current_token);
        writeVarUInt(lcp, ostr);
        writeVarUInt(current_token.size() - lcp, ostr);
        ostr.write(current_token.data() + lcp, current_token.size() - lcp);
        previous_token = current_token;
    }
}

template <typename TokenGetter>
void serializeTokensImpl(
    const TokenGetter & token_getter,
    WriteBuffer & ostr,
    TextIndexSerialization::TokensFormat format,
    size_t block_begin,
    size_t block_end)
{
    size_t num_tokens_in_block = block_end - block_begin;
    writeVarUInt(static_cast<UInt64>(format), ostr);
    writeVarUInt(num_tokens_in_block, ostr);

    switch (format)
    {
        case TextIndexSerialization::TokensFormat::RawStrings:
            serializeTokensRaw(token_getter, ostr, block_begin, block_end);
            break;
        case TextIndexSerialization::TokensFormat::FrontCodedStrings:
            serializeTokensFrontCoding(token_getter, ostr, block_begin, block_end);
            break;
    }
}

}

void TextIndexSerialization::serializePostingsAndTokenInfo(
    PostingListBuilder && postings,
    const PostingListBuildContext & context,
    MergeTreeIndexWriterStream & dictionary_stream,
    MergeTreeIndexWriterStream & postings_stream,
    MergeTreeIndexWriterStream * positions_stream)
{
    using enum PostingsSerialization::Flags;

    TokenPostingsInfo info;
    std::span<const UInt32> raw_values;
    std::span<const UInt32> raw_tf_minus_one;

    if (postings.hasInline())
    {
        const auto & inline_state = postings.getInline();
        info.cardinality = inline_state.size;
        raw_values = {inline_state.values.data(), inline_state.size};
    }
    else
    {
        auto & large = postings.getLarge();

        /// If the encoder already holds full segments, flush everything left into it.
        /// Otherwise flush only when the buffered values exceed the raw-postings threshold.
        if (large.encoder || large.values.size() > MAX_CARDINALITY_FOR_RAW_POSTINGS)
        {
            large.flush(context);
        }

        if (large.values.empty())
        {
            chassert(large.encoder);
            info.cardinality = static_cast<UInt32>(large.encoder->cardinality());
        }
        else
        {
            chassert(!large.encoder);
            info.cardinality = static_cast<UInt32>(large.values.size());
            raw_values = {large.values.data(), info.cardinality};

            if (large.term_frequencies)
                raw_tf_minus_one = large.term_frequencies->getTfMinusOne();
        }
    }

    if (!raw_tf_minus_one.empty())
    {
        chassert(raw_tf_minus_one.size() == raw_values.size());
        info.header |= HasTermFrequencies;
    }

    if (positions_stream)
    {
        auto * positions = postings.getPositions();
        chassert(positions);
        positions->finalizeOrdering();
        const auto & position_entries = positions->getEntries();

        info.header |= HasPositions;
        info.position_offset = positions_stream->plain_hashing.count();
        TextIndexBlockedPositionsCodec::encode(position_entries, positions_stream->plain_hashing);
        info.position_bytes = positions_stream->plain_hashing.count() - info.position_offset;
    }

    /// Tiny posting lists are embedded into the dictionary block
    if (info.cardinality <= MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS)
    {
        chassert(raw_values.size() == info.cardinality);
        info.header |= RawPostings;
        info.header |= EmbeddedPostings;

        TextIndexSerialization::serializeTokenInfo(dictionary_stream.compressed_hashing, info);
        TextIndexSerialization::serializeRawPostings(raw_values, raw_tf_minus_one, dictionary_stream.compressed_hashing);
        return;
    }

    /// Small posting lists are serialized as raw VarUInts.
    if (info.cardinality <= MAX_CARDINALITY_FOR_RAW_POSTINGS)
    {
        chassert(raw_values.size() == info.cardinality);
        info.header |= RawPostings;
        info.header |= SingleBlock;

        info.offsets.emplace_back(postings_stream.plain_hashing.count());
        info.ranges.emplace_back(raw_values.front(), raw_values.back());

        TextIndexSerialization::serializeRawPostings(raw_values, raw_tf_minus_one, postings_stream.plain_hashing);
        TextIndexSerialization::serializeTokenInfo(dictionary_stream.compressed_hashing, info);
        return;
    }

    /// The flush above put everything into the encoder (the cardinality is above the raw threshold).
    chassert(postings.hasLarge());
    auto & large = postings.getLarge();
    chassert(large.values.empty() && large.encoder);
    large.encoder->finalize(postings_stream.plain_hashing, info);
    TextIndexSerialization::serializeTokenInfo(dictionary_stream.compressed_hashing, info);
}

void TextIndexSerialization::serializeRawPostings(std::span<const UInt32> row_ids, std::span<const UInt32> tf_minus_one, WriteBuffer & ostr)
{
    for (UInt32 row_id : row_ids)
        writeVarUInt(row_id, ostr);

    /// One exact VarUInt-encoded `(tf - 1)` per row id, parallel to `row_ids`, when scoring.
    for (UInt32 tf_m1 : tf_minus_one)
        writeVarUInt(tf_m1, ostr);
}

void TextIndexSerialization::checkTokenSize(size_t token_size)
{
    if (token_size > SerializationString::MAX_STRING_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size: {}. The maximum is: {}.", token_size, SerializationString::MAX_STRING_SIZE);
}

void TextIndexSerialization::serializeTokens(const ColumnString & tokens, WriteBuffer & ostr, TokensFormat format)
{
    serializeTokensImpl(
        [&](size_t i) { return tokens.getDataAt(i); },
        ostr,
        format,
        /*block_begin=*/ 0,
        /*block_end=*/ tokens.size());
}

void TextIndexSerialization::serializeTokenInfo(WriteBuffer & ostr, const TokenPostingsInfo & token_info)
{
    using enum PostingsSerialization::Flags;
    chassert(token_info.offsets.size() == token_info.ranges.size());

    writeVarUInt(token_info.header, ostr);
    writeVarUInt(token_info.cardinality, ostr);

    /// Position metadata is right after (header, cardinality), before posting data.
    if (token_info.header & HasPositions)
    {
        writeVarUInt(token_info.position_offset, ostr);
        writeVarUInt(token_info.position_bytes, ostr);
    }

    /// Embedded postings will be serialized later into the dictionary block.
    if (token_info.header & EmbeddedPostings)
        return;

    if (!(token_info.header & SingleBlock))
        writeVarUInt(token_info.offsets.size(), ostr);

    for (size_t i = 0; i < token_info.offsets.size(); ++i)
    {
        writeVarUInt(token_info.offsets[i], ostr);
        writeVarUInt(token_info.ranges[i].begin, ostr);
        writeVarUInt(token_info.ranges[i].end, ostr);
    }
}

void TextIndexSerialization::serializeHeader(const TextIndexHeader & header, WriteBuffer & ostr)
{
    const auto version = header.version;

    /// `textIndexCreator` raises the version to one that can represent the codec
    /// and positions, so a violation here is a logical error, not a user error.
    if (header.codec_type != IPostingListCodec::Type::None && version < MergeTreeTextIndexSerializationVersion::V1_WithCodec)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index version 'v0_initial' does not support a posting list codec");

    if (header.has_positions && version < MergeTreeTextIndexSerializationVersion::V2_WithPositions)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index version {} does not support positions", static_cast<UInt64>(version));

    if (header.has_scoring && version < MergeTreeTextIndexSerializationVersion::V3_WithScoring)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index version {} does not support BM25 scoring", static_cast<UInt64>(version));

    writeVarUInt(static_cast<UInt64>(version), ostr);

    if (version >= MergeTreeTextIndexSerializationVersion::V1_WithCodec)
        writeVarUInt(static_cast<UInt64>(header.codec_type), ostr);

    if (version >= MergeTreeTextIndexSerializationVersion::V2_WithPositions)
    {
        writeVarUInt(static_cast<UInt64>(header.has_positions), ostr);
        /// Only a part that has positions carries their codec.
        if (header.has_positions)
            writeVarUInt(static_cast<UInt64>(header.positions_codec), ostr);
    }

    if (version >= MergeTreeTextIndexSerializationVersion::V3_WithScoring)
        writeVarUInt(static_cast<UInt64>(header.has_scoring), ostr);

    if (header.has_scoring)
    {
        writeVarUInt(header.scoring_stats.num_docs, ostr);
        writeVarUInt(header.scoring_stats.sum_doc_length, ostr);
        writeVarUInt(header.scoring_stats.doc_lengths_segment_size, ostr);
        writeVarUInt(header.scoring_stats.doc_lengths_segment_offsets.size(), ostr);

        UInt64 prev = 0;
        for (UInt64 off : header.scoring_stats.doc_lengths_segment_offsets)
        {
            writeVarUInt(off - prev, ostr);
            prev = off;
        }
    }

    /// Sparse indexes are created with raw columns and bit-packed only by optimize.
    /// The write path never calls optimize, so expect the raw columns here.
    auto tokens_column = header.sparse_index.getTokensColumn();
    auto offsets_column = header.sparse_index.getOffsetsColumn();
    chassert(tokens_column->size() == offsets_column->size());

    auto serialization_string = SerializationString::create();
    auto serialization_number = SerializationNumber<UInt64>::create();

    writeVarUInt(tokens_column->size(), ostr);
    serialization_string->serializeBinaryBulk(*tokens_column, ostr, 0, tokens_column->size());
    serialization_number->serializeBinaryBulk(*offsets_column, ostr, 0, offsets_column->size());
}

TextIndexHeader TextIndexSerialization::deserializeHeaderPrefix(ReadBuffer & istr)
{
    UInt64 version = 0;
    readVarUInt(version, istr);

    if (version > static_cast<UInt64>(MergeTreeTextIndexSerializationVersion::V3_WithScoring))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unsupported version of sparse index ({})", version);

    TextIndexHeader header;
    header.version = static_cast<MergeTreeTextIndexSerializationVersion>(version);

    if (header.version >= MergeTreeTextIndexSerializationVersion::V1_WithCodec)
    {
        UInt64 codec_type = 0;
        readVarUInt(codec_type, istr);

        if (codec_type > static_cast<UInt64>(IPostingListCodec::Type::Bitpacking))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown posting list codec type in text index header: {}", codec_type);

        header.codec_type = static_cast<IPostingListCodec::Type>(codec_type);
    }

    /// The `has_positions` flag is written after the codec for v >= `V2_WithPositions`.
    if (header.version >= MergeTreeTextIndexSerializationVersion::V2_WithPositions)
    {
        UInt64 has_positions = 0;
        readVarUInt(has_positions, istr);
        header.has_positions = has_positions != 0;

        if (header.has_positions)
        {
            UInt64 positions_codec = 0;
            readVarUInt(positions_codec, istr);
            if (positions_codec != static_cast<UInt64>(TextIndexPositionCodec::Encoding::BlockedPfor))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Unknown positions codec {} in text index header", positions_codec);
            header.positions_codec = static_cast<UInt8>(positions_codec);
        }
    }

    /// The `has_scoring` flag is written after `has_positions` for v >= `V3_WithScoring`.
    if (header.version >= MergeTreeTextIndexSerializationVersion::V3_WithScoring)
    {
        UInt64 has_scoring = 0;
        readVarUInt(has_scoring, istr);
        header.has_scoring = has_scoring != 0;
    }

    /// BM25 corpus stats.
    if (header.has_scoring)
    {
        readVarUInt(header.scoring_stats.num_docs, istr);
        readVarUInt(header.scoring_stats.sum_doc_length, istr);
    }

    return header;
}

TextIndexHeader TextIndexSerialization::deserializeHeader(ReadBuffer & istr)
{
    ProfileEvents::increment(ProfileEvents::TextIndexReadSparseIndexBlocks);
    TextIndexHeader header = deserializeHeaderPrefix(istr);

    if (header.has_scoring)
    {
        readVarUInt(header.scoring_stats.doc_lengths_segment_size, istr);

        UInt64 num_segments = 0;
        readVarUInt(num_segments, istr);

        header.scoring_stats.doc_lengths_segment_offsets.resize(num_segments);
        UInt64 offset = 0;

        for (UInt64 i = 0; i < num_segments; ++i)
        {
            UInt64 delta = 0;
            readVarUInt(delta, istr);
            offset += delta;
            header.scoring_stats.doc_lengths_segment_offsets[i] = offset;
        }
    }

    size_t num_sparse_index_tokens = 0;
    readVarUInt(num_sparse_index_tokens, istr);

    auto tokens = deserializeTokensRaw(istr, num_sparse_index_tokens);
    auto offsets = ColumnUInt64::create();

    auto serialization_number = SerializationNumber<UInt64>::create();
    serialization_number->deserializeBinaryBulk(*offsets, istr, num_sparse_index_tokens, 0.0);
    header.sparse_index = DictionarySparseIndex(std::move(tokens), std::move(offsets));
    return header;
}

TokenPostingsInfo TextIndexSerialization::deserializeTokenInfo(ReadBuffer & istr, bool with_postings)
{
    using enum PostingsSerialization::Flags;
    TokenPostingsInfo info;

    readVarUInt(info.header, istr);
    readVarUInt(info.cardinality, istr);

    /// Position metadata is always right after (header, cardinality),
    /// before any posting data, to keep the layout consistent for all token types.
    if (info.header & HasPositions)
    {
        readVarUInt(info.position_offset, istr);
        readVarUInt(info.position_bytes, istr);
    }

    if (info.header & EmbeddedPostings)
    {
        chassert(info.header & RawPostings);

        if (!with_postings)
        {
            skipVarUInts(istr, info.cardinality);
        }
        else if (info.cardinality != 0)
        {
            readVarUInts(istr, info.cardinality, info.embedded_postings);
            info.offsets.emplace_back(0);
            info.ranges.emplace_back(info.embedded_postings.front(), info.embedded_postings.back());
        }

        if (info.header & HasTermFrequencies)
        {
            if (!with_postings)
            {
                skipVarUInts(istr, info.cardinality);
            }
            else if (info.cardinality != 0)
            {
                readVarUInts(istr, info.cardinality, info.embedded_term_frequencies);
                for (auto & tf : info.embedded_term_frequencies)
                    tf += 1;
            }
        }
    }
    else
    {
        UInt64 num_postings_blocks = 1;

        if (!(info.header & SingleBlock))
            readVarUInt(num_postings_blocks, istr);

        for (size_t j = 0; j < num_postings_blocks; ++j)
        {
            UInt64 offset_in_file = 0;
            RowsRange rows_range{};

            readVarUInt(offset_in_file, istr);
            readVarUInt(rows_range.begin, istr);
            readVarUInt(rows_range.end, istr);

            if (rows_range.begin > std::numeric_limits<UInt32>::max() || rows_range.end > std::numeric_limits<UInt32>::max())
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupted data in text index: posting list row range [{}, {}] exceeds UInt32 max",
                    rows_range.begin, rows_range.end);
            }

            info.offsets.emplace_back(offset_in_file);
            info.ranges.emplace_back(std::move(rows_range));
        }
    }
    return info;
}

void TextIndexSerialization::skipTokenInfo(ReadBuffer & istr)
{
    using enum PostingsSerialization::Flags;

    UInt64 header = 0;
    UInt64 cardinality = 0;

    readVarUInt(header, istr);
    readVarUInt(cardinality, istr);

    /// Position metadata (offset, bytes) is right after (header, cardinality).
    if (header & HasPositions)
    {
        ignoreVarUInt(istr);
        ignoreVarUInt(istr);
    }

    if (header & EmbeddedPostings)
    {
        chassert(header & RawPostings);
        skipVarUInts(istr, cardinality);

        /// Embedded postings store one VarUInt-encoded `(tf - 1)` per row inline after the row ids.
        if (header & HasTermFrequencies)
            skipVarUInts(istr, cardinality);
    }
    else
    {
        UInt64 num_postings_blocks = 1;

        if (!(header & SingleBlock))
            readVarUInt(num_postings_blocks, istr);

        for (size_t j = 0; j < num_postings_blocks; ++j)
        {
            ignoreVarUInt(istr);
            ignoreVarUInt(istr);
            ignoreVarUInt(istr);
        }
    }
}

std::pair<ColumnPtr, UInt64> TextIndexSerialization::deserializeTokens(ReadBuffer & istr)
{
    UInt64 tokens_format = 0;
    readVarUInt(tokens_format, istr);

    size_t num_tokens = 0;
    readVarUInt(num_tokens, istr);

    switch (tokens_format)
    {
        case static_cast<UInt64>(TokensFormat::RawStrings):
            return {deserializeTokensRaw(istr, num_tokens), tokens_format};
        case static_cast<UInt64>(TokensFormat::FrontCodedStrings):
            return {deserializeTokensFrontCoding(istr, num_tokens), tokens_format};
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown tokens serialization format ({}) in dictionary block", tokens_format);
    }
}

std::vector<TokenPostingsInfoPtr> TextIndexSerialization::deserializeTokenInfos(ReadBuffer & istr, size_t num_tokens, const std::vector<size_t> & matched_indices)
{
    std::vector<TokenPostingsInfoPtr> result;
    result.reserve(matched_indices.size());

    if (matched_indices.empty())
        return result;

    chassert(matched_indices.back() < num_tokens);
    chassert(std::is_sorted(matched_indices.begin(), matched_indices.end()));

    for (size_t i = 0, j = 0; i < num_tokens && j < matched_indices.size(); ++i)
    {
        if (matched_indices[j] != i)
        {
            skipTokenInfo(istr);
            continue;
        }

        auto info = deserializeTokenInfo(istr, /*with_postings=*/true);
        result.emplace_back(std::make_shared<TokenPostingsInfo>(std::move(info)));
        ++j;
    }

    return result;
}

DictionaryBlock TextIndexSerialization::deserializeDictionaryBlock(ReadBuffer & istr, bool with_postings)
{
    ProfileEvents::increment(ProfileEvents::TextIndexReadDictionaryBlocks);

    auto [tokens_column, tokens_format] = deserializeTokens(istr);
    size_t num_tokens = tokens_column->size();

    std::vector<TokenPostingsInfo> token_infos;
    token_infos.reserve(num_tokens);

    for (size_t i = 0; i < num_tokens; ++i)
        token_infos.emplace_back(deserializeTokenInfo(istr, with_postings));

    return DictionaryBlock{std::move(tokens_column), std::move(token_infos), std::move(tokens_format)};
}

static DictionarySparseIndex serializeTokensAndPostings(
    const SortedTokens & sorted_tokens,
    const PostingListBuildContext & context,
    const MergeTreeIndexTextParams & params,
    MergeTreeIndexWriterStream & dictionary_stream,
    MergeTreeIndexWriterStream & postings_stream,
    MergeTreeIndexWriterStream * positions_stream)
{
    size_t num_tokens = sorted_tokens.size();
    size_t num_blocks = (num_tokens + params.dictionary_block_size - 1) / params.dictionary_block_size;

    auto sparse_index_tokens = ColumnString::create();
    auto & sparse_index_str = assert_cast<ColumnString &>(*sparse_index_tokens);
    sparse_index_str.reserve(num_blocks);

    auto sparse_index_offsets = ColumnUInt64::create();
    auto & sparse_index_offsets_data = sparse_index_offsets->getData();
    sparse_index_offsets_data.reserve(num_blocks);

    auto tokens_format = params.dictionary_block_frontcoding_compression
        ? TextIndexSerialization::TokensFormat::FrontCodedStrings
        : TextIndexSerialization::TokensFormat::RawStrings;

    for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx)
    {
        size_t block_begin = block_idx * params.dictionary_block_size;
        size_t block_end = std::min(block_begin + params.dictionary_block_size, num_tokens);

        /// Start a new compressed block because the dictionary blocks
        /// are usually read with random reads and it is more efficient
        /// to decompress only the needed data.
        dictionary_stream.compressed_hashing.next();
        auto dictionary_mark = dictionary_stream.getCurrentMark();
        chassert(dictionary_mark.offset_in_decompressed_block == 0);

        const auto & first_token = sorted_tokens[block_begin].token;
        TextIndexSerialization::checkTokenSize(first_token.size());
        sparse_index_offsets_data.emplace_back(dictionary_mark.offset_in_compressed_file);
        sparse_index_str.insertData(first_token.data(), first_token.size());

        serializeTokensImpl(
            [&](size_t i) { return sorted_tokens[i].token; },
            dictionary_stream.compressed_hashing,
            tokens_format,
            block_begin,
            block_end);

        for (size_t i = block_begin; i < block_end; ++i)
        {
            const auto & entry = sorted_tokens[i];
            TextIndexSerialization::serializePostingsAndTokenInfo(
                std::move(*entry.postings),
                context,
                dictionary_stream,
                postings_stream,
                positions_stream);
        }
    }

    return DictionarySparseIndex(std::move(sparse_index_tokens), std::move(sparse_index_offsets));
}

/// Writes the per-row `SmallFloat` document-length bytes to the `.dl` substream
/// in segments  and returns the compressed-stream byte offset of each segment start.
static VectorWithMemoryTracking<UInt64> serializeDocumentLengths(const PaddedPODArray<UInt8> & doc_lengths, MergeTreeIndexOutputStreams & streams)
{
    auto * doc_lengths_stream = streams.at(MergeTreeIndexSubstream::Type::TextIndexDocLengths);
    if (!doc_lengths_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index with BM25 scoring is missing its document-lengths (.dl) stream");

    VectorWithMemoryTracking<UInt64> segment_offsets;
    const size_t num_rows = doc_lengths.size();

    for (size_t seg_start = 0; seg_start < num_rows; seg_start += ScoringStats::DOC_LENGTHS_SEGMENT_SIZE)
    {
        doc_lengths_stream->compressed_hashing.next();
        auto mark = doc_lengths_stream->getCurrentMark();
        chassert(mark.offset_in_decompressed_block == 0);
        segment_offsets.push_back(mark.offset_in_compressed_file);

        const size_t seg_len = std::min<size_t>(ScoringStats::DOC_LENGTHS_SEGMENT_SIZE, num_rows - seg_start);
        doc_lengths_stream->compressed_hashing.write(reinterpret_cast<const char *>(doc_lengths.data() + seg_start), seg_len);
    }

    return segment_offsets;
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

    MergeTreeIndexWriterStream * positions_stream = nullptr;

    if (params.enable_positions)
    {
        auto it = streams.find(MergeTreeIndexSubstream::Type::TextIndexPositions);
        if (it == streams.end() || !it->second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' and positions enabled must have a positions stream");

        positions_stream = it->second;
    }

    auto postings_codec = PostingListCodecFactory::createPostingListCodec(posting_list_codec_type);
    PostingsSerialization postings_serialization(std::move(postings_codec), params.serialization_version);
    const auto * codec = postings_serialization.getPostingListCodec();
    chassert(codec);

    /// Built once for the whole granule and passed down to every token's posting list.
    const PostingListBuildContext context
    {
        .codec = *codec,
        .segment_size = codec->getSegmentSize(params.posting_list_block_size),
        .enable_positions = params.enable_positions,
        .enable_scoring = params.enable_scoring,
        .doc_lengths = params.enable_scoring ? &doc_lengths : nullptr,
        .doc_lengths_first_row_id = static_cast<UInt32>(num_docs - doc_lengths.size()),
    };

    auto sparse_index_block = serializeTokensAndPostings(
        sorted_tokens,
        context,
        params,
        *dictionary_stream,
        *postings_stream,
        positions_stream);

    ScoringStats scoring_stats;

    if (params.enable_scoring)
    {
        /// Write the doc lengths and record the compressed-stream byte offset of each `.dl` segment start in the
        /// header, so scoring can seek to and decompress only the segment holding a doc instead of the whole array.
        auto doc_lengths_segment_offsets = serializeDocumentLengths(*context.doc_lengths, streams);

        scoring_stats =
        {
            .num_docs = num_docs,
            .sum_doc_length = sum_doc_length,
            .doc_lengths_segment_size = ScoringStats::DOC_LENGTHS_SEGMENT_SIZE,
            .doc_lengths_segment_offsets = std::move(doc_lengths_segment_offsets),
        };
    }

    TextIndexHeader header
    {
        .version = params.serialization_version,
        .codec_type = posting_list_codec_type,
        .has_positions = params.enable_positions,
        .positions_codec = params.positions_codec,
        .has_scoring = params.enable_scoring,
        .sparse_index = std::move(sparse_index_block),
        .scoring_stats = std::move(scoring_stats),
    };

    TextIndexSerialization::serializeHeader(header, index_stream->compressed_hashing);
}

void MergeTreeIndexGranuleTextWritable::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deserialization of MergeTreeIndexGranuleTextWritable is not implemented");
}

size_t MergeTreeIndexGranuleTextWritable::memoryUsageBytes() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method memoryUsageBytes is not implemented for MergeTreeIndexGranuleTextWritable");
}

PostingListBuilder::PostingListBuilder(UInt32 first_value, UInt32 first_position, const PostingListBuildContext & context)
{
    if (context.enable_positions)
    {
        state.emplace<Large>(first_value, first_position);
    }
    else
    {
        auto & inline_state = std::get<Inline>(state);
        inline_state.values[0] = first_value;
        inline_state.size = 1;
    }
}

void PostingListBuilder::add(UInt32 value, UInt32 position, const PostingListBuildContext & context)
{
    if (auto * inline_state = std::get_if<Inline>(&state))
    {
        /// Values are added in non-descending order.
        chassert(inline_state->size != 0);
        chassert(value >= inline_state->values[inline_state->size - 1]);
        chassert(!context.enable_positions);

        /// In-row repeat.
        if (value == inline_state->values[inline_state->size - 1])
        {
            if (context.enable_scoring)
            {
                state.emplace<Large>(inline_state->values, inline_state->size);
                std::get<Large>(state).addRowRepeat(value);
            }
            return;
        }

        if (inline_state->size < inline_capacity)
        {
            inline_state->values[inline_state->size++] = value;
            return;
        }

        auto inline_values = std::move(inline_state->values);
        size_t inline_size = inline_state->size;
        state.emplace<Large>(std::move(inline_values), inline_size, value);
        return;
    }

    auto & large = std::get<Large>(state);
    chassert(!large.values.empty() && value >= large.values.back());

    /// Positions are recorded for every occurrence, including in-row repeats.
    if (context.enable_positions)
    {
        chassert(large.positions);
        large.positions->add(value, position);
    }

    /// In-row repeat.
    if (value == large.values.back())
    {
        if (context.enable_scoring)
            large.addRowRepeat(value);
        return;
    }

    /// Flush on new value to ensure that term frequencies are final.
    if (large.values.size() >= IPostingListEncoder::append_granularity)
        large.flush(context);

    large.values.push_back(value);

    if (large.term_frequencies)
        large.term_frequencies->addNewRow();
}

PositionListBuilder * PostingListBuilder::getPositions()
{
    auto * large = std::get_if<Large>(&state);
    return large ? large->positions.get() : nullptr;
}

PostingListBuilder::Large::Large(std::array<UInt32, inline_capacity> values_, UInt8 inline_size_)
{
    chassert(inline_size_ != 0);
    values.insert(values_.begin(), values_.begin() + inline_size_);
}

PostingListBuilder::Large::Large(std::array<UInt32, inline_capacity> values_, UInt8 inline_size_, UInt32 added_value_)
{
    values.insert(values_.begin(), values_.begin() + inline_size_);
    values.push_back(added_value_);
}

PostingListBuilder::Large::Large(UInt32 first_value, UInt32 first_position)
    : positions(std::make_unique<PositionListBuilder>())
{
    values.push_back(first_value);
    positions->add(first_value, first_position);
}

void PostingListBuilder::Large::addRowRepeat(UInt32 row_id)
{
    chassert(!values.empty() && values.back() == row_id);

    if (!term_frequencies)
        term_frequencies = std::make_unique<TermFrequenciesBuilder>(values.size());
    else
        term_frequencies->addRowRepeat();
}

void PostingListBuilder::Large::flush(const PostingListBuildContext & context)
{
    if (values.empty())
        return;

    if (!encoder)
        encoder = context.codec.createEncoder();

    /// The encoder gathers the doc lengths of the flushed rows itself.
    /// The `(tf - 1)` are empty unless the token repeated within a row.
    encoder->append(
        {values.data(), values.size()},
        term_frequencies ? term_frequencies->getTfMinusOne() : std::span<const UInt32>{},
        context);

    values.clear();
    term_frequencies.reset();
}

MergeTreeIndexTextGranuleBuilder::MergeTreeIndexTextGranuleBuilder(
    MergeTreeIndexTextParams params_,
    TokenizerPtr tokenizer_,
    const IPostingListCodec * posting_list_codec_)
    : params(std::move(params_))
    , tokenizer(tokenizer_)
    , posting_list_codec(posting_list_codec_)
    , arena(std::make_unique<Arena>())
{
}

PostingListBuildContext MergeTreeIndexTextGranuleBuilder::buildContext() const
{
    chassert(posting_list_codec);

    return
    {
        .codec = *posting_list_codec,
        .segment_size = posting_list_codec->getSegmentSize(params.posting_list_block_size),
        .enable_positions = params.enable_positions,
        .enable_scoring = params.enable_scoring,
        .doc_lengths = &doc_lengths,
        .doc_lengths_first_row_id = static_cast<UInt32>(current_row - doc_lengths.size()),
    };
}

void MergeTreeIndexTextGranuleBuilder::addDocument(std::string_view document, const PostingListBuildContext & context)
{
    UInt32 token_position = 0;
    forEachToken(
        *tokenizer,
        document.data(),
        document.size(),
        [&](const char * token_start, size_t token_length)
        {
            addToken({token_start, token_length}, token_position, context);
            ++token_position;
            return false;
        });
}

template <typename... Args>
static PostingListBuilder & constructBuilder(TokenToPostingsBuilderMap::LookupResult it, Args &&... args)
{
    return *new (&it->getMapped()) PostingListBuilder(std::forward<Args>(args)...);
}

void MergeTreeIndexTextGranuleBuilder::seedDropFilter()
{
    if (!postprocessor_drop_filter || postprocessor_drop_filter->drop_on_match)
        return;

    const auto & filter_tokens = postprocessor_drop_filter->tokens;

    /// StringHashTable::dispatch reads whole 8-byte words around short keys.
    static constexpr size_t pad_left = 8;
    const size_t total_size = std::accumulate(
        filter_tokens.begin(), filter_tokens.end(), pad_left,
        [](size_t sum, const auto & filter_token) { return sum + filter_token.size(); });

    char * data = arena->alloc(total_size) + pad_left;

    bool inserted = false;
    TokenToPostingsBuilderMap::LookupResult it;

    for (const auto & filter_token : filter_tokens)
    {
        memcpy(data, filter_token.data(), filter_token.size());
        std::string_view key(data, filter_token.size());
        data += filter_token.size();

        tokens_map.emplace(key, it, inserted);
        chassert(inserted);
        constructBuilder(it, PostingListBuilder::Filtered{});
    }
}

void MergeTreeIndexTextGranuleBuilder::addToken(std::string_view token, UInt32 token_position, const PostingListBuildContext & context)
{
    const auto row = static_cast<UInt32>(current_row);

    /// Keep-set mode: the map is pre-seeded with the only tokens to keep, everything else is skipped.
    if (postprocessor_drop_filter && !postprocessor_drop_filter->drop_on_match)
    {
        auto it = tokens_map.find(token);
        if (!it)
            return;

        auto & mapped = it->getMapped();

        /// The first occurrence of a pre-seeded token: replace the `Filtered` placeholder with a real builder.
        if (mapped.isFiltered())
            mapped = PostingListBuilder(row, token_position, context);
        else
            mapped.add(row, token_position, context);

        ++tokens_in_current_row;
        return;
    }

    bool inserted = false;
    TokenToPostingsBuilderMap::LookupResult it;

    ArenaKeyHolder key_holder(token, *arena);
    tokens_map.emplace(key_holder, it, inserted);

    if (inserted)
    {
        /// A token from the drop set holds no postings.
        if (postprocessor_drop_filter && postprocessor_drop_filter->tokens.contains(token))
        {
            constructBuilder(it, PostingListBuilder::Filtered{});
            return;
        }

        constructBuilder(it, row, token_position, context);
    }
    else
    {
        auto & mapped = it->getMapped();

        /// A token from the drop set holds no postings.
        if (mapped.isFiltered())
            return;

        mapped.add(row, token_position, context);
    }

    ++tokens_in_current_row;
}

void MergeTreeIndexTextGranuleBuilder::incrementCurrentRow()
{
    is_empty = false;
    ++current_row;
    num_processed_tokens += tokens_in_current_row;

    if (params.enable_scoring)
    {
        UInt8 dl_norm = SmallFloat::toInt4Byte(static_cast<UInt32>(tokens_in_current_row));
        doc_lengths.push_back(dl_norm);
        sum_doc_length += tokens_in_current_row;
    }

    tokens_in_current_row = 0;
}

std::unique_ptr<MergeTreeIndexGranuleTextWritable> MergeTreeIndexTextGranuleBuilder::build()
{
    SortedTokens sorted_tokens;
    sorted_tokens.reserve(tokens_map.size());

    tokens_map.forEachValue([&](const auto & key, auto & mapped)
    {
        std::string_view token = key;
        if (mapped.isFiltered())
            return;
        sorted_tokens.push_back(SortedToken{token, &mapped});
    });

    std::ranges::sort(sorted_tokens, [](const auto & lhs, const auto & rhs) { return lhs.token < rhs.token; });

    return std::make_unique<MergeTreeIndexGranuleTextWritable>(
        params,
        posting_list_codec ? posting_list_codec->getType() : IPostingListCodec::Type::None,
        std::move(tokens_map),
        std::move(arena),
        std::move(sorted_tokens),
        std::move(doc_lengths),
        /*num_docs=*/current_row,
        sum_doc_length);
}

void MergeTreeIndexTextGranuleBuilder::reset()
{
    is_empty = true;
    current_row = 0;
    num_processed_tokens = 0;
    tokens_in_current_row = 0;
    doc_lengths.clear();
    sum_doc_length = 0;
    tokens_map = {};
    arena = std::make_unique<Arena>();

    seedDropFilter();
}

MergeTreeIndexAggregatorText::MergeTreeIndexAggregatorText(
    String index_column_name_,
    MergeTreeIndexTextParams params_,
    TokenizerPtr tokenizer_,
    const IPostingListCodec * posting_list_codec_,
    MergeTreeIndexTextPreprocessorPtr preprocessor_,
    MergeTreeIndexTextPostprocessorPtr postprocessor_)
    : index_column_name(std::move(index_column_name_))
    , params(std::move(params_))
    , owned_tokenizer(tokenizer_ && tokenizer_->isStateful() ? std::shared_ptr<const ITokenizer>(tokenizer_->clone()) : nullptr)
    , tokenizer(owned_tokenizer ? owned_tokenizer.get() : tokenizer_)
    , granule_builder(params, tokenizer, posting_list_codec_)
    , preprocessor(preprocessor_)
    , postprocessor(postprocessor_)
{
    /// Fast path for IN/NOT IN filter-only postprocessors only: drops are decided per distinct token in
    /// addToken so dropped tokens never build postings. Positions must be disabled (phrase search needs
    /// dense position renumbering after drops). Any other postprocessor uses the general per-batch path.
    if (postprocessor->hasActions() && !params.enable_positions)
    {
        if (const auto * inline_filter = postprocessor->getInlineFilter())
        {
            granule_builder.postprocessor_drop_filter = inline_filter;
            granule_builder.seedDropFilter();
            use_postprocessor_drop_fast_path = true;
        }
    }
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
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}",
            *pos, block.rows());
    }

    if (granule_builder.current_row + limit > std::numeric_limits<UInt32>::max())
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot build text index in part with {} rows. Materialization of text index is not supported for parts with more than {} rows",
            granule_builder.current_row + limit, std::numeric_limits<UInt32>::max());
    }

    const size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    const PostingListBuildContext context = granule_builder.buildContext();
    const auto & index_column = block.getByName(index_column_name);
    auto [preprocessed_column, offset] = preprocessor->processColumn(index_column, *pos, rows_read);

    if (postprocessor->hasActions() && !use_postprocessor_drop_fast_path)
    {
        ColumnPtr tokenized = tokenizeToArray(*tokenizer, *preprocessed_column, offset, rows_read);
        ColumnPtr postprocessed = postprocessor->processTokensArrayBatch(assert_cast<const ColumnArray *>(tokenized.get()));
        addDocumentsFromArray<false>(postprocessed, 0, rows_read, context);
    }
    else if (isArray(index_column.type))
    {
        addDocumentsFromArray<true>(preprocessed_column, offset, rows_read, context);
    }
    else
    {
        const bool column_is_nullable = isColumnNullableOrLowCardinalityNullable(*preprocessed_column);

        for (size_t i = offset; i < offset + rows_read; ++i)
        {
            if (!column_is_nullable || !preprocessed_column->isNullAt(i))
            {
                const std::string_view ref = preprocessed_column->getDataAt(i);
                granule_builder.addDocument(ref, context);
            }

            granule_builder.incrementCurrentRow();
        }
    }

    *pos += rows_read;
}

template <bool tokenize>
void MergeTreeIndexAggregatorText::addDocumentsFromArray(ColumnPtr column, size_t start_row, size_t rows_read, const PostingListBuildContext & context)
{
    const ColumnArray * column_array = assert_cast<const ColumnArray *>(column.get());
    const IColumn & column_data = column_array->getData();
    const IColumn::Offsets & column_offsets = column_array->getOffsets();
    const bool data_is_nullable = isColumnNullableOrLowCardinalityNullable(column_data);

    for (size_t i = start_row; i < start_row + rows_read; ++i)
    {
        /// Dense position counter: dropped (empty/null) tokens leave no gap, so positions
        /// reflect the surviving token sequence only.
        UInt32 token_position = 0;
        for (size_t element_idx = column_offsets[i - 1]; element_idx < column_offsets[i]; ++element_idx)
        {
            if (data_is_nullable && column_data.isNullAt(element_idx))
                continue;

            const std::string_view ref = column_data.getDataAt(element_idx);
            if (ref.empty())
                continue;

            if constexpr (tokenize)
                granule_builder.addDocument(ref, context);
            else
                granule_builder.addToken(ref, token_position++, context);
        }

        granule_builder.incrementCurrentRow();
    }
}

namespace
{

/// Rewrites `x = ''` / `x != ''` to `empty(x)` / `notEmpty(x)`, like `optimize_empty_string_comparisons` does in queries.
void normalizeColumnExpression(ASTPtr & ast)
{
    for (auto & child : ast->children)
        normalizeColumnExpression(child);

    const auto * function = ast->as<ASTFunction>();
    if (!function || (function->name != "equals" && function->name != "notEquals")
        || !function->arguments || function->arguments->children.size() != 2)
        return;

    auto is_empty_string_literal = [](const ASTPtr & node)
    {
        const auto * literal = node->as<ASTLiteral>();
        return literal && literal->value.getType() == Field::Types::String && literal->value.safeGet<String>().empty();
    };

    const auto & arguments = function->arguments->children;
    ASTPtr expression;

    if (is_empty_string_literal(arguments[1]))
        expression = arguments[0];
    else if (is_empty_string_literal(arguments[0]))
        expression = arguments[1];
    else
        return;

    ast = makeASTFunction(function->name == "equals" ? "empty" : "notEmpty", expression);
}

/// Queries are analyzed with `optimize_empty_string_comparisons`, index expressions are not, so an index such as
/// `arrayFilter(s -> s != '', arr)` is never matched by name (issue #111788). Returns the name of the index
/// expression after the same rewrite, or `std::nullopt` if it does not change.
std::optional<String> getNormalizedIndexColumnName(const IndexDescription & index)
{
    /// A text index is always defined on a single expression.
    if (!index.expression_list_ast || index.expression_list_ast->children.size() != 1)
        return {};

    ASTPtr normalized = index.expression_list_ast->children.front()->clone();
    normalizeColumnExpression(normalized);

    String name = normalized->getColumnName();
    if (index.sample_block.has(name))
        return {};

    return name;
}

}

MergeTreeIndexText::MergeTreeIndexText(
    StorageMetadataPtr metadata_snapshot_,
    const IndexDescription & index_,
    MergeTreeIndexTextParams params_,
    std::unique_ptr<ITokenizer> tokenizer_,
    std::unique_ptr<IPostingListCodec> posting_list_codec_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , params(std::move(params_))
    , tokenizer(std::move(tokenizer_))
    , posting_list_codec(std::move(posting_list_codec_))
    , preprocessor(std::make_shared<MergeTreeIndexTextPreprocessor>(params.preprocessor, index_))
    , postprocessor(std::make_shared<MergeTreeIndexTextPostprocessor>(params.postprocessor, index_))
    , normalized_index_column_name(getNormalizedIndexColumnName(index_))
{
}

MergeTreeIndexSubstreams MergeTreeIndexText::getSubstreams() const
{
    MergeTreeIndexSubstreams substreams =
    {
        {MergeTreeIndexSubstream::Type::Regular, "", ".idx"},
        {MergeTreeIndexSubstream::Type::TextIndexDictionary, ".dct", ".idx"},
        {MergeTreeIndexSubstream::Type::TextIndexPostings, ".pst", ".idx"}
    };

    if (params.enable_scoring)
        substreams.push_back({MergeTreeIndexSubstream::Type::TextIndexDocLengths, ".dl", ".idx"});

    if (params.enable_positions)
        substreams.push_back({MergeTreeIndexSubstream::Type::TextIndexPositions, ".pos", ".idx"});

    return substreams;
}

MergeTreeIndexFormat MergeTreeIndexText::getPhysicalFormat(
    const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const
{
    if (!indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx", &storage))
        return {0, {}};

    MergeTreeIndexVersion version = 1;
    MergeTreeIndexSubstreams substreams =
    {
        {MergeTreeIndexSubstream::Type::Regular, "", ".idx"},
        {MergeTreeIndexSubstream::Type::TextIndexDictionary, ".dct", ".idx"},
        {MergeTreeIndexSubstream::Type::TextIndexPostings, ".pst", ".idx"}
    };

    /// V2: positions file exists on disk.
    if (indexFileExistsInChecksums(checksums, relative_path_prefix + ".pos", ".idx", &storage))
    {
        substreams.push_back({MergeTreeIndexSubstream::Type::TextIndexPositions, ".pos", ".idx"});
        version = 2;
    }

    /// V3: doc lengths file exists on disk.
    if (indexFileExistsInChecksums(checksums, relative_path_prefix + ".dl", ".idx", &storage))
    {
        substreams.push_back({MergeTreeIndexSubstream::Type::TextIndexDocLengths, ".dl", ".idx"});
        version = 3;
    }

    return {version, std::move(substreams)};
}

MergeTreeIndexGranulePtr MergeTreeIndexText::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleText>(params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexText::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorText>(index.column_names[0], params, tokenizer.get(), posting_list_codec.get(), preprocessor, postprocessor);
}

MergeTreeIndexConditionPtr MergeTreeIndexText::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return createIndexCondition(predicate, context, /*scoring_enabled=*/false);
}

MergeTreeIndexConditionPtr MergeTreeIndexText::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context, bool scoring_enabled) const
{
    return std::make_shared<MergeTreeIndexConditionText>(
        predicate,
        context,
        index.sample_block,
        normalized_index_column_name,
        tokenizer.get(),
        preprocessor,
        postprocessor,
        params.enable_positions,
        scoring_enabled);
}

DataTypePtr MergeTreeIndexText::getNestedDataType(const DataTypePtr & data_type)
{
    DataTypePtr nested_type = data_type;
    while (true)
    {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(nested_type.get()))
            nested_type = array_type->getNestedType();
        else if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(nested_type.get()))
            nested_type = nullable_type->getNestedType();
        else if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(nested_type.get()))
            nested_type = lc_type->getDictionaryType();
        else
            break;
    }
    return nested_type;
}

static const String ARGUMENT_TOKENIZER = "tokenizer";
static const String ARGUMENT_PREPROCESSOR = "preprocessor";
static const String ARGUMENT_POSTPROCESSOR = "postprocessor";
static const String ARGUMENT_DICTIONARY_BLOCK_SIZE = "dictionary_block_size";
static const String ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION = "dictionary_block_frontcoding_compression";
static const String ARGUMENT_POSTING_LIST_BLOCK_SIZE = "posting_list_block_size";
static const String ARGUMENT_POSTING_LIST_CODEC = "posting_list_codec";
static const String ARGUMENT_ENABLE_SCORING = "enable_scoring";
static const String ARGUMENT_POSITIONS = "support_phrase_search";

namespace
{

template <typename Type>
Type castAs(const Field & field, std::string_view argument_name)
{
    auto expected_type = Field::TypeToEnum<Type>::value;
    if (expected_type != field.getType())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Text index argument '{}' expected to be {}, but got {}",
            argument_name, fieldTypeToString(Field::TypeToEnum<Type>::value), field.getTypeName());
    }
    return field.safeGet<Type>();
}

template <typename Type>
std::optional<Type> extractFieldOption(std::unordered_map<String, ASTPtr> & options, const String & option)
{
    auto it = options.find(option);
    if (it == options.end())
        return {};

    Field value = getFieldFromIndexArgumentAST(it->second);
    value = castAs<Type>(value, option);

    options.erase(it);
    return value.safeGet<Type>();
}

ASTPtr extractASTOption(std::unordered_map<String, ASTPtr> & options, const String & option, bool is_required)
{
    auto it = options.find(option);

    if (it != options.end())
    {
        ASTPtr ast = it->second;
        options.erase(it);
        return ast;
    }

    if (is_required)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' is required", option);

    return nullptr;
}

std::pair<String, ASTPtr> parseNamedArgument(const ASTFunction * ast_equal_function)
{
    if (!ast_equal_function
        || ast_equal_function->name != "equals"
        || ast_equal_function->arguments->children.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot mix key-value pair and single argument as text index arguments");

    const auto & arguments = ast_equal_function->arguments;
    const auto * key_identifier = arguments->children[0]->as<ASTIdentifier>();

    if (!key_identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument must be a key-value pair. got {}", ast_equal_function->formatForErrorMessage());

    return {key_identifier->name(), arguments->children[1]};
}

std::unordered_map<String, ASTPtr> convertArgumentsToOptionsMap(const ASTPtr & arguments)
{
    std::unordered_map<String, ASTPtr> options;
    if (!arguments)
        return options;

    for (const auto & child : arguments->children)
    {
        const auto * ast_equal_function = child->as<ASTFunction>();
        auto [key, ast] = parseNamedArgument(ast_equal_function);

        if (!options.emplace(key, ast).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index '{}' argument is specified more than once", key);
    }
    return options;
}

}

MergeTreeIndexPtr textIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & settings)
{
    auto options = convertArgumentsToOptionsMap(index.arguments);

    auto tokenizer_ast = extractASTOption(options, ARGUMENT_TOKENIZER, true);
    auto preprocessor_ast = extractASTOption(options, ARGUMENT_PREPROCESSOR, false);
    auto postprocessor_ast = extractASTOption(options, ARGUMENT_POSTPROCESSOR, false);
    auto tokenizer = TokenizerFactory::instance().get(tokenizer_ast);

    /// The parameters below can be set in the index definition or via the `text_index_*` table settings.
    /// A value from the index definition wins; otherwise the table setting is used.
    UInt64 dictionary_block_size = extractFieldOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE)
        .value_or(settings[MergeTreeSetting::text_index_dictionary_block_size]);

    UInt64 dictionary_block_frontcoding_compression = extractFieldOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION)
        .value_or(settings[MergeTreeSetting::text_index_dictionary_block_frontcoding_compression]);

    UInt64 posting_list_block_size = extractFieldOption<UInt64>(options, ARGUMENT_POSTING_LIST_BLOCK_SIZE)
        .value_or(settings[MergeTreeSetting::text_index_posting_list_block_size]);

    bool enable_positions = extractFieldOption<UInt64>(options, ARGUMENT_POSITIONS).value_or(DEFAULT_POSITIONS) != 0;
    bool enable_scoring = extractFieldOption<UInt64>(options, ARGUMENT_ENABLE_SCORING).value_or(0) != 0;

    String posting_list_codec_name = extractFieldOption<String>(options, ARGUMENT_POSTING_LIST_CODEC)
        .value_or(settings[MergeTreeSetting::text_index_posting_list_codec].toString());

    auto posting_list_codec = PostingListCodecFactory::createPostingListCodec(posting_list_codec_name, index.name);
    bool has_codec = posting_list_codec && posting_list_codec->getType() != IPostingListCodec::Type::None;

    /// The setting is a preference to preserve compatibility, not a hard constraint.
    /// If the setting contradicts the index features on the current version, the index features take precedence.
    using enum MergeTreeTextIndexSerializationVersion;
    MergeTreeTextIndexSerializationVersion min_version = V0_Initial;
    MergeTreeTextIndexSerializationVersion max_version = V3_WithScoring;

    if (has_codec)
        min_version = V1_WithCodec;

    if (enable_positions)
        min_version = V2_WithPositions;

    if (enable_scoring)
        min_version = V3_WithScoring;

    const MergeTreeTextIndexSerializationVersion version_setting = settings[MergeTreeSetting::text_index_serialization_version];
    MergeTreeTextIndexSerializationVersion serialization_version = std::clamp(version_setting, min_version, max_version);

    MergeTreeIndexTextParams index_params{
        dictionary_block_size,
        dictionary_block_frontcoding_compression,
        posting_list_block_size,
        enable_positions,
        enable_scoring,
        static_cast<UInt8>(TextIndexPositionCodec::Encoding::BlockedPfor), /// not user-configurable yet
        std::move(preprocessor_ast),
        std::move(postprocessor_ast),
        serialization_version};

    if (!options.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected text index arguments: {}", fmt::join(std::views::keys(options), ", "));

    if (enable_scoring)
    {
        /// BM25 scoring relies on the per-block term-frequency payload that cannot be stored in the `none` codec.
        if (posting_list_codec->getType() == IPostingListCodec::Type::None)
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Text index argument '{}' requires the posting list codec, but '{}' is used",
                ARGUMENT_ENABLE_SCORING, posting_list_codec_name);
        }

        if (!tokenizer->supportsScoring())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Text index argument '{}' is not supported with the '{}' tokenizer",
                ARGUMENT_ENABLE_SCORING, tokenizer->getTokenizerExternalName());
        }
    }

    return std::make_shared<MergeTreeIndexText>(std::move(metadata_snapshot), index, index_params, std::move(tokenizer), std::move(posting_list_codec));
}

void textIndexValidator(const IndexDescription & index, bool /*attach*/, const MergeTreeSettings & settings)
{
    auto options = convertArgumentsToOptionsMap(index.arguments);

    auto tokenizer_ast = extractASTOption(options, ARGUMENT_TOKENIZER, true);
    auto preprocessor_ast = extractASTOption(options, ARGUMENT_PREPROCESSOR, false);
    auto postprocessor_ast = extractASTOption(options, ARGUMENT_POSTPROCESSOR, false);
    auto tokenizer = TokenizerFactory::instance().get(tokenizer_ast);

    UInt64 dictionary_block_size = extractFieldOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE)
        .value_or(settings[MergeTreeSetting::text_index_dictionary_block_size]);

    if (dictionary_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be greater than 0, but got {}", ARGUMENT_DICTIONARY_BLOCK_SIZE, dictionary_block_size);

    UInt64 dictionary_block_use_fc_compression = extractFieldOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION)
        .value_or(settings[MergeTreeSetting::text_index_dictionary_block_frontcoding_compression]);

    if (dictionary_block_use_fc_compression > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be 0 or 1, but got {}", ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION, dictionary_block_use_fc_compression);

    UInt64 posting_list_block_size = extractFieldOption<UInt64>(options, ARGUMENT_POSTING_LIST_BLOCK_SIZE)
        .value_or(settings[MergeTreeSetting::text_index_posting_list_block_size]);

    if (posting_list_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be greater than 0, but got {}", ARGUMENT_POSTING_LIST_BLOCK_SIZE, posting_list_block_size);

    UInt64 positions = extractFieldOption<UInt64>(options, ARGUMENT_POSITIONS).value_or(DEFAULT_POSITIONS);
    if (positions > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be 0 or 1, but got {}", ARGUMENT_POSITIONS, positions);

    if (positions && !settings[MergeTreeSetting::allow_experimental_text_index_phrase_search])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Text index argument '{}' is experimental. Enable it with the MergeTree setting "
            "`allow_experimental_text_index_phrase_search = 1`.", ARGUMENT_POSITIONS);

    /// The `text_index_serialization_version` setting is not validated against the index features:
    /// it is a preference, and `textIndexCreator` raises it to a version that can represent them.
    String posting_list_codec_name = extractFieldOption<String>(options, ARGUMENT_POSTING_LIST_CODEC)
        .value_or(settings[MergeTreeSetting::text_index_posting_list_codec].toString());

    auto posting_list_codec = PostingListCodecFactory::createPostingListCodec(posting_list_codec_name, index.name);
    bool enable_scoring = extractFieldOption<UInt64>(options, ARGUMENT_ENABLE_SCORING).value_or(0) != 0;

    if (!options.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected text index arguments: {}", fmt::join(std::views::keys(options), ", "));

    /// Check that the index is created on a single column
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Text index must be created on a single column");

    DataTypePtr index_data_type = index.data_types[0];
    WhichDataType which_data_type(MergeTreeIndexText::getNestedDataType(index_data_type));

    if (enable_scoring)
    {
        if (!settings[MergeTreeSetting::allow_experimental_text_index_scoring])
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Text index argument '{}' is experimental. Enable it with the MergeTree setting "
                "`allow_experimental_text_index_scoring = 1`.", ARGUMENT_ENABLE_SCORING);
        }

        /// BM25 scoring relies on the per-block term-frequency payload that cannot be stored in the `none` codec.
        if (posting_list_codec->getType() == IPostingListCodec::Type::None)
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Text index argument '{}' requires the posting list codec, but '{}' is used",
                ARGUMENT_ENABLE_SCORING, posting_list_codec_name);
        }

        if (!tokenizer->supportsScoring())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Text index argument '{}' is not supported with the '{}' tokenizer",
                ARGUMENT_ENABLE_SCORING, tokenizer->getTokenizerExternalName());
        }
    }

    if (!which_data_type.isString() && !which_data_type.isFixedString())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Text index must be created on columns of type with base type of String or FixedString, got: {}",
            index_data_type->getName());
    }

    /// Create the preprocessor for validation.
    /// For very strict validation of the expression we fully parse it here.
    /// However it will be parsed again for index construction, generally immediately after this call.
    /// This is a bit redundant but that doesn't impact performance anyhow because the expression is intended to be simple enough.
    MergeTreeIndexTextPreprocessor preprocessor(preprocessor_ast, index);

    /// Create the postprocessor for validation.
    /// This validates the token transformation expression (always String -> String).
    MergeTreeIndexTextPostprocessor postprocessor(postprocessor_ast, index);
}

}
