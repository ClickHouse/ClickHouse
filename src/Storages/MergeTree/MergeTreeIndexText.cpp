#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/HashTable/HashSet.h>
#include <Common/Logger.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/ITokenExtractor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>


#include <base/range.h>
#include <base/types.h>
#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event TextIndexReadDictionaryBlocks;
    extern const Event TextIndexReadSparseIndexBlocks;
    extern const Event TextIndexReadGranulesMicroseconds;
    extern const Event TextIndexReadPostings;
    extern const Event TextIndexUsedEmbeddedPostings;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int CORRUPTED_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

static constexpr UInt64 MAX_CARDINALITY_FOR_RAW_POSTINGS = 12;
static constexpr UInt64 MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS = 6;

static_assert(MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS <= MAX_CARDINALITY_FOR_RAW_POSTINGS, "MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS must be less or equal to MAX_CARDINALITY_FOR_RAW_POSTINGS");
static_assert(PostingListBuilder::max_small_size <= MAX_CARDINALITY_FOR_RAW_POSTINGS, "max_small_size must be less than or equal to MAX_CARDINALITY_FOR_RAW_POSTINGS");

static constexpr UInt64 DEFAULT_DICTIONARY_BLOCK_SIZE = 512;
static constexpr bool DEFAULT_DICTIONARY_BLOCK_USE_FRONTCODING = true;
static constexpr UInt64 DEFAULT_POSTING_LIST_BLOCK_SIZE = 1024 * 1024;

bool DictionaryBlockBase::empty() const
{
    return !tokens || tokens->empty();
}

size_t DictionaryBlockBase::size() const
{
    return tokens ? tokens->size() : 0;
}

size_t DictionaryBlockBase::upperBound(const std::string_view token) const
{
    auto range = collections::range(0, tokens->size());

    auto it = std::upper_bound(range.begin(), range.end(), token, [this](const std::string_view lhs_ref, size_t rhs_idx)
    {
        return lhs_ref < assert_cast<const ColumnString &>(*tokens).getDataAt(rhs_idx);
    });

    return it - range.begin();
}

DictionarySparseIndex::DictionarySparseIndex(ColumnPtr tokens_, ColumnPtr offsets_in_file_)
    : DictionaryBlockBase(std::move(tokens_)), offsets_in_file(std::move(offsets_in_file_))
{
}

UInt64 DictionarySparseIndex::getOffsetInFile(size_t idx) const
{
    return assert_cast<const ColumnUInt64 &>(*offsets_in_file).getData()[idx];
}

size_t DictionarySparseIndex::memoryUsageBytes() const
{
    return sizeof(*this) + tokens->allocatedBytes() + offsets_in_file->allocatedBytes();
}

DictionaryBlock::DictionaryBlock(ColumnPtr tokens_, std::vector<TokenPostingsInfo> token_infos_)
    : DictionaryBlockBase(std::move(tokens_))
    , token_infos(std::move(token_infos_))
{
}

void PostingsSerialization::serialize(const roaring::api::roaring_bitmap_t & postings, UInt64 header, WriteBuffer & ostr)
{
    if (header & RawPostings)
    {
        roaring::api::roaring_uint32_iterator_t it;
        roaring_iterator_init(&postings, &it);

        while (it.has_value)
        {
            writeVarUInt(it.current_value, ostr);
            roaring::api::roaring_uint32_iterator_advance(&it);
        }
    }
    else
    {
        size_t num_bytes = roaring::api::roaring_bitmap_portable_size_in_bytes(&postings);
        writeVarUInt(num_bytes, ostr);

        std::vector<char> memory(num_bytes);
        roaring::api::roaring_bitmap_portable_serialize(&postings, memory.data());
        ostr.write(memory.data(), num_bytes);
    }
}

void PostingsSerialization::serialize(PostingListBuilder & postings, UInt64 header, WriteBuffer & ostr)
{
    if (postings.isLarge())
    {
        postings.getLarge().runOptimize();
        serialize(postings.getLarge().roaring, header, ostr);
    }
    else
    {
        chassert(header & RawPostings);
        size_t cardinality = postings.size();
        const auto & array = postings.getSmall();

        for (size_t i = 0; i < cardinality; ++i)
            writeVarUInt(array[i], ostr);
    }
}

PostingListPtr PostingsSerialization::deserialize(ReadBuffer & istr, UInt64 header, UInt64 cardinality)
{
    if (header & Flags::RawPostings)
    {
        std::vector<UInt32> values(cardinality);
        for (size_t i = 0; i < cardinality; ++i)
            readVarUInt(values[i], istr);

        auto postings = std::make_shared<PostingList>();
        postings->addMany(cardinality, values.data());
        return postings;
    }
    else
    {
        size_t num_bytes;
        readVarUInt(num_bytes, istr);

        /// If the posting list is completely in the buffer, avoid copying.
        if (istr.position() && istr.position() + num_bytes <= istr.buffer().end())
            return std::make_shared<PostingList>(PostingList::read(istr.position()));

        std::vector<char> buf(num_bytes);
        istr.readStrict(buf.data(), num_bytes);
        return std::make_shared<PostingList>(PostingList::read(buf.data()));
    }
}


bool RowsRange::intersects(const RowsRange & other) const
{
    return (begin <= other.begin && other.begin <= end) || (other.begin <= begin && begin <= other.end);
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

    SerializationString serialization_string;
    serialization_string.deserializeBinaryBulk(*tokens_column, istr, 0, num_tokens, 0.0);

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

            offset += lcp + data_size;

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

void MergeTreeIndexGranuleText::deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReadGranulesMicroseconds);

    auto * index_stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    auto * dictionary_stream = streams.at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto * postings_stream = streams.at(MergeTreeIndexSubstream::Type::TextIndexPostings);

    if (!index_stream || !dictionary_stream || !postings_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with type 'text' must be deserialized with 3 streams: index, dictionary, postings. One of the streams is missing");

    readSparseIndex(*index_stream, state);
    analyzeDictionary(*dictionary_stream, state);
    readPostingsForRareTokens(*postings_stream, state);
}

void MergeTreeIndexGranuleText::readSparseIndex(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state)
{
    /// Either retrieves a text index header from cache or from disk when cache is disabled.
    const auto load_sparse_index = [&]
    {
        auto index = TextIndexSerialization::deserializeSparseIndex(*stream.getDataBuffer());
        return std::make_shared<DictionarySparseIndex>(std::move(index));
    };

    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    auto hash = TextIndexHeaderCache::hash(state.part.getDataPartStorage().getFullPath(), state.index.getFileName());
    sparse_index = condition_text.headerCache()->getOrSet(hash, load_sparse_index);
}

void MergeTreeIndexGranuleText::analyzeDictionary(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state)
{
    if (sparse_index->empty())
        return;

    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    auto global_search_mode = condition_text.getGlobalSearchMode();
    const auto & all_search_tokens = condition_text.getAllSearchTokens();
    std::map<size_t, std::vector<std::string_view>> block_to_tokens;

    for (const auto & token : all_search_tokens)
    {
        size_t idx = sparse_index->upperBound(token);

        if (idx != 0)
            --idx;

        block_to_tokens[idx].emplace_back(token);
    }

    auto * data_buffer = stream.getDataBuffer();

    /// Either retrieves a dictionary block from cache or from disk when cache is disabled.
    const auto get_dictionary_block = [&](size_t block_id)
    {
        const auto load_dictionary_block = [&] -> TextIndexDictionaryBlockCacheEntryPtr
        {
            UInt64 offset_in_file = sparse_index->getOffsetInFile(block_id);
            stream.seekToMark({offset_in_file, 0});
            return std::make_shared<TextIndexDictionaryBlockCacheEntry>(TextIndexSerialization::deserializeDictionaryBlock(*data_buffer));
        };

        auto hash = TextIndexDictionaryBlockCache::hash(state.part.getDataPartStorage().getFullPath(), state.index.getFileName(), block_id);
        return condition_text.dictionaryBlockCache()->getOrSet(hash, load_dictionary_block);
    };

    for (const auto & [block_idx, tokens] : block_to_tokens)
    {
        const auto dictionary_block = get_dictionary_block(block_idx);

        for (const auto & token : tokens)
        {
            auto * token_info = dictionary_block->getTokenInfo(token);

            if (token_info)
            {
                remaining_tokens.emplace(token, *token_info);
            }
            else if (global_search_mode == TextSearchMode::All)
            {
                remaining_tokens.clear();
                return;
            }
        }
    }
}

PostingListPtr MergeTreeIndexGranuleText::readPostingsBlock(
    MergeTreeIndexReaderStream & stream,
    MergeTreeIndexDeserializationState & state,
    const TokenPostingsInfo & token_info,
    size_t block_idx)
{
    auto * data_buffer = stream.getDataBuffer();

    const String & data_path = state.part.getDataPartStorage().getFullPath();
    const String & index_name = state.index.getFileName();
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*state.condition);

    const auto load_postings = [&]() -> PostingListPtr
    {
        ProfileEvents::increment(ProfileEvents::TextIndexReadPostings);
        stream.seekToMark({token_info.offsets[block_idx], 0});
        return PostingsSerialization::deserialize(*data_buffer, token_info.header, token_info.cardinality);
    };

    auto hash = TextIndexPostingsCache::hash(data_path, index_name, token_info.offsets[block_idx]);
    return condition_text.postingsCache()->getOrSet(hash, load_postings);
}

void MergeTreeIndexGranuleText::readPostingsForRareTokens(MergeTreeIndexReaderStream & stream, MergeTreeIndexDeserializationState & state)
{
    using enum PostingsSerialization::Flags;

    for (const auto & [token, token_info] : remaining_tokens)
    {
        if (token_info.header & EmbeddedPostings)
        {
            chassert(token_info.embedded_postings);
            rare_tokens_postings.emplace(token, token_info.embedded_postings);
            ProfileEvents::increment(ProfileEvents::TextIndexUsedEmbeddedPostings);
        }
        else if (token_info.header & SingleBlock)
        {
            chassert(token_info.offsets.size() == 1);
            rare_tokens_postings.emplace(token, readPostingsBlock(stream, state, token_info, 0));
        }
    }
}

size_t MergeTreeIndexGranuleText::memoryUsageBytes() const
{
    return sizeof(*this)
        + sparse_index->memoryUsageBytes()
        + remaining_tokens.capacity() * sizeof(*remaining_tokens.begin())
        + rare_tokens_postings.capacity() * sizeof(*rare_tokens_postings.begin());
}

bool MergeTreeIndexGranuleText::hasAnyQueryTokens(const TextSearchQuery & query) const
{
    if (!current_range.has_value())
    {
        return std::ranges::any_of(query.tokens, [this](const auto & token)
        {
            return remaining_tokens.contains(token);
        });
    }

    PostingList range_posting;
    range_posting.addRangeClosed(current_range->begin, current_range->end);

    for (const auto & token : query.tokens)
    {
        auto it = remaining_tokens.find(token);
        if (it == remaining_tokens.end())
            continue;

        bool has_any_range = std::ranges::any_of(it->second.ranges, [this](const auto & range)
        {
            return current_range->intersects(range);
        });

        if (!has_any_range)
            continue;

        /// We read postings only for tokens that has one block.
        /// Otherwise, assume that the token is not useful
        /// for filtering and is present in all granules.
        auto postings = getPostingsForRareToken(token);
        if (!postings)
            return true;

        auto intersection = *postings & range_posting;
        if (!intersection.isEmpty())
            return true;
    }

    return false;
}

bool MergeTreeIndexGranuleText::hasAllQueryTokens(const TextSearchQuery & query) const
{
    if (query.tokens.empty())
        return false;

    return hasAllQueryTokensOrEmpty(query);
}

bool MergeTreeIndexGranuleText::hasAllQueryTokensOrEmpty(const TextSearchQuery & query) const
{
    if (!current_range.has_value())
    {
        return std::ranges::all_of(query.tokens, [this](const auto & token)
        {
            return remaining_tokens.contains(token);
        });
    }

    PostingList intersection;
    intersection.addRangeClosed(current_range->begin, current_range->end);

    for (const auto & token : query.tokens)
    {
        auto it = remaining_tokens.find(token);
        if (it == remaining_tokens.end())
            return false;

        bool has_any_range = std::ranges::any_of(it->second.ranges, [this](const auto & range)
        {
            return current_range->intersects(range);
        });

        if (!has_any_range)
            return false;

        /// We read postings only for tokens that has one block.
        /// Otherwise, assume that the token is not useful
        /// for filtering and is present in all granules.
        if (auto postings = getPostingsForRareToken(token))
        {
            intersection &= *postings;
            if (intersection.cardinality() == 0)
                return false;
        }
    }

    return true;
}

PostingListPtr MergeTreeIndexGranuleText::getPostingsForRareToken(std::string_view token) const
{
    auto it = rare_tokens_postings.find(token);
    return it == rare_tokens_postings.end() ? nullptr : it->second;
}

void MergeTreeIndexGranuleText::resetAfterAnalysis()
{
    /// Reset data that is not needed after the analysis.
    /// Keep only remaining tokens with postings lists.
    sparse_index.reset();
}

MergeTreeIndexGranuleTextWritable::MergeTreeIndexGranuleTextWritable(
    MergeTreeIndexTextParams params_,
    SortedTokensAndPostings && tokens_and_postings_,
    TokenToPostingsBuilderMap && tokens_map_,
    std::list<PostingList> && posting_lists_,
    std::unique_ptr<Arena> && arena_)
    : params(std::move(params_))
    , tokens_and_postings(std::move(tokens_and_postings_))
    , tokens_map(std::move(tokens_map_))
    , posting_lists(std::move(posting_lists_))
    , arena(std::move(arena_))
    , logger(getLogger("TextIndexGranuleWriter"))
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
    writeVarUInt(first_token.size(), ostr);
    ostr.write(first_token.data(), first_token.size());

    std::string_view previous_token = first_token;
    for (size_t i = block_begin + 1; i < block_end; ++i)
    {
        auto current_token = token_getter(i);
        auto lcp = computeCommonPrefixLength(previous_token, current_token);
        writeVarUInt(lcp, ostr);
        writeVarUInt(current_token.size() - lcp, ostr);
        ostr.write(current_token.data() + lcp, current_token.size() - lcp);
        previous_token = current_token;
    }
}

/// Split postings into smaller blocks without copying the data.
/// We use the fact that the Roaring Bitmap is split into small
/// containers that are stored in contiguous memory and sorted
/// by the key. Therefore, to create a view to the smaller bitmap,
/// we need only to adjust the pointers to the containers.
std::vector<roaring::api::roaring_bitmap_t> splitPostings(const PostingList & postings, size_t block_size)
{
    std::vector<roaring::api::roaring_bitmap_t> result;
    result.reserve((postings.cardinality() + block_size - 1) / block_size);
    const auto & container = postings.roaring.high_low_container;

    auto create_bitmap_view = [&](size_t begin, size_t size)
    {
        roaring::api::roaring_bitmap_t bitmap;
        auto & new_container = bitmap.high_low_container;

        new_container.size = size;
        new_container.allocation_size = 0;
        new_container.containers = container.containers + begin;
        new_container.typecodes = container.typecodes + begin;
        new_container.keys = container.keys + begin;
        new_container.flags = container.flags;
        return bitmap;
    };

    size_t begin_index = 0;
    size_t total_cardinality = 0;

    for (ssize_t i = 0; i < container.size; ++i)
    {
        size_t container_cardinality = roaring::internal::container_get_cardinality(container.containers[i], container.typecodes[i]);
        total_cardinality += container_cardinality;

        /// The result block size may exceed the threshold, but that's ok,
        /// since sizes of containers are much smaller than the target block size.
        if (total_cardinality >= block_size)
        {
            result.emplace_back(create_bitmap_view(begin_index, i - begin_index + 1));
            begin_index = i + 1;
            total_cardinality = 0;
        }
    }

    if (begin_index < static_cast<size_t>(container.size))
        result.emplace_back(create_bitmap_view(begin_index, container.size - begin_index));

    return result;
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

TokenPostingsInfo TextIndexSerialization::serializePostings(
    PostingListBuilder & postings,
    MergeTreeIndexWriterStream & postings_stream,
    size_t posting_list_block_size)
{
    using enum PostingsSerialization::Flags;
    TokenPostingsInfo info;
    info.header = 0;
    info.cardinality = postings.size();

    if (info.cardinality <= MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS)
    {
        info.header |= RawPostings;
        info.header |= EmbeddedPostings;
        return info;
    }
    else if (info.cardinality <= MAX_CARDINALITY_FOR_RAW_POSTINGS)
    {
        info.header |= RawPostings;
        info.header |= SingleBlock;
    }
    else if (info.cardinality <= posting_list_block_size)
    {
        info.header |= SingleBlock;
    }

    if (info.header & SingleBlock)
    {
        info.offsets.emplace_back(postings_stream.plain_hashing.count());
        info.ranges.emplace_back(postings.minimum(), postings.maximum());
        PostingsSerialization::serialize(postings, info.header, postings_stream.plain_hashing);
    }
    else
    {
        chassert(postings.isLarge());
        postings.getLarge().runOptimize();
        auto blocks = splitPostings(postings.getLarge(), posting_list_block_size);

        for (const auto & block : blocks)
        {
            if (roaring::api::roaring_bitmap_get_cardinality(&block) == 0)
                continue;

            info.offsets.emplace_back(postings_stream.plain_hashing.count());
            info.ranges.emplace_back(roaring::api::roaring_bitmap_minimum(&block), roaring::api::roaring_bitmap_maximum(&block));
            PostingsSerialization::serialize(block, info.header, postings_stream.plain_hashing);
        }
    }

    return info;
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

void TextIndexSerialization::serializeSparseIndex(const DictionarySparseIndex & sparse_index, WriteBuffer & ostr)
{
    UInt64 version = static_cast<UInt64>(SparseIndexVersion::Initial);
    writeVarUInt(version, ostr);
    chassert(sparse_index.tokens->size() == sparse_index.offsets_in_file->size());

    SerializationString serialization_string;
    SerializationNumber<UInt64> serialization_number;

    writeVarUInt(sparse_index.tokens->size(), ostr);
    serialization_string.serializeBinaryBulk(*sparse_index.tokens, ostr, 0, sparse_index.tokens->size());
    serialization_number.serializeBinaryBulk(*sparse_index.offsets_in_file, ostr, 0, sparse_index.offsets_in_file->size());
}

DictionarySparseIndex TextIndexSerialization::deserializeSparseIndex(ReadBuffer & istr)
{
    ProfileEvents::increment(ProfileEvents::TextIndexReadSparseIndexBlocks);

    UInt64 version;
    readVarUInt(version, istr);

    if (version != static_cast<UInt64>(SparseIndexVersion::Initial))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unsupported version of sparse index ({})", version);

    size_t num_sparse_index_tokens;
    readVarUInt(num_sparse_index_tokens, istr);

    auto tokens = deserializeTokensRaw(istr, num_sparse_index_tokens);
    auto offsets = ColumnUInt64::create();

    SerializationNumber<UInt64> serialization_number;
    serialization_number.deserializeBinaryBulk(*offsets, istr, 0, num_sparse_index_tokens, 0.0);
    return DictionarySparseIndex(std::move(tokens), std::move(offsets));
}

TokenPostingsInfo TextIndexSerialization::deserializeTokenInfo(ReadBuffer & istr)
{
    using enum PostingsSerialization::Flags;
    TokenPostingsInfo info;

    readVarUInt(info.header, istr);
    readVarUInt(info.cardinality, istr);

    if (info.header & EmbeddedPostings)
    {
        auto postings = PostingsSerialization::deserialize(istr, info.header, info.cardinality);
        info.offsets.emplace_back(0);
        info.ranges.emplace_back(postings->minimum(), postings->maximum());
        info.embedded_postings = std::move(postings);
    }
    else
    {
        UInt64 num_postings_blocks = 1;

        if (!(info.header & SingleBlock))
            readVarUInt(num_postings_blocks, istr);

        for (size_t j = 0; j < num_postings_blocks; ++j)
        {
            UInt64 offset_in_file;
            RowsRange rows_range;

            readVarUInt(offset_in_file, istr);
            readVarUInt(rows_range.begin, istr);
            readVarUInt(rows_range.end, istr);

            info.offsets.emplace_back(offset_in_file);
            info.ranges.emplace_back(std::move(rows_range));
        }
    }
    return info;
}

DictionaryBlock TextIndexSerialization::deserializeDictionaryBlock(ReadBuffer & istr)
{
    ProfileEvents::increment(ProfileEvents::TextIndexReadDictionaryBlocks);

    UInt64 tokens_format;
    readVarUInt(tokens_format, istr);

    size_t num_tokens = 0;
    readVarUInt(num_tokens, istr);

    ColumnPtr tokens_column;
    switch (tokens_format)
    {
        case static_cast<UInt64>(TextIndexSerialization::TokensFormat::RawStrings):
            tokens_column = deserializeTokensRaw(istr, num_tokens);
            break;
        case static_cast<UInt64>(TextIndexSerialization::TokensFormat::FrontCodedStrings):
            tokens_column = deserializeTokensFrontCoding(istr, num_tokens);
            break;
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown tokens serialization format ({}) in dictionary block", tokens_format);
    }

    std::vector<TokenPostingsInfo> token_infos;
    token_infos.reserve(num_tokens);

    for (size_t i = 0; i < num_tokens; ++i)
        token_infos.emplace_back(TextIndexSerialization::deserializeTokenInfo(istr));

    return DictionaryBlock{std::move(tokens_column), std::move(token_infos)};
}

template <typename Stream>
DictionarySparseIndex serializeTokensAndPostings(
    const SortedTokensAndPostings & tokens_and_postings,
    Stream & dictionary_stream,
    Stream & postings_stream,
    const MergeTreeIndexTextParams & params)
{
    size_t num_tokens = tokens_and_postings.size();
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

        const auto & first_token = tokens_and_postings[block_begin].first;
        sparse_index_offsets_data.emplace_back(dictionary_mark.offset_in_compressed_file);
        sparse_index_str.insertData(first_token.data(), first_token.size());

        serializeTokensImpl(
            [&](size_t i) { return tokens_and_postings[i].first; },
            dictionary_stream.compressed_hashing,
            tokens_format,
            block_begin,
            block_end);

        for (size_t i = block_begin; i < block_end; ++i)
        {
            auto & postings = *tokens_and_postings[i].second;
            auto token_info = TextIndexSerialization::serializePostings(postings, postings_stream, params.posting_list_block_size);
            TextIndexSerialization::serializeTokenInfo(dictionary_stream.compressed_hashing, token_info);

            if (token_info.header & PostingsSerialization::Flags::EmbeddedPostings)
                PostingsSerialization::serialize(postings, token_info.header, dictionary_stream.compressed_hashing);
        }
    }

    return DictionarySparseIndex(std::move(sparse_index_tokens), std::move(sparse_index_offsets));
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
        params);

    TextIndexSerialization::serializeSparseIndex(sparse_index_block, index_stream->compressed_hashing);
}

void MergeTreeIndexGranuleTextWritable::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deserialization of MergeTreeIndexGranuleTextWritable is not implemented");
}

size_t MergeTreeIndexGranuleTextWritable::memoryUsageBytes() const
{
    size_t posting_lists_size = 0;
    for (const auto & plist : posting_lists)
        posting_lists_size += plist.getSizeInBytes();

    return sizeof(*this)
        /// can ignore the sizeof(PostingListBuilder) here since it is just references to tokens_map
        + tokens_and_postings.capacity() * sizeof(SortedTokensAndPostings::value_type)
        + tokens_map.getBufferSizeInBytes()
        + posting_lists_size
        + arena->allocatedBytes();
}

MergeTreeIndexTextGranuleBuilder::MergeTreeIndexTextGranuleBuilder(
    MergeTreeIndexTextParams params_,
    TokenExtractorPtr token_extractor_)
    : params(std::move(params_))
    , token_extractor(token_extractor_)
    , arena(std::make_unique<Arena>())
{
}

PostingListBuilder::PostingListBuilder(PostingList * posting_list)
    : large{posting_list, roaring::BulkContext()}
    , small_size(max_small_size)
{
}

void PostingListBuilder::add(UInt32 value, PostingListsHolder & postings_holder)
{
    if (small_size < max_small_size)
    {
        if (small_size)
        {
            /// Values are added in non-descending order.
            chassert(small[small_size - 1] <= value);
            if (small[small_size - 1] == value)
                return;
        }

        small[small_size++] = value;

        if (small_size == max_small_size)
        {
            auto small_copy = std::move(small);
            large.postings = &postings_holder.emplace_back();
            large.context = roaring::BulkContext();

            for (size_t i = 0; i < max_small_size; ++i)
                large.postings->addBulk(large.context, small_copy[i]);
        }
    }
    else
    {
        /// Use addBulk to optimize consecutive insertions into the posting list.
        large.postings->addBulk(large.context, value);
    }
}

void MergeTreeIndexTextGranuleBuilder::addDocument(std::string_view document)
{
    forEachTokenPadded(
        *token_extractor,
        document.data(),
        document.size(),
        [&](const char * token_start, size_t token_length)
        {
            bool inserted;
            TokenToPostingsBuilderMap::LookupResult it;

            ArenaKeyHolder key_holder{std::string_view(token_start, token_length), *arena};
            tokens_map.emplace(key_holder, it, inserted);

            auto & posting_list_builder = it->getMapped();
            posting_list_builder.add(current_row, posting_lists);
            ++num_processed_tokens;
            return false;
        });

    ++num_processed_documents;
}

std::unique_ptr<MergeTreeIndexGranuleTextWritable> MergeTreeIndexTextGranuleBuilder::build()
{
    SortedTokensAndPostings sorted_values;
    sorted_values.reserve(tokens_map.size());

    tokens_map.forEachValue([&](const auto & key, auto & mapped)
    {
        sorted_values.emplace_back(key, &mapped);
    });

    std::ranges::sort(sorted_values, [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    return std::make_unique<MergeTreeIndexGranuleTextWritable>(
        params,
        std::move(sorted_values),
        std::move(tokens_map),
        std::move(posting_lists),
        std::move(arena));
}

void MergeTreeIndexTextGranuleBuilder::reset()
{
    current_row = 0;
    num_processed_tokens = 0;
    tokens_map = {};
    posting_lists.clear();
    arena = std::make_unique<Arena>();
}

MergeTreeIndexAggregatorText::MergeTreeIndexAggregatorText(
    String index_column_name_,
    MergeTreeIndexTextParams params_,
    TokenExtractorPtr token_extractor_,
    MergeTreeIndexTextPreprocessorPtr preprocessor_)
    : index_column_name(std::move(index_column_name_))
    , params(std::move(params_))
    , token_extractor(token_extractor_)
    , granule_builder(params, token_extractor_)
    , preprocessor(preprocessor_)
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

    if (*pos + limit > std::numeric_limits<UInt32>::max())
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot build text index in part with {} rows. Materialization of text index is not supported for parts with more than {} rows",
            *pos + limit, std::numeric_limits<UInt32>::max());
    }

    const size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    const auto & index_column = block.getByName(index_column_name);

    if (isArray(index_column.type))
    {
        size_t offset = *pos;

        const auto & column_array = assert_cast<const ColumnArray &>(*index_column.column);
        const auto & column_data = column_array.getData();
        const auto & column_offsets = column_array.getOffsets();

        for (size_t i = 0; i < rows_read; ++i)
        {
            size_t element_start_row = column_offsets[offset + i - 1];
            size_t elements_size = column_offsets[offset + i] - element_start_row;

            for (size_t element_idx = 0; element_idx < elements_size; ++element_idx)
            {
                auto ref = column_data.getDataAt(element_start_row + element_idx);
                granule_builder.addDocument(ref);
            }

            granule_builder.incrementCurrentRow();
        }
    }
    else
    {
        auto [processed_column, offset] = preprocessor->processColumn(index_column, *pos, rows_read);

        for (size_t i = 0; i < rows_read; ++i)
        {
            const std::string_view ref = processed_column->getDataAt(offset + i);
            granule_builder.addDocument(ref);
            granule_builder.incrementCurrentRow();
        }
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
    , preprocessor(std::make_shared<MergeTreeIndexTextPreprocessor>(params.preprocessor, index_))
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

MergeTreeIndexAggregatorPtr MergeTreeIndexText::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorText>(index.column_names[0], params, token_extractor.get(), preprocessor);
}

MergeTreeIndexConditionPtr MergeTreeIndexText::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionText>(predicate, context, index.sample_block, token_extractor.get(), preprocessor);
}

static const String ARGUMENT_TOKENIZER = "tokenizer";
static const String ARGUMENT_PREPROCESSOR = "preprocessor";
static const String ARGUMENT_DICTIONARY_BLOCK_SIZE = "dictionary_block_size";
static const String ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION = "dictionary_block_frontcoding_compression";
static const String ARGUMENT_POSTING_LIST_BLOCK_SIZE = "posting_list_block_size";

namespace
{

template <typename Type>
std::optional<Type> tryCastAs(const Field & field)
{
    auto expected_type = Field::TypeToEnum<Type>::value;
    return expected_type == field.getType() ? std::make_optional(field.safeGet<Type>()) : std::nullopt;
}

template <typename Type>
Type castAs(const Field & field, std::string_view argument_name)
{
    auto result = tryCastAs<Type>(field);

    if (!result.has_value())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Text index argument '{}' expected to be {}, but got {}",
            argument_name, fieldTypeToString(Field::TypeToEnum<Type>::value), field.getTypeName());
    }

    return result.value();
}

template <typename Type>
std::optional<Type> extractOption(std::unordered_map<String, Field> & options, const String & option, bool throw_on_unexpected_type = true)
{
    auto it = options.find(option);
    if (it == options.end())
        return {};

    Field value;

    if (throw_on_unexpected_type)
    {
        value = castAs<Type>(it->second, option);
    }
    else
    {
        auto maybe_value = tryCastAs<Type>(it->second);
        if (!maybe_value.has_value())
            return {};

        value = maybe_value.value();
    }

    options.erase(it);
    return value.safeGet<Type>();
}

std::unordered_map<String, Field> convertArgumentsToOptionsMap(const FieldVector & arguments)
{
    std::unordered_map<String, Field> options;
    for (const Field & argument : arguments)
    {
        if (argument.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Arguments of text index must be key-value pair (identifier = literal)");

        Tuple tuple = argument.safeGet<Tuple>();
        String key = tuple[0].safeGet<String>();

        if (options.contains(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index '{}' argument is specified more than once", key);

        options[key] = tuple[1];
    }
    return options;
}

/**
 * Tokenizer option can be String literal, identifier or function.
 * In case of a function, tokenizer specific argument is provided as parameter of the function.
 * This function is responsible to extract the tokenizer name and parameter if provided.
 */
std::pair<String, std::vector<Field>> extractTokenizer(std::unordered_map<String, Field> & options)
{
    /// Check that tokenizer is present
    if (!options.contains(ARGUMENT_TOKENIZER))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index must have an '{}' argument", ARGUMENT_TOKENIZER);

    /// Tokenizer is provided as Literal or Identifier.
    if (auto tokenizer_str = extractOption<String>(options, ARGUMENT_TOKENIZER, false); tokenizer_str)
        return {tokenizer_str.value(), {}};

    /// Tokenizer is provided as Function.
    if (auto tokenizer_tuple = extractOption<Tuple>(options, ARGUMENT_TOKENIZER, false); tokenizer_tuple)
    {
        /// Functions are converted into Tuples as the first entry is the name of the function and rest is arguments.
        chassert(!tokenizer_tuple->empty());

        const auto & function_name = tokenizer_tuple->at(0);
        if (function_name.getType() != Field::Types::Which::String)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Text index argument '{}': function name expected to be String, but got {}",
                ARGUMENT_TOKENIZER,
                function_name.getTypeName());
        }

        std::vector<Field> params(tokenizer_tuple->begin() + 1, tokenizer_tuple->end());
        return {function_name.safeGet<String>(), std::move(params)};
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Text index argument '{}' expected to be either String or Function, but got {}",
        ARGUMENT_TOKENIZER,
        options.at(ARGUMENT_TOKENIZER).getTypeName());
}

}

MergeTreeIndexPtr textIndexCreator(const IndexDescription & index)
{
    std::unordered_map<String, Field> options = convertArgumentsToOptionsMap(index.arguments);
    const auto [tokenizer, params] = extractTokenizer(options);

    static std::vector<String> allowed_tokenizers
        = {NgramsTokenExtractor::getExternalName(),
           SplitByNonAlphaTokenExtractor::getExternalName(),
           SplitByStringTokenExtractor::getExternalName(),
           ArrayTokenExtractor::getExternalName(),
           SparseGramsTokenExtractor::getExternalName()};

    auto token_extractor = TokenizerFactory::createTokenizer(tokenizer, params, allowed_tokenizers, index.name);

    String preprocessor = extractOption<String>(options, ARGUMENT_PREPROCESSOR).value_or("");
    UInt64 dictionary_block_size = extractOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE).value_or(DEFAULT_DICTIONARY_BLOCK_SIZE);
    UInt64 dictionary_block_frontcoding_compression = extractOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION).value_or(DEFAULT_DICTIONARY_BLOCK_USE_FRONTCODING);
    UInt64 posting_list_block_size = extractOption<UInt64>(options, ARGUMENT_POSTING_LIST_BLOCK_SIZE).value_or(DEFAULT_POSTING_LIST_BLOCK_SIZE);

    MergeTreeIndexTextParams index_params{
        dictionary_block_size,
        dictionary_block_frontcoding_compression,
        posting_list_block_size,
        preprocessor};

    if (!options.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected text index arguments: {}", fmt::join(std::views::keys(options), ", "));

    return std::make_shared<MergeTreeIndexText>(index, index_params, std::move(token_extractor));
}

void textIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    std::unordered_map<String, Field> options = convertArgumentsToOptionsMap(index.arguments);
    const auto [tokenizer, params] = extractTokenizer(options);

    static std::vector<String> allowed_tokenizers
        = {NgramsTokenExtractor::getExternalName(),
           SplitByNonAlphaTokenExtractor::getExternalName(),
           SplitByStringTokenExtractor::getExternalName(),
           ArrayTokenExtractor::getExternalName(),
           SparseGramsTokenExtractor::getExternalName()};

    TokenizerFactory::createTokenizer(tokenizer, params, allowed_tokenizers, index.name, /*only_validate = */ true);

    UInt64 dictionary_block_size = extractOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_SIZE).value_or(DEFAULT_DICTIONARY_BLOCK_SIZE);
    if (dictionary_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be greater than 0, but got {}", ARGUMENT_DICTIONARY_BLOCK_SIZE, dictionary_block_size);

    UInt64 dictionary_block_use_fc_compression = extractOption<UInt64>(options, ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION).value_or(DEFAULT_DICTIONARY_BLOCK_USE_FRONTCODING);
    if (dictionary_block_use_fc_compression > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be 0 or 1, but got {}", ARGUMENT_DICTIONARY_BLOCK_FRONTCODING_COMPRESSION, dictionary_block_use_fc_compression);

    UInt64 posting_list_block_size = extractOption<UInt64>(options, ARGUMENT_POSTING_LIST_BLOCK_SIZE).value_or(DEFAULT_POSTING_LIST_BLOCK_SIZE);
    if (posting_list_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index argument '{}' must be greater than 0, but got {}", ARGUMENT_POSTING_LIST_BLOCK_SIZE, posting_list_block_size);

    auto preprocessor = extractOption<String>(options, ARGUMENT_PREPROCESSOR, false);

    if (!options.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected text index arguments: {}", fmt::join(std::views::keys(options), ", "));

    /// Check that the index is created on a single column
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Text index must be created on a single column");

    WhichDataType data_type(index.data_types[0]);
    if (data_type.isArray())
    {
        if (preprocessor.has_value())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index created on Array columns does not support preprocessor argument yet.");

        const auto & array_type = assert_cast<const DataTypeArray &>(*index.data_types[0]);
        data_type = WhichDataType(array_type.getNestedType());
    }
    else if (data_type.isLowCardinality())
    {
        const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index.data_types[0]);
        data_type = WhichDataType(low_cardinality.getDictionaryType());
    }

    if (!data_type.isString() && !data_type.isFixedString())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Text index must be created on columns of type `String`, `FixedString`, `LowCardinality(String)`, `LowCardinality(FixedString)`, `Array(String)` or `Array(FixedString)`");
    }

    /// Check preprocessor now, after data_type and column_names and index.data_types were already checked because we want to get accurate
    /// error messages.
    if (preprocessor.has_value())
    {
        /// For very strict validation of the expression we fully parse it here.  However it will be parsed again for index construction,
        /// generally immediately after this call.
        /// This is a bit redundant but I won't expect that this impact performance anyhow because the expression is intended to be simple
        /// enough.  But if this redundant construction represents an issue we could simple build the "intermediate" ASTPtr and use it for
        /// validation. That way we skip the ActionsDAG and ExpressionActions constructions.
        ExpressionActions expression = MergeTreeIndexTextPreprocessor::parseExpression(index, preprocessor.value());

        const Names required_columns = expression.getRequiredColumns();

        /// This is expected that never happen because the `validatePreprocessorASTExpression` already checks that we have a single identifier.
        /// But once again, with user inputs: Don't trust, validate!
        if (required_columns.size() != 1 || required_columns.front() != index.column_names.front())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index preprocessor expression must depend only of column: {}", index.column_names.front());
    }

}

static Tuple parseNamedArgumentFromAST(const ASTFunction * ast_equal_function)
{
    if (ast_equal_function == nullptr
        || ast_equal_function->name != "equals"
        || ast_equal_function->arguments->children.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot mix key-value pair and single argument as text index arguments");

    Tuple result;

    ASTPtr arguments = ast_equal_function->arguments;
    /// Parse parameter name. It can be Identifier.
    {
        const auto * identifier = arguments->children[0]->as<ASTIdentifier>();
        if (identifier == nullptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index parameter name: Expected identifier");

        result.emplace_back(identifier->name());
    }

    if (result.back() == ARGUMENT_PREPROCESSOR)
    {
        const ASTFunction * preprocessor_function = arguments->children[1]->as<ASTFunction>();
        if (preprocessor_function == nullptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index preprocessor argument must be an expression");

        /// preprocessor_function->getColumnName() returns the string representation for the expression. That string will be parsed again on
        /// index recreation but can also be stored in the index metadata as is.
        result.emplace_back(preprocessor_function->getColumnName());
    }
    else
    {
        /// Parse parameter value. It can be Literal, Identifier or Function.
        if (const auto * literal_arg = arguments->children[1]->as<ASTLiteral>(); literal_arg != nullptr)
        {
            result.emplace_back(literal_arg->value);
        }
        else if (const auto * identifier_arg = arguments->children[1]->as<ASTIdentifier>(); identifier_arg != nullptr)
        {
            result.emplace_back(identifier_arg->name());
        }
        else if (const auto * function_arg = arguments->children[1]->as<ASTFunction>(); function_arg != nullptr)
        {
            Tuple tuple;
            tuple.emplace_back(function_arg->name);
            for (const auto & subargument : function_arg->arguments->children)
            {
                const auto * arg_literal = subargument->as<ASTLiteral>();
                if (arg_literal == nullptr)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index function argument: Expected literal");

                tuple.emplace_back(arg_literal->value);
            }
            result.emplace_back(tuple);
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index parameter value: Expected literal, identifier or function");
        }
    }

    return result;
}

FieldVector MergeTreeIndexText::parseArgumentsListFromAST(const ASTPtr & arguments)
{
    FieldVector result;
    result.reserve(arguments->children.size());

    for (const auto & argument : arguments->children)
    {
        const auto * ast_equal_function = argument->as<ASTFunction>();
        result.emplace_back(parseNamedArgumentFromAST(ast_equal_function));
    }

    return result;
}

}
