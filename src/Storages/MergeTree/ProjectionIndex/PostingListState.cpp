#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexSerializationContext.h>
#include <Common/Arena.h>
#include <base/scope_guard.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}

void parsePostingListCodecType(const String & codec_name)
{
    if (codec_name.empty() || codec_name == "none" || codec_name == "bitpacking")
        return;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown projection posting list codec: '{}'. Supported: 'none', 'bitpacking'", codec_name);
}

void PostingListParams::validate() const
{
    if (format_version > POSTING_LIST_FORMAT_VERSION_CURRENT)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported PostingList format version: {}", format_version);
    parsePostingListCodecType(posting_list_codec);

    /// Limit `posting_list_block_size` to ensure per-large-block cumulative byte counters
    /// (`packed_block_cum_bytes`, UInt32) cannot overflow. Each of the (block_size / 256)
    /// packed blocks contributes at most ~2052 bytes (doc + freq worst case).
    /// 256M docs × 2052 / 256 ≈ 2 GB — well within UInt32 range.
    static constexpr size_t MAX_POSTING_LIST_BLOCK_SIZE = 256 * 1024 * 1024;
    if (posting_list_block_size > MAX_POSTING_LIST_BLOCK_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "posting_list_block_size {} exceeds maximum allowed value {}",
            posting_list_block_size,
            MAX_POSTING_LIST_BLOCK_SIZE);
}

/// AggregateFunctionPostingList is not a regular aggregate function — it only participates in
/// merge paths (vertical merge of projection parts). The add/serialize/deserialize methods
/// are never called in normal operation; they throw to catch accidental misuse.

void AggregateFunctionPostingList::add(AggregateDataPtr, const IColumn **, size_t, Arena *) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "AggregateFunctionPostingList::add() is not supported. "
        "This aggregate function is only used for merge-time posting list combining, not row-by-row aggregation.");
}
void AggregateFunctionPostingList::serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t>) const
{
    const auto & posting_list_data = data(place);
    chassert(posting_list_data.isStream());
    const auto & stream = posting_list_data.stream;

    writeVarUInt(stream.doc_count, buf);
    writeVarUInt(stream.first_doc_id, buf);
    writeVarUInt(stream.first_doc_freq, buf);

    if (stream.doc_count > 0)
    {
        PODArray<UInt32> doc_ids(stream.doc_count);
        stream.collect(doc_ids.data());
        buf.write(reinterpret_cast<const char *>(doc_ids.data()), stream.doc_count * sizeof(UInt32));
    }
}

void AggregateFunctionPostingList::deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t>, Arena *) const
{
    auto & posting_list_data = data(place);
    auto & stream = posting_list_data.stream;

    UInt64 doc_count_u64 = 0;
    UInt64 first_doc_id = 0;
    UInt64 first_doc_freq = 0;
    readVarUInt(doc_count_u64, buf);
    readVarUInt(first_doc_id, buf);
    readVarUInt(first_doc_freq, buf);

    UInt32 doc_count = static_cast<UInt32>(doc_count_u64);
    stream.doc_count = doc_count;
    stream.first_doc_id = static_cast<UInt32>(first_doc_id);
    stream.first_doc_freq = static_cast<UInt32>(first_doc_freq);

    if (doc_count > 0)
    {
        /// Read raw doc_ids and store as embedded Roaring bitmap in a LazyPostingStream.
        /// This enables the standard merge path (PostingListStream::merge) to consume them.
        PODArray<UInt32> doc_ids(doc_count);
        buf.readStrict(reinterpret_cast<char *>(doc_ids.data()), doc_count * sizeof(UInt32));

        if (!stream.lazy)
            stream.lazy = new LazyPostingStream;

        auto & entries = stream.lazy->streams.entries;
        entries.clear();
        entries.emplace_back(nullptr, static_cast<UInt32>(first_doc_id), doc_count, LargePostingBlockMetas{});
        entries.back().first_doc_freq = static_cast<UInt32>(first_doc_freq);

        /// Build embedded Roaring bitmap from doc_ids
        auto bitmap = std::make_shared<PostingList>();
        bitmap->addMany(doc_count, doc_ids.data());
        entries.back().embedded_postings = std::move(bitmap);
    }
}

void AggregateFunctionPostingList::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * /* arena */) const
{
    auto & lhs_posting_list_data = data(place);
    auto & rhs_posting_list_data = const_cast<PostingListData &>(data(rhs));

    /// Both sides must be in stream state for merge.
    /// During projection rebuild (mutation), states are created via custom deserialization
    /// which produces stream states. If either side is not stream, it's an unexpected state.
    if (lhs_posting_list_data.isStream() && rhs_posting_list_data.isStream())
    {
        lhs_posting_list_data.stream.merge(rhs_posting_list_data.stream);
    }
    else if (lhs_posting_list_data.isWriter() && rhs_posting_list_data.isWriter())
    {
        /// Both Writer: append rhs chunks to lhs.
        auto & lhs_w = lhs_posting_list_data.writer;
        auto & rhs_w = rhs_posting_list_data.writer;

        if (rhs_w.doc_count == 0)
            return;

        if (lhs_w.doc_count == 0)
        {
            lhs_w.doc_count = rhs_w.doc_count;
            lhs_w.first_doc_id = rhs_w.first_doc_id;
            lhs_w.first_doc_freq = rhs_w.first_doc_freq;
            lhs_w.blocks_head = rhs_w.blocks_head;
            return;
        }

        auto * tail = lhs_w.blocks_head;
        if (tail)
        {
            while (tail->next)
                tail = tail->next;
            tail->next = rhs_w.blocks_head;
        }
        else
        {
            lhs_w.blocks_head = rhs_w.blocks_head;
        }
        lhs_w.doc_count += rhs_w.doc_count;
    }
    else if (lhs_posting_list_data.isStream() && lhs_posting_list_data.stream.doc_count == 0
             && rhs_posting_list_data.isWriter())
    {
        /// lhs is empty Stream (from create()), rhs is Writer.
        /// Convert lhs to Writer by replacing the union content.
        lhs_posting_list_data.destroy();
        new (&lhs_posting_list_data.writer) PostingListWriter;
        auto & lhs_w = lhs_posting_list_data.writer;
        auto & rhs_w = rhs_posting_list_data.writer;
        lhs_w.doc_count = rhs_w.doc_count;
        lhs_w.first_doc_id = rhs_w.first_doc_id;
        lhs_w.first_doc_freq = rhs_w.first_doc_freq;
        lhs_w.blocks_head = rhs_w.blocks_head;
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "PostingList merge: incompatible states (lhs.isStream={}, lhs.isWriter={}, rhs.isStream={}, rhs.isWriter={})",
            lhs_posting_list_data.isStream(), lhs_posting_list_data.isWriter(),
            rhs_posting_list_data.isStream(), rhs_posting_list_data.isWriter());
    }
}

void AggregateFunctionPostingList::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * /* arena */) const
{
    ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
    ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
    const auto & posting_list_data = data(place);
    chassert(posting_list_data.isStream());
    const auto & posting_list = posting_list_data.stream;

    const auto prev_offset = offsets_to.empty() ? IColumn::Offset{0} : offsets_to.back();
    offsets_to.push_back(prev_offset + posting_list.doc_count);

    if (posting_list.doc_count == 0)
        return;

    typename ColumnVector<UInt32>::Container & data_to = assert_cast<ColumnVector<UInt32> &>(arr_to.getData()).getData();
    size_t pos = data_to.size();
    data_to.resize(data_to.size() + posting_list.doc_count);
    posting_list.collect(&data_to[pos]);
}

// DataTypePtr AggregateFunctionPostingList::getNormalizedStateType() const
// {
//     return getPostingListType({});
// }

/// Serialization for PostingList values stored in dictionary blocks of the projection-based text index.
///
/// This serialization is responsible only for encoding and decoding the dictionary-level representation of posting
/// lists (e.g. headers, cardinality, and references to large posting list streams). Large posting lists are written to
/// and read from a separate index data stream owned by the projection index.
class SerializationPostingList final : public SimpleTextSerialization
{
private:
    std::shared_ptr<AggregateFunctionPostingList> function;

public:
    explicit SerializationPostingList(std::shared_ptr<AggregateFunctionPostingList> function_)
        : function(function_)
    {
    }

    [[noreturn]] static void throwNoSerialization()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization is not implemented for type PostingList");
    }

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }
    void serializeBinaryBulk(const IColumn &, WriteBuffer &, size_t, size_t) const override { throwNoSerialization(); }
    void deserializeBinaryBulk(IColumn &, ReadBuffer &, size_t, size_t, double) const override { throwNoSerialization(); }

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & /* state */) const override
    {
        settings.path.push_back(Substream::Regular);
        SCOPE_EXIT({ settings.path.pop_back(); });
        if (WriteBuffer * stream = settings.getter(settings.path))
        {
            /// Some code paths (e.g. MergeTreeDataPartWriterCompact::initColumnsSubstreamsIfNeeded) use NullWriteBuffer
            /// only to enumerate substreams for initialization purposes. This is unrelated to posting list writing and
            /// can be safely ignored here.
            if (!settings.projection_index_context)
                return;

            const ColumnAggregateFunction & real_column = typeid_cast<const ColumnAggregateFunction &>(column);
            const ColumnAggregateFunction::Container & vec = real_column.getData();

            size_t end = vec.size();
            if (limit)
                end = std::min(end, offset + limit);

            if (offset >= end)
                return;

            chassert(settings.projection_index_context && settings.projection_index_context->large_posting_getter);

            auto * large_posting_stream = settings.projection_index_context->large_posting_getter(settings.path);
            chassert(large_posting_stream);

            auto * idx_stream = settings.projection_index_context->index_getter
                ? settings.projection_index_context->index_getter(settings.path)
                : nullptr;

            /// Invariant: posting list data in this range is homogeneous
            if (reinterpret_cast<const PostingListData *>(vec[offset])->isWriter())
            {
                LargePostingListWriterStream * pos_stream = nullptr;
                if (function->params.enable_phrase_query_support)
                {
                    chassert(settings.projection_index_context->position_getter);
                    pos_stream = settings.projection_index_context->position_getter(settings.path);
                    chassert(pos_stream);
                }

                WriteBuffer * idx_buf = idx_stream ? &idx_stream->plain_hashing : nullptr;

                for (size_t i = offset; i < end; ++i)
                {
                    const auto * posting_list_data = reinterpret_cast<const PostingListData *>(vec[i]);
                    chassert(posting_list_data->isWriter());
                    posting_list_data->writer.finish(*stream, *large_posting_stream, function->params, idx_buf, pos_stream);
                }
            }
            else if (reinterpret_cast<const PostingListData *>(vec[offset])->isStream())
            {
                LargePostingListWriterStream * pos_stream = nullptr;
                if (function->params.enable_phrase_query_support)
                {
                    chassert(settings.projection_index_context->position_getter);
                    pos_stream = settings.projection_index_context->position_getter(settings.path);
                    chassert(pos_stream);
                }
                WriteBuffer * pos_buf = pos_stream ? &pos_stream->plain_hashing : nullptr;

                for (size_t i = offset; i < end; ++i)
                {
                    const auto * posting_list_data = reinterpret_cast<const PostingListData *>(vec[i]);
                    chassert(posting_list_data->isStream());
                    posting_list_data->stream.write(*stream, *large_posting_stream, function->params, idx_stream, pos_buf);
                }
            }
        }
    }

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & /* state */,
        SubstreamsCache * cache) const override
    {
        if (!settings.projection_index_context)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "SerializationPostingList requires projection_index_context to be set; "
                "it cannot be used in contexts without MergeTree (e.g. client with native reader)");
        }

        settings.path.push_back(Substream::Regular);

        if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
        {
            /// Data was inserted from substreams cache.
        }
        else if (ReadBuffer * stream = settings.getter(settings.path))
        {
            size_t prev_size = column->size();
            auto mutable_column = column->assumeMutable();

            if (rows_offset)
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "SerializationPostingList does not support cases where rows_offset {} is non-zero",
                    rows_offset);
            }

            ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(*mutable_column);
            ColumnAggregateFunction::Container & vec = real_column.getData();

            Arena & arena = real_column.createOrGetArena();

            size_t size_of_state = function->sizeOfData();
            size_t align_of_state = function->alignOfData();

            /// Adjust the size of state to make all states aligned in vector.
            size_t total_size_of_state = (size_of_state + align_of_state - 1) / align_of_state * align_of_state;

            const auto * matched = settings.projection_index_context->matched_row_indices;
            size_t num_to_alloc = matched ? matched->size() : limit;
            vec.reserve(vec.size() + num_to_alloc);
            char * place = arena.alignedAlloc(total_size_of_state * num_to_alloc, align_of_state);

            auto large_posting_stream = settings.projection_index_context->large_posting_getter(settings.path);
            LargePostingListReaderStreamPtr position_stream;
            if (settings.projection_index_context->position_getter)
            {
                position_stream = settings.projection_index_context->position_getter(settings.path);
                chassert(position_stream);
            }

            size_t match_cursor = 0;

            for (size_t i = 0; i < limit; ++i)
            {
                if (stream->eof())
                    break;

                if (matched && (match_cursor >= matched->size() || (*matched)[match_cursor] != i))
                {
                    /// Skip non-matched row: advance the stream, no state allocation.
                    PostingListStream::skip(*stream, function->params);
                    continue;
                }

                if (matched)
                    ++match_cursor;

                new (place) PostingListData(PostingListKind::Stream);

                try
                {
                    auto & posting_list_data = reinterpret_cast<PostingListData &>(*place);
                    chassert(posting_list_data.isStream());
                    posting_list_data.stream.read(*stream, large_posting_stream, function->params, position_stream);
                }
                catch (...)
                {
                    function->destroy(place);
                    throw;
                }

                vec.push_back(place);
                place += total_size_of_state;
            }

            column = std::move(mutable_column);
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, column->size() - prev_size);
        }

        settings.path.pop_back();
    }
};

String DataTypePostingList::getName() const
{
    WriteBufferFromOwnString stream;
    stream << "PostingList(";
    stream << format_version;
    for (const auto & parameter : parameters)
        stream << ", " << applyVisitor(FieldVisitorToString(), parameter);
    stream << ')';
    return stream.str();
}

static DataTypePtr buildPostingListType(const Array & params, const PostingListParams & posting_list_params)
{
    auto function = std::make_shared<AggregateFunctionPostingList>(posting_list_params);
    auto type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{}, Array{});
    /// Mirror format_version into DataTypeAggregateFunction::version so that it propagates to
    /// ColumnAggregateFunction::version via createColumn() / set(), matching the standard aggregate
    /// function versioning convention (see ColumnAggregateFunction.h:86).
    type->setVersion(posting_list_params.format_version, /*if_empty=*/false);
    type->setCustomization(
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypePostingList>(params, posting_list_params.format_version),
            std::make_shared<SerializationPostingList>(function)));
    return type;
}

/// Convert AST key=value arguments to an Array of Tuple fields for metadata serialization.
/// Each child of the AST is an ASTFunction(equals, [ASTIdentifier(key), value]).
static Array convertASTArgumentsToParams(const ASTPtr & arguments)
{
    Array params;
    if (!arguments)
        return params;

    params.reserve(arguments->children.size());
    for (const auto & child : arguments->children)
    {
        const auto * func = child->as<ASTFunction>();
        if (!func || func->name != "equals" || func->arguments->children.size() != 2)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Text index argument must be a key=value pair, got '{}'", child->formatForErrorMessage());
        }

        const auto * key = func->arguments->children[0]->as<ASTIdentifier>();
        if (!key)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Text index argument key must be an identifier");

        /// Serialize as "key=value" string instead of Tuple(key, value).
        /// Tuple literals like ('tokenizer', 'ngrams(3)') fail to parse as data type
        /// arguments in builds with -DENABLE_LIBRARIES=0 (fast test).
        String value_str;
        const auto * literal = func->arguments->children[1]->as<ASTLiteral>();
        if (literal)
            value_str = applyVisitor(FieldVisitorToString(), literal->value);
        else
            value_str = "'" + func->arguments->children[1]->getColumnName() + "'";

        params.emplace_back(key->name() + "=" + value_str);
    }
    return params;
}

DataTypePtr createPostingListType(const ASTPtr & text_index_definition, const PostingListParams & posting_list_params)
{
    Array params;
    if (text_index_definition && !text_index_definition->children.empty())
        params = convertASTArgumentsToParams(text_index_definition);

    return buildPostingListType(params, posting_list_params);
}

DataTypePtr createPostingListTypeFromPartMetadata(const ASTPtr & parsed_fields)
{
    if (!parsed_fields || parsed_fields->children.empty())
        return buildPostingListType({}, {});

    const auto & children = parsed_fields->children;
    const auto * descriptor_literal = children[0]->as<ASTLiteral>();
    if (!descriptor_literal || descriptor_literal->value.getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_DATA, "First PostingList metadata field must be format descriptor (UInt64)");

    const size_t format_version = descriptor_literal->value.safeGet<UInt64>();
    Array params;
    params.reserve(children.size() - 1);
    for (size_t i = 1; i < children.size(); ++i)
    {
        const auto * literal = children[i]->as<ASTLiteral>();
        if (!literal)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "PostingList metadata parameters must be literals, got '{}'",
                children[i]->formatForErrorMessage());
        }
        params.push_back(literal->value);
    }

    /// Extract PostingListParams directly from serialized Tuple parameters,
    /// without round-tripping through AST and parseTextIndexArguments.
    PostingListParams posting_list_params;
    posting_list_params.format_version = format_version;
    for (const auto & param : params)
    {
        /// Support both formats:
        /// - New: String "key=value" (e.g. "tokenizer='splitByNonAlpha'")
        /// - Legacy: Tuple(key, value) (e.g. ('tokenizer', 'splitByNonAlpha'))
        String key;
        Field value;

        if (param.getType() == Field::Types::String)
        {
            const auto & s = param.safeGet<String>();
            auto eq_pos = s.find('=');
            if (eq_pos == String::npos)
                throw Exception(ErrorCodes::INCORRECT_DATA, "PostingList metadata parameter must be 'key=value', got '{}'", s);
            key = s.substr(0, eq_pos);
            auto val_str = s.substr(eq_pos + 1);
            /// Strip surrounding quotes if present (e.g. 'splitByNonAlpha' -> splitByNonAlpha)
            if (val_str.size() >= 2 && val_str.front() == '\'' && val_str.back() == '\'')
                value = val_str.substr(1, val_str.size() - 2);
            else
            {
                /// Try to parse as UInt64, fall back to string. Ok: stoull throws on non-numeric input.
                try { value = std::stoull(val_str); }
                catch (...) { value = val_str; }
            }
        }
        else if (param.getType() == Field::Types::Tuple)
        {
            const auto & tuple = param.safeGet<Tuple>();
            if (tuple.size() != 2)
                throw Exception(ErrorCodes::INCORRECT_DATA, "PostingList metadata parameter must be a 2-element tuple");
            key = tuple[0].safeGet<String>();
            value = tuple[1];
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "PostingList metadata parameter must be String or Tuple");
        }

        if (key == "dictionary_block_size")
            posting_list_params.dictionary_block_size = value.safeGet<UInt64>();
        else if (key == "dictionary_block_frontcoding_compression")
            posting_list_params.dictionary_block_frontcoding_compression = value.safeGet<UInt64>();
        else if (key == "posting_list_block_size")
            posting_list_params.posting_list_block_size = value.safeGet<UInt64>();
        else if (key == "preprocessor")
            posting_list_params.preprocessor = make_intrusive<ASTLiteral>(value);
        else if (key == "posting_list_codec")
        {
            posting_list_params.posting_list_codec = value.safeGet<String>();
            parsePostingListCodecType(posting_list_params.posting_list_codec);
        }
        else if (key == "enable_phrase_query_support")
            posting_list_params.enable_phrase_query_support = static_cast<bool>(value.safeGet<UInt64>());
        else if (key == "has_block_index")
            posting_list_params.has_block_index = static_cast<bool>(value.safeGet<UInt64>());
        /// "tokenizer" is handled separately by MergeTreeIndexText. Skip silently.
    }

    posting_list_params.validate();
    return buildPostingListType(params, posting_list_params);
}

void registerDataTypePostingList(DataTypeFactory & factory);

void registerDataTypePostingList(DataTypeFactory & factory)
{
    factory.registerDataType("PostingList", createPostingListTypeFromPartMetadata);
}

}
