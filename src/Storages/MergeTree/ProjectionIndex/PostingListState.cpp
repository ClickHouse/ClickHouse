#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>

#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/Operators.h>
#include <Interpreters/ITokenExtractor.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexSerializationContext.h>
#include <Common/Arena.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}

void AggregateFunctionPostingList::add(AggregateDataPtr, const IColumn **, size_t, Arena *) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot add in memory posting list for the moment");
}
void AggregateFunctionPostingList::serialize(ConstAggregateDataPtr, WriteBuffer &, std::optional<size_t>) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot serialize in memory posting list for the moment");
}

void AggregateFunctionPostingList::deserialize(AggregateDataPtr, ReadBuffer &, std::optional<size_t>, Arena *) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot deserialize in memory posting list for the moment");
}

void AggregateFunctionPostingList::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * /* arena */) const
{
    auto & lhs_posting_list_data = data(place);
    const auto & rhs_posting_list_data = data(rhs);
    chassert(lhs_posting_list_data.isStream());

    if (rhs_posting_list_data.isStream())
        lhs_posting_list_data.stream.merge(rhs_posting_list_data.stream);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can only merge from stream state");
}

void AggregateFunctionPostingList::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * /* arena */) const
{
    ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
    ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
    const auto & posting_list_data = data(place);
    chassert(posting_list_data.isStream());
    const auto & posting_list = posting_list_data.stream;
    if (posting_list.doc_count == 0)
        return;

    offsets_to.push_back(offsets_to.back() + posting_list.doc_count);
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

            chassert(end > 0);
            chassert(settings.projection_index_context && settings.projection_index_context->large_posting_getter);

            auto * large_posting_stream = settings.projection_index_context->large_posting_getter(settings.path);
            chassert(large_posting_stream);

            /// Invariant: posting list data in this range is homogeneous
            if (reinterpret_cast<const PostingListData *>(vec[0])->isBitmap())
            {
                for (size_t i = offset; i < end; ++i)
                {
                    const auto * posting_list_data = reinterpret_cast<const PostingListData *>(vec[i]);
                    chassert(posting_list_data->isBitmap());
                    posting_list_data->bitmap.finish(
                        *stream,
                        large_posting_stream->plain_hashing,
                        large_posting_stream->doc_buffer,
                        large_posting_stream->packed_buffer,
                        function->index_params);
                }
            }
            else if (reinterpret_cast<const PostingListData *>(vec[0])->isWriter())
            {
                for (size_t i = offset; i < end; ++i)
                {
                    const auto * posting_list_data = reinterpret_cast<const PostingListData *>(vec[i]);
                    chassert(posting_list_data->isWriter());
                    posting_list_data->writer.finish(
                        *stream, large_posting_stream->plain_hashing, large_posting_stream->packed_buffer, function->index_params);
                }
            }
            else if (reinterpret_cast<const PostingListData *>(vec[0])->isStream())
            {
                for (size_t i = offset; i < end; ++i)
                {
                    const auto * posting_list_data = reinterpret_cast<const PostingListData *>(vec[i]);
                    chassert(posting_list_data->isStream());
                    posting_list_data->stream.write(*stream, *large_posting_stream, function->index_params);
                }
            }
        }
        settings.path.pop_back();
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
            vec.reserve(vec.size() + limit);

            size_t size_of_state = function->sizeOfData();
            size_t align_of_state = function->alignOfData();

            /// Adjust the size of state to make all states aligned in vector.
            size_t total_size_of_state = (size_of_state + align_of_state - 1) / align_of_state * align_of_state;
            char * place = arena.alignedAlloc(total_size_of_state * limit, align_of_state);

            auto large_posting_stream = settings.projection_index_context->large_posting_getter(settings.path);
            for (size_t i = 0; i < limit; ++i)
            {
                if (stream->eof())
                    break;

                new (place) PostingListData(PostingListKind::Stream);

                try
                {
                    auto & posting_list_data = reinterpret_cast<PostingListData &>(*place);
                    chassert(posting_list_data.isStream());
                    posting_list_data.stream.read(*stream, large_posting_stream, function->index_params);
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
    stream << version;
    for (const auto & parameter : parameters)
        stream << ", " << applyVisitor(FieldVisitorToString(), parameter);
    stream << ')';
    return stream.str();
}

static DataTypePtr buildPostingListType(const Array & params, const MergeTreeIndexTextParams & index_params, size_t format_version)
{
    auto function = std::make_shared<AggregateFunctionPostingList>(index_params, format_version);
    auto type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{}, Array{});
    type->setCustomization(std::make_unique<DataTypeCustomDesc>(
        std::make_unique<DataTypePostingList>(params, format_version), std::make_shared<SerializationPostingList>(function)));
    return type;
}

DataTypePtr createPostingListType(const ASTPtr & text_index_definition, size_t format_version)
{
    Array params;
    MergeTreeIndexTextParams index_params;

    if (text_index_definition && !text_index_definition->children.empty())
    {
        auto index_arguments = MergeTreeIndexText::parseArgumentsListFromAST(text_index_definition);
        params.assign_range(index_arguments);
        index_params = MergeTreeIndexText::parseTextIndexArguments("PostingList", index_arguments).first;
    }

    return buildPostingListType(params, index_params, format_version);
}

DataTypePtr createPostingListTypeFromPartMetadata(const ASTPtr & parsed_fields)
{
    if (!parsed_fields || parsed_fields->children.empty())
        return buildPostingListType({}, {}, POSTING_LIST_FORMAT_VERSION_INITIAL);

    const auto & children = parsed_fields->children;
    const auto * version_literal = children[0]->as<ASTLiteral>();
    if (!version_literal || version_literal->value.getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_DATA, "First PostingList metadata field must be format version (UInt64)");

    const size_t format_version = version_literal->value.safeGet<UInt64>();
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

    MergeTreeIndexTextParams index_params;
    if (!params.empty())
        index_params = MergeTreeIndexText::parseTextIndexArguments("PostingList", params).first;

    return buildPostingListType(params, index_params, format_version);
}

void registerDataTypePostingList(DataTypeFactory & factory)
{
    factory.registerDataType("PostingList", createPostingListTypeFromPartMetadata);
}

}
