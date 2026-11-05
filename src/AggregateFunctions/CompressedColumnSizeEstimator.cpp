#include <AggregateFunctions/CompressedColumnSizeEstimator.h>

#include <algorithm>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

CompressedColumnSizeEstimator::Writer::Writer(DataTypePtr type_, SerializationPtr serialization_, const WriterSettings & settings_)
    : type(std::move(type_))
    , serialization(std::move(serialization_))
    , settings(settings_.bulk)
{
}

CompressedColumnSizeEstimator::Writer::~Writer()
{
    /// Left over by an exception; the finished ones are gone already.
    for (auto & stream : streams)
        stream.calculator->cancel();
    if (counter)
        counter->cancel();
}

void CompressedColumnSizeEstimator::Writer::writePrefix(const IColumn & column)
{
    if (column.hasDynamicStructure())
    {
        auto first = column.cloneEmpty();
        first->takeExactDynamicStructureFrom(column);
        sample = std::move(first);
    }
    serialization->serializeBinaryBulkStatePrefix(column, settings, state);
}

WriteBuffer * CompressedColumnSizeEstimator::Writer::streamFor(const ISerialization::SubstreamPath & path)
{
    if (counter)
        return counter.get();

    const String key = ISerialization::getSubstreamsCacheKeyForStream(path);
    for (auto & stream : streams)
        if (stream.key == key)
            return stream.calculator.get();

    /// The writer has no stream for an ephemeral subcolumn either.
    if (ISerialization::isEphemeralSubcolumn(path, path.size()))
        return nullptr;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "No stream for substream {}", ISerialization::getSubcolumnNameForStream(path));
}

void CompressedColumnSizeEstimator::Writer::write(const IColumn & rows, size_t offset, size_t limit)
{
    if (sample && !rows.dynamicStructureEquals(*sample))
    {
        auto rebuilt = type->createColumn();
        rebuilt->takeExactDynamicStructureFrom(*sample);
        rebuilt->insertRangeFrom(rows, offset, limit);
        serialization->serializeBinaryBulkWithMultipleStreams(*rebuilt, 0, limit, settings, state);
    }
    else
        serialization->serializeBinaryBulkWithMultipleStreams(rows, offset, limit, settings, state);
}

CompressedColumnSizeEstimator::Sizes CompressedColumnSizeEstimator::Writer::finish()
{
    chassert(!finished);
    finished = true;

    serialization->serializeBinaryBulkStateSuffix(settings, state);
    state.reset();

    Sizes sizes;
    if (counter)
    {
        counter->finalize();
        sizes.uncompressed = counter->count();
        counter.reset();
        return sizes;
    }

    for (auto & stream : streams)
    {
        stream.calculator->finalize();
        sizes.uncompressed += stream.calculator->getUncompressedBytes();
        sizes.compressed += stream.calculator->getCompressedBytes();
    }
    streams.clear();
    return sizes;
}

CompressedColumnSizeEstimator::CompressedColumnSizeEstimator(
    DataTypePtr type_, ASTPtr codec_description_, CompressionCodecPtr default_codec_, WriterSettings settings_)
    : type(std::move(type_))
    , codec_description(std::move(codec_description_))
    , default_codec(std::move(default_codec_))
    , settings(std::move(settings_))
{
    type->getDefaultSerialization()->enumerateStreams(
        [&](const ISerialization::SubstreamPath & path)
        {
            if (!ISerialization::isEphemeralSubcolumn(path, path.size()))
                stream_layouts.emplace(ISerialization::getSubstreamsCacheKeyForStream(path), resolveStreamLayout(path));
        },
        type);
}

CompressedColumnSizeEstimator::StreamLayout CompressedColumnSizeEstimator::resolveStreamLayout(const ISerialization::SubstreamPath & path) const
{
    /// As `MergeTreeDataPartWriterOnDisk::getSubstreamCodec` and `MergeTreeDataPartWriterWide::addStreams`: the codec applies to the
    /// data substreams, a structural substream keeps only its generic codecs, and a compressed block holds whole values.
    const DataTypePtr & substream_type = path.back().data.type;
    const bool is_data_substream = ISerialization::isSpecialCompressionAllowed(path);

    StreamLayout layout;
    layout.codec = CompressionCodecFactory::instance().get(
        codec_description, is_data_substream ? substream_type.get() : nullptr, default_codec, /*only_generic=*/ !is_data_substream);
    layout.block_size = substream_type
        ? roundCompressBlockSizeToWholeValues(settings.max_compress_block_size, *substream_type)
        : settings.max_compress_block_size;
    return layout;
}

size_t CompressedColumnSizeEstimator::granuleRows(const IColumn & column) const
{
    /// The writer counts the bytes of all columns of the table, so the granules of a table with wide rows are smaller.
    if (settings.index_granularity_bytes == 0)
        return settings.index_granularity;

    const size_t bytes_per_row = std::max<size_t>(column.byteSize() / std::max<size_t>(column.size(), 1), 1);
    return std::clamp<size_t>(settings.index_granularity_bytes / bytes_per_row, 1, settings.index_granularity);
}

std::unique_ptr<CompressedColumnSizeEstimator::Writer> CompressedColumnSizeEstimator::startWriter(const IColumn & column) const
{
    auto serialization = type->getSerialization(*type->getSerializationInfo(column, settings.serialization_info));
    std::unique_ptr<Writer> writer(new Writer(type, serialization, settings));

    Writer * raw = writer.get();
    serialization->enumerateStreams(
        [&](const ISerialization::SubstreamPath & path)
        {
            if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                return;

            const String key = ISerialization::getSubstreamsCacheKeyForStream(path);
            auto it = stream_layouts.find(key);
            const StreamLayout layout = it != stream_layouts.end() ? it->second : resolveStreamLayout(path);
            raw->streams.push_back({key, std::make_unique<CompressedSizeCalculator>(layout.codec, layout.block_size)});
        },
        type,
        column.getPtr());
    writer->settings.getter = [raw](const ISerialization::SubstreamPath & path) { return raw->streamFor(path); };
    writer->writePrefix(column);
    return writer;
}

std::unique_ptr<CompressedColumnSizeEstimator::Writer>
CompressedColumnSizeEstimator::startCounting(const DataTypePtr & type, const WriterSettings & settings, const IColumn & column)
{
    auto serialization = type->getSerialization(*type->getSerializationInfo(column, settings.serialization_info));
    std::unique_ptr<Writer> writer(new Writer(type, serialization, settings));
    writer->counter = std::make_unique<NullWriteBuffer>();
    writer->settings.getter = [raw = writer.get()](const ISerialization::SubstreamPath & path) { return raw->streamFor(path); };
    writer->writePrefix(column);
    return writer;
}

}
