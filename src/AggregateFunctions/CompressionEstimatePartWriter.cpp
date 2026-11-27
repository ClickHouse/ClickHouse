#include <AggregateFunctions/CompressionEstimatePartWriter.h>

#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/Exception.h>


namespace DB
{

namespace Setting
{
extern const SettingsUInt64 low_cardinality_max_dictionary_size;
extern const SettingsBool low_cardinality_use_single_dictionary_for_part;
extern const SettingsUInt64 max_compress_block_size;
extern const SettingsUInt64 min_compress_block_size;
}

namespace MergeTreeSetting
{
extern const MergeTreeSettingsBool compute_exact_num_defaults_for_sparse_columns;
extern const MergeTreeSettingsUInt64 index_granularity;
extern const MergeTreeSettingsUInt64 index_granularity_bytes;
extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version_for_zero_level_parts;
extern const MergeTreeSettingsUInt64 max_compress_block_size;
extern const MergeTreeSettingsUInt64 min_compress_block_size;
extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
extern const MergeTreeSettingsBool propagate_types_serialization_versions_to_nested_types;
extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
extern const MergeTreeSettingsBool use_compact_variant_discriminators_serialization;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace
{

/// The defaults of `MergeTreeSettings`, and of `Settings` where the writer takes a setting from the query.
struct MergeTreeDefaults
{
    size_t granule_rows = 0;
    size_t granule_bytes = 0;
    size_t min_compress_block_size = 0;
    size_t max_compress_block_size = 0;
    /// As `MergeTreeDataWriter` chooses the serialization of a new part.
    SerializationInfoSettings info_settings;
    /// As `MergeTreeWriterSettings`, for the types estimated here.
    ISerialization::SerializeBinaryBulkSettings bulk_settings;
};

const MergeTreeDefaults & mergeTreeDefaults()
{
    static const MergeTreeDefaults defaults = []
    {
        const MergeTreeSettings table;
        const Settings query;
        MergeTreeDefaults result;
        result.granule_rows = table[MergeTreeSetting::index_granularity];
        result.granule_bytes = table[MergeTreeSetting::index_granularity_bytes];
        /// The setting of the table if it is not 0, otherwise the one of the query.
        result.min_compress_block_size = table[MergeTreeSetting::min_compress_block_size]
            ? table[MergeTreeSetting::min_compress_block_size] : query[Setting::min_compress_block_size];
        result.max_compress_block_size = table[MergeTreeSetting::max_compress_block_size]
            ? table[MergeTreeSetting::max_compress_block_size] : query[Setting::max_compress_block_size];
        result.info_settings = SerializationInfoSettings(
            static_cast<double>(table[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization]),
            /*choose_kind_=*/true,
            table[MergeTreeSetting::compute_exact_num_defaults_for_sparse_columns],
            table[MergeTreeSetting::serialization_info_version],
            table[MergeTreeSetting::string_serialization_version],
            table[MergeTreeSetting::nullable_serialization_version],
            table[MergeTreeSetting::map_serialization_version_for_zero_level_parts],
            table[MergeTreeSetting::propagate_types_serialization_versions_to_nested_types]);
        result.bulk_settings.low_cardinality_max_dictionary_size = query[Setting::low_cardinality_max_dictionary_size];
        result.bulk_settings.low_cardinality_use_single_dictionary_for_part = query[Setting::low_cardinality_use_single_dictionary_for_part];
        result.bulk_settings.use_compact_variant_discriminators_serialization
            = table[MergeTreeSetting::use_compact_variant_discriminators_serialization];
        return result;
    }();
    return defaults;
}

/// The file name of a substream; substreams with the same name share a stream.
String streamName(const ISerialization::SubstreamPath & path)
{
    return ISerialization::getFileNameForStream("c", path, ISerialization::StreamFileNameSettings{});
}

/// Calls `callback(name, path)` for each substream that is written (not ephemeral).
template <typename Callback>
void forEachStream(const ISerialization & serialization, const ISerialization::SubstreamData & data, Callback && callback)
{
    ISerialization::EnumerateStreamsSettings settings;
    serialization.enumerateStreams(
        settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            if (!ISerialization::isEphemeralSubcolumn(path, path.size()))
                callback(streamName(path), path);
        },
        data);
}

}

CompressionEstimatePartWriter::Layout::Layout(DataTypePtr target_type_, ASTPtr codec_description_, std::optional<UInt64> block_size_bytes)
    : target_type(std::move(target_type_))
    , codec_description(std::move(codec_description_))
    , granule_rows(mergeTreeDefaults().granule_rows)
    , granule_bytes(mergeTreeDefaults().granule_bytes)
    , min_compress_block_size(block_size_bytes.value_or(mergeTreeDefaults().min_compress_block_size))
    , max_compress_block_size(block_size_bytes.value_or(mergeTreeDefaults().max_compress_block_size))
{
    auto dense_serialization = target_type->getSerialization(mergeTreeDefaults().info_settings);
    forEachStream(
        *dense_serialization,
        ISerialization::SubstreamData(dense_serialization).withType(target_type),
        [&](String name, const ISerialization::SubstreamPath & path)
        {
            if (!streams.contains(name))
                streams.emplace(std::move(name), resolveStream(path));
        });
}

/// As `MergeTreeDataPartWriterOnDisk::getSubstreamCodec`: only the generic codecs on structural substreams (sizes, null maps,
/// indexes). Blocks hold whole values, as in `MergeTreeDataPartWriterWide::addStreams`.
CompressionEstimatePartWriter::Layout::Stream
CompressionEstimatePartWriter::Layout::resolveStream(const ISerialization::SubstreamPath & path) const
{
    const auto & substream_type = path.back().data.type;
    const bool is_data_substream = ISerialization::isSpecialCompressionAllowed(path);
    auto codec = CompressionCodecFactory::instance().get(
        codec_description, is_data_substream ? substream_type.get() : nullptr, nullptr, /*only_generic=*/!is_data_substream);
    if (codec->needsVectorDimensionUpfront())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Aggregate function estimateCompressionRatio with a target type does not support codec {}",
            codec->getFullCodecDescription()->formatForErrorMessage());

    const size_t block_size
        = substream_type ? roundCompressBlockSizeToWholeValues(max_compress_block_size, *substream_type) : max_compress_block_size;
    return {std::move(codec), block_size};
}

CompressionEstimatePartWriter::CompressionEstimatePartWriter(const Layout & layout, const IColumn & first_rows)
    : granule_rows_limit(layout.granule_rows)
    , granule_bytes_limit(layout.granule_bytes)
    , min_compress_block_size(layout.min_compress_block_size)
{
    const auto & target_type = layout.target_type;
    auto info = target_type->createSerializationInfo(mergeTreeDefaults().info_settings);
    info->add(first_rows);
    serialization = target_type->getSerialization(*info);

    /// A sparse serialization has a stream that the layout has not: the offsets of the values.
    forEachStream(
        *serialization,
        ISerialization::SubstreamData(serialization).withType(target_type).withColumn(first_rows.getPtr()),
        [&](String name, const ISerialization::SubstreamPath & path)
        {
            if (streams.contains(name))
                return;
            auto it = layout.streams.find(name);
            auto stream = it != layout.streams.end() ? it->second : layout.resolveStream(path);
            streams.emplace(
                std::move(name),
                std::make_unique<CompressedSizeCalculator>(stream.codec, stream.block_size, DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE));
        });

    settings = mergeTreeDefaults().bulk_settings;
    settings.getter = [this](const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        if (ISerialization::isEphemeralSubcolumn(path, path.size()))
            return nullptr;
        auto it = streams.find(streamName(path));
        if (it == streams.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No stream {} in the estimate of a part", streamName(path));
        return it->second.get();
    };
    serialization->serializeBinaryBulkStatePrefix(first_rows, settings, state);
}

CompressionEstimatePartWriter::~CompressionEstimatePartWriter()
{
    for (auto & [_, stream] : streams)
        stream->cancel();
}

void CompressionEstimatePartWriter::write(const IColumn & rows)
{
    chassert(rows.size() <= rowsLeftInGranule());
    for (size_t offset = 0; offset < rows.size();)
    {
        const auto [write_rows, write_bytes] = rowsToGranuleEnd(rows, offset);
        chassert(write_rows > 0);

        /// As `MergeTreeDataPartWriterWide::getCurrentMarksForColumn`: before a granule, a stream that holds
        /// `min_compress_block_size` bytes ends its block.
        if (granule_rows == 0)
            for (auto & [_, stream] : streams)
                if (stream->offset() >= min_compress_block_size)
                    stream->endBlock();
        serialization->serializeBinaryBulkWithMultipleStreams(rows, offset, write_rows, settings, state);

        const bool ends_granule
            = granule_rows + write_rows >= granule_rows_limit || granule_bytes + write_bytes >= granule_bytes_limit;
        granule_rows = ends_granule ? 0 : granule_rows + write_rows;
        granule_bytes = ends_granule ? 0 : granule_bytes + write_bytes;
        offset += write_rows;
    }
}

/// The rows from `offset` that the open granule takes, and their bytes. Approximates `computeIndexGranularity`, which sizes
/// granules per insert block from all columns. `byteSizeAt` of `LowCardinality` counts each row's value, not the dictionary.
std::pair<size_t, size_t> CompressionEstimatePartWriter::rowsToGranuleEnd(const IColumn & rows, size_t offset) const
{
    if (offset == 0)
    {
        const size_t bytes = rows.byteSize();
        if (granule_bytes + bytes < granule_bytes_limit)
            return {rows.size(), bytes};
    }

    size_t end = offset;
    size_t bytes = 0;
    while (end < rows.size() && granule_bytes + bytes < granule_bytes_limit)
        bytes += rows.byteSizeAt(end++);
    return {end - offset, bytes};
}

UInt64 CompressionEstimatePartWriter::finish()
{
    serialization->serializeBinaryBulkStateSuffix(settings, state);
    UInt64 bytes = 0;
    for (auto & [_, stream] : streams)
    {
        stream->finalize();
        bytes += stream->getCompressedBytes();
    }
    return bytes;
}

}
