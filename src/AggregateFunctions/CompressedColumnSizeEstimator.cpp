#include <AggregateFunctions/CompressedColumnSizeEstimator.h>

#include <algorithm>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Formats/MarkInCompressedFile.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 low_cardinality_max_dictionary_size;
    extern const SettingsBool low_cardinality_use_single_dictionary_for_part;
    extern const SettingsUInt64 max_compress_block_size;
    extern const SettingsBool merge_tree_use_v1_object_and_dynamic_serialization;
    extern const SettingsUInt64 min_compress_block_size;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool compute_exact_num_defaults_for_sparse_columns;
    extern const MergeTreeSettingsMergeTreeDynamicSerializationVersion dynamic_serialization_version;
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsFloat map_buckets_coefficient;
    extern const MergeTreeSettingsUInt64 map_buckets_min_avg_size;
    extern const MergeTreeSettingsMergeTreeMapBucketsStrategy map_buckets_strategy;
    extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version;
    extern const MergeTreeSettingsNonZeroUInt64 max_buckets_in_map;
    extern const MergeTreeSettingsUInt64 max_compress_block_size;
    extern const MergeTreeSettingsUInt64 min_compress_block_size;
    extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
    extern const MergeTreeSettingsMergeTreeObjectSerializationVersion object_serialization_version;
    extern const MergeTreeSettingsNonZeroUInt64 object_shared_data_buckets_for_wide_part;
    extern const MergeTreeSettingsMergeTreeObjectSharedDataSerializationVersion object_shared_data_serialization_version;
    extern const MergeTreeSettingsNonZeroUInt64 object_shared_data_target_chunk_rows;
    extern const MergeTreeSettingsMergeTreeObjectTypedSharedDataSerializationVersion object_typed_shared_data_serialization_version;
    extern const MergeTreeSettingsBool propagate_types_serialization_versions_to_nested_types;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
    extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
    extern const MergeTreeSettingsBool use_compact_variant_discriminators_serialization;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

CompressedColumnSizeEstimator::WriterSettings CompressedColumnSizeEstimator::WriterSettings::fromServer(const Settings * query_settings)
{
    /// The context is not kept: the function may outlive the query, as part of an `AggregateFunction(...)` data type.
    ContextPtr context;
    if (CurrentThread::isInitialized())
        context = CurrentThread::get().tryGetQueryContext();
    if (!context)
        context = Context::getGlobalContextInstance();

    static const MergeTreeSettings default_merge_tree_settings;
    static const Settings default_query_settings;
    const MergeTreeSettings & merge_tree = context ? context->getMergeTreeSettings() : default_merge_tree_settings;
    const Settings & query = query_settings ? *query_settings : default_query_settings;

    WriterSettings settings;

    /// As the serialization infos of a part are set up by `MergeTreeDataWriter` and `MergeTask`. The ratio of defaults would only
    /// gate the sparse serialization, and the kind comes from the column: a column that is stored sparse arrives sparse.
    settings.serialization_info = SerializationInfoSettings(
        /*ratio_of_defaults_for_sparse_=*/ 0.0,
        /*choose_kind_=*/ false,
        merge_tree[MergeTreeSetting::compute_exact_num_defaults_for_sparse_columns],
        merge_tree[MergeTreeSetting::serialization_info_version],
        merge_tree[MergeTreeSetting::string_serialization_version],
        merge_tree[MergeTreeSetting::nullable_serialization_version],
        merge_tree[MergeTreeSetting::map_serialization_version],
        merge_tree[MergeTreeSetting::propagate_types_serialization_versions_to_nested_types]);

    /// As `MergeTreeWriterSettings` and `MergeTreeDataPartWriterWide::getSerializationSettings`, for a merged wide part.
    const bool v1_object_and_dynamic = query[Setting::merge_tree_use_v1_object_and_dynamic_serialization];
    auto & bulk = settings.bulk;
    bulk.data_part_type = MergeTreeDataPartType::Wide;
    bulk.low_cardinality_max_dictionary_size = query[Setting::low_cardinality_max_dictionary_size];
    bulk.low_cardinality_use_single_dictionary_for_part = query[Setting::low_cardinality_use_single_dictionary_for_part];
    bulk.use_compact_variant_discriminators_serialization = merge_tree[MergeTreeSetting::use_compact_variant_discriminators_serialization];
    bulk.dynamic_serialization_version
        = v1_object_and_dynamic ? MergeTreeDynamicSerializationVersion::V1 : merge_tree[MergeTreeSetting::dynamic_serialization_version];
    bulk.object_serialization_version
        = v1_object_and_dynamic ? MergeTreeObjectSerializationVersion::V1 : merge_tree[MergeTreeSetting::object_serialization_version];
    bulk.object_shared_data_serialization_version = merge_tree[MergeTreeSetting::object_shared_data_serialization_version];
    bulk.object_shared_data_buckets = merge_tree[MergeTreeSetting::object_shared_data_buckets_for_wide_part];
    bulk.object_shared_data_target_chunk_rows = merge_tree[MergeTreeSetting::object_shared_data_target_chunk_rows];
    bulk.object_typed_shared_data_serialization_version = merge_tree[MergeTreeSetting::object_typed_shared_data_serialization_version];
    bulk.max_buckets_in_map = merge_tree[MergeTreeSetting::max_buckets_in_map];
    bulk.map_buckets_strategy = merge_tree[MergeTreeSetting::map_buckets_strategy];
    bulk.map_buckets_coefficient = static_cast<double>(merge_tree[MergeTreeSetting::map_buckets_coefficient]);
    bulk.map_buckets_min_avg_size = merge_tree[MergeTreeSetting::map_buckets_min_avg_size];
    bulk.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::SUFFIX;
    bulk.stream_mark_getter = [](const ISerialization::SubstreamPath &) { return MarkInCompressedFile(); };

    settings.index_granularity = merge_tree[MergeTreeSetting::index_granularity];
    settings.index_granularity_bytes = merge_tree[MergeTreeSetting::index_granularity_bytes];

    /// The MergeTree setting when it is set, else the setting of the query, as `MergeTreeWriterSettings` picks them.
    const UInt64 merge_tree_min = merge_tree[MergeTreeSetting::min_compress_block_size];
    const UInt64 merge_tree_max = merge_tree[MergeTreeSetting::max_compress_block_size];
    settings.min_compress_block_size
        = std::min<size_t>(merge_tree_min ? merge_tree_min : query[Setting::min_compress_block_size], MAX_COMPRESS_BLOCK_SIZE);
    settings.max_compress_block_size
        = std::min<size_t>(merge_tree_max ? merge_tree_max : query[Setting::max_compress_block_size], MAX_COMPRESS_BLOCK_SIZE);
    /// The serializations that end compressed blocks themselves (the shared data of `JSON`) do so from this size on.
    bulk.min_compress_block_size = settings.min_compress_block_size;

    return settings;
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
    type->getSerialization(settings.serialization_info)->enumerateStreams(
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
