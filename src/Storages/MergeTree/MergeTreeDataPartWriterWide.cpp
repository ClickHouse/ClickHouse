#include <algorithm>
#include <set>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <Disks/WriteMode.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MapWithKeyColumnsMerge.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/ParallelSyncFiles.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Logger.h>
#include <Common/SipHash.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/FailPoint.h>
#include <IO/NullWriteBuffer.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 map_max_key_columns;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_FILE_NAME;
    extern const int FAULT_INJECTED;
    extern const int NOT_IMPLEMENTED;
    extern const int LIMIT_EXCEEDED;
}

namespace FailPoints
{
    extern const char wide_part_writer_fail_in_add_streams[];
}

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace
{

/// Get granules for block using index_granularity
Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, size_t rows_written_in_last_mark)
{
    if (current_mark >= index_granularity.getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Request to get granules from mark {} but index granularity size is {}",
                        current_mark, index_granularity.getMarksCount());

    Granules result;
    size_t current_row = 0;

    /// When our last mark is not finished yet and we have to write rows into it
    if (rows_written_in_last_mark > 0)
    {
        size_t rows_left_in_last_mark = index_granularity.getMarkRows(current_mark) - rows_written_in_last_mark;
        size_t rows_left_in_block = block_rows - current_row;
        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, rows_left_in_last_mark),
            .mark_number = current_mark,
            .mark_on_start = false, /// Don't mark this granule because we have already marked it
            .is_complete = (rows_left_in_block >= rows_left_in_last_mark),
        });
        current_row += result.back().rows_to_write;
        ++current_mark;
    }

    /// Calculating normal granules for block
    while (current_row < block_rows)
    {
        size_t expected_rows_in_mark = index_granularity.getMarkRows(current_mark);
        size_t rows_left_in_block  = block_rows - current_row;
        /// If we have less rows in block than expected in granularity
        /// save incomplete granule
        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, expected_rows_in_mark),
            .mark_number = current_mark,
            .mark_on_start = true,
            .is_complete = (rows_left_in_block >= expected_rows_in_mark),
        });
        current_row += result.back().rows_to_write;
        ++current_mark;
    }

    return result;
}

/// `rows_to_read` is an untrusted per-mark granularity and may be arbitrarily large, so read in
/// bounded batches rather than letting deserializeBinaryBulk resize the column to it up front.
ColumnPtr readColumnForValidation(const ISerialization & serialization, const IDataType & type, ReadBuffer & istr, size_t rows_to_read)
{
    static constexpr size_t max_rows_per_batch = 1ULL << 20;

    auto column = type.createColumn();
    size_t rows_left = rows_to_read;
    while (rows_left > 0 && !istr.eof())
    {
        size_t batch = std::min(rows_left, max_rows_per_batch);
        size_t size_before = column->size();
        serialization.deserializeBinaryBulk(*column, istr, batch, 0.0);
        rows_left -= batch;
        if (column->size() - size_before < batch) /// reached EOF mid-batch
            break;
    }
    return column;
}

}

MergeTreeDataPartWriterWide::MergeTreeDataPartWriterWide(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    MergeTreeIndexGranularityPtr index_granularity_,
    WrittenOffsetSubstreams * written_offset_substreams_)
    : MergeTreeDataPartWriterOnDisk(
            data_part_name_, logger_name_, serializations_,
            data_part_storage_, index_granularity_info_, storage_settings_,
            columns_list_, metadata_snapshot_,
            indices_to_recalc_, marks_file_extension_,
            default_codec_, settings_, std::move(index_granularity_),
            written_offset_substreams_)
{
    if (settings.save_marks_in_cache)
    {
        auto columns_vec = getColumnsToPrewarmMarks(*storage_settings, columns_list);
        columns_to_load_marks = NameSet(columns_vec.begin(), columns_vec.end());
    }
}

ISerialization::EnumerateStreamsSettings MergeTreeDataPartWriterWide::getEnumerateSettings(const MergeTreeWriterSettings & settings_)
{
    ISerialization::EnumerateStreamsSettings enumerate_settings;
    enumerate_settings.object_serialization_version = settings_.object_serialization_version;
    enumerate_settings.object_shared_data_serialization_version = settings_.object_shared_data_serialization_version;
    enumerate_settings.object_shared_data_buckets = settings_.object_shared_data_buckets;
    enumerate_settings.object_shared_data_target_chunk_rows = settings_.object_shared_data_target_chunk_rows;
    enumerate_settings.max_buckets_in_map = settings_.max_buckets_in_map;
    enumerate_settings.map_buckets_strategy = settings_.map_buckets_strategy;
    enumerate_settings.map_buckets_coefficient = settings_.map_buckets_coefficient;
    enumerate_settings.map_buckets_min_avg_size = settings_.map_buckets_min_avg_size;
    enumerate_settings.data_part_type = MergeTreeDataPartType::Wide;
    return enumerate_settings;
}

void MergeTreeDataPartWriterWide::initStreamsAndSubstreamsIfNeeded()
{
    initColumnsSubstreamsIfNeeded();
    initStreamsToOpenCount();
    initStreamsIfNeeded();

    chassert(column_streams.size() == *streams_to_open_in_part);
}

std::optional<String> MergeTreeDataPartWriterWide::newStreamNameForPath(
    const NameAndTypePair & name_and_type,
    const ISerialization::SubstreamPath & substream_path) const
{
    if (substream_path.empty() || ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
        return std::nullopt;

    auto full_stream_name = ISerialization::getFileNameForStream(
        name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));
    String stream_name = replaceFileNameToHashIfNeeded(full_stream_name, *storage_settings, data_part_storage.get());

    if (column_streams.contains(stream_name))
        return std::nullopt;

    /// Same skip as addStreamForPath: a Nested offset already written by another column is not opened here.
    if (written_offset_substreams
        && substream_path.back().type == ISerialization::Substream::ArraySizes
        && written_offset_substreams->contains(stream_name))
    {
        return std::nullopt;
    }

    return stream_name;
}

void MergeTreeDataPartWriterWide::initStreamsToOpenCount()
{
    if (streams_to_open_in_part)
        return;

    NameSet stream_names;
    size_t column_position = 0;
    for (const auto & name_and_type : columns_list)
    {
        auto serialization = getSerialization(name_and_type.name);
        if (const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get()))
        {
            /// The dry-run inventory is a placeholder (or a partial LowCardinality prefix). The handles
            /// opened for this column are its template streams; per-key streams are counted when registered.
            auto data = ISerialization::SubstreamData(serialization)
                .withType(name_and_type.type)
                .withColumn(block_sample.getByName(name_and_type.name).column);
            auto enumerate_settings = getEnumerateSettings(settings);
            per_key->enumerateTemplateStreams(
                enumerate_settings,
                [&](const ISerialization::SubstreamPath & substream_path)
                {
                    if (auto stream_name = newStreamNameForPath(name_and_type, substream_path))
                        stream_names.insert(std::move(*stream_name));
                },
                data);
        }
        else
        {
            for (const auto & full_stream_name : columns_substreams.getColumnSubstreams(column_position))
            {
                String stream_name = replaceFileNameToHashIfNeeded(full_stream_name, *storage_settings, data_part_storage.get());
                /// Skip offset streams that has been written before (not using this object)
                if (written_offset_substreams && written_offset_substreams->contains(stream_name))
                    continue;
                stream_names.insert(std::move(stream_name));
            }
        }
        ++column_position;
    }

    streams_to_open_in_part = stream_names.size();
}

void MergeTreeDataPartWriterWide::addStreamForPath(
    const NameAndTypePair & name_and_type,
    const ASTPtr & effective_codec_desc,
    const ISerialization::SubstreamPath & substream_path,
    WriteMode write_mode)
{
    chassert(!substream_path.empty());

    /// Don't create streams for ephemeral subcolumns that don't store any real data.
    if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
        return;

    const bool column_uses_default_codec = columnUsesDefaultCodec(name_and_type.getNameInStorage());

    auto full_stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));

    String stream_name = replaceFileNameToHashIfNeeded(full_stream_name, *storage_settings, data_part_storage.get());

    bool is_template_stream = false;
    for (const auto & item : substream_path)
    {
        if (item.type == ISerialization::Substream::MapKeyValueTemplate
            || item.type == ISerialization::Substream::MapKeyExistsTemplate)
        {
            is_template_stream = true;
            break;
        }
    }

    /// Shared offsets for Nested type.
    if (column_streams.contains(stream_name))
    {
        if (is_template_stream)
            per_key_template_stream_names.insert(stream_name);
        return;
    }

    /// Don't write offsets more than one time for Nested type in case elements of nested had been written separately, i.e. via Vertical merge.
    if (written_offset_substreams)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets && written_offset_substreams->contains(stream_name))
            return;
    }

    auto it = stream_name_to_full_name.find(stream_name);
    if (it != stream_name_to_full_name.end() && it->second != full_stream_name)
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME,
            "Stream with name {} already created (full stream name: {}). Current full stream name: {}."
            " It is a collision between a filename for one column and a hash of filename for another column or a bug",
            stream_name, it->second, full_stream_name);

    auto compression_codec = getSubstreamCodec(effective_codec_desc, substream_path, column_uses_default_codec);

    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + Poco::toUpper(settings.marks_compression_codec) + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    CompressionCodecPtr marks_compression_codec = CompressionCodecFactory::instance().get(ast, nullptr);

    const auto column_desc = metadata_snapshot->columns.tryGetColumnDescription(GetColumnsOptions(GetColumnsOptions::AllPhysical), name_and_type.getNameInStorage());

    UInt64 max_compress_block_size = 0;
    if (column_desc)
        if (const auto * value = column_desc->settings.tryGet("max_compress_block_size"))
            max_compress_block_size = value->safeGet<UInt64>();
    if (!max_compress_block_size)
        max_compress_block_size = settings.max_compress_block_size;
    /// Clamp to prevent absurd memory allocations from fuzzed or misconfigured column settings.
    max_compress_block_size = std::min<UInt64>(max_compress_block_size, MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE);

    /// A write buffer is allocated per stream below, and a single column can own thousands of
    /// streams (a Map with many buckets, a deeply nested Array or Tuple), so the threshold is
    /// compared against streams rather than columns.
    chassert(streams_to_open_in_part.has_value());
    WriteSettings query_write_settings = settings.query_write_settings;
    query_write_settings.use_adaptive_write_buffer =
        (settings.min_columns_to_activate_adaptive_write_buffer && *streams_to_open_in_part >= settings.min_columns_to_activate_adaptive_write_buffer)
        || (settings.use_adaptive_write_buffer_for_dynamic_subcolumns && ISerialization::isDynamicSubcolumn(substream_path, substream_path.size()));
    query_write_settings.adaptive_write_buffer_initial_size = settings.adaptive_write_buffer_initial_size;

    fiu_do_on(FailPoints::wide_part_writer_fail_in_add_streams,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure in Wide part writer addStreams");
    });

    column_streams.emplace(stream_name, std::make_unique<MergeTreeWriterStream>(
        stream_name,
        data_part_storage,
        stream_name,
        DATA_FILE_EXTENSION,
        stream_name,
        marks_file_extension,
        compression_codec,
        max_compress_block_size,
        marks_compression_codec,
        settings.marks_compress_block_size,
        query_write_settings,
        SizeAdaptivePacking{},
        write_mode));

    if (columns_to_load_marks.contains(name_and_type.name))
        cached_marks.emplace(stream_name, std::make_unique<MarksInCompressedFile::PlainArray>());

    full_name_to_stream_name.emplace(full_stream_name, stream_name);
    stream_name_to_full_name.emplace(stream_name, full_stream_name);

    if (is_template_stream)
        per_key_template_stream_names.insert(stream_name);
}

void MergeTreeDataPartWriterWide::addStreams(
    const NameAndTypePair & name_and_type,
    const ASTPtr & effective_codec_desc)
{
    ISerialization::StreamCallback callback = [&](const auto & substream_path)
    {
        addStreamForPath(name_and_type, effective_codec_desc, substream_path, WriteMode::Rewrite);
    };

    auto serialization = getSerialization(name_and_type.name);
    auto data = ISerialization::SubstreamData(serialization).withType(name_and_type.type).withColumn(block_sample.getByName(name_and_type.name).column);
    auto enumerate_settings = getEnumerateSettings(settings);
    /// Key files are created when the key is registered (`ensureMapKeyColumnsStreams`), not from the
    /// sample column. Opening them here would leave a `Rewrite` handle that a later template copy
    /// cannot retarget.
    if (const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get()))
    {
        per_key->enumerateTemplateStreams(enumerate_settings, callback, data);
        return;
    }
    serialization->enumerateStreams(enumerate_settings, callback, data);
}

bool MergeTreeDataPartWriterWide::isMapKeyColumnsTemplateStreamName(const String & stream_name) const
{
    return per_key_template_stream_names.contains(stream_name);
}

void MergeTreeDataPartWriterWide::updateMapKeyColumnsSampleColumn(const NameAndTypePair & name_and_type, const std::vector<Field> & keys)
{
    const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(getSerialization(name_and_type.name).get());
    if (!per_key)
        return;

    auto keys_column = per_key->getKeyType()->createColumn();
    auto values_column = per_key->getValueType()->createColumn();
    keys_column->reserve(keys.size());
    values_column->reserve(keys.size());
    for (const auto & key : keys)
    {
        keys_column->insert(key);
        values_column->insertDefault();
    }

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert(keys.size());
    block_sample.getByName(name_and_type.name).column = ColumnMap::create(
        std::move(keys_column), std::move(values_column), std::move(offsets));
}

void MergeTreeDataPartWriterWide::enumerateWrittenStreams(
    const NameAndTypePair & name_and_type,
    const ISerialization::StreamCallback & callback,
    bool with_template_streams) const
{
    auto serialization = getSerialization(name_and_type.name);
    auto data = ISerialization::SubstreamData(serialization)
        .withType(name_and_type.type)
        .withColumn(block_sample.getByName(name_and_type.name).column);
    auto enumerate_settings = getEnumerateSettings(settings);

    const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get());
    if (!per_key)
    {
        serialization->enumerateStreams(enumerate_settings, callback, data);
        return;
    }

    /// Keys live in the serialize state; going through the sample column would work too, but the
    /// state is the authoritative registered set.
    auto state_it = serialization_states.find(name_and_type.name);
    per_key->enumerateRegisteredKeyStreams(
        enumerate_settings, callback, data, state_it == serialization_states.end() ? nullptr : state_it->second);

    if (with_template_streams)
        per_key->enumerateTemplateStreams(enumerate_settings, callback, data);
}

void MergeTreeDataPartWriterWide::ensureMapKeyColumnsStreams(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    ISerialization::SerializeBinaryBulkStatePtr & state)
{
    const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(getSerialization(name_and_type.name).get());
    if (!per_key)
        return;

    auto effective_codec_desc = getCodecDescriptionOrDefault(name_and_type.name, default_codec);
    auto data = ISerialization::SubstreamData(getSerialization(name_and_type.name))
        .withType(name_and_type.type)
        .withColumn(block_sample.getByName(name_and_type.name).column);
    auto enumerate_settings = getEnumerateSettings(settings);

    if (!map_key_columns_has_history.contains(name_and_type.name))
    {
        per_key->enumerateTemplateStreams(
            enumerate_settings,
            [&](const ISerialization::SubstreamPath & substream_path)
            {
                addStreamForPath(name_and_type, effective_codec_desc, substream_path, WriteMode::Rewrite);
            },
            data);
        map_key_columns_has_history[name_and_type.name] = false;
    }

    if (!state)
        return;

    const bool has_history = map_key_columns_has_history[name_and_type.name];

    /// Before the part has history, a merge-stamped statistics key set is the full union and must
    /// be registered now, so every key is written from row 0. Inserts do not stamp that set; they
    /// still discover keys from the block, and keys that appear after history copy the template.
    std::vector<Field> new_keys;
    std::set<Field> seen;
    for (const auto & existing : per_key->getRegisteredKeys(*state))
        seen.insert(existing);

    if (!has_history)
    {
        if (const auto * column_map = typeid_cast<const ColumnMap *>(&column))
        {
            if (const auto & stats = column_map->getStatistics(); stats && stats->collect_keys)
            {
                for (const auto & key : stats->keys)
                {
                    if (seen.insert(key).second)
                        new_keys.push_back(key);
                }
            }
        }
    }

    auto data_keys = per_key->collectNewKeys(column, *state);
    for (auto & key : data_keys)
    {
        if (seen.insert(key).second)
            new_keys.push_back(std::move(key));
    }

    if (new_keys.empty())
    {
        updateMapKeyColumnsSampleColumn(name_and_type, per_key->getRegisteredKeys(*state));
        return;
    }

    const UInt64 max_keys = (*storage_settings)[MergeTreeSetting::map_max_key_columns];
    if (max_keys)
    {
        const size_t total_keys = per_key->getRegisteredKeyCount(*state) + new_keys.size();
        if (total_keys > max_keys)
            throw Exception(
                ErrorCodes::LIMIT_EXCEEDED,
                "Number of distinct keys in Map column {} is {}, exceeds map_max_key_columns ({})",
                backQuoteIfNeed(name_and_type.name),
                total_keys,
                max_keys);
    }

    if (has_history && data_part_storage->isStoredOnRemoteDisk())
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Copying a with_key_columns Map template stream is not supported on object storage");
    }

    /// A late key copies both templates as its history prefix, but the value template is written
    /// with the value type's default serialization. A value type that contains LowCardinality has
    /// a shared dictionary the copy would not reproduce, so refuse it (matching the reference).
    bool value_contains_low_cardinality = per_key->getValueType()->lowCardinality();
    per_key->getValueType()->forEachChild([&](const IDataType & child)
    {
        value_contains_low_cardinality |= child.lowCardinality();
    });
    if (has_history && value_contains_low_cardinality)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Creating a new LowCardinality key in Map column {} after the first written block is not supported "
            "when map_serialization_version = 'with_key_columns'",
            backQuoteIfNeed(name_and_type.name));
    }

    std::vector<ISerialization::SubstreamPath> template_paths;
    per_key->enumerateTemplateStreams(
        enumerate_settings,
        [&](const ISerialization::SubstreamPath & substream_path)
        {
            if (!ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                template_paths.push_back(substream_path);
        },
        data);

    if (has_history)
    {
        for (const auto & template_path : template_paths)
        {
            auto template_full = ISerialization::getFileNameForStream(
                name_and_type, template_path, ISerialization::StreamFileNameSettings(*storage_settings));
            String template_stream = replaceFileNameToHashIfNeeded(template_full, *storage_settings, data_part_storage.get());
            column_streams.at(template_stream)->sync();
        }
    }

    /// Map the template substream path to this key's path: the front element becomes
    /// the per-key value or exists substream, keeping the rest of the path.
    auto keyPathFromTemplate = [&](const ISerialization::SubstreamPath & template_path, const Field & key)
    {
        auto key_path = template_path;
        if (key_path.front().type == ISerialization::Substream::MapKeyValueTemplate)
            key_path.front().type = ISerialization::Substream::MapKey;
        else
            key_path.front().type = ISerialization::Substream::MapKeyExists;
        key_path.front().name_of_substream = per_key->getKeySubcolumnName(key);
        return key_path;
    };

    /// Publish the widened total before opening handles. addStreamForPath reads it for the adaptive
    /// write buffer, and the next initStreamsAndSubstreamsIfNeeded asserts it matches column_streams.
    {
        NameSet incoming;
        for (const auto & key : new_keys)
        {
            if (has_history)
            {
                for (const auto & template_path : template_paths)
                {
                    if (auto stream_name = newStreamNameForPath(name_and_type, keyPathFromTemplate(template_path, key)))
                        incoming.insert(std::move(*stream_name));
                }
            }
            else
            {
                per_key->enumerateKeyStreams(
                    enumerate_settings,
                    [&](const ISerialization::SubstreamPath & substream_path)
                    {
                        if (auto stream_name = newStreamNameForPath(name_and_type, substream_path))
                            incoming.insert(std::move(*stream_name));
                    },
                    data,
                    key);
            }
        }
        chassert(streams_to_open_in_part.has_value());
        chassert(column_streams.size() == *streams_to_open_in_part);
        streams_to_open_in_part = *streams_to_open_in_part + incoming.size();
    }

    for (const auto & key : new_keys)
    {
        if (has_history)
        {
            for (const auto & template_path : template_paths)
            {
                auto key_path = keyPathFromTemplate(template_path, key);

                auto template_full = ISerialization::getFileNameForStream(
                    name_and_type, template_path, ISerialization::StreamFileNameSettings(*storage_settings));
                auto key_full = ISerialization::getFileNameForStream(
                    name_and_type, key_path, ISerialization::StreamFileNameSettings(*storage_settings));
                String template_stream = replaceFileNameToHashIfNeeded(template_full, *storage_settings, data_part_storage.get());
                String key_stream = replaceFileNameToHashIfNeeded(key_full, *storage_settings, data_part_storage.get());

                data_part_storage->copyFileFrom(*data_part_storage, template_stream + DATA_FILE_EXTENSION, key_stream + DATA_FILE_EXTENSION);
                data_part_storage->copyFileFrom(*data_part_storage, template_stream + marks_file_extension, key_stream + marks_file_extension);

                addStreamForPath(name_and_type, effective_codec_desc, key_path, WriteMode::Append);
                column_streams.at(template_stream)->deepCopyTo(*column_streams.at(key_stream));

                if (auto cached = cached_marks.find(template_stream); cached != cached_marks.end())
                    cached_marks.at(key_stream)->assign(*cached->second);

                if (auto pending = last_non_written_marks.find(name_and_type.name); pending != last_non_written_marks.end())
                {
                    auto & marks = pending->second;
                    auto template_mark = std::ranges::find(marks, template_stream, &StreamNameAndMark::stream_name);
                    if (template_mark == marks.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "No pending mark for Map template stream {}", template_stream);

                    /// The copied data starts at the same granule boundary as the template, not at the current write position.
                    StreamNameAndMark key_mark{key_stream, template_mark->mark};
                    marks.push_back(std::move(key_mark));
                }
            }
        }
        else
        {
            per_key->enumerateKeyStreams(
                enumerate_settings,
                [&](const ISerialization::SubstreamPath & substream_path)
                {
                    addStreamForPath(name_and_type, effective_codec_desc, substream_path, WriteMode::Rewrite);
                },
                data,
                key);
        }
    }

    chassert(column_streams.size() == *streams_to_open_in_part);

    per_key->addKeys(state, new_keys);
    if (has_history)
        per_key->markKeysCopiedFromTemplate(state, new_keys);
    updateMapKeyColumnsSampleColumn(name_and_type, per_key->getRegisteredKeys(*state));
}

const String & MergeTreeDataPartWriterWide::getStreamName(
    const NameAndTypePair & column,
    const ISerialization::SubstreamPath & substream_path) const
{
    auto full_stream_name = ISerialization::getFileNameForStream(column, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));
    String stream_name = replaceFileNameToHashIfNeeded(full_stream_name, *storage_settings, data_part_storage.get());

    if (written_offset_substreams)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        /// If it has been written already return an empty string placeholder, to avoid writing it again.
        if (is_offsets && written_offset_substreams->contains(stream_name))
            return already_written_stream_holder;
    }

    auto it = full_name_to_stream_name.find(full_stream_name);
    if (it == full_name_to_stream_name.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stream {} not found", full_stream_name);

    return it->second;
}

ISerialization::OutputStreamGetter MergeTreeDataPartWriterWide::createStreamGetter(const NameAndTypePair & column,
    const WrittenOffsetSubstreams & offset_substreams) const
{
    return [&, this] (const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
    {
        /// Skip ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return nullptr;

        auto stream_name = getStreamName(column, substream_path);
        if (stream_name.empty())
            return nullptr;

        /// Don't write offsets more than one time for Nested type.
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets && offset_substreams.contains(stream_name))
            return nullptr;


        return &column_streams.at(stream_name)->compressed_hashing;
    };
}


void MergeTreeDataPartWriterWide::shiftCurrentMark(const Granules & granules_written)
{
    auto last_granule = granules_written.back();
    /// If we didn't finished last granule than we will continue to write it from new block
    if (!last_granule.is_complete)
    {
        if (settings.can_use_adaptive_granularity && settings.blocks_are_granules_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incomplete granules are not allowed while blocks are granules size. "
                "Mark number {} (rows {}), rows written in last mark {}, rows to write in last mark from block {} (from row {}), "
                "total marks currently {}", last_granule.mark_number, index_granularity->getMarkRows(last_granule.mark_number),
                rows_written_in_last_mark, last_granule.rows_to_write, last_granule.start_row, index_granularity->getMarksCount());

        /// Shift forward except last granule
        setCurrentMark(getCurrentMark() + granules_written.size() - 1);
        bool still_in_the_same_granule = granules_written.size() == 1;
        /// We wrote whole block in the same granule, but didn't finished it.
        /// So add written rows to rows written in last_mark
        if (still_in_the_same_granule)
            rows_written_in_last_mark += last_granule.rows_to_write;
        else
            rows_written_in_last_mark = last_granule.rows_to_write;
    }
    else
    {
        setCurrentMark(getCurrentMark() + granules_written.size());
        rows_written_in_last_mark = 0;
    }
}

void MergeTreeDataPartWriterWide::write(const Block & block, const IColumnPermutation * permutation, Block * permuted_columns_cache)
{
    Block block_to_write = block;

    /// For some columns the set of streams may depend on the actual column data.
    /// For example: dynamic structure and statistics for JSON, Dynamic and Map (with adaptive number of buckets).
    /// We must ensure that all blocks will be written in the same set of streams, so we have to make some
    /// preparations to achieve it.
    prepareBlockForWriting(block_to_write);

    initStreamsAndSubstreamsIfNeeded();

    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical part of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = 0;
        if (auto constant_granularity = index_granularity->getConstantGranularity())
            index_granularity_for_block = *constant_granularity;
        else
            index_granularity_for_block = computeIndexGranularity(block_to_write);

        if (rows_written_in_last_mark > 0)
        {
            size_t rows_left_in_last_mark = index_granularity->getMarkRows(getCurrentMark()) - rows_written_in_last_mark;
            /// Previous granularity was much bigger than our new block's
            /// granularity let's adjust it, because we want add new
            /// heavy-weight blocks into small old granule.
            if (rows_left_in_last_mark > index_granularity_for_block)
            {
                /// We have already written more rows than granularity of our block.
                /// adjust last mark rows and flush to disk.
                if (rows_written_in_last_mark >= index_granularity_for_block)
                    adjustLastMarkIfNeedAndFlushToDisk(rows_written_in_last_mark);
                else /// We still can write some rows from new block into previous granule. So the granule size will be block granularity size.
                    adjustLastMarkIfNeedAndFlushToDisk(index_granularity_for_block);
            }
        }

        fillIndexGranularity(index_granularity_for_block, block_to_write.rows());
    }

    auto granules_to_write = getGranulesToWrite(*index_granularity, block_to_write.rows(), getCurrentMark(), rows_written_in_last_mark);

    WrittenOffsetSubstreams offset_substreams = written_offset_substreams ? *written_offset_substreams : WrittenOffsetSubstreams{};

    Block primary_key_block;
    if (settings.rewrite_primary_key)
        primary_key_block = getIndexBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation, permuted_columns_cache);

    Block skip_indexes_block = getIndexBlockAndPermute(block, getSkipIndicesColumns(), permutation, permuted_columns_cache);

    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        auto & column = block_to_write.getByName(it->name);

        if (!ISerialization::hasKind(getSerialization(it->name)->getKindStack(), ISerialization::Kind::SPARSE))
            column.column = recursiveRemoveSparse(column.column);

        if (permutation)
        {
            if (primary_key_block.has(it->name))
            {
                const auto & primary_column = *primary_key_block.getByName(it->name).column;
                writeColumn(*it, primary_column, offset_substreams, granules_to_write);
            }
            else if (skip_indexes_block.has(it->name))
            {
                const auto & index_column = *skip_indexes_block.getByName(it->name).column;
                writeColumn(*it, index_column, offset_substreams, granules_to_write);
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                /// The permuted columns cache is populated above only with PK and skip-index columns
                /// (via `getIndexBlockAndPermute`), so by construction it cannot contain this column,
                /// and there is no point looking it up here.
                ColumnPtr permuted_column = column.column->permute(*permutation, 0);
                writeColumn(*it, *permuted_column, offset_substreams, granules_to_write);
            }
        }
        else
        {
            writeColumn(*it, *column.column, offset_substreams, granules_to_write);
        }
    }

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);

    calculateAndSerializeSkipIndices(skip_indexes_block, granules_to_write);

    shiftCurrentMark(granules_to_write);
}

void MergeTreeDataPartWriterWide::writeSingleMark(const NameAndTypePair & name_and_type,
    const WrittenOffsetSubstreams & offset_substreams,
    size_t number_of_rows)
{
    StreamsWithMarks marks = getCurrentMarksForColumn(name_and_type, offset_substreams);
    for (const auto & mark : marks)
        flushMarkToFile(mark, number_of_rows);
}

void MergeTreeDataPartWriterWide::flushMarkToFile(const StreamNameAndMark & stream_with_mark, size_t rows_in_mark)
{
    auto & stream = *column_streams.at(stream_with_mark.stream_name);
    WriteBuffer & marks_out = stream.compress_marks ? stream.marks_compressed_hashing : stream.marks_hashing;

    writeBinaryLittleEndian(stream_with_mark.mark.offset_in_compressed_file, marks_out);
    writeBinaryLittleEndian(stream_with_mark.mark.offset_in_decompressed_block, marks_out);

    if (settings.can_use_adaptive_granularity)
        writeBinaryLittleEndian(rows_in_mark, marks_out);

    if (auto it = cached_marks.find(stream_with_mark.stream_name); it != cached_marks.end())
        it->second->push_back(stream_with_mark.mark);
}

StreamsWithMarks MergeTreeDataPartWriterWide::getCurrentMarksForColumn(const NameAndTypePair & name_and_type,
    const WrittenOffsetSubstreams & offset_substreams)
{
    StreamsWithMarks result;
    const UInt64 min_compress_block_size = getEffectiveMinCompressBlockSize(name_and_type);

    auto callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        /// Skip ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
           return;

        auto stream_name = getStreamName(name_and_type, substream_path);
        if (stream_name.empty())
            return;

        /// Don't write offsets more than one time for Nested type.
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets && offset_substreams.contains(stream_name))
            return;

        auto & stream = *column_streams.at(stream_name);

        /// There could already be enough data to compress into the new block.
        if (stream.compressed_hashing.offset() >= min_compress_block_size)
            stream.compressed_hashing.next();

        StreamNameAndMark stream_with_mark;
        stream_with_mark.stream_name = stream_name;
        stream_with_mark.mark.offset_in_compressed_file = stream.plain_hashing.count();
        stream_with_mark.mark.offset_in_decompressed_block = stream.compressed_hashing.offset();

        result.push_back(stream_with_mark);
    };

    /// For a per-key Map the registered key set and template streams are not visible to the
    /// generic enumerateStreams, so use enumerateWrittenStreams.
    enumerateWrittenStreams(name_and_type, callback, /*with_template_streams=*/ true);
    return result;
}

void MergeTreeDataPartWriterWide::writeSingleGranule(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    const WrittenOffsetSubstreams & offset_substreams,
    ISerialization::SerializeBinaryBulkStatePtr & serialization_state,
    ISerialization::SerializeBinaryBulkSettings & serialize_settings,
    const Granule & granule)
{
    const auto & serialization = getSerialization(name_and_type.name);

    serialize_settings.granule_is_complete = granule.is_complete;
    serialization->serializeBinaryBulkWithMultipleStreams(column, granule.start_row, granule.rows_to_write, serialize_settings, serialization_state);

    if (const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get()))
    {
        per_key->writeTemplateDefaults(granule.rows_to_write, serialize_settings, serialization_state);
        map_key_columns_has_history[name_and_type.name] = true;
    }

    /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
    auto callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        /// Skip ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        auto stream_name = getStreamName(name_and_type, substream_path);
        if (stream_name.empty())
            return;

        /// Don't write offsets more than one time for Nested type.
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets && offset_substreams.contains(stream_name))
            return;

        column_streams.at(stream_name)->compressed_hashing.nextIfAtEnd();
    };

    enumerateWrittenStreams(name_and_type, callback, /*with_template_streams=*/ true);
}

ISerialization::SerializeBinaryBulkSettings MergeTreeDataPartWriterWide::getSerializationSettings() const
{
    ISerialization::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.data_part_type = MergeTreeDataPartType::Wide;
    serialize_settings.use_compact_variant_discriminators_serialization = settings.use_compact_variant_discriminators_serialization;
    serialize_settings.dynamic_serialization_version = settings.dynamic_serialization_version;
    serialize_settings.object_serialization_version = settings.object_serialization_version;
    serialize_settings.object_shared_data_serialization_version = settings.object_shared_data_serialization_version;
    serialize_settings.object_shared_data_buckets = settings.object_shared_data_buckets;
    serialize_settings.object_shared_data_target_chunk_rows = settings.object_shared_data_target_chunk_rows;
    serialize_settings.max_buckets_in_map = settings.max_buckets_in_map;
    serialize_settings.map_buckets_strategy = settings.map_buckets_strategy;
    serialize_settings.map_buckets_coefficient = settings.map_buckets_coefficient;
    serialize_settings.map_buckets_min_avg_size = settings.map_buckets_min_avg_size;
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part;
    serialize_settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::SUFFIX;
    serialize_settings.min_compress_block_size = settings.min_compress_block_size;
    return serialize_settings;
}

/// Column must not be empty. (column.size() !== 0)
void MergeTreeDataPartWriterWide::writeColumn(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetSubstreams & offset_substreams,
    const Granules & granules)
{
    if (granules.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty granules for column {}, current mark {}",
                        backQuoteIfNeed(name_and_type.name), getCurrentMark());

    const auto & [name, type] = name_and_type;
    auto [it, inserted] = serialization_states.emplace(name, nullptr);
    auto serialization = getSerialization(name_and_type.name);

    if (inserted)
    {
        /// Template streams must exist before the prefix writes the template serialization state.
        ensureMapKeyColumnsStreams(name_and_type, column, it->second);
        auto serialize_settings = getSerializationSettings();
        serialize_settings.getter = createStreamGetter(name_and_type, offset_substreams);
        /// Use the sample column (from block_sample) for the state prefix because
        /// serializeBinaryBulkStatePrefix only reads column structure and statistics
        /// (not actual row data) to determine things like the number of Map buckets.
        /// block_sample always has statistics consistent with what was used in
        /// enumerateStreams (via addStreams), so using it here guarantees that the
        /// bucket count written to the prefix matches the streams that were created.
        serialization->serializeBinaryBulkStatePrefix(*block_sample.getByName(name).column, serialize_settings, it->second);
    }

    /// Register any new keys that appeared in this block (a late key on merge or a later block),
    /// creating (and, when the part already has history, seeding) their per-key streams.
    ensureMapKeyColumnsStreams(name_and_type, column, it->second);

    auto serialize_settings = getSerializationSettings();
    serialize_settings.getter = createStreamGetter(name_and_type, offset_substreams);
    serialize_settings.min_compress_block_size = getEffectiveMinCompressBlockSize(name_and_type);
    /// Key prefixes must precede marks, otherwise a `LowCardinality` dictionary mark points at its version header.
    if (const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get()))
        per_key->initializeKeyPrefixes(serialize_settings, it->second);
    serialize_settings.stream_mark_getter = [&](const ISerialization::SubstreamPath & substream_path) -> MarkInCompressedFile
    {
        auto stream_name = getStreamName(name_and_type, substream_path);
        auto & stream = column_streams.at(stream_name);
        return {stream->plain_hashing.count(), stream->compressed_hashing.offset()};
    };

    /// Some vector codecs (e.g., SZ3) used for compressing arrays like Array<Float> require the array
    /// dimension to be set before compression starts (for 1D arrays it's simply the length). The dimension
    /// is a property of the whole column, so compute it once here rather than rescanning the entire column
    /// on every granule below - the per-granule scan would make SZ3 writes O(rows * granules) in the
    /// insert/merge hot path. This must run before serializing any granule, because serialization may
    /// already fill a compressed buffer and trigger compression.
    {
        auto vector_dim_data = ISerialization::SubstreamData(serialization).withType(name_and_type.type).withColumn(block_sample.getByName(name_and_type.name).column);
        auto vector_dim_enumerate_settings = getEnumerateSettings(settings);
        serialization->enumerateStreams(vector_dim_enumerate_settings, [&] (const ISerialization::SubstreamPath & substream_path)
        {
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;

            auto stream_name = getStreamName(name_and_type, substream_path);
            if (stream_name.empty())
                return;

            bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
            if (is_offsets && offset_substreams.contains(stream_name))
                return;

            auto compression_codec = column_streams.at(stream_name)->compressor.getCodec();
            setVectorDimensionsIfNeeded(compression_codec, &column);
        }, vector_dim_data);
    }

    for (const auto & granule : granules)
    {
        data_written = true;

        if (granule.mark_on_start)
        {
            if (last_non_written_marks.contains(name))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "We have to add new mark for column, but already have non written mark. "
                                "Current mark {}, total marks {}, offset {}",
                                getCurrentMark(), index_granularity->getMarksCount(), rows_written_in_last_mark);
            last_non_written_marks[name] = getCurrentMarksForColumn(name_and_type, offset_substreams);
        }

        writeSingleGranule(
            name_and_type,
            column,
            offset_substreams,
            it->second,
            serialize_settings,
            granule
        );

        if (granule.is_complete)
        {
            auto marks_it = last_non_written_marks.find(name);
            if (marks_it == last_non_written_marks.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No mark was saved for incomplete granule for column {}", backQuoteIfNeed(name));

            for (const auto & mark : marks_it->second)
                flushMarkToFile(mark, index_granularity->getMarkRows(granule.mark_number));
            last_non_written_marks.erase(marks_it);
        }
    }

    auto callback = [&](const ISerialization::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets)
            offset_substreams.insert(getStreamName(name_and_type, substream_path));
    };
    enumerateWrittenStreams(name_and_type, callback, /*with_template_streams=*/ false);
}


void MergeTreeDataPartWriterWide::validateColumnOfFixedSize(const NameAndTypePair & name_type)
{
    const auto & [name, type] = name_type;
    const auto & serialization = getSerialization(name_type.name);

    if (!type->isValueRepresentedByNumber() || type->haveSubtypes() || serialization->getKindStack() != ISerialization::KindStack{ISerialization::Kind::DEFAULT})
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot validate column of non fixed type {}", type->getName());

    String stream_name = replaceFileNameToHashIfNeeded(escapeForFileName(name), *storage_settings, data_part_storage.get());
    String mrk_path = stream_name + marks_file_extension;
    String bin_path = stream_name + DATA_FILE_EXTENSION;

    /// Some columns may be removed because of ttl. Skip them.
    if (!getDataPartStorage().existsFile(mrk_path))
        return;

    auto mrk_file_in = getDataPartStorage().readFile(mrk_path, {}, std::nullopt);
    std::unique_ptr<ReadBuffer> mrk_in;
    if (index_granularity_info.mark_type.compressed)
        mrk_in = std::make_unique<CompressedReadBufferFromFile>(std::move(mrk_file_in));
    else
        mrk_in = std::move(mrk_file_in);

    DB::CompressedReadBufferFromFile bin_in(getDataPartStorage().readFile(bin_path, {}, std::nullopt), /* allow_different_codecs */ true);
    bool must_be_last = false;
    UInt64 offset_in_compressed_file = 0;
    UInt64 offset_in_decompressed_block = 0;
    UInt64 index_granularity_rows = index_granularity_info.fixed_index_granularity;

    size_t mark_num = 0;

    for (mark_num = 0; !mrk_in->eof(); ++mark_num)
    {
        if (mark_num > index_granularity->getMarksCount())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incorrect number of marks in memory {}, on disk (at least) {}",
                            index_granularity->getMarksCount(), mark_num + 1);

        readBinaryLittleEndian(offset_in_compressed_file, *mrk_in);
        readBinaryLittleEndian(offset_in_decompressed_block, *mrk_in);
        if (settings.can_use_adaptive_granularity)
            readBinaryLittleEndian(index_granularity_rows, *mrk_in);
        else
            /// Non-adaptive mark files do not store per-mark row counts. The writer uses the
            /// in-memory `index_granularity` to determine how many rows belong to each mark,
            /// and `MergeTreeIndexGranularityConstant` allows the last data mark to have fewer
            /// rows than `fixed_index_granularity` (e.g. after `fixFromRowsCount` adjusts it
            /// during part loading). Read back the per-mark row count from the in-memory
            /// granularity rather than blindly assuming `fixed_index_granularity`, otherwise
            /// the comparison below would falsely fail for parts whose last mark is incomplete
            /// (issue #98585).
            index_granularity_rows = index_granularity->getMarkRows(mark_num);

        if (must_be_last)
        {
            if (index_granularity_rows != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "We ran out of binary data but still have non empty mark #{} with rows number {}",
                                mark_num, index_granularity_rows);

            if (!mrk_in->eof())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark #{} must be last, but we still have some to read", mark_num);

            break;
        }

        if (index_granularity_rows == 0)
        {
            auto column = readColumnForValidation(*serialization, *type, bin_in, 1000000000);

            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Still have {} rows in bin stream, last mark #{}"
                            " index granularity size {}, last rows {}",
                            column->size(), mark_num, index_granularity->getMarksCount(), index_granularity_rows);
        }

        if (index_granularity_rows != index_granularity->getMarkRows(mark_num))
        {
            throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Incorrect mark rows for part {} for mark #{}"
                            " (compressed offset {}, decompressed offset {}), in-memory {}, on disk {}, total marks {}",
                            getDataPartStorage().getFullPath(),
                            mark_num, offset_in_compressed_file, offset_in_decompressed_block,
                            index_granularity->getMarkRows(mark_num), index_granularity_rows,
                            index_granularity->getMarksCount());
        }

        auto column = readColumnForValidation(*serialization, *type, bin_in, index_granularity_rows);

        if (bin_in.eof())
        {
            must_be_last = true;
        }

        /// Now they must be equal
        if (column->size() != index_granularity_rows)
        {

            if (must_be_last)
            {
                /// The only possible mark after bin.eof() is final mark. When we
                /// cannot use adaptive granularity we cannot have last mark.
                /// So finish validation.
                if (!settings.can_use_adaptive_granularity)
                    break;

                /// If we don't compute granularity then we are not responsible
                /// for last mark (for example we mutating some column from part
                /// with fixed granularity where last mark is not adjusted)
                if (!compute_granularity)
                    continue;
            }

            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Incorrect mark rows for mark #{} (compressed offset {}, decompressed offset {}), "
                "actually in bin file {}, in mrk file {}, total marks {}",
                mark_num, offset_in_compressed_file, offset_in_decompressed_block, column->size(),
                index_granularity->getMarkRows(mark_num), index_granularity->getMarksCount());
        }
    }

    if (!mrk_in->eof())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Still have something in marks stream, last mark #{}"
                        " index granularity size {}, last rows {}",
                        mark_num, index_granularity->getMarksCount(), index_granularity_rows);
    if (!bin_in.eof())
    {
        auto column = readColumnForValidation(*serialization, *type, bin_in, 1000000000);

        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Still have {} rows in bin stream, last mark #{}"
                            " index granularity size {}, last rows {}",
                            column->size(), mark_num, index_granularity->getMarksCount(), index_granularity_rows);
    }
}

void MergeTreeDataPartWriterWide::finalizeIndexGranularity()
{
    /// If no data was written, streams and columns substreams will be uninitialized, but we need them.
    initStreamsAndSubstreamsIfNeeded();

    auto serialize_settings = getSerializationSettings();
    if (rows_written_in_last_mark > 0)
    {
        if (settings.can_use_adaptive_granularity && settings.blocks_are_granules_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incomplete granule is not allowed while blocks are granules size even for last granule. "
                            "Mark number {} (rows {}), rows written for last mark {}, total marks {}",
                            getCurrentMark(), index_granularity->getMarkRows(getCurrentMark()),
                            rows_written_in_last_mark, index_granularity->getMarksCount());

        adjustLastMarkIfNeedAndFlushToDisk(rows_written_in_last_mark);
    }

    WrittenOffsetSubstreams dummy_offset_substreams;
    WrittenOffsetSubstreams & offset_substreams = written_offset_substreams ? *written_offset_substreams : dummy_offset_substreams;
    bool write_final_mark = (with_final_mark && data_written);
    {
        auto it = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++it)
        {
            if (!serialization_states.empty())
            {
                serialize_settings.getter = createStreamGetter(*it, offset_substreams);
                getSerialization(it->name)->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[it->name]);
            }

            if (write_final_mark)
                writeFinalMark(*it, offset_substreams);
        }
    }
}

void MergeTreeDataPartWriterWide::fillDataChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove)
{
    /// Replace the dry-run placeholder with the streams that actually stay in the part.
    /// Per-key files are not known when `columns_substreams` is first built.
    ISerialization::StreamFileNameSettings stream_file_name_settings(*storage_settings);
    auto enumerate_settings = getEnumerateSettings(settings);
    for (const auto & column : columns_list)
    {
        const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(getSerialization(column.name).get());
        if (!per_key)
            continue;

        auto data = ISerialization::SubstreamData(getSerialization(column.name)).withType(column.type);
        if (block_sample.has(column.name))
            data.withColumn(block_sample.getByName(column.name).column);

        std::vector<String> substreams;
        auto collect = [&](const ISerialization::SubstreamPath & substream_path)
        {
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;
            substreams.push_back(ISerialization::getFileNameForStream(column, substream_path, stream_file_name_settings));
        };

        auto state_it = serialization_states.find(column.name);
        const auto state = state_it == serialization_states.end() ? nullptr : state_it->second;
        const bool has_keys = state && !per_key->getRegisteredKeys(*state).empty();
        if (has_keys)
            per_key->enumerateRegisteredKeyStreams(enumerate_settings, collect, data, state);
        else
        {
            per_key->enumerateTemplateStreams(enumerate_settings, collect, data);
            for (const auto & substream : substreams)
            {
                kept_map_key_columns_template_streams.insert(
                    replaceFileNameToHashIfNeeded(substream, *storage_settings, data_part_storage.get()));
            }
        }

        if (!substreams.empty())
            columns_substreams.setColumnSubstreams(column.name, substreams);
    }

    for (auto & [stream_name, stream] : column_streams)
    {
        /// Template streams only seed late keys. A column that registered keys drops them.
        /// A column that registered none keeps them: they are the only data files that column has.
        if (isMapKeyColumnsTemplateStreamName(stream_name) && !kept_map_key_columns_template_streams.contains(stream_name))
        {
            cached_marks.erase(stream_name);
            continue;
        }

        /// Remove checksums for old stream name if file was
        /// renamed due to replacing the name to the hash of name.
        const auto & full_stream_name = stream_name_to_full_name.at(stream_name);
        if (stream_name != full_stream_name)
        {
            checksums_to_remove.insert(full_stream_name + stream->data_file_extension);
            checksums_to_remove.insert(full_stream_name + stream->marks_file_extension);
        }

        stream->preFinalize();
        stream->addToChecksums(checksums, true);
    }

    /// One plain key list per `with_key_columns` Map column. It is part metadata, not a mark stream.
    for (const auto & column : columns_list)
    {
        const auto * per_key = typeid_cast<const SerializationMapWithKeyColumns *>(getSerialization(column.name).get());
        if (!per_key)
            continue;

        MapKeyManifest manifest;
        auto state_it = serialization_states.find(column.name);
        if (state_it != serialization_states.end() && state_it->second)
        {
            for (const auto & key : per_key->getRegisteredKeys(*state_it->second))
                manifest.keys.push_back(MapKeyManifestEntry{.key = key, .presence_kind = MapKeyPresenceKind::Tracked});
        }

        key_columns_files.push_back(writeMapKeyColumnsFile(
            *data_part_storage,
            column.name,
            *storage_settings,
            per_key->getKeyType(),
            manifest,
            settings.query_write_settings,
            checksums));
    }
}

void MergeTreeDataPartWriterWide::finishDataSerialization(bool sync)
{
    /// Drop the per-key Map template streams: they exist only to seed late keys and are not
    /// part of the on-disk column.
    for (const auto & stream_name : per_key_template_stream_names)
    {
        if (kept_map_key_columns_template_streams.contains(stream_name))
            continue;

        auto it = column_streams.find(stream_name);
        if (it == column_streams.end())
            continue;

        if (it->second)
            it->second->cancel();

        const auto & full_stream_name = stream_name_to_full_name.at(stream_name);
        data_part_storage->removeFileIfExists(stream_name + DATA_FILE_EXTENSION);
        data_part_storage->removeFileIfExists(stream_name + marks_file_extension);
        if (stream_name != full_stream_name)
        {
            data_part_storage->removeFileIfExists(full_stream_name + DATA_FILE_EXTENSION);
            data_part_storage->removeFileIfExists(full_stream_name + marks_file_extension);
        }
        column_streams.erase(it);
    }

    for (auto & stream : column_streams)
        stream.second->finalize();

    if (sync)
    {
        std::vector<const MergeTreeWriterStream *> streams_to_sync;
        streams_to_sync.reserve(column_streams.size());
        for (const auto & stream : column_streams)
            streams_to_sync.push_back(stream.second.get());
        parallelSyncFiles(streams_to_sync);
    }

    for (auto & file : key_columns_files)
    {
        if (sync)
            file->sync();
        file->finalize();
    }
    key_columns_files.clear();

    column_streams.clear();
    serialization_states.clear();

#ifndef NDEBUG
    /// Heavy weight validation of written data. Checks that we are able to read
    /// data according to marks. Otherwise throws LOGICAL_ERROR (equal to abort in debug mode)
    for (const auto & column : columns_list)
    {
        if (column.type->isValueRepresentedByNumber()
            && !column.type->haveSubtypes()
            && getSerialization(column.name)->getKindStack() == ISerialization::KindStack{ISerialization::Kind::DEFAULT})
        {
            validateColumnOfFixedSize(column);
        }
    }
#endif

}

void MergeTreeDataPartWriterWide::fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove)
{
    // If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        fillDataChecksums(checksums, checksums_to_remove);

    if (settings.rewrite_primary_key)
        fillPrimaryIndexChecksums(checksums);

    fillSkipIndicesChecksums(checksums);
}

void MergeTreeDataPartWriterWide::finish(bool sync)
{
    // If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        finishDataSerialization(sync);

    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(sync);

    finishSkipIndicesSerialization(sync);
}

void MergeTreeDataPartWriterWide::cancel() noexcept
{
    for (auto & stream : column_streams)
        if (stream.second)
            stream.second->cancel();

    for (auto & file : key_columns_files)
        if (file)
            file->cancel();

    column_streams.clear();
    key_columns_files.clear();
    serialization_states.clear();

    Base::cancel();
}

void MergeTreeDataPartWriterWide::writeFinalMark(const NameAndTypePair & name_and_type,
    WrittenOffsetSubstreams & offset_substreams)
{
    writeSingleMark(name_and_type, offset_substreams, 0);

    /// Memorize information about offsets
    auto callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets)
            offset_substreams.insert(getStreamName(name_and_type, substream_path));
    };
    enumerateWrittenStreams(name_and_type, callback, /*with_template_streams=*/ false);
}

static void fillIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity,
    size_t index_offset,
    size_t index_granularity_for_block,
    size_t rows_in_block)
{
    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

void MergeTreeDataPartWriterWide::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    if (getCurrentMark() < index_granularity->getMarksCount() && getCurrentMark() != index_granularity->getMarksCount() - 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to add marks, while current mark {}, but total marks {}",
                        getCurrentMark(), index_granularity->getMarksCount());

    size_t index_offset = 0;
    if (rows_written_in_last_mark != 0)
        index_offset = index_granularity->getLastMarkRows() - rows_written_in_last_mark;

    fillIndexGranularityImpl(
        *index_granularity,
        index_offset,
        index_granularity_for_block,
        rows_in_block);
}


void MergeTreeDataPartWriterWide::adjustLastMarkIfNeedAndFlushToDisk(size_t new_rows_in_last_mark)
{
    /// We don't want to split already written granules to smaller
    if (rows_written_in_last_mark > new_rows_in_last_mark)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tryin to make mark #{} smaller ({} rows) then it already has {}",
                        getCurrentMark(), new_rows_in_last_mark, rows_written_in_last_mark);

    /// We can adjust marks only if we computed granularity for blocks.
    /// Otherwise we cannot change granularity because it will differ from
    /// other columns
    if (compute_granularity && settings.can_use_adaptive_granularity)
    {
        if (getCurrentMark() != index_granularity->getMarksCount() - 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Non last mark {} (with {} rows) having rows offset {}, total marks {}",
                            getCurrentMark(), index_granularity->getMarkRows(getCurrentMark()),
                            rows_written_in_last_mark, index_granularity->getMarksCount());

        index_granularity->adjustLastMark(new_rows_in_last_mark);
    }

    /// Last mark should be filled, otherwise it's a bug
    if (last_non_written_marks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No saved marks for last mark {} having rows offset {}, total marks {}",
                        getCurrentMark(), rows_written_in_last_mark, index_granularity->getMarksCount());

    if (rows_written_in_last_mark == new_rows_in_last_mark)
    {
        for (const auto & [name, marks] : last_non_written_marks)
        {
            for (const auto & mark : marks)
                flushMarkToFile(mark, index_granularity->getMarkRows(getCurrentMark()));
        }

        last_non_written_marks.clear();

        if (compute_granularity && settings.can_use_adaptive_granularity)
        {
            /// Also we add mark to each skip index because all of them
            /// already accumulated all rows from current adjusting mark
            for (size_t i = 0; i < skip_indices.size(); ++i)
                ++skip_index_accumulated_marks[i];

            /// This mark completed, go further
            setCurrentMark(getCurrentMark() + 1);
            /// Without offset
            rows_written_in_last_mark = 0;
        }
    }
}

}
