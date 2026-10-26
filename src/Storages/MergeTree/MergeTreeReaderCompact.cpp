#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/DeserializationPrefixesCache.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <DataTypes/Serializations/getSubcolumnsDeserializationOrder.h>
#include <DataTypes/Serializations/SerializationQuantizedVector.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Common/ProfileEvents.h>
#include <ranges>

namespace ProfileEvents
{
    extern const Event ColumnsCacheHits;
    extern const Event ColumnsCacheMisses;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
}

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

MergeTreeReaderCompact::MergeTreeReaderCompact(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    NamesAndTypesList columns_,
    const VirtualFields & virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeSettingsPtr & storage_settings_,
    UncompressedCache * uncompressed_cache_,
    ColumnsCache * columns_cache_,
    MarkCache * mark_cache_,
    DeserializationPrefixesCache * deserialization_prefixes_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_info_for_read_,
        columns_,
        virtual_fields_,
        storage_snapshot_,
        storage_settings_,
        uncompressed_cache_,
        columns_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
    , columns_substreams(data_part_info_for_read_->getColumnsSubstreams())
    , marks_loader(std::make_shared<MergeTreeMarksLoader>(
        data_part_info_for_read_,
        mark_cache,
        data_part_info_for_read_->getIndexGranularityInfo().getMarksFilePath(MergeTreeDataPartCompact::DATA_FILE_NAME),
        data_part_info_for_read_->getMarksCount(),
        data_part_info_for_read_->getIndexGranularityInfo(),
        settings.save_marks_in_cache,
        settings.read_settings,
        settings_.load_marks_asynchronously ? &data_part_info_for_read_->getContext()->getLoadMarksThreadpool() : nullptr,
        data_part_info_for_read_->getIndexGranularityInfo().mark_type.with_substreams
            ? columns_substreams.getTotalSubstreams() : data_part_info_for_read_->getColumns().size(),
        settings.use_streaming_marks_compression))
    , profile_callback(profile_callback_)
    , clock_type(clock_type_)
    , has_substream_marks(data_part_info_for_read_->getIndexGranularityInfo().mark_type.with_substreams)
    , deserialization_prefixes_cache(deserialization_prefixes_cache_)
{
    marks_loader->startAsyncLoad();
}

void MergeTreeReaderCompact::fillColumnPositions()
{
    size_t columns_num = columns_to_read.size();

    column_positions.resize(columns_num);
    columns_for_offsets.resize(columns_num);
    column_cacheable.assign(columns_num, false);

    for (size_t i = 0; i < columns_num; ++i)
    {
        auto & column_to_read = columns_to_read[i];
        auto position = data_part_info_for_read->getColumnPosition(column_to_read.getNameInStorage());

        /// Column was dropped by a pending mutation or invalidated. Don't read stale data;
        if (position.has_value() && (isColumnDroppedByPendingMutation(i) || isSystemColumnInvalidated(i)))
            position.reset();

        if (position.has_value() && column_to_read.isSubcolumn())
        {
            auto name_in_storage = column_to_read.getNameInStorage();
            auto subcolumn_name = column_to_read.getSubcolumnName();
            auto storage_column_from_part = part_columns.getColumn(GetColumnsOptions::All, name_in_storage);

            /// The `Quantize` codec's custom serialization exposes companion `quantized`/`pq_codebook` subcolumns that
            /// the part's plain columns list cannot represent - they round-trip to the bare type name and are lost, so
            /// the subcolumn would be treated as missing and recomputed/defaulted after a reload. Decide presence from
            /// the requested column's storage type in that case. Restricted to that specific serialization so it does
            /// not change presence decisions for ordinary subcolumns (e.g. of sparse columns).
            const auto * custom = column_to_read.getTypeInStorage()->getCustomSerialization();
            const bool is_quantize = custom && typeid(*custom) == typeid(SerializationQuantizedVector);
            /// For a Quantize column the part's serialization is the re-wrapped one that lost the companion
            /// subcolumns, so the presence is decided from the storage type and its own custom serialization.
            const bool has_subcolumn = is_quantize
                ? column_to_read.getTypeInStorage()->hasSubcolumn(subcolumn_name)
                : hasSubcolumnInPart(name_in_storage, *storage_column_from_part.type, subcolumn_name);
            if (!has_subcolumn)
                position.reset();
        }

        column_positions[i] = std::move(position);

        /// A column that is not in the part is either left to `fillMissingColumns`, or read for
        /// the offsets of a `Nested` sibling by `findPositionForMissedNested`, which
        /// `fillMissingColumns` refills as well: neither has an entry in the cache.
        column_cacheable[i] = column_positions[i].has_value() && !isColumnNotCacheable(column_to_read);

        /// If array of Nested column is missing in part,
        /// we have to read its offsets if they exist.
        if (!column_positions[i])
            findPositionForMissedNested(i);

        if (column_positions[i] && !column_cacheable[i])
            has_columns_read_only_from_part = true;

        if (column_positions[i] && column_to_read.isSubcolumn())
        {
            has_subcolumns = true;
            column_to_subcolumns_indexes[column_to_read.getNameInStorage()].push_back(i);
        }
    }

    initColumnsCache(/*can_read=*/ true);
    accumulations.resize(columns_num);
}

NameAndTypePair MergeTreeReaderCompact::getColumnConvertedToSubcolumnOfNested(const NameAndTypePair & column)
{
    if (!(*storage_settings)[MergeTreeSetting::share_nested_offsets])
        return column;

    if (!isArray(column.type))
        return column;

    /// If it is a part of Nested, we need to get the column from
    /// storage metadata which is converted to Nested type with subcolumns.
    /// It is required for proper counting of shared streams.
    auto [name_in_storage, subcolumn_name] = Nested::splitName(column.name);

    if (subcolumn_name.empty())
        return column;

    if (!storage_columns_with_collected_nested)
    {
        auto options = GetColumnsOptions(GetColumnsOptions::All);
        auto storage_columns_list = Nested::collect(storage_snapshot->getColumns(options));
        storage_columns_with_collected_nested = ColumnsDescription(std::move(storage_columns_list));
    }

    return storage_columns_with_collected_nested->getColumnOrSubcolumn(
        GetColumnsOptions::All,
        Nested::concatenateName(name_in_storage, subcolumn_name));
}

void MergeTreeReaderCompact::findPositionForMissedNested(size_t pos)
{
    if (!(*storage_settings)[MergeTreeSetting::share_nested_offsets])
        return;

    auto & column = columns_to_read[pos];

    bool is_array = isArray(column.type);
    bool is_offsets_subcolumn = isArray(column.getTypeInStorage()) && column.getSubcolumnName() == "size0";

    if (!is_array && !is_offsets_subcolumn)
        return;

    NameAndTypePair column_in_storage{column.getNameInStorage(), column.getTypeInStorage()};

    auto column_to_read_with_subcolumns = getColumnConvertedToSubcolumnOfNested(column_in_storage);
    auto column_for_offsets = findColumnForOffsets(column_to_read_with_subcolumns);

    if (!column_for_offsets)
        return;

    if (is_offsets_subcolumn)
    {
        /// Read offsets from antoher array from the same Nested column.
        column = {column_for_offsets->column.name, column.getSubcolumnName(), column.getTypeInStorage(), column.type};
    }
    else
    {
        columns_for_offsets[pos] = column_for_offsets;
        partially_read_columns.insert(column.name);
    }

    column_positions[pos] = data_part_info_for_read->getColumnPosition(column_for_offsets->column.name);
    serializations_of_full_columns[column.getNameInStorage()] = column_for_offsets->serialization;
}

static ColumnPtr getFullColumnFromCache(std::unordered_map<String, ColumnPtr> * columns_cache_for_subcolumns, const String & column_name)
{
    if (!columns_cache_for_subcolumns)
        return nullptr;

    auto it = columns_cache_for_subcolumns->find(column_name);
    if (it == columns_cache_for_subcolumns->end())
        return nullptr;

    return it->second;
}

void MergeTreeReaderCompact::readData(
    size_t column_idx,
    IColumn & column,
    size_t rows_to_read,
    size_t from_mark,
    size_t column_size_before_reading,
    MergeTreeReaderStream & stream,
    std::unordered_map<String, ColumnPtr> & output_columns_cache,
    std::unordered_map<String, ColumnPtr> * columns_cache_for_subcolumns,
    ISerialization::SubstreamsCache * substreams_cache)
{
    const auto & name_and_type = columns_to_read[column_idx];
    const auto & name = name_and_type.name;

    bool seek_to_substream_mark = name_and_type.isSubcolumn() && has_substream_marks;
    auto buffer_getter = [&](const ISerialization::SubstreamPath & substream_path) -> ReadBuffer *
    {
        if (needSkipStream(column_idx, substream_path))
            return nullptr;

        if (seek_to_substream_mark)
        {
            size_t substream_position = columns_substreams.getSubstreamPosition(*column_positions[column_idx], name_and_type, substream_path, storage_settings);
            stream.seekToMarkAndColumn(from_mark, substream_position);
        }

        return stream.getDataBuffer();
    };

    try
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.getter = buffer_getter;
        deserialize_settings.use_specialized_prefixes_and_suffixes_substreams = true;
        deserialize_settings.data_part_type = MergeTreeDataPartType::Compact;
        /// Only the columns that are read for their shared `Nested` offsets (see
        /// `findColumnForOffsets`) are refilled by `IMergeTreeReader::fillMissingColumns`; any
        /// other column has to be read in full.
        deserialize_settings.partially_read_columns_are_refilled = partially_read_columns.contains(name);
        deserialize_settings.get_avg_value_size_hint_callback
            = [&](const ISerialization::SubstreamPath & substream_path) -> double
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (!stream_name)
                return 0.0;

            return avg_value_size_hints[*stream_name];
        };

        deserialize_settings.update_avg_value_size_hint_callback
            = [&](const ISerialization::SubstreamPath & substream_path, const IColumn & column_)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (!stream_name)
                return;

            IDataType::updateAvgValueSizeHint(column_, avg_value_size_hints[*stream_name]);
        };

        if (has_substream_marks)
        {
            deserialize_settings.seek_stream_to_mark_callback = [&](const ISerialization::SubstreamPath &, const MarkInCompressedFile & mark)
            {
                stream.seekToMark(mark);
            };

            deserialize_settings.seek_stream_to_current_mark_callback = [&](const ISerialization::SubstreamPath & substream_path)
            {
                size_t substream_position = columns_substreams.getSubstreamPosition(*column_positions[column_idx], name_and_type, substream_path, storage_settings);
                stream.seekToMarkAndColumn(from_mark, substream_position);
            };
        }

        auto it = output_columns_cache.find(name);
        if (it != output_columns_cache.end() && it->second != nullptr)
        {
            /// The same physical column was already read for another requested column in this granule
            /// (e.g. shared Nested offsets). Copy only the newly-read rows from it instead of re-reading.
            chassert(column.size() <= it->second->size());
            column.insertRangeFrom(*it->second, column.size(), it->second->size() - column.size());
            return;
        }

        if (name_and_type.isSubcolumn())
        {
            if (has_substream_marks)
            {
                const auto & serialization = serializations[column_idx];
                serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, deserialize_binary_bulk_state_map_for_subcolumns[name], substreams_cache);
            }
            else
            {
                const auto & type_in_storage = name_and_type.getTypeInStorage();
                const auto & name_in_storage = name_and_type.getNameInStorage();
                const auto & serialization = serializations_of_full_columns.at(name_in_storage);

                ColumnPtr temp_full_column = getFullColumnFromCache(columns_cache_for_subcolumns, name_in_storage);

                if (!temp_full_column)
                {
                    auto mutable_temp = type_in_storage->createColumn(*serialization);
                    serialization->deserializeBinaryBulkWithMultipleStreams(*mutable_temp, rows_to_read, deserialize_settings, deserialize_binary_bulk_state_map_for_subcolumns[name_in_storage], substreams_cache);
                    temp_full_column = std::move(mutable_temp);

                    if (columns_cache_for_subcolumns)
                        columns_cache_for_subcolumns->emplace(name_in_storage, temp_full_column);
                }
                else if (temp_full_column->size() > rows_to_read)
                {
                    /// A full-column request caches the accumulated output. Extract shared paths only from this granule.
                    temp_full_column = temp_full_column->cut(temp_full_column->size() - rows_to_read, rows_to_read);
                    columns_cache_for_subcolumns->at(name_in_storage) = temp_full_column;
                }

                auto subcolumn = type_in_storage->getSubcolumn(name_and_type.getSubcolumnName(), temp_full_column);
                column.insertRangeFrom(*subcolumn, subcolumn->size() - rows_to_read, rows_to_read);
            }
        }
        else
        {
            auto full_column = !has_substream_marks ? getFullColumnFromCache(columns_cache_for_subcolumns, name) : nullptr;
            if (full_column)
                column.insertRangeFrom(*full_column, full_column->size() - rows_to_read, rows_to_read);
            else
            {
                const auto & serialization = serializations[column_idx];
                setStringValueFilter(name_and_type, deserialize_settings);
                auto & states = !has_substream_marks && !columns_for_offsets[column_idx]
                    ? deserialize_binary_bulk_state_map_for_subcolumns : deserialize_binary_bulk_state_map;
                serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, states[name], substreams_cache);
                if (!has_substream_marks && columns_cache_for_subcolumns)
                    columns_cache_for_subcolumns->emplace(name, column.getPtr());
            }
        }

        /// Cache the just-read column so other requested columns mapping to the same physical column in this
        /// granule (e.g. shared Nested offsets) can copy from it. The cache lives only for the current granule.
        output_columns_cache[name] = column.getPtr();

        size_t read_rows_in_column = column.size() - column_size_before_reading;
        if (read_rows_in_column != rows_to_read)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data in MergeTreeReaderCompact. Rows read: {}. Rows expected: {}.",
                read_rows_in_column, rows_to_read);
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading column " + name_and_type.name + ")");
        throw;
    }
}

void MergeTreeReaderCompact::readSubcolumnsPrefixes(size_t from_mark)
{
    if (!has_subcolumns || !has_substream_marks)
        return;

    /// First, call adjustRightMark for each stream before deserialization.
    /// We don't call it during prefixes deserialization because we can get prefixes from cache and
    /// don't call it at all.
    for (auto index : column_to_subcolumns_indexes | std::views::values | std::views::join)
        getStream(columns_to_read[index]).adjustRightMark(last_mark_to_read);

    /// Second, deserialize prefixes of get the from cache.
    auto deserialize = [&]() -> DeserializeBinaryBulkStateMap
    {
        for (const auto & [column, subcolumns_indexes] : column_to_subcolumns_indexes)
        {
            ISerialization::SubstreamsDeserializeStatesCache deserialize_states_cache;
            for (auto index : subcolumns_indexes)
            {
                auto & stream = getStream(columns_to_read[index]);
                readPrefix(index, from_mark, stream, &deserialize_states_cache);
            }
        }

        return deserialize_binary_bulk_state_map_for_subcolumns;
    };

    /// Use deserialization prefixes cache only for remote disks, for local disks it might be inefficient.
    if (deserialization_prefixes_cache && settings.use_deserialization_prefixes_cache && data_part_info_for_read->getDataPartStorage()->isStoredOnRemoteDisk())
        deserialize_binary_bulk_state_map_for_subcolumns = deserialization_prefixes_cache->getOrSet(deserialize);
    else
        deserialize();
}

void MergeTreeReaderCompact::initSubcolumnsDeserializationOrder()
{
    if (!has_subcolumns || !has_substream_marks || !subcolumns_deserialization_order.empty())
        return;

    /// When we read multiple subcolumns of the same column it's better to read them in order of their
    /// serialization to avoid multiple seeks back to the file (especially if we read from remote filesystem).
    /// Here we determine this order.
    ISerialization::EnumerateStreamsSettings enumerate_settings;
    enumerate_settings.data_part_type = MergeTreeDataPartType::Compact;
    enumerate_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    for (const auto & [column, subcolumns_indexes] : column_to_subcolumns_indexes)
    {
        auto pos = data_part_info_for_read->getColumnPosition(column);
        auto & deserialization_order = subcolumns_deserialization_order[column];
        /// If there is no such column in the part, the subcolumns order doesn't matter.
        if (!pos)
        {
            deserialization_order = subcolumns_indexes;
            continue;
        }

        std::vector<ISerialization::SubstreamData> subcolumns_data;
        subcolumns_data.reserve(subcolumns_indexes.size());
        /// Some subcolumns might not exist in this part and for them we don't have substreams in columns_substreams.
        std::vector<size_t> non_existing_subcolumns_indexes;
        /// Mapping from index in subcolumns_data to subcolumn index from subcolumns_indexes.
        std::unordered_map<size_t, size_t> subcolumn_data_index_to_subcolumn_index;
        subcolumn_data_index_to_subcolumn_index.reserve(subcolumns_indexes.size());
        auto column_from_part = part_columns.getColumn(GetColumnsOptions::All, column);
        for (size_t index : subcolumns_indexes)
        {
            if (hasSubcolumnInPart(column, *column_from_part.type, columns_to_read[index].getSubcolumnName()))
            {
                subcolumns_data.push_back(ISerialization::SubstreamData(serializations[index])
                                          .withType(columns_to_read[index].type)
                                          .withDeserializeState(deserialize_binary_bulk_state_map_for_subcolumns[columns_to_read[index].name]));
                subcolumn_data_index_to_subcolumn_index[subcolumns_data.size() - 1] = index;
            }
            else
            {
                non_existing_subcolumns_indexes.push_back(index);
            }
        }

        /// Set check_stream_exists_callback so that enumerateStreams can skip substreams
        /// that do not exist in this part (e.g. MapBucketIndexes in old bucketed Map parts).
        enumerate_settings.check_stream_exists_callback = [&, column_pos = *pos](const ISerialization::SubstreamPath & substream_path) -> bool
        {
            return columns_substreams.tryGetSubstreamPosition(column_pos, column_from_part, substream_path, storage_settings).has_value();
        };

        auto order = getSubcolumnsDeserializationOrder(column, subcolumns_data, columns_substreams.getColumnSubstreams(*pos), enumerate_settings, ISerialization::StreamFileNameSettings(*storage_settings));
        deserialization_order.reserve(subcolumns_indexes.size());
        for (size_t i : order)
            deserialization_order.push_back(subcolumn_data_index_to_subcolumn_index[i]);
        /// Add indexes of non-existing subcolumns at the end.
        deserialization_order.insert(deserialization_order.end(), non_existing_subcolumns_indexes.begin(), non_existing_subcolumns_indexes.end());
    }
}

void MergeTreeReaderCompact::readPrefix(size_t column_idx, size_t from_mark, MergeTreeReaderStream & stream, ISerialization::SubstreamsDeserializeStatesCache * cache)
{
    if (columns_for_offsets[column_idx])
    {
        /// If we read only offsets we have to read prefix anyway
        /// to preserve correctness of serialization.
        auto buffer_getter = [&](const auto &) -> ReadBuffer *
        {
            return stream.getDataBuffer();
        };

        ISerialization::DeserializeBinaryBulkStatePtr state;
        readPrefix(columns_for_offsets[column_idx]->column, columns_for_offsets[column_idx]->serialization, state, buffer_getter, cache);
    }

    const auto & column = columns_to_read[column_idx];
    auto name_in_storage = column.getNameInStorage();

    bool seek_to_substream_mark = column.isSubcolumn() && has_substream_marks;
    auto buffer_getter = [&](const ISerialization::SubstreamPath & substream_path) -> ReadBuffer *
    {
        if (needSkipStream(column_idx, substream_path))
            return nullptr;

        if (seek_to_substream_mark)
        {
            size_t substream_position = columns_substreams.getSubstreamPosition(*column_positions[column_idx], column, substream_path, storage_settings);
            stream.seekToMarkAndColumn(from_mark, substream_position);
        }

        return stream.getDataBuffer();
    };

    /// Build check_stream_exists_callback for this column if we have a column position.
    ISerialization::DeserializeBinaryBulkSettings::CheckStreamExistsCallback check_stream_exists_callback;
    if (column_positions[column_idx])
    {
        check_stream_exists_callback = [&](const ISerialization::SubstreamPath & substream_path) -> bool
        {
            return columns_substreams.tryGetSubstreamPosition(*column_positions[column_idx], column, substream_path, storage_settings).has_value();
        };
    }

    if (!has_substream_marks && !columns_for_offsets[column_idx])
    {
        /// Full-column and subcolumn requests share the same physical parent in this granule.
        if (deserialize_binary_bulk_state_map_for_subcolumns.contains(name_in_storage))
            return;

        const auto & serialization = serializations_of_full_columns.at(name_in_storage);
        auto & state = deserialize_binary_bulk_state_map_for_subcolumns[name_in_storage];
        readPrefix(column, serialization, state, buffer_getter, nullptr, check_stream_exists_callback);
    }
    else if (column.isSubcolumn())
    {
        if (has_substream_marks)
        {
            const auto & serialization = serializations[column_idx];
            auto & state = deserialize_binary_bulk_state_map_for_subcolumns[column.name];
            readPrefix(column, serialization, state, buffer_getter, seek_to_substream_mark ? cache : nullptr, check_stream_exists_callback);
        }
        else
        {
            /// If we don't have substream marks we deserialize the whole column once and extract subcolumns in memory.
            if (deserialize_binary_bulk_state_map_for_subcolumns.contains(name_in_storage))
                return;

            const auto & serialization = serializations_of_full_columns.at(name_in_storage);
            auto & state = deserialize_binary_bulk_state_map_for_subcolumns[name_in_storage];
            readPrefix(column, serialization, state, buffer_getter, nullptr, check_stream_exists_callback);
        }
    }
    else
    {
        const auto & serialization = serializations[column_idx];
        auto & state = deserialize_binary_bulk_state_map[column.name];
        readPrefix(column, serialization, state, buffer_getter, seek_to_substream_mark ? cache : nullptr, check_stream_exists_callback);
    }
}

void MergeTreeReaderCompact::readPrefix(
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    const InputStreamGetter & buffer_getter,
    ISerialization::SubstreamsDeserializeStatesCache * cache,
    ISerialization::DeserializeBinaryBulkSettings::CheckStreamExistsCallback check_stream_exists_callback)
{
    try
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.getter = buffer_getter;
        deserialize_settings.read_statistics = true;
        deserialize_settings.use_specialized_prefixes_and_suffixes_substreams = true;
        deserialize_settings.data_part_type = MergeTreeDataPartType::Compact;
        deserialize_settings.check_stream_exists_callback = std::move(check_stream_exists_callback);

        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, state, cache);
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading column " + name_and_type.name + ")");
        throw;
    }
}

void MergeTreeReaderCompact::createColumnsForReading(MutableColumns & res_columns) const
{
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (column_positions[i] && res_columns[i] == nullptr)
            res_columns[i] = columns_to_read[i].type->createColumn(*serializations[i]);
    }
}

bool MergeTreeReaderCompact::needSkipStream(size_t column_pos, const ISerialization::SubstreamPath & substream) const
{
    /// Offset stream can be read only from columns of current level or
    /// below (since it is OK to read all parent streams from the
    /// alternative).
    ///
    /// Consider the following columns in nested "root":
    /// - root.array Array(UInt8) - exists
    /// - root.nested_array Array(Array(UInt8)) - does not exist (only_offsets_level=1)
    ///
    /// For root.nested_array it will try to read multiple streams:
    /// - offsets (substream_path = {ArraySizes})
    ///   OK
    /// - root.nested_array elements (substream_path = {ArrayElements, ArraySizes})
    ///   NOT OK - cannot use root.array offsets stream for this
    ///
    /// Here only_offsets_level is the level of the alternative stream,
    /// and substream_path.size() is the level of the current stream.

    if (!columns_for_offsets[column_pos])
        return false;

    bool is_offsets = !substream.empty() && substream.back().type == ISerialization::Substream::ArraySizes;
    return !is_offsets || columns_for_offsets[column_pos]->level < ISerialization::getArrayLevel(substream);
}


size_t MergeTreeReaderCompact::readGranules(
    size_t from_mark, size_t current_range_last_mark, bool continue_reading, size_t max_rows_to_read,
    MutableColumns & res_columns, Columns & shared_columns)
{
    const size_t num_columns = columns_to_read.size();
    checkNumberOfColumns(num_columns);

    if (columns_cache_reads_possible || columns_cache_writes_possible)
    {
        if (shared_columns.size() != num_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} shared columns, got {}", num_columns, shared_columns.size());

        /// A call with `continue_reading == false` begins a new contiguous mark range, or re-enters
        /// the current one at another position after granules were skipped. Cache lookups are
        /// bounded by the contiguous mark range, not by the last mark of the whole task: the
        /// granules between the ranges of a multi-range task are not read.
        if (!continue_reading)
            startColumnsCacheRange(from_mark, current_range_last_mark ? current_range_last_mark : last_mark_to_read);

        return readGranulesWithColumnsCache(from_mark, max_rows_to_read, res_columns, shared_columns);
    }

    createColumnsForReading(res_columns);

    size_t read_rows = 0;
    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part_info_for_read->getIndexGranularity().getMarkRows(from_mark);
        readGranule(from_mark, rows_to_read, res_columns, nullptr);
        ++from_mark;
        read_rows += rows_to_read;
    }

    next_mark = from_mark;
    return read_rows;
}

void MergeTreeReaderCompact::startColumnsCacheRange(size_t from_mark, size_t end_mark)
{
    /// The previous range is over. The granules of it that were copied but not written yet -
    /// the range ended inside a stripe - are written now.
    finishAllAccumulations();
    flushPendingColumnsCacheWrites();

    range_first_mark = from_mark;
    range_end_mark = std::max(from_mark, std::min(end_mark, data_part_info_for_read->getIndexGranularity().getMarksCount()));
    range_first_stripe = stripes.stripeOf(from_mark);

    cached_entries.clear();
    if (columns_cache_reads_possible)
        cached_entries = lookupColumnsCache(range_first_mark, range_end_mark);
}

std::vector<std::vector<ColumnsCache::MappedPtr>> MergeTreeReaderCompact::lookupColumnsCache(size_t first_mark, size_t end_mark) const
{
    std::vector<std::vector<ColumnsCache::MappedPtr>> entries(columns_to_read.size());
    if (first_mark >= end_mark)
        return entries;

    const size_t first_stripe = stripes.stripeOf(first_mark);
    const size_t end_stripe = stripes.stripeOf(end_mark - 1) + 1;
    for (size_t pos = 0; pos < columns_to_read.size(); ++pos)
        if (column_cacheable[pos])
            entries[pos] = columns_cache->getMany(column_identities[pos], first_stripe, end_stripe);
    return entries;
}

const ColumnsCache::MappedPtr & MergeTreeReaderCompact::cachedEntryFor(size_t pos, size_t mark) const
{
    static const ColumnsCache::MappedPtr no_entry;
    if (pos >= cached_entries.size() || mark < range_first_mark)
        return no_entry;
    const size_t index = stripes.stripeOf(mark) - range_first_stripe;
    if (index >= cached_entries[pos].size())
        return no_entry;
    return cached_entries[pos][index];
}

bool MergeTreeReaderCompact::isGranuleColumnCached(size_t pos, size_t mark) const
{
    const auto & entry = cachedEntryFor(pos, mark);
    return entry && entry->coversMarks(mark, mark + 1);
}

/// The cache can still reject a stripe read whole for it (too large for a shard, budget used up, invalidated), leaving
/// a block larger than `preferred_block_size_bytes`, see `MergeTreeReaderWide::canReadWholeColumnsCacheStripe`.
bool MergeTreeReaderCompact::canReadWholeColumnsCacheStripe(size_t mark)
{
    if (has_columns_read_only_from_part)
        return false;

    const size_t stripe = stripes.stripeOf(mark);
    if (columns_cache_reads_possible)
    {
        const size_t first_mark = stripes.firstMark(stripe);
        const size_t end_mark = stripes.endMark(stripe, data_part_info_for_read->getIndexGranularity().getMarksCountWithoutFinal());

        /// The stripe is in the range being read: its lookup is done. Otherwise it is the beginning
        /// of the next range, which is not looked up yet.
        const bool in_range = range_first_mark <= first_mark && end_mark <= range_end_mark && !cached_entries.empty();
        const auto entries = in_range ? std::vector<std::vector<ColumnsCache::MappedPtr>>{} : lookupColumnsCache(first_mark, end_mark);

        bool served = first_mark < end_mark;
        for (size_t pos = 0; served && pos < columns_to_read.size(); ++pos)
        {
            if (!column_cacheable[pos])
                continue;
            const auto & entry = in_range ? cachedEntryFor(pos, first_mark) : entries[pos].front();
            served = entry && entry->coversMarks(first_mark, end_mark);
        }
        if (served)
            return true;
    }

    /// A stripe the cache would not admit is read as asked: a whole one would only be a larger block.
    return columns_cache_writes_possible && canWriteToColumnsCache() && admitStripeToColumnsCache(stripe);
}

size_t MergeTreeReaderCompact::readGranulesWithColumnsCache(
    size_t from_mark, size_t max_rows_to_read, MutableColumns & res_columns, Columns & shared_columns)
{
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
    const size_t num_columns = columns_to_read.size();

    /// The granules of the block: whole granules, at least `max_rows_to_read` rows.
    size_t end_mark = from_mark;
    size_t read_rows = 0;
    while (read_rows < max_rows_to_read)
        read_rows += index_granularity.getMarkRows(end_mark++);

    next_mark = end_mark;
    if (end_mark == from_mark)
    {
        finishAllAccumulations();
        flushPendingColumnsCacheWrites();
        return 0;
    }

    const size_t block_first_stripe = stripes.stripeOf(from_mark);
    const bool block_in_one_stripe = stripes.stripeOf(end_mark - 1) == block_first_stripe;
    const size_t block_first_stripe_end = stripes.endMark(block_first_stripe, index_granularity.getMarksCountWithoutFinal());

    /// Per column, whether every granule of the block is served from the cache, or read from the part.
    std::vector<bool> all_served(num_columns, false);
    std::vector<bool> all_from_disk(num_columns, false);
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        if (!column_positions[pos])
            continue;

        bool served = column_cacheable[pos];
        bool from_disk = true;
        for (size_t mark = from_mark; mark < end_mark && (served || from_disk); ++mark)
        {
            const bool cached = column_cacheable[pos] && isGranuleColumnCached(pos, mark);
            served = served && cached;
            from_disk = from_disk && !cached;
        }
        all_served[pos] = served;
        all_from_disk[pos] = from_disk;
    }

    /// A column served from the cache for the whole block is served stripe by stripe: one copy per
    /// stripe, and none for a block that is a whole entry.
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        if (!all_served[pos])
            continue;

        for (size_t mark = from_mark; mark < end_mark;)
        {
            const size_t chunk_end = std::min(end_mark, stripes.endMark(stripes.stripeOf(mark), index_granularity.getMarksCountWithoutFinal()));
            serveRowsFromColumnsCacheEntry(*cachedEntryFor(pos, mark), pos,
                index_granularity.getMarkStartingRow(mark), index_granularity.getRowsCountInRange(mark, chunk_end), res_columns, shared_columns);
            if (count_compressed_bytes_read)
                compressed_bytes_served_from_columns_cache += getCompressedBytesInPart(pos, mark, chunk_end);
            mark = chunk_end;
        }
        ProfileEvents::increment(ProfileEvents::ColumnsCacheHits, end_mark - from_mark);
    }

    /// The columns read from the part for the whole block that can become an entry as they are,
    /// without a copy: they hold nothing but the block, which is a whole stripe, or the beginning
    /// of one where the range ends, and does not continue a run copied by the previous block.
    std::vector<bool> written_as_is(num_columns, false);
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        const auto & accumulation = accumulations[pos];
        written_as_is[pos] = columns_cache_writes_possible && column_cacheable[pos] && all_from_disk[pos]
            && block_in_one_stripe && (end_mark == block_first_stripe_end || end_mark == range_end_mark)
            && !(accumulation.column && accumulation.stripe == block_first_stripe && accumulation.next_mark == from_mark)
            && (!res_columns[pos] || res_columns[pos]->empty()) && (!shared_columns[pos] || shared_columns[pos]->empty());
    }

    /// The other columns, granule by granule.
    std::vector<bool> columns_mask(num_columns);
    std::vector<size_t> sizes_before_reading(num_columns);
    for (size_t mark = from_mark; mark < end_mark; ++mark)
    {
        const size_t granule_rows = index_granularity.getMarkRows(mark);
        const size_t granule_row_begin = index_granularity.getMarkStartingRow(mark);

        bool any_from_disk = false;
        size_t served_columns = 0;
        size_t disk_columns = 0;
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            columns_mask[pos] = false;
            if (!column_positions[pos] || all_served[pos])
                continue;

            if (column_cacheable[pos] && isGranuleColumnCached(pos, mark))
            {
                serveRowsFromColumnsCacheEntry(*cachedEntryFor(pos, mark), pos, granule_row_begin, granule_rows, res_columns, shared_columns);
                if (count_compressed_bytes_read)
                    compressed_bytes_served_from_columns_cache += getCompressedBytesInPart(pos, mark, mark + 1);
                ++served_columns;
                continue;
            }

            auto & column = takeColumnForAppending(res_columns, shared_columns, pos);
            if (!column)
                column = columns_to_read[pos].type->createColumn(*serializations[pos]);
            sizes_before_reading[pos] = column->size();
            columns_mask[pos] = true;
            any_from_disk = true;
            if (column_cacheable[pos])
                ++disk_columns;
        }

        if (any_from_disk)
            readGranule(mark, granule_rows, res_columns, &columns_mask);

        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (!columns_mask[pos] || !column_cacheable[pos])
                continue;
            if (columns_cache_writes_possible && !written_as_is[pos])
                accumulateGranuleForColumnsCache(pos, mark, *res_columns[pos], sizes_before_reading[pos], granule_rows);
            else
                finishAccumulation(pos);
        }

        if (served_columns)
            ProfileEvents::increment(ProfileEvents::ColumnsCacheHits, served_columns);
        if (columns_cache_reads_possible && disk_columns)
            ProfileEvents::increment(ProfileEvents::ColumnsCacheMisses, disk_columns);
    }

    /// A served granule breaks the run of the granules copied for the column.
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        auto & accumulation = accumulations[pos];
        if (accumulation.column && accumulation.next_mark != end_mark)
            finishAccumulation(pos);
    }

    bool write_as_is = false;
    for (size_t pos = 0; pos < num_columns; ++pos)
        write_as_is = write_as_is || written_as_is[pos];

    if (write_as_is && canWriteToColumnsCache() && admitStripeToColumnsCache(block_first_stripe))
    {
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (!written_as_is[pos])
                continue;
            /// See `finishAccumulation`. The column is still uniquely owned here.
            res_columns[pos]->shrinkToFit();
            ColumnPtr column = std::move(res_columns[pos]);
            shared_columns[pos] = column;
            addPendingColumnsCacheEntry(pos, from_mark, end_mark, std::move(column));
        }
    }

    /// The range is read to its end: the granules copied for a stripe the range ends in are
    /// written now, as far as they go.
    if (end_mark >= range_end_mark)
        finishAllAccumulations();

    flushPendingColumnsCacheWrites();
    return read_rows;
}

void MergeTreeReaderCompact::accumulateGranuleForColumnsCache(size_t pos, size_t mark, const IColumn & column, size_t offset, size_t rows)
{
    auto & accumulation = accumulations[pos];
    const size_t stripe = stripes.stripeOf(mark);

    /// The granule continues the run when it is the next one of the same stripe. Anything else
    /// ends the run, and a new one begins with this granule.
    if (!accumulation.column || accumulation.stripe != stripe || accumulation.next_mark != mark)
    {
        finishAccumulation(pos);

        if (!canWriteToColumnsCache() || !admitStripeToColumnsCache(stripe))
            return;

        accumulation.stripe = stripe;
        accumulation.first_mark = mark;
        accumulation.column = column.cloneEmpty();
        /// The stripe at most; the copy is shrunk to its rows before it is admitted.
        accumulation.column->reserve(std::min(ColumnsCacheStripes::TARGET_ROWS * 2, stripes.stripe_marks * std::max<size_t>(rows, 1)));
    }

    accumulation.column->insertRangeFrom(column, offset, rows);
    accumulation.next_mark = mark + 1;

    /// The stripe is read to its end.
    if (accumulation.next_mark >= stripes.endMark(stripe, data_part_info_for_read->getIndexGranularity().getMarksCountWithoutFinal()))
        finishAccumulation(pos);
}

void MergeTreeReaderCompact::finishAccumulation(size_t pos)
{
    auto & accumulation = accumulations[pos];
    if (!accumulation.column)
        return;

    auto column = std::move(accumulation.column);
    accumulation.column = nullptr;
    if (column->empty())
        return;

    /// Give back the capacity the copy holds beyond its rows. `PODArray` rounds an allocation up
    /// to a power of two elements, and the copy grew as its rows came, so without this an entry
    /// would occupy - and, since the cache is bounded by the memory an entry retains, be charged
    /// for - up to twice the memory of its rows.
    column->shrinkToFit();
    addPendingColumnsCacheEntry(pos, accumulation.first_mark, accumulation.next_mark, std::move(column));
}

void MergeTreeReaderCompact::finishAllAccumulations()
{
    for (size_t pos = 0; pos < accumulations.size(); ++pos)
        finishAccumulation(pos);
}

}
