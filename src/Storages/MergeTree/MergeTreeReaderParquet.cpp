#include <Storages/MergeTree/MergeTreeReaderParquet.h>

#if USE_PARQUET

#include <Storages/MergeTree/MergeTreeDataPartParquet.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/ColumnsCache.h>
#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/ParquetMetadataCache.h>
#include <Formats/FormatFilterInfo.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NestedUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <Common/Hex.h>
#include <Common/SipHash.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace ProfileEvents
{
    extern const Event ReadCompressedBytes;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
}

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

struct MergeTreeReaderParquet::State
{
    /// Must outlive the reader, which reads from it.
    std::unique_ptr<ReadBufferFromFileBase> data_buffer;
    Parquet::Reader reader;

    /// The row group that is being read. It is the same as the mark.
    size_t row_group = 0;
    /// The next row to read in the row group.
    size_t row = 0;
    /// Whether any rows of the row group `row_group` were decoded.
    bool row_group_started = false;
    /// The decoded chunk of the row group that uses the columns cache, see `readRowsFromRowGroup`:
    /// the columns as in the file, for rows [chunk_start, chunk_start + COLUMNS_CACHE_CHUNK_ROWS).
    Columns chunk_columns;
    size_t chunk_start = 0;
};

MergeTreeReaderParquet::MergeTreeReaderParquet(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    NamesAndTypesList columns_,
    const VirtualFields & virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeSettingsPtr & storage_settings_,
    UncompressedCache * uncompressed_cache_,
    ColumnsCache * columns_cache_,
    MarkCache * mark_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_)
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
    , profile_callback(profile_callback_)
{
    fillColumns();
}

MergeTreeReaderParquet::~MergeTreeReaderParquet() = default;

size_t MergeTreeReaderParquet::addColumnInFile(const NameAndTypePair & column_in_part, size_t position_in_part)
{
    for (size_t i = 0; i < columns_in_file.size(); ++i)
        if (columns_in_file[i].position_in_part == position_in_part)
            return i;

    columns_in_file.push_back({column_in_part, position_in_part, /*binary=*/ false});
    return columns_in_file.size() - 1;
}

void MergeTreeReaderParquet::findColumnForOffsetsOfMissingNested(size_t pos, std::optional<size_t> & position_in_part)
{
    if (!(*storage_settings)[MergeTreeSetting::share_nested_offsets])
        return;

    auto & column = columns_to_read[pos];

    bool is_array = isArray(column.type);
    bool is_offsets_subcolumn = isArray(column.getTypeInStorage()) && column.getSubcolumnName() == "size0";

    if (!is_array && !is_offsets_subcolumn)
        return;

    NameAndTypePair column_in_storage{column.getNameInStorage(), column.getTypeInStorage()};

    /// If it is a part of Nested, we need to get the column from
    /// storage metadata which is converted to Nested type with subcolumns.
    if (isArray(column_in_storage.type))
    {
        auto [nested_name, nested_subcolumn_name] = Nested::splitName(column_in_storage.name);
        if (!nested_subcolumn_name.empty())
        {
            auto storage_columns_list = Nested::collect(storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All)));
            ColumnsDescription storage_columns_with_collected_nested(std::move(storage_columns_list));
            column_in_storage = storage_columns_with_collected_nested.getColumnOrSubcolumn(
                GetColumnsOptions::All, Nested::concatenateName(nested_name, nested_subcolumn_name));
        }
    }

    auto column_for_offsets = findColumnForOffsets(column_in_storage);
    if (!column_for_offsets)
        return;

    position_in_part = data_part_info_for_read->getColumnPosition(column_for_offsets->column.name);
    if (!position_in_part)
        return;

    size_t position_in_sample_block = addColumnInFile(column_for_offsets->column, *position_in_part);
    requested_columns[pos].position_in_sample_block = position_in_sample_block;

    if (is_offsets_subcolumn)
    {
        /// Read offsets from another array from the same Nested column.
        requested_columns[pos].subcolumn_name = column.getSubcolumnName();
    }
    else
    {
        /// Only the offsets are read, the column is filled later with default values.
        requested_columns[pos].only_offsets = true;
        partially_read_columns.insert(column.name);
    }
}

void MergeTreeReaderParquet::fillColumns()
{
    size_t columns_num = columns_to_read.size();
    requested_columns.resize(columns_num);

    for (size_t i = 0; i < columns_num; ++i)
    {
        const auto & column_to_read = columns_to_read[i];
        auto name_in_storage = column_to_read.getNameInStorage();
        auto position = data_part_info_for_read->getColumnPosition(name_in_storage);

        /// Column was dropped by a pending mutation or invalidated. Don't read stale data.
        if (position.has_value() && (isColumnDroppedByPendingMutation(i) || isSystemColumnInvalidated(i)))
            position.reset();

        if (position.has_value() && column_to_read.isSubcolumn())
        {
            auto storage_column_from_part = part_columns.getColumn(GetColumnsOptions::All, name_in_storage);
            if (!hasSubcolumnInPart(name_in_storage, *storage_column_from_part.type, column_to_read.getSubcolumnName()))
                position.reset();
        }

        if (!position)
        {
            /// If array of Nested column is missing in part,
            /// we have to read its offsets if they exist.
            findColumnForOffsetsOfMissingNested(i, position);
            continue;
        }

        auto column_in_part = part_columns.getColumn(GetColumnsOptions::All, name_in_storage);
        requested_columns[i].position_in_sample_block = addColumnInFile(column_in_part, *position);
        if (column_to_read.isSubcolumn())
            requested_columns[i].subcolumn_name = column_to_read.getSubcolumnName();
    }
}

namespace
{

/// Names of the top-level columns in the schema of the Parquet file.
Strings getTopLevelColumnNames(const Parquet::parq::FileMetaData & file_metadata)
{
    const auto & schema = file_metadata.schema;
    if (schema.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Empty schema in Parquet file");

    Strings names;
    size_t num_top_level_columns = static_cast<size_t>(std::max(schema[0].num_children, 0));
    size_t schema_idx = 1;
    for (size_t i = 0; i < num_top_level_columns; ++i)
    {
        if (schema_idx >= schema.size())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid schema tree in Parquet file");

        names.push_back(schema[schema_idx].name);

        /// Skip the subtree of the column.
        size_t elements_to_skip = 1;
        while (elements_to_skip > 0)
        {
            if (schema_idx >= schema.size())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid schema tree in Parquet file");
            elements_to_skip += static_cast<size_t>(std::max(schema[schema_idx].num_children, 0));
            --elements_to_skip;
            ++schema_idx;
        }
    }

    return names;
}

/// Whether the columns of the file are stored in the ClickHouse binary format, from the key-value metadata.
std::vector<bool> getBinaryColumns(const Parquet::parq::FileMetaData & file_metadata, size_t num_columns)
{
    for (const auto & key_value : file_metadata.key_value_metadata)
    {
        if (key_value.key != MergeTreeDataPartParquet::COLUMNS_METADATA_KEY)
            continue;

        Poco::JSON::Parser parser;
        auto metadata = parser.parse(key_value.value).extract<Poco::JSON::Object::Ptr>();
        auto columns = metadata->getArray("columns");
        if (!columns || columns->size() != num_columns)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "The description of columns in the metadata of Parquet file has {} columns, but the file has {} columns",
                columns ? columns->size() : 0, num_columns);

        std::vector<bool> result(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
        {
            auto encoding = columns->getObject(static_cast<unsigned>(i))->getValue<String>("encoding");
            if (encoding == "binary")
                result[i] = true;
            else if (encoding != "parquet")
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown encoding of column in the metadata of Parquet file: {}", encoding);
        }
        return result;
    }

    throw Exception(ErrorCodes::CORRUPTED_DATA, "No key {} in the metadata of Parquet file", MergeTreeDataPartParquet::COLUMNS_METADATA_KEY);
}

}

std::shared_ptr<const Parquet::ColumnsCacheOptions> MergeTreeReaderParquet::getColumnsCacheOptions() const
{
    /// The entries of a part are keyed by the UUID of the table and the name of the part, like the entries of Wide parts,
    /// so they are removed with the part. The names of projection parts are not unique.
    if (!columns_cache
        || (!settings.enable_columns_cache_reads && !settings.enable_columns_cache_writes)
        || data_part_info_for_read->getTableUUID() == UUIDHelpers::Nil
        || data_part_info_for_read->isProjectionPart())
        return nullptr;

    auto cache = data_part_info_for_read->getContext()->getColumnsCache();
    if (!cache || cache.get() != columns_cache || cache->maxSizeInBytes() == 0)
        return nullptr;

    auto options = std::make_shared<Parquet::ColumnsCacheOptions>();
    options->cache = std::move(cache);
    options->file_path = data_part_info_for_read->getPartName();
    options->table_uuid = data_part_info_for_read->getTableUUID();
    options->enable_reads = settings.enable_columns_cache_reads;
    options->enable_writes = settings.enable_columns_cache_writes
        && !(settings.columns_cache_writes_disabled && settings.columns_cache_writes_disabled->load(std::memory_order_relaxed));
    options->max_bytes_to_write = settings.columns_cache_max_bytes_to_write_to_cache;
    /// The read pool has charged the estimate budget for this part, see `MergeTreeReadPoolBase::stageColumnsCacheWriteEstimate`.
    options->charge_estimated_bytes = false;
    options->write_budget = settings.columns_cache_write_budget;
    if (!options->write_budget)
        options->write_budget = std::make_shared<ColumnsCacheWriteBudget>();

    /// The schema of the query and the content of the data file. The name of the part already identifies its content,
    /// the checksum makes it explicit.
    SipHash hash;
    hash.update(settings.columns_cache_schema_identity);
    const auto & checksums = data_part_info_for_read->getChecksums();
    if (auto it = checksums.files.find(MergeTreeDataPartParquet::DATA_FILE_NAME_WITH_EXTENSION); it != checksums.files.end())
    {
        hash.update(it->second.file_hash.low64);
        hash.update(it->second.file_hash.high64);
    }
    options->file_identity = hash.get64();
    return options;
}

void MergeTreeReaderParquet::initState()
{
    state = std::make_unique<State>();

    /// Nothing is read from the file, only the numbers of rows in the row groups (granules) are needed.
    if (columns_in_file.empty())
        return;

    const auto & data_part_storage = data_part_info_for_read->getDataPartStorage();
    const String data_file_name = MergeTreeDataPartParquet::DATA_FILE_NAME_WITH_EXTENSION;
    size_t file_size = data_part_info_for_read->getFileSizeOrZero(data_file_name);

    /// The Parquet reader reads arbitrary byte ranges of the file at arbitrary offsets, which `O_DIRECT` doesn't allow.
    auto read_settings = settings.read_settings;
    read_settings.local_fs_settings.direct_io_threshold = 0;
    state->data_buffer = data_part_storage->readFile(data_file_name, read_settings, file_size);
    if (profile_callback)
        state->data_buffer->setProfileCallback(profile_callback, CLOCK_MONOTONIC_COARSE);

    Parquet::ReadOptions options;
    options.format.seekable_read = true;
    options.format.parquet.allow_missing_columns = false;
    options.format.parquet.case_insensitive_column_matching = false;
    options.format.parquet.filter_push_down = false;
    options.format.parquet.bloom_filter_push_down = false;
    options.format.parquet.dictionary_filter_push_down = 0;
    options.format.parquet.page_filter_push_down = false;
    options.format.parquet.use_offset_index = false;
    options.format.parquet.enable_json_parsing = false;
    options.format.parquet.allow_geoparquet_parser = false;
    options.format.parquet.spatial_filter_push_down = false;
    options.format.parquet.detect_variant_by_structure = false;
    options.format.parquet.verify_checksums = true;
    options.format.defaults_for_omitted_fields = false;
    options.dictionary_filter_limit_bytes = 0;
    options.columns_cache = getColumnsCacheOptions();
    /// The columns of the part are the top-level fields of the file, e.g. a column `m.value` is not the field
    /// `value` of the map `m`.
    options.match_only_top_level_columns = true;

    auto & reader = state->reader;
    reader.prefetcher.init(state->data_buffer.get(), options, /*parser_shared_resources_=*/ nullptr);

    auto load_footer = [&] { return Parquet::Reader::readFileFooter(reader.prefetcher, /*footer_read_size=*/ 0); };

    auto metadata_cache = data_part_info_for_read->getContext()->getParquetMetadataCache();
    const auto & checksums = data_part_info_for_read->getChecksums();
    auto checksum_it = checksums.files.find(data_file_name);
    std::optional<ParquetMetadataCacheKey> cache_key;
    ParquetMetadataCache::MappedPtr cache_cell;
    bool cache_hit = false;
    if (metadata_cache && checksum_it != checksums.files.end())
    {
        /// The files of data parts are immutable, and their checksum identifies the content.
        cache_key = ParquetMetadataCache::createKey(
            data_part_storage->getDiskName() + ":" + data_part_storage->getFullPath() + data_file_name,
            getHexUIntLowercase(checksum_it->second.file_hash.high64) + getHexUIntLowercase(checksum_it->second.file_hash.low64));
        std::tie(cache_cell, cache_hit) = metadata_cache->getOrSetMetadata(*cache_key, load_footer);
        reader.setFileFooter(cache_cell->footer, cache_cell->column_chunks);
    }
    else
    {
        reader.setFileFooter(load_footer());
    }

    if (sample_block.empty())
    {
        auto names_in_file = getTopLevelColumnNames(reader.file_metadata());
        size_t num_columns_in_part = data_part_info_for_read->getColumns().size();
        if (names_in_file.size() != num_columns_in_part)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Parquet file {} has {} columns, but the part has {} columns",
                data_file_name, names_in_file.size(), num_columns_in_part);

        auto binary_columns = getBinaryColumns(reader.file_metadata(), names_in_file.size());

        for (auto & column : columns_in_file)
        {
            column.binary = binary_columns.at(column.position_in_part);
            DataTypePtr type_in_file = column.binary ? std::make_shared<DataTypeString>() : column.column_in_part.type;
            sample_block.insert({type_in_file->createColumn(), type_in_file, names_in_file.at(column.position_in_part)});
        }
    }

    reader.init(options, sample_block, std::make_shared<FormatFilterInfo>());

    /// All row groups are initialized (they correspond to the marks one to one), but only the ones
    /// of the mark ranges of this reader are looked up in the columns cache.
    std::unordered_set<UInt64> row_groups_to_read;
    for (const auto & range : all_mark_ranges)
        for (size_t mark = range.begin; mark < range.end; ++mark)
            row_groups_to_read.insert(mark);
    reader.prefilterAndInitRowGroups(row_groups_to_read);
    /// Not after the miss that inserted the cell: replacing it would move it to SLRU's protected queue.
    if (cache_hit)
        metadata_cache->addColumnChunks(*cache_key, cache_cell, reader.getColumnChunks());

    /// All row groups are read (there are no filters), so they correspond to the marks one to one.
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
    size_t num_row_groups = reader.file_metadata().row_groups.size();
    if (num_row_groups != index_granularity.getMarksCountWithoutFinal())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Parquet file {} has {} row groups, but the part has {} marks",
            data_file_name, num_row_groups, index_granularity.getMarksCountWithoutFinal());

    for (size_t i = 0; i < num_row_groups; ++i)
    {
        size_t rows_in_row_group = static_cast<size_t>(reader.file_metadata().row_groups[i].num_rows);
        if (rows_in_row_group != index_granularity.getMarkRows(i))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Row group {} of Parquet file {} has {} rows, but the mark has {} rows",
                i, data_file_name, rows_in_row_group, index_granularity.getMarkRows(i));
    }

    if (reader.row_groups.size() != num_row_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Parquet reader has {} row groups, but the file has {} row groups", reader.row_groups.size(), num_row_groups);
}

void MergeTreeReaderParquet::finishRowGroup(size_t row_group)
{
    if (columns_in_file.empty() || row_group >= state->reader.row_groups.size())
        return;

    state->chunk_columns.clear();

    /// Release the memory of the column chunks, they are not needed anymore.
    Parquet::MemoryUsageDiff diff(Parquet::ReadStage::ColumnData);
    for (auto & column : state->reader.row_groups[row_group].columns)
    {
        column.dictionary.reset();
        for (auto & page : column.data_pages)
            page.prefetch.reset(&diff);
        column.bloom_filter_header_prefetch.reset(&diff);
        column.bloom_filter_data_prefetch.reset(&diff);
        column.dictionary_page_prefetch.reset(&diff);
        column.column_index_prefetch.reset(&diff);
        column.offset_index_prefetch.reset(&diff);
        column.data_pages_prefetch.reset(&diff);
        for (auto & block : column.bloom_filter_blocks)
            block.prefetch.reset(&diff);
        column = {};
    }
    diff.finalized = true;
}

void MergeTreeReaderParquet::seek(size_t row_group)
{
    if (state->row_group == row_group && !state->row_group_started)
        return;

    /// The decoder of a column chunk can only move forward, so start anew to read a row group again.
    if (row_group < state->row_group || (row_group == state->row_group && state->row_group_started))
    {
        initState();
    }
    else
    {
        finishRowGroup(state->row_group);
    }

    state->row_group = row_group;
    state->row = 0;
    state->row_group_started = false;
}

void MergeTreeReaderParquet::readRowsFromRowGroup(size_t row_group, size_t start_row, size_t num_rows, MutableColumns & res_columns)
{
    if (columns_in_file.empty())
        return;

    const auto & row_group_state = state->reader.row_groups.at(row_group);
    const bool uses_columns_cache = !row_group_state.cached_columns.empty() || !row_group_state.columns_cache_writes.empty();
    if (!uses_columns_cache)
    {
        appendRows(decodeRows(row_group, start_row, num_rows), 0, num_rows, res_columns);
        return;
    }

    /// The columns cache holds a row group in chunks of `COLUMNS_CACHE_CHUNK_ROWS` rows: a column is formed from
    /// the cache only for rows within one chunk, and a chunk is written to the cache only when it is decoded in full.
    /// So the rows are decoded a whole chunk at a time, and the requested rows are taken from the decoded chunk.
    const size_t chunk_rows_max = Parquet::Reader::COLUMNS_CACHE_CHUNK_ROWS;
    const size_t rows_in_row_group = size_t(row_group_state.meta->num_rows);
    while (num_rows > 0)
    {
        const size_t chunk_start = start_row / chunk_rows_max * chunk_rows_max;
        const size_t chunk_rows = std::min(chunk_rows_max, rows_in_row_group - chunk_start);
        if (state->chunk_columns.empty() || state->chunk_start != chunk_start)
        {
            /// Release the previous chunk before decoding the next one.
            state->chunk_columns.clear();
            state->chunk_columns = decodeRows(row_group, chunk_start, chunk_rows);
            state->chunk_start = chunk_start;
        }

        const size_t rows = std::min(num_rows, chunk_start + chunk_rows - start_row);
        appendRows(state->chunk_columns, start_row - chunk_start, rows, res_columns);
        start_row += rows;
        num_rows -= rows;
    }
}

Columns MergeTreeReaderParquet::decodeRows(size_t row_group, size_t start_row, size_t num_rows)
{
    auto & reader = state->reader;
    auto & row_group_state = reader.row_groups.at(row_group);

    /// The column chunks are read when the first rows of the row group are read.
    if (!state->row_group_started)
    {
        size_t compressed_bytes = 0;
        for (const auto & column_info : reader.primitive_columns)
            compressed_bytes += static_cast<size_t>(reader.getColumnChunk(row_group, column_info.column_idx).meta_data.total_compressed_size);
        ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, compressed_bytes);
        state->row_group_started = true;
    }

    Parquet::Reader::RowSubgroup subgroup;
    subgroup.start_row_idx = start_row;
    subgroup.filter.rows_total = num_rows;
    subgroup.filter.rows_pass = num_rows;
    subgroup.columns.resize(reader.primitive_columns.size());
    subgroup.output = std::vector<Parquet::Reader::OutputColumnState>(reader.extended_sample_block.columns());
    for (size_t idx = 0; idx < subgroup.output.size(); ++idx)
    {
        const auto & output_idx = reader.sample_block_to_output_columns_idx.at(idx);
        if (output_idx.has_value())
        {
            const auto & info = reader.output_columns.at(*output_idx);
            /// A column served from the columns cache is formed from the cached column, not from primitive columns.
            const bool from_cache = !row_group_state.cached_columns.empty() && !row_group_state.cached_columns[idx].empty();
            subgroup.output[idx].primitive_columns_remaining.store(from_cache ? 0 : info.primitive_end - info.primitive_start);
        }
    }

    Parquet::MemoryUsageDiff diff(Parquet::ReadStage::ColumnData);
    try
    {
        for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
        {
            auto & column = row_group_state.columns.at(i);
            const auto & column_info = reader.primitive_columns.at(i);

            /// The data of the column is neither read nor decoded.
            if (column.served_from_cache)
                continue;

            std::vector<Parquet::PrefetchHandle *> prefetches;
            if (!column.dictionary.isInitialized() && column.dictionary_page_prefetch)
                prefetches.push_back(&column.dictionary_page_prefetch);
            if (column.data_pages.empty() && column.data_pages_prefetch)
                prefetches.push_back(&column.data_pages_prefetch);
            reader.prefetcher.startPrefetch(prefetches, &diff);

            if (!column.dictionary.isInitialized() && column.dictionary_page_prefetch)
            {
                if (!reader.decodeDictionaryPage(column, column_info))
                    column.dictionary_page_prefetch.reset(&diff);
            }

            reader.decodePrimitiveColumn(column, column_info, subgroup.columns.at(i), row_group_state, subgroup);
        }

        Columns columns_from_file(sample_block.columns());
        for (size_t i = 0; i < columns_from_file.size(); ++i)
        {
            ColumnPtr column = std::move(reader.getOrFormOutputColumn(row_group_state, subgroup, i));
            if (column->size() != num_rows)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Read {} rows of column {} from Parquet file, expected {}", column->size(), sample_block.getByPosition(i).name, num_rows);
            columns_from_file[i] = std::move(column);
        }

        for (auto & column : subgroup.columns)
            column.column_and_offsets_memory.reset(&diff);
        diff.finalized = true;
        return columns_from_file;
    }
    catch (...)
    {
        diff.finalized = true;
        throw;
    }
}

void MergeTreeReaderParquet::appendRows(const Columns & columns_from_file, size_t start, size_t num_rows, MutableColumns & res_columns)
{
    Columns columns(columns_from_file.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        ColumnPtr column = columns_from_file[i];
        if (start != 0 || num_rows != column->size())
            column = column->cut(start, num_rows);

        const auto & column_in_file = columns_in_file[i];
        if (column_in_file.binary)
        {
            const auto & type = column_in_file.column_in_part.type;
            auto serialization = type->getDefaultSerialization();
            auto result = type->createColumn();
            result->reserve(num_rows);

            const auto & binary_column = assert_cast<const ColumnString &>(*column);
            FormatSettings format_settings;
            for (size_t row = 0; row < num_rows; ++row)
            {
                auto value = binary_column.getDataAt(row);
                ReadBufferFromMemory buf(value.data(), value.size());
                serialization->deserializeBinary(*result, buf, format_settings);
                if (!buf.eof())
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "Value of column {} in binary format in Parquet file has extra bytes", column_in_file.column_in_part.name);
            }
            column = std::move(result);
        }

        columns[i] = std::move(column);
    }

    for (size_t pos = 0; pos < requested_columns.size(); ++pos)
    {
        const auto & requested = requested_columns[pos];
        if (!requested.position_in_sample_block || !res_columns[pos])
            continue;

        const auto & column_in_file = columns_in_file[*requested.position_in_sample_block];
        ColumnPtr column = columns[*requested.position_in_sample_block];

        if (requested.only_offsets)
        {
            /// Only the offsets are needed, the column is filled with default values later in `fillMissingColumns`.
            const auto & offsets = assert_cast<const ColumnArray &>(*column).getOffsets();
            auto & result_offsets = assert_cast<ColumnArray &>(*res_columns[pos]).getOffsets();
            IColumn::Offset base = result_offsets.empty() ? 0 : result_offsets.back();
            for (auto offset : offsets)
                result_offsets.push_back(base + offset);
            continue;
        }

        if (requested.subcolumn_name)
            column = column_in_file.column_in_part.type->getSubcolumn(*requested.subcolumn_name, column);

        res_columns[pos]->insertRangeFrom(*column, 0, column->size());
    }
}

void MergeTreeReaderParquet::updateAllMarkRanges(const MarkRanges & ranges)
{
    IMergeTreeReader::updateAllMarkRanges(ranges);

    /// The row groups that are looked up in the columns cache and written to it are the ones of the mark ranges
    /// at the time the state is created, so it is created anew for the ranges of the next task.
    if (state && state->reader.options.columns_cache)
        state.reset();
}

size_t MergeTreeReaderParquet::readRows(
    size_t from_mark,
    size_t /* current_range_last_mark */,
    bool continue_reading,
    size_t max_rows_to_read,
    MutableColumns & res_columns,
    Columns & /* shared_columns */)
try
{
    if (!state)
        initState();

    if (!continue_reading)
        seek(from_mark);

    checkNumberOfColumns(columns_to_read.size());

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (requested_columns[i].position_in_sample_block && res_columns[i] == nullptr)
            res_columns[i] = columns_to_read[i].type->createColumn(*serializations[i]);
    }

    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
    size_t num_row_groups = index_granularity.getMarksCountWithoutFinal();

    size_t read_rows = 0;
    while (read_rows < max_rows_to_read && state->row_group < num_row_groups)
    {
        size_t rows_in_row_group = index_granularity.getMarkRows(state->row_group);
        if (state->row >= rows_in_row_group)
        {
            finishRowGroup(state->row_group);
            ++state->row_group;
            state->row = 0;
            state->row_group_started = false;
            continue;
        }

        size_t rows_to_read = std::min(max_rows_to_read - read_rows, rows_in_row_group - state->row);
        readRowsFromRowGroup(state->row_group, state->row, rows_to_read, res_columns);
        state->row_group_started = true;

        state->row += rows_to_read;
        read_rows += rows_to_read;

        /// The whole row group is read, so its columns can be written to the columns cache.
        if (state->row == rows_in_row_group && !columns_in_file.empty())
            state->reader.flushColumnsCacheWrites(state->reader.row_groups.at(state->row_group));
    }

    return read_rows;
}
catch (...)
{
    if (!isRetryableException(std::current_exception()))
        data_part_info_for_read->reportBroken();

    /// Better diagnostics.
    try
    {
        rethrow_exception(std::current_exception());
    }
    catch (Exception & e)
    {
        e.addMessage(getMessageForDiagnosticOfBrokenPart(from_mark, max_rows_to_read));
    }

    throw;
}

}

#endif
