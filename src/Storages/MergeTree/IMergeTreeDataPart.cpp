#include "IMergeTreeDataPart.h"

#include <optional>
#include <Core/Defines.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/localBackup.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/FileSyncGuard.h>
#include <common/JSON.h>
#include <common/logger_useful.h>
#include <Compression/getCompressionCodecForFile.h>
#include <Parsers/queryToString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int EXPECTED_END_OF_FILE;
    extern const int CORRUPTED_DATA;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int BAD_TTL_FILE;
    extern const int NOT_IMPLEMENTED;
}

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const DiskPtr & disk, const String & path)
{
    return disk->readFile(path, std::min(size_t(DBMS_DEFAULT_BUFFER_SIZE), disk->getFileSize(path)));
}

void IMergeTreeDataPart::MinMaxIndex::load(const MergeTreeData & data, const DiskPtr & disk_, const String & part_path)
{
    size_t minmax_idx_size = data.minmax_idx_column_types.size();
    hyperrectangle.reserve(minmax_idx_size);
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = part_path + "minmax_" + escapeForFileName(data.minmax_idx_columns[i]) + ".idx";
        auto file = openForReading(disk_, file_name);
        const DataTypePtr & data_type = data.minmax_idx_column_types[i];

        Field min_val;
        data_type->deserializeBinary(min_val, *file);
        Field max_val;
        data_type->deserializeBinary(max_val, *file);

        hyperrectangle.emplace_back(min_val, true, max_val, true);
    }
    initialized = true;
}

void IMergeTreeDataPart::MinMaxIndex::store(
    const MergeTreeData & data, const DiskPtr & disk_, const String & part_path, Checksums & out_checksums) const
{
    store(data.minmax_idx_columns, data.minmax_idx_column_types, disk_, part_path, out_checksums);
}

void IMergeTreeDataPart::MinMaxIndex::store(
    const Names & column_names,
    const DataTypes & data_types,
    const DiskPtr & disk_,
    const String & part_path,
    Checksums & out_checksums) const
{
    if (!initialized)
        throw Exception("Attempt to store uninitialized MinMax index for part " + part_path + ". This is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        String file_name = "minmax_" + escapeForFileName(column_names[i]) + ".idx";
        const DataTypePtr & data_type = data_types.at(i);

        auto out = disk_->writeFile(part_path + file_name);
        HashingWriteBuffer out_hashing(*out);
        data_type->serializeBinary(hyperrectangle[i].left, out_hashing);
        data_type->serializeBinary(hyperrectangle[i].right, out_hashing);
        out_hashing.next();
        out_checksums.files[file_name].file_size = out_hashing.count();
        out_checksums.files[file_name].file_hash = out_hashing.getHash();
        out->finalize();
    }
}

void IMergeTreeDataPart::MinMaxIndex::update(const Block & block, const Names & column_names)
{
    if (!initialized)
        hyperrectangle.reserve(column_names.size());

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        FieldRef min_value;
        FieldRef max_value;
        const ColumnWithTypeAndName & column = block.getByName(column_names[i]);
        column.column->getExtremes(min_value, max_value);

        if (!initialized)
            hyperrectangle.emplace_back(min_value, true, max_value, true);
        else
        {
            hyperrectangle[i].left = std::min(hyperrectangle[i].left, min_value);
            hyperrectangle[i].right = std::max(hyperrectangle[i].right, max_value);
        }
    }

    initialized = true;
}

void IMergeTreeDataPart::MinMaxIndex::merge(const MinMaxIndex & other)
{
    if (!other.initialized)
        return;

    if (!initialized)
    {
        hyperrectangle = other.hyperrectangle;
        initialized = true;
    }
    else
    {
        for (size_t i = 0; i < hyperrectangle.size(); ++i)
        {
            hyperrectangle[i].left = std::min(hyperrectangle[i].left, other.hyperrectangle[i].left);
            hyperrectangle[i].right = std::max(hyperrectangle[i].right, other.hyperrectangle[i].right);
        }
    }
}


IMergeTreeDataPart::IMergeTreeDataPart(
    MergeTreeData & storage_, const String & name_, const VolumePtr & volume_, const std::optional<String> & relative_path_, Type part_type_)
    : storage(storage_)
    , name(name_)
    , info(MergeTreePartInfo::fromPartName(name_, storage.format_version))
    , volume(volume_)
    , relative_path(relative_path_.value_or(name_))
    , index_granularity_info(storage_, part_type_)
    , part_type(part_type_)
{
}

IMergeTreeDataPart::IMergeTreeDataPart(
    const MergeTreeData & storage_,
    const String & name_,
    const MergeTreePartInfo & info_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    Type part_type_)
    : storage(storage_)
    , name(name_)
    , info(info_)
    , volume(volume_)
    , relative_path(relative_path_.value_or(name_))
    , index_granularity_info(storage_, part_type_)
    , part_type(part_type_)
{
}


String IMergeTreeDataPart::getNewName(const MergeTreePartInfo & new_part_info) const
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// NOTE: getting min and max dates from the part name (instead of part data) because we want
        /// the merged part name be determined only by source part names.
        /// It is simpler this way when the real min and max dates for the block range can change
        /// (e.g. after an ALTER DELETE command).
        DayNum min_date;
        DayNum max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);
        return new_part_info.getPartNameV0(min_date, max_date);
    }
    else
        return new_part_info.getPartName();
}

std::optional<size_t> IMergeTreeDataPart::getColumnPosition(const String & column_name) const
{
    auto it = column_name_to_position.find(column_name);
    if (it == column_name_to_position.end())
        return {};
    return it->second;
}

DayNum IMergeTreeDataPart::getMinDate() const
{
    if (storage.minmax_idx_date_column_pos != -1 && minmax_idx.initialized)
        return DayNum(minmax_idx.hyperrectangle[storage.minmax_idx_date_column_pos].left.get<UInt64>());
    else
        return DayNum();
}


DayNum IMergeTreeDataPart::getMaxDate() const
{
    if (storage.minmax_idx_date_column_pos != -1 && minmax_idx.initialized)
        return DayNum(minmax_idx.hyperrectangle[storage.minmax_idx_date_column_pos].right.get<UInt64>());
    else
        return DayNum();
}

time_t IMergeTreeDataPart::getMinTime() const
{
    if (storage.minmax_idx_time_column_pos != -1 && minmax_idx.initialized)
        return minmax_idx.hyperrectangle[storage.minmax_idx_time_column_pos].left.get<UInt64>();
    else
        return 0;
}


time_t IMergeTreeDataPart::getMaxTime() const
{
    if (storage.minmax_idx_time_column_pos != -1 && minmax_idx.initialized)
        return minmax_idx.hyperrectangle[storage.minmax_idx_time_column_pos].right.get<UInt64>();
    else
        return 0;
}

void IMergeTreeDataPart::setColumns(const NamesAndTypesList & new_columns)
{
    columns = new_columns;
    column_name_to_position.clear();
    column_name_to_position.reserve(new_columns.size());
    size_t pos = 0;
    for (const auto & column : columns)
        column_name_to_position.emplace(column.name, pos++);
}

IMergeTreeDataPart::~IMergeTreeDataPart() = default;

void IMergeTreeDataPart::removeIfNeeded()
{
    if (state == State::DeleteOnDestroy || is_temp)
    {
        try
        {
            auto path = getFullRelativePath();

            if (!volume->getDisk()->exists(path))
                return;

            if (is_temp)
            {
                String file_name = fileName(relative_path);

                if (file_name.empty())
                    throw Exception("relative_path " + relative_path + " of part " + name + " is invalid or not set", ErrorCodes::LOGICAL_ERROR);

                if (!startsWith(file_name, "tmp"))
                {
                    LOG_ERROR(storage.log, "~DataPart() should remove part {} but its name doesn't start with tmp. Too suspicious, keeping the part.", path);
                    return;
                }
            }

            remove();

            if (state == State::DeleteOnDestroy)
            {
                LOG_TRACE(storage.log, "Removed part from old location {}", path);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


UInt64 IMergeTreeDataPart::getIndexSizeInBytes() const
{
    UInt64 res = 0;
    for (const ColumnPtr & column : index)
        res += column->byteSize();
    return res;
}

UInt64 IMergeTreeDataPart::getIndexSizeInAllocatedBytes() const
{
    UInt64 res = 0;
    for (const ColumnPtr & column : index)
        res += column->allocatedBytes();
    return res;
}

String IMergeTreeDataPart::stateToString(IMergeTreeDataPart::State state)
{
    switch (state)
    {
        case State::Temporary:
            return "Temporary";
        case State::PreCommitted:
            return "PreCommitted";
        case State::Committed:
            return "Committed";
        case State::Outdated:
            return "Outdated";
        case State::Deleting:
            return "Deleting";
        case State::DeleteOnDestroy:
            return "DeleteOnDestroy";
    }

    __builtin_unreachable();
}

String IMergeTreeDataPart::stateString() const
{
    return stateToString(state);
}

void IMergeTreeDataPart::assertState(const std::initializer_list<IMergeTreeDataPart::State> & affordable_states) const
{
    if (!checkState(affordable_states))
    {
        String states_str;
        for (auto affordable_state : affordable_states)
            states_str += stateToString(affordable_state) + " ";

        throw Exception("Unexpected state of part " + getNameWithState() + ". Expected: " + states_str, ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);
    }
}

void IMergeTreeDataPart::assertOnDisk() const
{
    if (!isStoredOnDisk())
        throw Exception("Data part '" + name + "' with type '"
            + getType().toString() + "' is not stored on disk", ErrorCodes::LOGICAL_ERROR);
}


UInt64 IMergeTreeDataPart::getMarksCount() const
{
    return index_granularity.getMarksCount();
}

size_t IMergeTreeDataPart::getFileSizeOrZero(const String & file_name) const
{
    auto checksum = checksums.files.find(file_name);
    if (checksum == checksums.files.end())
        return 0;
    return checksum->second.file_size;
}

String IMergeTreeDataPart::getColumnNameWithMinimumCompressedSize(const StorageMetadataPtr & metadata_snapshot) const
{
    const auto & storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    auto alter_conversions = storage.getAlterConversionsForPart(shared_from_this());

    std::optional<std::string> minimum_size_column;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();

    for (const auto & column : storage_columns)
    {
        auto column_name = column.name;
        auto column_type = column.type;
        if (alter_conversions.isColumnRenamed(column.name))
            column_name = alter_conversions.getColumnOldName(column.name);

        if (!hasColumnFiles(column_name, *column_type))
            continue;

        const auto size = getColumnSize(column_name, *column_type).data_compressed;
        if (size < minimum_size)
        {
            minimum_size = size;
            minimum_size_column = column_name;
        }
    }

    if (!minimum_size_column)
        throw Exception("Could not find a column of minimum size in MergeTree, part " + getFullPath(), ErrorCodes::LOGICAL_ERROR);

    return *minimum_size_column;
}

String IMergeTreeDataPart::getFullPath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);

    return storage.getFullPathOnDisk(volume->getDisk()) + relative_path + "/";
}

String IMergeTreeDataPart::getFullRelativePath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);

    return storage.relative_data_path + relative_path + "/";
}

void IMergeTreeDataPart::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency)
{
    assertOnDisk();

    /// Memory should not be limited during ATTACH TABLE query.
    /// This is already true at the server startup but must be also ensured for manual table ATTACH.
    /// Motivation: memory for index is shared between queries - not belong to the query itself.
    MemoryTracker::BlockerInThread temporarily_disable_memory_tracker;

    loadUUID();
    loadColumns(require_columns_checksums);
    loadChecksums(require_columns_checksums);
    loadIndexGranularity();
    calculateColumnsSizesOnDisk();
    loadIndex();     /// Must be called after loadIndexGranularity as it uses the value of `index_granularity`
    loadRowsCount(); /// Must be called after loadIndexGranularity() as it uses the value of `index_granularity`.
    loadPartitionAndMinMaxIndex();
    loadTTLInfos();
    if (check_consistency)
        checkConsistency(require_columns_checksums);
    loadDefaultCompressionCodec();

}

void IMergeTreeDataPart::loadIndexGranularity()
{
    throw Exception("Method 'loadIndexGranularity' is not implemented for part with type " + getType().toString(), ErrorCodes::NOT_IMPLEMENTED);
}

void IMergeTreeDataPart::loadIndex()
{
    /// It can be empty in case of mutations
    if (!index_granularity.isInitialized())
        throw Exception("Index granularity is not loaded before index loading", ErrorCodes::LOGICAL_ERROR);

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    size_t key_size = primary_key.column_names.size();

    if (key_size)
    {
        MutableColumns loaded_index;
        loaded_index.resize(key_size);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i] = primary_key.data_types[i]->createColumn();
            loaded_index[i]->reserve(index_granularity.getMarksCount());
        }

        String index_path = getFullRelativePath() + "primary.idx";
        auto index_file = openForReading(volume->getDisk(), index_path);

        size_t marks_count = index_granularity.getMarksCount();

        for (size_t i = 0; i < marks_count; ++i) //-V756
            for (size_t j = 0; j < key_size; ++j)
                primary_key.data_types[j]->deserializeBinary(*loaded_index[j], *index_file);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i]->protect();
            if (loaded_index[i]->size() != marks_count)
                throw Exception("Cannot read all data from index file " + index_path
                    + "(expected size: " + toString(marks_count) + ", read: " + toString(loaded_index[i]->size()) + ")",
                    ErrorCodes::CANNOT_READ_ALL_DATA);
        }

        if (!index_file->eof())
            throw Exception("Index file " + fullPath(volume->getDisk(), index_path) + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);

        index.assign(std::make_move_iterator(loaded_index.begin()), std::make_move_iterator(loaded_index.end()));
    }
}

NameSet IMergeTreeDataPart::getFileNamesWithoutChecksums() const
{
    if (!isStoredOnDisk())
        return {};

    NameSet result = {"checksums.txt", "columns.txt"};
    String default_codec_path = getFullRelativePath() + DEFAULT_COMPRESSION_CODEC_FILE_NAME;

    if (volume->getDisk()->exists(default_codec_path))
        result.emplace(DEFAULT_COMPRESSION_CODEC_FILE_NAME);

    return result;
}

void IMergeTreeDataPart::loadDefaultCompressionCodec()
{
    /// In memory parts doesn't have any compression
    if (!isStoredOnDisk())
    {
        default_codec = CompressionCodecFactory::instance().get("NONE", {});
        return;
    }

    String path = getFullRelativePath() + DEFAULT_COMPRESSION_CODEC_FILE_NAME;
    if (!volume->getDisk()->exists(path))
    {
        default_codec = detectDefaultCompressionCodec();
    }
    else
    {

        auto file_buf = openForReading(volume->getDisk(), path);
        String codec_line;
        readEscapedStringUntilEOL(codec_line, *file_buf);

        ReadBufferFromString buf(codec_line);

        if (!checkString("CODEC", buf))
        {
            LOG_WARNING(storage.log, "Cannot parse default codec for part {} from file {}, content '{}'. Default compression codec will be deduced automatically, from data on disk", name, path, codec_line);
            default_codec = detectDefaultCompressionCodec();
        }

        try
        {
            ParserCodec codec_parser;
            auto codec_ast = parseQuery(codec_parser, codec_line.data() + buf.getPosition(), codec_line.data() + codec_line.length(), "codec parser", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            default_codec = CompressionCodecFactory::instance().get(codec_ast, {});
        }
        catch (const DB::Exception & ex)
        {
            LOG_WARNING(storage.log, "Cannot parse default codec for part {} from file {}, content '{}', error '{}'. Default compression codec will be deduced automatically, from data on disk.", name, path, codec_line, ex.what());
            default_codec = detectDefaultCompressionCodec();
        }
    }
}

CompressionCodecPtr IMergeTreeDataPart::detectDefaultCompressionCodec() const
{
    /// In memory parts doesn't have any compression
    if (!isStoredOnDisk())
        return CompressionCodecFactory::instance().get("NONE", {});

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    const auto & storage_columns = metadata_snapshot->getColumns();
    CompressionCodecPtr result = nullptr;
    for (const auto & part_column : columns)
    {
        /// It was compressed with default codec and it's not empty
        auto column_size = getColumnSize(part_column.name, *part_column.type);
        if (column_size.data_compressed != 0 && !storage_columns.hasCompressionCodec(part_column.name))
        {
            result = getCompressionCodecForFile(volume->getDisk(), getFullRelativePath() + getFileNameForColumn(part_column) + ".bin");
            break;
        }
    }

    if (!result)
        result = CompressionCodecFactory::instance().getDefaultCodec();

    return result;
}

void IMergeTreeDataPart::loadPartitionAndMinMaxIndex()
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum min_date;
        DayNum max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);

        const auto & date_lut = DateLUT::instance();
        partition = MergeTreePartition(date_lut.toNumYYYYMM(min_date));
        minmax_idx = MinMaxIndex(min_date, max_date);
    }
    else
    {
        String path = getFullRelativePath();
        partition.load(storage, volume->getDisk(), path);
        if (!isEmpty())
            minmax_idx.load(storage, volume->getDisk(), path);
    }

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    String calculated_partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part " + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}

void IMergeTreeDataPart::loadChecksums(bool require)
{
    String path = getFullRelativePath() + "checksums.txt";
    if (volume->getDisk()->exists(path))
    {
        auto buf = openForReading(volume->getDisk(), path);
        if (checksums.read(*buf))
        {
            assertEOF(*buf);
            bytes_on_disk = checksums.getTotalSizeOnDisk();
        }
        else
            bytes_on_disk = calculateTotalSizeOnDisk(volume->getDisk(), getFullRelativePath());
    }
    else
    {
        if (require)
            throw Exception("No checksums.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        /// If the checksums file is not present, calculate the checksums and write them to disk.
        /// Check the data while we are at it.
        LOG_WARNING(storage.log, "Checksums for part {} not found. Will calculate them from data on disk.", name);
        checksums = checkDataPart(shared_from_this(), false);
        {
            auto out = volume->getDisk()->writeFile(getFullRelativePath() + "checksums.txt.tmp", 4096);
            checksums.write(*out);
        }

        volume->getDisk()->moveFile(getFullRelativePath() + "checksums.txt.tmp", getFullRelativePath() + "checksums.txt");

        bytes_on_disk = checksums.getTotalSizeOnDisk();
    }
}

void IMergeTreeDataPart::loadRowsCount()
{
    String path = getFullRelativePath() + "count.txt";
    if (index_granularity.empty())
    {
        rows_count = 0;
    }
    else if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING || part_type == Type::COMPACT)
    {
        if (!volume->getDisk()->exists(path))
            throw Exception("No count.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        auto buf = openForReading(volume->getDisk(), path);
        readIntText(rows_count, *buf);
        assertEOF(*buf);

#ifndef NDEBUG
        /// columns have to be loaded
        for (const auto & column : getColumns())
        {
            /// Most trivial types
            if (column.type->isValueRepresentedByNumber() && !column.type->haveSubtypes())
            {
                auto size = getColumnSize(column.name, *column.type);

                if (size.data_uncompressed == 0)
                    continue;

                size_t rows_in_column = size.data_uncompressed / column.type->getSizeOfValueInMemory();
                if (rows_in_column != rows_count)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} has rows count {} according to size in memory "
                        "and size of single value, but data part {} has {} rows", backQuote(column.name), rows_in_column, name, rows_count);
                }
            }
        }
#endif
    }
    else
    {
        for (const NameAndTypePair & column : columns)
        {
            ColumnPtr column_col = column.type->createColumn();
            if (!column_col->isFixedAndContiguous() || column_col->lowCardinality())
                continue;

            size_t column_size = getColumnSize(column.name, *column.type).data_uncompressed;
            if (!column_size)
                continue;

            size_t sizeof_field = column_col->sizeOfValueIfFixed();
            rows_count = column_size / sizeof_field;

            if (column_size % sizeof_field != 0)
            {
                throw Exception(
                    "Uncompressed size of column " + column.name + "(" + toString(column_size)
                    + ") is not divisible by the size of value (" + toString(sizeof_field) + ")",
                    ErrorCodes::LOGICAL_ERROR);
            }

            size_t last_mark_index_granularity = index_granularity.getLastNonFinalMarkRows();
            size_t rows_approx = index_granularity.getTotalRows();
            if (!(rows_count <= rows_approx && rows_approx < rows_count + last_mark_index_granularity))
                throw Exception(
                    "Unexpected size of column " + column.name + ": " + toString(rows_count) + " rows, expected "
                    + toString(rows_approx) + "+-" + toString(last_mark_index_granularity) + " rows according to the index",
                    ErrorCodes::LOGICAL_ERROR);

            return;
        }

        throw Exception("Data part doesn't contain fixed size column (even Date column)", ErrorCodes::LOGICAL_ERROR);
    }
}

void IMergeTreeDataPart::loadTTLInfos()
{
    String path = getFullRelativePath() + "ttl.txt";
    if (volume->getDisk()->exists(path))
    {
        auto in = openForReading(volume->getDisk(), path);
        assertString("ttl format version: ", *in);
        size_t format_version;
        readText(format_version, *in);
        assertChar('\n', *in);

        if (format_version == 1)
        {
            try
            {
                ttl_infos.read(*in);
            }
            catch (const JSONException &)
            {
                throw Exception("Error while parsing file ttl.txt in part: " + name, ErrorCodes::BAD_TTL_FILE);
            }
        }
        else
            throw Exception("Unknown ttl format version: " + toString(format_version), ErrorCodes::BAD_TTL_FILE);
    }
}

void IMergeTreeDataPart::loadUUID()
{
    String path = getFullRelativePath() + UUID_FILE_NAME;

    if (volume->getDisk()->exists(path))
    {
        auto in = openForReading(volume->getDisk(), path);
        readText(uuid, *in);
        if (uuid == UUIDHelpers::Nil)
            throw Exception("Unexpected empty " + String(UUID_FILE_NAME) + " in part: " + name, ErrorCodes::LOGICAL_ERROR);
    }
}

void IMergeTreeDataPart::loadColumns(bool require)
{
    String path = getFullRelativePath() + "columns.txt";
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (!volume->getDisk()->exists(path))
    {
        /// We can get list of columns only from columns.txt in compact parts.
        if (require || part_type == Type::COMPACT)
            throw Exception("No columns.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        /// If there is no file with a list of columns, write it down.
        for (const NameAndTypePair & column : metadata_snapshot->getColumns().getAllPhysical())
            if (volume->getDisk()->exists(getFullRelativePath() + getFileNameForColumn(column) + ".bin"))
                columns.push_back(column);

        if (columns.empty())
            throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        {
            auto buf = volume->getDisk()->writeFile(path + ".tmp", 4096);
            columns.writeText(*buf);
        }
        volume->getDisk()->moveFile(path + ".tmp", path);
    }
    else
    {
        columns.readText(*volume->getDisk()->readFile(path));
    }

    size_t pos = 0;
    for (const auto & column : columns)
        column_name_to_position.emplace(column.name, pos++);
}

bool IMergeTreeDataPart::shallParticipateInMerges(const StoragePolicyPtr & storage_policy) const
{
    /// `IMergeTreeDataPart::volume` describes space where current part belongs, and holds
    /// `SingleDiskVolume` object which does not contain up-to-date settings of corresponding volume.
    /// Therefore we shall obtain volume from storage policy.
    auto volume_ptr = storage_policy->getVolume(storage_policy->getVolumeIndexByDisk(volume->getDisk()));

    return !volume_ptr->areMergesAvoided();
}

UInt64 IMergeTreeDataPart::calculateTotalSizeOnDisk(const DiskPtr & disk_, const String & from)
{
    if (disk_->isFile(from))
        return disk_->getFileSize(from);
    std::vector<std::string> files;
    disk_->listFiles(from, files);
    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDisk(disk_, from + file);
    return res;
}


void IMergeTreeDataPart::renameTo(const String & new_relative_path, bool remove_new_dir_if_exists) const
{
    assertOnDisk();

    String from = getFullRelativePath();
    String to = storage.relative_data_path + new_relative_path + "/";

    std::optional<FileSyncGuard> sync_guard;
    if (storage.getSettings()->fsync_part_directory)
        sync_guard.emplace(volume->getDisk(), to);

    if (!volume->getDisk()->exists(from))
        throw Exception("Part directory " + fullPath(volume->getDisk(), from) + " doesn't exist. Most likely it is logical error.", ErrorCodes::FILE_DOESNT_EXIST);

    if (volume->getDisk()->exists(to))
    {
        if (remove_new_dir_if_exists)
        {
            Names files;
            volume->getDisk()->listFiles(to, files);

            LOG_WARNING(storage.log, "Part directory {} already exists and contains {} files. Removing it.", fullPath(volume->getDisk(), to), files.size());

            volume->getDisk()->removeRecursive(to);
        }
        else
        {
            throw Exception("Part directory " + fullPath(volume->getDisk(), to) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
        }
    }

    volume->getDisk()->setLastModified(from, Poco::Timestamp::fromEpochTime(time(nullptr)));
    volume->getDisk()->moveFile(from, to);
    relative_path = new_relative_path;
}


void IMergeTreeDataPart::remove() const
{
    if (!isStoredOnDisk())
        return;

    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. This is bug.", ErrorCodes::LOGICAL_ERROR);

    /** Atomic directory removal:
      * - rename directory to temporary name;
      * - remove it recursive.
      *
      * For temporary name we use "delete_tmp_" prefix.
      *
      * NOTE: We cannot use "tmp_delete_" prefix, because there is a second thread,
      *  that calls "clearOldTemporaryDirectories" and removes all directories, that begin with "tmp_" and are old enough.
      * But when we removing data part, it can be old enough. And rename doesn't change mtime.
      * And a race condition can happen that will lead to "File not found" error here.
      */

    String from = storage.relative_data_path + relative_path;
    String to = storage.relative_data_path + "delete_tmp_" + name;
    // TODO directory delete_tmp_<name> is never removed if server crashes before returning from this function

    if (volume->getDisk()->exists(to))
    {
        LOG_WARNING(storage.log, "Directory {} (to which part must be renamed before removing) already exists. Most likely this is due to unclean restart. Removing it.", fullPath(volume->getDisk(), to));

        try
        {
            volume->getDisk()->removeRecursive(to + "/");
        }
        catch (...)
        {
            LOG_ERROR(storage.log, "Cannot recursively remove directory {}. Exception: {}", fullPath(volume->getDisk(), to), getCurrentExceptionMessage(false));
            throw;
        }
    }

    try
    {
        volume->getDisk()->moveFile(from, to);
    }
    catch (const Poco::FileNotFoundException &)
    {
        LOG_ERROR(storage.log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(volume->getDisk(), to));

        return;
    }

    if (checksums.empty())
    {
        /// If the part is not completely written, we cannot use fast path by listing files.
        volume->getDisk()->removeRecursive(to + "/");
    }
    else
    {
        try
        {
            /// Remove each expected file in directory, then remove directory itself.

    #if !__clang__
    #    pragma GCC diagnostic push
    #    pragma GCC diagnostic ignored "-Wunused-variable"
    #endif
            for (const auto & [file, _] : checksums.files)
                volume->getDisk()->remove(to + "/" + file);
    #if !__clang__
    #    pragma GCC diagnostic pop
    #endif

            for (const auto & file : {"checksums.txt", "columns.txt"})
                volume->getDisk()->remove(to + "/" + file);

            volume->getDisk()->removeIfExists(to + "/" + DEFAULT_COMPRESSION_CODEC_FILE_NAME);
            volume->getDisk()->removeIfExists(to + "/" + DELETE_ON_DESTROY_MARKER_FILE_NAME);

            volume->getDisk()->remove(to);
        }
        catch (...)
        {
            /// Recursive directory removal does many excessive "stat" syscalls under the hood.

            LOG_ERROR(storage.log, "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: {}", fullPath(volume->getDisk(), to), getCurrentExceptionMessage(false));

            volume->getDisk()->removeRecursive(to + "/");
        }
    }
}

String IMergeTreeDataPart::getRelativePathForPrefix(const String & prefix) const
{
    String res;

    /** If you need to detach a part, and directory into which we want to rename it already exists,
        *  we will rename to the directory with the name to which the suffix is added in the form of "_tryN".
        * This is done only in the case of `to_detached`, because it is assumed that in this case the exact name does not matter.
        * No more than 10 attempts are made so that there are not too many junk directories left.
        */
    for (int try_no = 0; try_no < 10; try_no++)
    {
        res = (prefix.empty() ? "" : prefix + "_") + name + (try_no ? "_try" + DB::toString(try_no) : "");

        if (!volume->getDisk()->exists(getFullRelativePath() + res))
            return res;

        LOG_WARNING(storage.log, "Directory {} (to detach to) already exists. Will detach to directory with '_tryN' suffix.", res);
    }

    return res;
}

String IMergeTreeDataPart::getRelativePathForDetachedPart(const String & prefix) const
{
    /// Do not allow underscores in the prefix because they are used as separators.
    assert(prefix.find_first_of('_') == String::npos);
    return "detached/" + getRelativePathForPrefix(prefix);
}

void IMergeTreeDataPart::renameToDetached(const String & prefix) const
{
    renameTo(getRelativePathForDetachedPart(prefix), true);
}

void IMergeTreeDataPart::makeCloneInDetached(const String & prefix, const StorageMetadataPtr & /*metadata_snapshot*/) const
{
    String destination_path = storage.relative_data_path + getRelativePathForDetachedPart(prefix);

    /// Backup is not recursive (max_level is 0), so do not copy inner directories
    localBackup(volume->getDisk(), getFullRelativePath(), destination_path, 0);
    volume->getDisk()->removeIfExists(destination_path + "/" + DELETE_ON_DESTROY_MARKER_FILE_NAME);
}

void IMergeTreeDataPart::makeCloneOnDisk(const DiskPtr & disk, const String & directory_name) const
{
    assertOnDisk();

    if (disk->getName() == volume->getDisk()->getName())
        throw Exception("Can not clone data part " + name + " to same disk " + volume->getDisk()->getName(), ErrorCodes::LOGICAL_ERROR);
    if (directory_name.empty())
        throw Exception("Can not clone data part " + name + " to empty directory.", ErrorCodes::LOGICAL_ERROR);

    String path_to_clone = storage.relative_data_path + directory_name + '/';

    if (disk->exists(path_to_clone + relative_path))
    {
        LOG_WARNING(storage.log, "Path " + fullPath(disk, path_to_clone + relative_path) + " already exists. Will remove it and clone again.");
        disk->removeRecursive(path_to_clone + relative_path + '/');
    }
    disk->createDirectories(path_to_clone);

    volume->getDisk()->copy(getFullRelativePath(), disk, path_to_clone);
    volume->getDisk()->removeIfExists(path_to_clone + '/' + DELETE_ON_DESTROY_MARKER_FILE_NAME);
}

void IMergeTreeDataPart::checkConsistencyBase() const
{
    String path = getFullRelativePath();

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    const auto & pk = metadata_snapshot->getPrimaryKey();
    if (!checksums.empty())
    {
        if (!pk.column_names.empty() && !checksums.files.count("primary.idx"))
            throw Exception("No checksum for primary.idx", ErrorCodes::NO_FILE_IN_DATA_PART);

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (!checksums.files.count("count.txt"))
                throw Exception("No checksum for count.txt", ErrorCodes::NO_FILE_IN_DATA_PART);

            if (metadata_snapshot->hasPartitionKey() && !checksums.files.count("partition.dat"))
                throw Exception("No checksum for partition.dat", ErrorCodes::NO_FILE_IN_DATA_PART);

            if (!isEmpty())
            {
                for (const String & col_name : storage.minmax_idx_columns)
                {
                    if (!checksums.files.count("minmax_" + escapeForFileName(col_name) + ".idx"))
                        throw Exception("No minmax idx file checksum for column " + col_name, ErrorCodes::NO_FILE_IN_DATA_PART);
                }
            }
        }

        checksums.checkSizes(volume->getDisk(), path);
    }
    else
    {
        auto check_file_not_empty = [&path](const DiskPtr & disk_, const String & file_path)
        {
            UInt64 file_size;
            if (!disk_->exists(file_path) || (file_size = disk_->getFileSize(file_path)) == 0)
                throw Exception("Part " + fullPath(disk_, path) + " is broken: " + fullPath(disk_, file_path) + " is empty", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
            return file_size;
        };

        /// Check that the primary key index is not empty.
        if (!pk.column_names.empty())
            check_file_not_empty(volume->getDisk(), path + "primary.idx");

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            check_file_not_empty(volume->getDisk(), path + "count.txt");

            if (metadata_snapshot->hasPartitionKey())
                check_file_not_empty(volume->getDisk(), path + "partition.dat");

            for (const String & col_name : storage.minmax_idx_columns)
                check_file_not_empty(volume->getDisk(), path + "minmax_" + escapeForFileName(col_name) + ".idx");
        }
    }
}

void IMergeTreeDataPart::checkConsistency(bool /* require_part_metadata */) const
{
    throw Exception("Method 'checkConsistency' is not implemented for part with type " + getType().toString(), ErrorCodes::NOT_IMPLEMENTED);
}


void IMergeTreeDataPart::calculateColumnsSizesOnDisk()
{
    if (getColumns().empty() || checksums.empty())
        throw Exception("Cannot calculate columns sizes when columns or checksums are not initialized", ErrorCodes::LOGICAL_ERROR);

    calculateEachColumnSizes(columns_sizes, total_columns_size);
}

ColumnSize IMergeTreeDataPart::getColumnSize(const String & column_name, const IDataType & /* type */) const
{
    /// For some types of parts columns_size maybe not calculated
    auto it = columns_sizes.find(column_name);
    if (it != columns_sizes.end())
        return it->second;

    return ColumnSize{};
}

void IMergeTreeDataPart::accumulateColumnSizes(ColumnToSize & column_to_size) const
{
    for (const auto & [column_name, size] : columns_sizes)
        column_to_size[column_name] = size.data_compressed;
}


bool IMergeTreeDataPart::checkAllTTLCalculated(const StorageMetadataPtr & metadata_snapshot) const
{
    if (!metadata_snapshot->hasAnyTTL())
        return false;

    if (metadata_snapshot->hasRowsTTL())
    {
        if (isEmpty()) /// All rows were finally deleted and we don't store TTL
            return true;
        else if (ttl_infos.table_ttl.min == 0)
            return false;
    }

    for (const auto & [column, desc] : metadata_snapshot->getColumnTTLs())
    {
        /// Part has this column, but we don't calculated TTL for it
        if (!ttl_infos.columns_ttl.count(column) && getColumns().contains(column))
            return false;
    }

    for (const auto & move_desc : metadata_snapshot->getMoveTTLs())
    {
        /// Move TTL is not calculated
        if (!ttl_infos.moves_ttl.count(move_desc.result_column))
            return false;
    }

    return true;
}

bool isCompactPart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::COMPACT);
}

bool isWidePart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::WIDE);
}

bool isInMemoryPart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::IN_MEMORY);
}

}
