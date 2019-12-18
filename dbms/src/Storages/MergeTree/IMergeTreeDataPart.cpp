#include "IMergeTreeDataPart.h"

#include <optional>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/HashingWriteBuffer.h>
#include <Core/Defines.h>
#include <Common/SipHash.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/localBackup.h>
#include <Compression/CompressionInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>
#include <common/logger_useful.h>
#include <common/JSON.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int EXPECTED_END_OF_FILE;
    extern const int CORRUPTED_DATA;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int BAD_TTL_FILE;
    extern const int CANNOT_UNLINK;
    extern const int NOT_IMPLEMENTED;
}


static ReadBufferFromFile openForReading(const String & path)
{
    return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

void IMergeTreeDataPart::MinMaxIndex::load(const MergeTreeData & data, const String & part_path)
{
    size_t minmax_idx_size = data.minmax_idx_column_types.size();
    parallelogram.reserve(minmax_idx_size);
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = part_path + "minmax_" + escapeForFileName(data.minmax_idx_columns[i]) + ".idx";
        ReadBufferFromFile file = openForReading(file_name);
        const DataTypePtr & type = data.minmax_idx_column_types[i];

        Field min_val;
        type->deserializeBinary(min_val, file);
        Field max_val;
        type->deserializeBinary(max_val, file);

        parallelogram.emplace_back(min_val, true, max_val, true);
    }
    initialized = true;
}

void IMergeTreeDataPart::MinMaxIndex::store(const MergeTreeData & data, const String & part_path, Checksums & out_checksums) const
{
    store(data.minmax_idx_columns, data.minmax_idx_column_types, part_path, out_checksums);
}

void IMergeTreeDataPart::MinMaxIndex::store(const Names & column_names, const DataTypes & data_types, const String & part_path, Checksums & out_checksums) const
{
    if (!initialized)
        throw Exception("Attempt to store uninitialized MinMax index for part " + part_path + ". This is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        String file_name = "minmax_" + escapeForFileName(column_names[i]) + ".idx";
        const DataTypePtr & type = data_types.at(i);

        WriteBufferFromFile out(part_path + file_name);
        HashingWriteBuffer out_hashing(out);
        type->serializeBinary(parallelogram[i].left, out_hashing);
        type->serializeBinary(parallelogram[i].right, out_hashing);
        out_hashing.next();
        out_checksums.files[file_name].file_size = out_hashing.count();
        out_checksums.files[file_name].file_hash = out_hashing.getHash();
    }
}

void IMergeTreeDataPart::MinMaxIndex::update(const Block & block, const Names & column_names)
{
    if (!initialized)
        parallelogram.reserve(column_names.size());

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        Field min_value;
        Field max_value;
        const ColumnWithTypeAndName & column = block.getByName(column_names[i]);
        column.column->getExtremes(min_value, max_value);

        if (!initialized)
            parallelogram.emplace_back(min_value, true, max_value, true);
        else
        {
            parallelogram[i].left = std::min(parallelogram[i].left, min_value);
            parallelogram[i].right = std::max(parallelogram[i].right, max_value);
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
        parallelogram = other.parallelogram;
        initialized = true;
    }
    else
    {
        for (size_t i = 0; i < parallelogram.size(); ++i)
        {
            parallelogram[i].left = std::min(parallelogram[i].left, other.parallelogram[i].left);
            parallelogram[i].right = std::max(parallelogram[i].right, other.parallelogram[i].right);
        }
    }
}


IMergeTreeDataPart::IMergeTreeDataPart(
        MergeTreeData & storage_,
        const String & name_,
        const DiskSpace::DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : storage(storage_)
    , name(name_)
    , info(MergeTreePartInfo::fromPartName(name_, storage.format_version))
    , disk(disk_)
    , relative_path(relative_path_.value_or(name_))
{
}

IMergeTreeDataPart::IMergeTreeDataPart(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskSpace::DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : storage(storage_)
    , name(name_)
    , info(info_)
    , disk(disk_)
    , relative_path(relative_path_.value_or(name_))
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
    if (!sample_block.has(column_name))
        return {};
    return sample_block.getPositionByName(column_name);  
}   

DayNum IMergeTreeDataPart::getMinDate() const
{
    if (storage.minmax_idx_date_column_pos != -1 && minmax_idx.initialized)
        return DayNum(minmax_idx.parallelogram[storage.minmax_idx_date_column_pos].left.get<UInt64>());
    else
        return DayNum();
}


DayNum IMergeTreeDataPart::getMaxDate() const
{
    if (storage.minmax_idx_date_column_pos != -1 && minmax_idx.initialized)
        return DayNum(minmax_idx.parallelogram[storage.minmax_idx_date_column_pos].right.get<UInt64>());
    else
        return DayNum();
}

time_t IMergeTreeDataPart::getMinTime() const
{
    if (storage.minmax_idx_time_column_pos != -1 && minmax_idx.initialized)
        return minmax_idx.parallelogram[storage.minmax_idx_time_column_pos].left.get<UInt64>();
    else
        return 0;
}


time_t IMergeTreeDataPart::getMaxTime() const
{
    if (storage.minmax_idx_time_column_pos != -1 && minmax_idx.initialized)
        return minmax_idx.parallelogram[storage.minmax_idx_time_column_pos].right.get<UInt64>();
    else
        return 0;
}

void IMergeTreeDataPart::setColumns(const NamesAndTypesList & columns_)
{
    columns = columns_;
    for (const auto & column : columns)
        sample_block.insert({column.type, column.name});
    index_granularity_info.initialize(storage, getType(), columns.size());
}

IMergeTreeDataPart::~IMergeTreeDataPart() = default;

void IMergeTreeDataPart::removeIfNeeded()
{
    if (state == State::DeleteOnDestroy || is_temp)
    {
        try
        {
            std::string path = getFullPath();

            Poco::File dir(path);
            if (!dir.exists())
                return;

            if (is_temp)
            {
                String file_name = Poco::Path(relative_path).getFileName();

                if (file_name.empty())
                    throw Exception("relative_path " + relative_path + " of part " + name + " is invalid or not set", ErrorCodes::LOGICAL_ERROR);

                if (!startsWith(file_name, "tmp"))
                {
                    LOG_ERROR(storage.log, "~DataPart() should remove part " << path
                        << " but its name doesn't start with tmp. Too suspicious, keeping the part.");
                    return;
                }
            }

            dir.remove(true);
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
            + typeToString(getType()) + "' is not stored on disk", ErrorCodes::LOGICAL_ERROR);
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

String IMergeTreeDataPart::getFullPath() const
{
    assertOnDisk();

    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);

    return storage.getFullPathOnDisk(disk) + relative_path + "/";
}

void IMergeTreeDataPart::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency)
{
    assertOnDisk();

    /// Memory should not be limited during ATTACH TABLE query.
    /// This is already true at the server startup but must be also ensured for manual table ATTACH.
    /// Motivation: memory for index is shared between queries - not belong to the query itself.
    auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

    loadColumns(require_columns_checksums);
    loadChecksums(require_columns_checksums);
    loadIndexGranularity();
    loadIndex(); /// Must be called after loadIndexGranularity as it uses the value of `index_granularity`
    loadColumnSizes();
    loadRowsCount(); /// Must be called after loadIndex() as it uses the value of `index_granularity`.
    loadPartitionAndMinMaxIndex();
    loadTTLInfos();

    if (check_consistency)
        checkConsistency(require_columns_checksums);
}

void IMergeTreeDataPart::loadIndexGranularity()
{
    throw Exception("Method 'loadIndexGranularity' is not implemented for part with type " + typeToString(getType()), ErrorCodes::NOT_IMPLEMENTED);
}

void IMergeTreeDataPart::loadIndex()
{
    /// It can be empty in case of mutations
    if (!index_granularity.isInitialized())
        throw Exception("Index granularity is not loaded before index loading", ErrorCodes::LOGICAL_ERROR);

    size_t key_size = storage.primary_key_columns.size();

    if (key_size)
    {
        MutableColumns loaded_index;
        loaded_index.resize(key_size);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i] = storage.primary_key_data_types[i]->createColumn();
            loaded_index[i]->reserve(index_granularity.getMarksCount());
        }

        String index_path = getFullPath() + "primary.idx";
        ReadBufferFromFile index_file = openForReading(index_path);

        for (size_t i = 0; i < index_granularity.getMarksCount(); ++i)    //-V756
            for (size_t j = 0; j < key_size; ++j)
                storage.primary_key_data_types[j]->deserializeBinary(*loaded_index[j], index_file);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i]->protect();
            if (loaded_index[i]->size() != index_granularity.getMarksCount())
                throw Exception("Cannot read all data from index file " + index_path
                    + "(expected size: " + toString(index_granularity.getMarksCount()) + ", read: " + toString(loaded_index[i]->size()) + ")",
                    ErrorCodes::CANNOT_READ_ALL_DATA);
        }

        if (!index_file.eof())
            throw Exception("Index file " + index_path + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);

        index.assign(std::make_move_iterator(loaded_index.begin()), std::make_move_iterator(loaded_index.end()));
    }
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
        String path = getFullPath();
        partition.load(storage, path);
        if (!isEmpty())
            minmax_idx.load(storage, path);
    }

    String calculated_partition_id = partition.getID(storage.partition_key_sample);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part " + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}

void IMergeTreeDataPart::loadChecksums(bool require)
{
    String path = getFullPath() + "checksums.txt";
    Poco::File checksums_file(path);
    if (checksums_file.exists())
    {
        ReadBufferFromFile file = openForReading(path);
        if (checksums.read(file))
        {
            assertEOF(file);
            bytes_on_disk = checksums.getTotalSizeOnDisk();
        }
        else
            bytes_on_disk = calculateTotalSizeOnDisk(getFullPath());
    }
    else
    {
        if (require)
            throw Exception("No checksums.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        bytes_on_disk = calculateTotalSizeOnDisk(getFullPath());
    }
}

void IMergeTreeDataPart::loadRowsCount()
{
    if (index_granularity.empty())
    {
        rows_count = 0;
    }
    else if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        String path = getFullPath() + "count.txt";
        if (!Poco::File(path).exists())
            throw Exception("No count.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        ReadBufferFromFile file = openForReading(path);
        readIntText(rows_count, file);
        assertEOF(file);
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
    String path = getFullPath() + "ttl.txt";
    if (Poco::File(path).exists())
    {
        ReadBufferFromFile in = openForReading(path);
        assertString("ttl format version: ", in);
        size_t format_version;
        readText(format_version, in);
        assertChar('\n', in);

        if (format_version == 1)
        {
            try
            {
                ttl_infos.read(in);
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

void IMergeTreeDataPart::loadColumns(bool require)
{
    String path = getFullPath() + "columns.txt";
    Poco::File poco_file_path{path};
    if (!poco_file_path.exists())
    {
        if (require)
            throw Exception("No columns.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        /// If there is no file with a list of columns, write it down.
        for (const NameAndTypePair & column : storage.getColumns().getAllPhysical())
            if (Poco::File(getFullPath() + escapeForFileName(column.name) + ".bin").exists())
                columns.push_back(column);

        if (columns.empty())
            throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        {
            WriteBufferFromFile out(path + ".tmp", 4096);
            columns.writeText(out);
        }
        Poco::File(path + ".tmp").renameTo(path);
    }
    else
    {
        is_frozen = !poco_file_path.canWrite();
        ReadBufferFromFile file = openForReading(path);
        columns.readText(file);    
    }

    index_granularity_info.initialize(storage, getType(), columns.size());
    for (const auto & it : columns)
        sample_block.insert({it.type, it.name});
}

void IMergeTreeDataPart::loadColumnSizes()
{
    size_t columns_num = columns.size();

    if (columns_num == 0)
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);
    
    auto column_sizes_path = getFullPath() + "columns_sizes.txt";
    auto columns_sizes_file = Poco::File(column_sizes_path);
    if (!columns_sizes_file.exists())
        return;

    ReadBufferFromFile buffer(column_sizes_path, columns_sizes_file.getSize());
    auto it = columns.begin();
    for (size_t i = 0; i < columns_num; ++i, ++it)
        readPODBinary(columns_sizes[it->name], buffer);
    assertEOF(buffer);
}


UInt64 IMergeTreeDataPart::calculateTotalSizeOnDisk(const String & from)
{
    Poco::File cur(from);
    if (cur.isFile())
        return cur.getSize();
    std::vector<std::string> files;
    cur.list(files);
    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDisk(from + file);
    return res;
}


void IMergeTreeDataPart::renameTo(const String & new_relative_path, bool remove_new_dir_if_exists) const
{
    assertOnDisk();

    String from = getFullPath();
    String to = storage.getFullPathOnDisk(disk) + new_relative_path + "/";

    Poco::File from_file(from);
    if (!from_file.exists())
        throw Exception("Part directory " + from + " doesn't exist. Most likely it is logical error.", ErrorCodes::FILE_DOESNT_EXIST);

    Poco::File to_file(to);
    if (to_file.exists())
    {
        if (remove_new_dir_if_exists)
        {
            Names files;
            Poco::File(from).list(files);

            LOG_WARNING(storage.log, "Part directory " << to << " already exists"
                << " and contains " << files.size() << " files. Removing it.");

            to_file.remove(true);
        }
        else
        {
            throw Exception("Part directory " + to + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
        }
    }

    from_file.setLastModified(Poco::Timestamp::fromEpochTime(time(nullptr)));
    from_file.renameTo(to);
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

    String full_path = storage.getFullPathOnDisk(disk);
    String from = full_path + relative_path;
    String to = full_path + "delete_tmp_" + name;
    // TODO directory delete_tmp_<name> is never removed if server crashes before returning from this function

    Poco::File from_dir{from};
    Poco::File to_dir{to};

    if (to_dir.exists())
    {
        LOG_WARNING(storage.log, "Directory " << to << " (to which part must be renamed before removing) already exists."
            " Most likely this is due to unclean restart. Removing it.");

        try
        {
            to_dir.remove(true);
        }
        catch (...)
        {
            LOG_ERROR(storage.log, "Cannot remove directory " << to << ". Check owner and access rights.");
            throw;
        }
    }

    try
    {
        from_dir.renameTo(to);
    }
    catch (const Poco::FileNotFoundException &)
    {
        LOG_ERROR(storage.log, "Directory " << from << " (part to remove) doesn't exist or one of nested files has gone."
            " Most likely this is due to manual removing. This should be discouraged. Ignoring.");

        return;
    }

    try
    {
        /// Remove each expected file in directory, then remove directory itself.

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#endif
        std::shared_lock<std::shared_mutex> lock(columns_lock);

        for (const auto & [file, _] : checksums.files)
        {
            String path_to_remove = to + "/" + file;
            if (0 != unlink(path_to_remove.c_str()))
                throwFromErrnoWithPath("Cannot unlink file " + path_to_remove, path_to_remove,
                                       ErrorCodes::CANNOT_UNLINK);
        }
#if !__clang__
#pragma GCC diagnostic pop
#endif

        for (const auto & file : {"checksums.txt", "columns.txt"})
        {
            String path_to_remove = to + "/" + file;
            if (0 != unlink(path_to_remove.c_str()))
                throwFromErrnoWithPath("Cannot unlink file " + path_to_remove, path_to_remove,
                                       ErrorCodes::CANNOT_UNLINK);
        }

        if (0 != rmdir(to.c_str()))
            throwFromErrnoWithPath("Cannot rmdir file " + to, to, ErrorCodes::CANNOT_UNLINK);
    }
    catch (...)
    {
        /// Recursive directory removal does many excessive "stat" syscalls under the hood.

        LOG_ERROR(storage.log, "Cannot quickly remove directory " << to << " by removing files; fallback to recursive removal. Reason: "
            << getCurrentExceptionMessage(false));

        to_dir.remove(true);
    }
}

void IMergeTreeDataPart::accumulateColumnSizes(ColumnToSize & /* column_to_size */) const
{
    throw Exception("Method 'accumulateColumnSizes' is not supported for data part with type " + typeToString(getType()), ErrorCodes::NOT_IMPLEMENTED);
}

void IMergeTreeDataPart::checkConsistency(bool /* require_part_metadata */) const
{
    throw Exception("Method 'checkConsistency' is not supported for data part with type " + typeToString(getType()), ErrorCodes::NOT_IMPLEMENTED);
}

String IMergeTreeDataPart::typeToString(Type type)
{
    switch(type)
    {
        case Type::WIDE:
            return "Wide";
        case Type::COMPACT:
            return "Striped";
        case Type::IN_MEMORY:
            return "InMemory";
        case Type::UNKNOWN:
            return "Unknown";
    }

    __builtin_unreachable();
}

String IMergeTreeDataPart::getRelativePathForDetachedPart(const String & prefix) const
{
    /// Do not allow underscores in the prefix because they are used as separators.

    assert(prefix.find_first_of('_') == String::npos);
    String res;

    /** If you need to detach a part, and directory into which we want to rename it already exists,
        *  we will rename to the directory with the name to which the suffix is added in the form of "_tryN".
        * This is done only in the case of `to_detached`, because it is assumed that in this case the exact name does not matter.
        * No more than 10 attempts are made so that there are not too many junk directories left.
        */
    for (int try_no = 0; try_no < 10; try_no++)
    {
        res = "detached/" + (prefix.empty() ? "" : prefix + "_")
            + name + (try_no ? "_try" + DB::toString(try_no) : "");

        if (!Poco::File(storage.getFullPathOnDisk(disk) + res).exists())
            return res;

        LOG_WARNING(storage.log, "Directory " << res << " (to detach to) already exists."
            " Will detach to directory with '_tryN' suffix.");
    }

    return res;
}

void IMergeTreeDataPart::renameToDetached(const String & prefix) const
{
    assertOnDisk();
    renameTo(getRelativePathForDetachedPart(prefix));
}

void IMergeTreeDataPart::makeCloneInDetached(const String & prefix) const
{
    assertOnDisk();
    LOG_INFO(storage.log, "Detaching " << relative_path);

    Poco::Path src(getFullPath());
    Poco::Path dst(storage.getFullPathOnDisk(disk) + getRelativePathForDetachedPart(prefix));
    /// Backup is not recursive (max_level is 0), so do not copy inner directories
    localBackup(src, dst, 0);
}

void IMergeTreeDataPart::makeCloneOnDiskDetached(const DiskSpace::ReservationPtr & reservation) const
{
    assertOnDisk();
    auto & reserved_disk = reservation->getDisk();
    if (reserved_disk->getName() == disk->getName())
        throw Exception("Can not clone data part " + name + " to same disk " + disk->getName(), ErrorCodes::LOGICAL_ERROR);

    String path_to_clone = storage.getFullPathOnDisk(reserved_disk) + "detached/";

    if (Poco::File(path_to_clone + relative_path).exists())
        throw Exception("Path " + path_to_clone + relative_path + " already exists. Can not clone ", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
    Poco::File(path_to_clone).createDirectory();

    Poco::File cloning_directory(getFullPath());
    cloning_directory.copyTo(path_to_clone);
}

bool isCompactPart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::COMPACT);
}

bool isWidePart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::WIDE);
}

}
