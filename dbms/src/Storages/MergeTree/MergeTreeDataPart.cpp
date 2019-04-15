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
#include <Storages/MergeTree/MergeTreeDataPart.h>
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
}


static ReadBufferFromFile openForReading(const String & path)
{
    return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

void MergeTreeDataPart::MinMaxIndex::load(const MergeTreeData & data, const String & part_path)
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

void MergeTreeDataPart::MinMaxIndex::store(const MergeTreeData & data, const String & part_path, Checksums & out_checksums) const
{
    store(data.minmax_idx_columns, data.minmax_idx_column_types, part_path, out_checksums);
}

void MergeTreeDataPart::MinMaxIndex::store(const Names & column_names, const DataTypes & data_types, const String & part_path, Checksums & out_checksums) const
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

void MergeTreeDataPart::MinMaxIndex::update(const Block & block, const Names & column_names)
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

void MergeTreeDataPart::MinMaxIndex::merge(const MinMaxIndex & other)
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


MergeTreeDataPart::MergeTreeDataPart(MergeTreeData & storage_, const String & name_)
    : storage(storage_)
    , name(name_)
    , info(MergeTreePartInfo::fromPartName(name_, storage.format_version))
{
}

MergeTreeDataPart::MergeTreeDataPart(const MergeTreeData & storage_, const String & name_, const MergeTreePartInfo & info_)
    : storage(storage_)
    , name(name_)
    , info(info_)
{
}


/// Takes into account the fact that several columns can e.g. share their .size substreams.
/// When calculating totals these should be counted only once.
MergeTreeDataPart::ColumnSize MergeTreeDataPart::getColumnSizeImpl(
    const String & column_name, const IDataType & type, std::unordered_set<String> * processed_substreams) const
{
    ColumnSize size;
    if (checksums.empty())
        return size;

    type.enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
    {
        String file_name = IDataType::getFileNameForStream(column_name, substream_path);

        if (processed_substreams && !processed_substreams->insert(file_name).second)
            return;

        auto bin_checksum = checksums.files.find(file_name + ".bin");
        if (bin_checksum != checksums.files.end())
        {
            size.data_compressed += bin_checksum->second.file_size;
            size.data_uncompressed += bin_checksum->second.uncompressed_size;
        }

        auto mrk_checksum = checksums.files.find(file_name + storage.index_granularity_info.marks_file_extension);
        if (mrk_checksum != checksums.files.end())
            size.marks += mrk_checksum->second.file_size;
    }, {});

    return size;
}

MergeTreeDataPart::ColumnSize MergeTreeDataPart::getColumnSize(const String & column_name, const IDataType & type) const
{
    return getColumnSizeImpl(column_name, type, nullptr);
}

MergeTreeDataPart::ColumnSize MergeTreeDataPart::getTotalColumnsSize() const
{
    ColumnSize totals;
    std::unordered_set<String> processed_substreams;
    for (const NameAndTypePair & column : columns)
    {
        ColumnSize size = getColumnSizeImpl(column.name, *column.type, &processed_substreams);
        totals.add(size);
    }
    return totals;
}


size_t MergeTreeDataPart::getFileSizeOrZero(const String & file_name) const
{
    auto checksum = checksums.files.find(file_name);
    if (checksum == checksums.files.end())
        return 0;
    return checksum->second.file_size;
}

/** Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
  * If no checksums are present returns the name of the first physically existing column.
  */
String MergeTreeDataPart::getColumnNameWithMinumumCompressedSize() const
{
    const auto & storage_columns = storage.getColumns().getAllPhysical();
    const std::string * minimum_size_column = nullptr;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();

    for (const auto & column : storage_columns)
    {
        if (!hasColumnFiles(column.name))
            continue;

        const auto size = getColumnSize(column.name, *column.type).data_compressed;
        if (size < minimum_size)
        {
            minimum_size = size;
            minimum_size_column = &column.name;
        }
    }

    if (!minimum_size_column)
        throw Exception("Could not find a column of minimum size in MergeTree, part " + getFullPath(), ErrorCodes::LOGICAL_ERROR);

    return *minimum_size_column;
}


String MergeTreeDataPart::getFullPath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. This is bug.", ErrorCodes::LOGICAL_ERROR);

    return storage.full_path + relative_path + "/";
}

String MergeTreeDataPart::getNameWithPrefix() const
{
    String res = Poco::Path(relative_path).getFileName();

    if (res.empty())
        throw Exception("relative_path " + relative_path + " of part " + name + " is invalid or not set", ErrorCodes::LOGICAL_ERROR);

    return res;
}

String MergeTreeDataPart::getNewName(const MergeTreePartInfo & new_part_info) const
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

DayNum MergeTreeDataPart::getMinDate() const
{
    if (storage.minmax_idx_date_column_pos != -1 && minmax_idx.initialized)
        return DayNum(minmax_idx.parallelogram[storage.minmax_idx_date_column_pos].left.get<UInt64>());
    else
        return DayNum();
}


DayNum MergeTreeDataPart::getMaxDate() const
{
    if (storage.minmax_idx_date_column_pos != -1 && minmax_idx.initialized)
        return DayNum(minmax_idx.parallelogram[storage.minmax_idx_date_column_pos].right.get<UInt64>());
    else
        return DayNum();
}

time_t MergeTreeDataPart::getMinTime() const
{
    if (storage.minmax_idx_time_column_pos != -1 && minmax_idx.initialized)
        return minmax_idx.parallelogram[storage.minmax_idx_time_column_pos].left.get<UInt64>();
    else
        return 0;
}


time_t MergeTreeDataPart::getMaxTime() const
{
    if (storage.minmax_idx_time_column_pos != -1 && minmax_idx.initialized)
        return minmax_idx.parallelogram[storage.minmax_idx_time_column_pos].right.get<UInt64>();
    else
        return 0;
}

MergeTreeDataPart::~MergeTreeDataPart()
{
    if (is_temp)
    {
        try
        {
            std::string path = getFullPath();

            Poco::File dir(path);
            if (!dir.exists())
                return;

            if (!startsWith(getNameWithPrefix(), "tmp"))
            {
                LOG_ERROR(storage.log, "~DataPart() should remove part " << path
                    << " but its name doesn't start with tmp. Too suspicious, keeping the part.");
                return;
            }

            dir.remove(true);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

UInt64 MergeTreeDataPart::calculateTotalSizeOnDisk(const String & from)
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

void MergeTreeDataPart::remove() const
{
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

    String from = storage.full_path + relative_path;
    String to = storage.full_path + "delete_tmp_" + name;

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

    to_dir.remove(true);
}


void MergeTreeDataPart::renameTo(const String & new_relative_path, bool remove_new_dir_if_exists) const
{
    String from = getFullPath();
    String to = storage.full_path + new_relative_path + "/";

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
            throw Exception("part directory " + to + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
        }
    }

    from_file.setLastModified(Poco::Timestamp::fromEpochTime(time(nullptr)));
    from_file.renameTo(to);
    relative_path = new_relative_path;
}


String MergeTreeDataPart::getRelativePathForDetachedPart(const String & prefix) const
{
    String res;
    unsigned try_no = 0;
    auto dst_name = [&, this] { return "detached/" + prefix + name + (try_no ? "_try" + DB::toString(try_no) : ""); };

    /** If you need to detach a part, and directory into which we want to rename it already exists,
        *  we will rename to the directory with the name to which the suffix is added in the form of "_tryN".
        * This is done only in the case of `to_detached`, because it is assumed that in this case the exact name does not matter.
        * No more than 10 attempts are made so that there are not too many junk directories left.
        */
    while (try_no < 10)
    {
        res = dst_name();

        if (!Poco::File(storage.full_path + res).exists())
            return res;

        LOG_WARNING(storage.log, "Directory " << dst_name() << " (to detach to) is already exist."
            " Will detach to directory with '_tryN' suffix.");
        ++try_no;
    }

    return res;
}

void MergeTreeDataPart::renameToDetached(const String & prefix) const
{
    renameTo(getRelativePathForDetachedPart(prefix));
}


UInt64 MergeTreeDataPart::getMarksCount() const
{
    return index_granularity.getMarksCount();
}

void MergeTreeDataPart::makeCloneInDetached(const String & prefix) const
{
    Poco::Path src(getFullPath());
    Poco::Path dst(storage.full_path + getRelativePathForDetachedPart(prefix));
    /// Backup is not recursive (max_level is 0), so do not copy inner directories
    localBackup(src, dst, 0);
}

void MergeTreeDataPart::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency)
{
    /// Memory should not be limited during ATTACH TABLE query.
    /// This is already true at the server startup but must be also ensured for manual table ATTACH.
    /// Motivation: memory for index is shared between queries - not belong to the query itself.
    auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

    loadColumns(require_columns_checksums);
    loadChecksums(require_columns_checksums);
    loadIndexGranularity();
    loadIndex(); /// Must be called after loadIndexGranularity as it uses the value of `index_granularity`
    loadRowsCount(); /// Must be called after loadIndex() as it uses the value of `index_granularity`.
    loadPartitionAndMinMaxIndex();
    loadTTLInfos();
    if (check_consistency)
        checkConsistency(require_columns_checksums);
}

void MergeTreeDataPart::loadIndexGranularity()
{
    if (columns.empty())
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);
    const auto & granularity_info = storage.index_granularity_info;

    /// We can use any column, it doesn't matter
    std::string marks_file_path = granularity_info.getMarksFilePath(getFullPath() + escapeForFileName(columns.front().name));
    if (!Poco::File(marks_file_path).exists())
        throw Exception("Marks file '" + marks_file_path + "' doesn't exist", ErrorCodes::NO_FILE_IN_DATA_PART);

    size_t marks_file_size = Poco::File(marks_file_path).getSize();

    /// old version of marks with static index granularity
    if (!granularity_info.is_adaptive)
    {
        size_t marks_count = marks_file_size / granularity_info.mark_size_in_bytes;
        index_granularity.resizeWithFixedGranularity(marks_count, granularity_info.fixed_index_granularity); /// all the same
    }
    else
    {
        ReadBufferFromFile buffer(marks_file_path, marks_file_size, -1);
        while (!buffer.eof())
        {
            buffer.seek(sizeof(size_t) * 2, SEEK_CUR); /// skip offset_in_compressed file and offset_in_decompressed_block
            size_t granularity;
            readIntBinary(granularity, buffer);
            index_granularity.appendMark(granularity);
        }
        if (index_granularity.getMarksCount() * granularity_info.mark_size_in_bytes != marks_file_size)
            throw Exception("Cannot read all marks from file " + marks_file_path, ErrorCodes::CANNOT_READ_ALL_DATA);
    }
    index_granularity.setInitialized();
}

void MergeTreeDataPart::loadIndex()
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

void MergeTreeDataPart::loadPartitionAndMinMaxIndex()
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
        String full_path = getFullPath();
        partition.load(storage, full_path);
        if (!isEmpty())
            minmax_idx.load(storage, full_path);
    }

    String calculated_partition_id = partition.getID(storage.partition_key_sample);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part " + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}

void MergeTreeDataPart::loadChecksums(bool require)
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

void MergeTreeDataPart::loadRowsCount()
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

            size_t last_mark_index_granularity = index_granularity.getLastMarkRows();
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

void MergeTreeDataPart::loadTTLInfos()
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

void MergeTreeDataPart::accumulateColumnSizes(ColumnToSize & column_to_size) const
{
    std::shared_lock<std::shared_mutex> part_lock(columns_lock);

    for (const NameAndTypePair & name_type : storage.getColumns().getAllPhysical())
    {
        IDataType::SubstreamPath path;
        name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
        {
            Poco::File bin_file(getFullPath() + IDataType::getFileNameForStream(name_type.name, substream_path) + ".bin");
            if (bin_file.exists())
                column_to_size[name_type.name] += bin_file.getSize();
        }, path);
    }
}

void MergeTreeDataPart::loadColumns(bool require)
{
    String path = getFullPath() + "columns.txt";
    if (!Poco::File(path).exists())
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

        return;
    }

    ReadBufferFromFile file = openForReading(path);
    columns.readText(file);
}

void MergeTreeDataPart::checkConsistency(bool require_part_metadata)
{
    String path = getFullPath();

    if (!checksums.empty())
    {
        if (!storage.primary_key_columns.empty() && !checksums.files.count("primary.idx"))
            throw Exception("No checksum for primary.idx", ErrorCodes::NO_FILE_IN_DATA_PART);

        if (require_part_metadata)
        {
            for (const NameAndTypePair & name_type : columns)
            {
                IDataType::SubstreamPath stream_path;
                name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
                {
                    String file_name = IDataType::getFileNameForStream(name_type.name, substream_path);
                    String mrk_file_name = file_name + storage.index_granularity_info.marks_file_extension;
                    String bin_file_name = file_name + ".bin";
                    if (!checksums.files.count(mrk_file_name))
                        throw Exception("No " + mrk_file_name + " file checksum for column " + name_type.name + " in part " + path,
                            ErrorCodes::NO_FILE_IN_DATA_PART);
                    if (!checksums.files.count(bin_file_name))
                        throw Exception("No " + bin_file_name + " file checksum for column " + name_type.name + " in part " + path,
                            ErrorCodes::NO_FILE_IN_DATA_PART);
                }, stream_path);
            }
        }

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (!checksums.files.count("count.txt"))
                throw Exception("No checksum for count.txt", ErrorCodes::NO_FILE_IN_DATA_PART);

            if (storage.partition_key_expr && !checksums.files.count("partition.dat"))
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

        checksums.checkSizes(path);
    }
    else
    {
        auto check_file_not_empty = [&path](const String & file_path)
        {
            Poco::File file(file_path);
            if (!file.exists() || file.getSize() == 0)
                throw Exception("Part " + path + " is broken: " + file_path + " is empty", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
            return file.getSize();
        };

        /// Check that the primary key index is not empty.
        if (!storage.primary_key_columns.empty())
            check_file_not_empty(path + "primary.idx");

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            check_file_not_empty(path + "count.txt");

            if (storage.partition_key_expr)
                check_file_not_empty(path + "partition.dat");

            for (const String & col_name : storage.minmax_idx_columns)
                check_file_not_empty(path + "minmax_" + escapeForFileName(col_name) + ".idx");
        }

        /// Check that all marks are nonempty and have the same size.

        std::optional<UInt64> marks_size;
        for (const NameAndTypePair & name_type : columns)
        {
            name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
            {
                Poco::File file(IDataType::getFileNameForStream(name_type.name, substream_path) + storage.index_granularity_info.marks_file_extension);

                /// Missing file is Ok for case when new column was added.
                if (file.exists())
                {
                    UInt64 file_size = file.getSize();

                    if (!file_size)
                        throw Exception("Part " + path + " is broken: " + file.path() + " is empty.",
                            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

                    if (!marks_size)
                        marks_size = file_size;
                    else if (file_size != *marks_size)
                        throw Exception("Part " + path + " is broken: marks have different sizes.",
                            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
                }
            });
        }
    }
}

bool MergeTreeDataPart::hasColumnFiles(const String & column) const
{
    /// NOTE: For multi-streams columns we check that just first file exist.
    /// That's Ok under assumption that files exist either for all or for no streams.

    String prefix = getFullPath();

    String escaped_column = escapeForFileName(column);
    return Poco::File(prefix + escaped_column + ".bin").exists()
        && Poco::File(prefix + escaped_column + storage.index_granularity_info.marks_file_extension).exists();
}


UInt64 MergeTreeDataPart::getIndexSizeInBytes() const
{
    UInt64 res = 0;
    for (const ColumnPtr & column : index)
        res += column->byteSize();
    return res;
}

UInt64 MergeTreeDataPart::getIndexSizeInAllocatedBytes() const
{
    UInt64 res = 0;
    for (const ColumnPtr & column : index)
        res += column->allocatedBytes();
    return res;
}

String MergeTreeDataPart::stateToString(MergeTreeDataPart::State state)
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
    }

    __builtin_unreachable();
}

String MergeTreeDataPart::stateString() const
{
    return stateToString(state);
}

void MergeTreeDataPart::assertState(const std::initializer_list<MergeTreeDataPart::State> & affordable_states) const
{
    if (!checkState(affordable_states))
    {
        String states_str;
        for (auto affordable_state : affordable_states)
            states_str += stateToString(affordable_state) + " ";

        throw Exception("Unexpected state of part " + getNameWithState() + ". Expected: " + states_str, ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);
    }
}

}
