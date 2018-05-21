#include <optional>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/CompressedStream.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/HashingWriteBuffer.h>
#include <Core/Defines.h>
#include <Common/SipHash.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/localBackup.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>

#include <common/logger_useful.h>

#define MERGE_TREE_MARK_SIZE (2 * sizeof(UInt64))


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
}


static ReadBufferFromFile openForReading(const String & path)
{
    return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

void MergeTreeDataPart::MinMaxIndex::load(const MergeTreeData & storage, const String & part_path)
{
    size_t minmax_idx_size = storage.minmax_idx_column_types.size();
    min_values.resize(minmax_idx_size);
    max_values.resize(minmax_idx_size);
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = part_path + "minmax_" + escapeForFileName(storage.minmax_idx_columns[i]) + ".idx";
        ReadBufferFromFile file = openForReading(file_name);
        const DataTypePtr & type = storage.minmax_idx_column_types[i];
        type->deserializeBinary(min_values[i], file);
        type->deserializeBinary(max_values[i], file);
    }
    initialized = true;
}

void MergeTreeDataPart::MinMaxIndex::store(const MergeTreeData & storage, const String & part_path, Checksums & checksums) const
{
    for (size_t i = 0; i < storage.minmax_idx_columns.size(); ++i)
    {
        String file_name = "minmax_" + escapeForFileName(storage.minmax_idx_columns[i]) + ".idx";
        const DataTypePtr & type = storage.minmax_idx_column_types[i];

        WriteBufferFromFile out(part_path + file_name);
        HashingWriteBuffer out_hashing(out);
        type->serializeBinary(min_values[i], out_hashing);
        type->serializeBinary(max_values[i], out_hashing);
        out_hashing.next();
        checksums.files[file_name].file_size = out_hashing.count();
        checksums.files[file_name].file_hash = out_hashing.getHash();
    }
}

void MergeTreeDataPart::MinMaxIndex::update(const Block & block, const Names & column_names)
{
    if (!initialized)
    {
        min_values.resize(column_names.size());
        max_values.resize(column_names.size());
    }

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        Field min_value;
        Field max_value;
        const ColumnWithTypeAndName & column = block.getByName(column_names[i]);
        column.column->getExtremes(min_value, max_value);

        if (!initialized)
        {
            min_values[i] = Field(min_value);
            max_values[i] = Field(max_value);
        }
        else
        {
            min_values[i] = std::min(min_values[i], min_value);
            max_values[i] = std::max(max_values[i], max_value);
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
        min_values.assign(other.min_values);
        max_values.assign(other.max_values);
        initialized = true;
    }
    else
    {
        for (size_t i = 0; i < min_values.size(); ++i)
        {
            min_values[i] = std::min(min_values[i], other.min_values[i]);
            max_values[i] = std::max(max_values[i], other.max_values[i]);
        }
    }
}


MergeTreeDataPart::MergeTreeDataPart(MergeTreeData & storage_, const String & name_)
    : storage(storage_), name(name_), info(MergeTreePartInfo::fromPartName(name_, storage.format_version))
{
}

/// Takes into account the fact that several columns can e.g. share their .size substreams.
/// When calculating totals these should be counted only once.
MergeTreeDataPart::ColumnSize MergeTreeDataPart::getColumnSizeImpl(const String & name, const IDataType & type, std::unordered_set<String> * processed_substreams) const
{
    ColumnSize size;
    if (checksums.empty())
        return size;

    type.enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
    {
        String file_name = IDataType::getFileNameForStream(name, substream_path);

        if (processed_substreams && !processed_substreams->insert(file_name).second)
            return;

        auto bin_checksum = checksums.files.find(file_name + ".bin");
        if (bin_checksum != checksums.files.end())
        {
            size.data_compressed += bin_checksum->second.file_size;
            size.data_uncompressed += bin_checksum->second.uncompressed_size;
        }

        auto mrk_checksum = checksums.files.find(file_name + ".mrk");
        if (mrk_checksum != checksums.files.end())
            size.marks += mrk_checksum->second.file_size;
    }, {});

    return size;
}

MergeTreeDataPart::ColumnSize MergeTreeDataPart::getColumnSize(const String & name, const IDataType & type) const
{
    return getColumnSizeImpl(name, type, nullptr);
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


/** Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
  * If no checksums are present returns the name of the first physically existing column.
  */
String MergeTreeDataPart::getColumnNameWithMinumumCompressedSize() const
{
    const auto & columns = storage.getColumns().getAllPhysical();
    const std::string * minimum_size_column = nullptr;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();

    for (const auto & column : columns)
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


DayNum_t MergeTreeDataPart::getMinDate() const
{
    if (storage.minmax_idx_date_column_pos != -1)
        return DayNum_t(minmax_idx.min_values[storage.minmax_idx_date_column_pos].get<UInt64>());
    else
        return DayNum_t();
}


DayNum_t MergeTreeDataPart::getMaxDate() const
{
    if (storage.minmax_idx_date_column_pos != -1)
        return DayNum_t(minmax_idx.max_values[storage.minmax_idx_date_column_pos].get<UInt64>());
    else
        return DayNum_t();
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

    String from = storage.full_path + relative_path;
    String to = storage.full_path + "tmp_delete_" + name;

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
    catch (const Poco::FileNotFoundException & e)
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
        throw Exception("Part directory " + from + " doesn't exists. Most likely it is logical error.", ErrorCodes::FILE_DOESNT_EXIST);

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


void MergeTreeDataPart::makeCloneInDetached(const String & prefix) const
{
    Poco::Path src(getFullPath());
    Poco::Path dst(storage.full_path + getRelativePathForDetachedPart(prefix));
    /// Backup is not recursive (max_level is 0), so do not copy inner directories
    localBackup(src, dst, 0);
}

void MergeTreeDataPart::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency)
{
    loadColumns(require_columns_checksums);
    loadChecksums(require_columns_checksums);
    loadIndex();
    loadRowsCount(); /// Must be called after loadIndex() as it uses the value of `marks_count`.
    loadPartitionAndMinMaxIndex();
    if (check_consistency)
        checkConsistency(require_columns_checksums);
}


void MergeTreeDataPart::loadIndex()
{
    if (!marks_count)
    {
        if (columns.empty())
            throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        marks_count = Poco::File(getFullPath() + escapeForFileName(columns.front().name) + ".mrk")
            .getSize() / MERGE_TREE_MARK_SIZE;
    }

    size_t key_size = storage.primary_sort_descr.size();

    if (key_size)
    {
        MutableColumns loaded_index;
        loaded_index.resize(key_size);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i] = storage.primary_key_data_types[i]->createColumn();
            loaded_index[i]->reserve(marks_count);
        }

        String index_path = getFullPath() + "primary.idx";
        ReadBufferFromFile index_file = openForReading(index_path);

        for (size_t i = 0; i < marks_count; ++i)
            for (size_t j = 0; j < key_size; ++j)
                storage.primary_key_data_types[j]->deserializeBinary(*loaded_index[j].get(), index_file);

        for (size_t i = 0; i < key_size; ++i)
            if (loaded_index[i]->size() != marks_count)
                throw Exception("Cannot read all data from index file " + index_path
                    + "(expected size: " + toString(marks_count) + ", read: " + toString(loaded_index[i]->size()) + ")",
                    ErrorCodes::CANNOT_READ_ALL_DATA);

        if (!index_file.eof())
            throw Exception("Index file " + index_path + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);

        index.assign(std::make_move_iterator(loaded_index.begin()), std::make_move_iterator(loaded_index.end()));
    }

    bytes_on_disk = calculateTotalSizeOnDisk(getFullPath());
}

void MergeTreeDataPart::loadPartitionAndMinMaxIndex()
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum_t min_date;
        DayNum_t max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);

        const auto & date_lut = DateLUT::instance();
        partition = MergeTreePartition(date_lut.toNumYYYYMM(min_date));
        minmax_idx = MinMaxIndex(min_date, max_date);
    }
    else
    {
        String full_path = getFullPath();
        partition.load(storage, full_path);
        minmax_idx.load(storage, full_path);
    }

    String calculated_partition_id = partition.getID(storage);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part "  + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}

void MergeTreeDataPart::loadChecksums(bool require)
{
    String path = getFullPath() + "checksums.txt";
    if (!Poco::File(path).exists())
    {
        if (require)
            throw Exception("No checksums.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        return;
    }
    ReadBufferFromFile file = openForReading(path);
    if (checksums.read(file))
        assertEOF(file);
}

void MergeTreeDataPart::loadRowsCount()
{
    if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
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
        size_t rows_approx = storage.index_granularity * marks_count;

        for (const NameAndTypePair & column : columns)
        {
            ColumnPtr column_col = column.type->createColumn();
            if (!column_col->isFixedAndContiguous())
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

            if (!(rows_count <= rows_approx && rows_approx < rows_count + storage.index_granularity))
                throw Exception(
                    "Unexpected size of column " + column.name + ": " + toString(rows_count) + " rows, expected "
                    + toString(rows_approx) + "+-" + toString(storage.index_granularity) + " rows according to the index",
                    ErrorCodes::LOGICAL_ERROR);

            return;
        }

        throw Exception("Data part doesn't contain fixed size column (even Date column)", ErrorCodes::LOGICAL_ERROR);
    }
}

void MergeTreeDataPart::accumulateColumnSizes(ColumnToSize & column_to_size) const
{
    std::shared_lock<std::shared_mutex> part_lock(columns_lock);

    for (const NameAndTypePair & name_type : storage.getColumns().getAllPhysical())
    {
        name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
        {
            Poco::File bin_file(getFullPath() + IDataType::getFileNameForStream(name_type.name, substream_path) + ".bin");
            if (bin_file.exists())
                column_to_size[name_type.name] += bin_file.getSize();
        }, {});
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
        if (!storage.primary_sort_descr.empty() && !checksums.files.count("primary.idx"))
            throw Exception("No checksum for primary.idx", ErrorCodes::NO_FILE_IN_DATA_PART);

        if (require_part_metadata)
        {
            for (const NameAndTypePair & name_type : columns)
            {
                name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
                {
                    String file_name = IDataType::getFileNameForStream(name_type.name, substream_path);
                    String mrk_file_name = file_name + ".mrk";
                    String bin_file_name = file_name + ".bin";
                    if (!checksums.files.count(mrk_file_name))
                        throw Exception("No " + mrk_file_name + " file checksum for column " + name + " in part " + path,
                            ErrorCodes::NO_FILE_IN_DATA_PART);
                    if (!checksums.files.count(bin_file_name))
                        throw Exception("No " + bin_file_name + " file checksum for column " + name + " in part " + path,
                            ErrorCodes::NO_FILE_IN_DATA_PART);
                }, {});
            }
        }

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (!checksums.files.count("count.txt"))
                throw Exception("No checksum for count.txt", ErrorCodes::NO_FILE_IN_DATA_PART);

            if (storage.partition_expr && !checksums.files.count("partition.dat"))
                throw Exception("No checksum for partition.dat", ErrorCodes::NO_FILE_IN_DATA_PART);

            for (const String & col_name : storage.minmax_idx_columns)
            {
                if (!checksums.files.count("minmax_" + escapeForFileName(col_name) + ".idx"))
                    throw Exception("No minmax idx file checksum for column " + col_name, ErrorCodes::NO_FILE_IN_DATA_PART);
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
        if (!storage.primary_sort_descr.empty())
            check_file_not_empty(path + "primary.idx");

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            check_file_not_empty(path + "count.txt");

            if (storage.partition_expr)
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
                Poco::File file(IDataType::getFileNameForStream(name_type.name, substream_path) + ".mrk");

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
            }, {});
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
        && Poco::File(prefix + escaped_column + ".mrk").exists();
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
        default:
            throw Exception("Unknown part state " + toString(static_cast<int>(state)), ErrorCodes::LOGICAL_ERROR);
    }
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
        for (auto state : affordable_states)
            states_str += stateToString(state) + " ";

        throw Exception("Unexpected state of part " + getNameWithState() + ". Expected: " + states_str, ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);
    }
}

}
