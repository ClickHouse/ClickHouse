#include "MergeTreeDataPartWide.h"

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

#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/IMergeTreeReader.h>


namespace DB
{

// namespace
// {
// }

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
}


static ReadBufferFromFile openForReading(const String & path)
{
    return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

MergeTreeDataPartWide::MergeTreeDataPartWide( 
       MergeTreeData & storage_,
        const String & name_,
        const DiskSpace::DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, disk_, relative_path_)
{
}

MergeTreeDataPartWide::MergeTreeDataPartWide(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskSpace::DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, info_, disk_, relative_path_)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartWide::getReader(
    const NamesAndTypesList & columns_to_read,
    const MarkRanges & mark_ranges,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const ReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback) const
{
    return std::make_unique<MergeTreeReaderWide>(shared_from_this(), columns_to_read, uncompressed_cache,
        mark_cache, mark_ranges, reader_settings, avg_value_size_hints, profile_callback);
}


/// Takes into account the fact that several columns can e.g. share their .size substreams.
/// When calculating totals these should be counted only once.
ColumnSize MergeTreeDataPartWide::getColumnSizeImpl(
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

        auto mrk_checksum = checksums.files.find(file_name + index_granularity_info.marks_file_extension);
        if (mrk_checksum != checksums.files.end())
            size.marks += mrk_checksum->second.file_size;
    }, {});

    return size;
}

ColumnSize MergeTreeDataPartWide::getColumnSize(const String & column_name, const IDataType & type) const
{
    return getColumnSizeImpl(column_name, type, nullptr);
}

/** Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
  * If no checksums are present returns the name of the first physically existing column.
  */
String MergeTreeDataPartWide::getColumnNameWithMinumumCompressedSize() const
{
    const auto & storage_columns = storage.getColumns().getAllPhysical();
    const std::string * minimum_size_column = nullptr;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();

    for (const auto & column : storage_columns)
    {
        if (!hasColumnFiles(column.name, *column.type))
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

MergeTreeDataPartWide::~MergeTreeDataPartWide()
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

UInt64 MergeTreeDataPartWide::calculateTotalSizeOnDisk(const String & from)
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

void MergeTreeDataPartWide::remove() const
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

void MergeTreeDataPartWide::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool /* check_consistency */)
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
    // if (check_consistency)
        // checkConsistency(require_columns_checksums);
}

void MergeTreeDataPartWide::loadIndexGranularity()
{
    String full_path = getFullPath();
    index_granularity_info.changeGranularityIfRequired(shared_from_this());

    if (columns.empty())
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

    /// We can use any column, it doesn't matter
    std::string marks_file_path = index_granularity_info.getMarksFilePath(full_path + escapeForFileName(columns.front().name));
    if (!Poco::File(marks_file_path).exists())
        throw Exception("Marks file '" + marks_file_path + "' doesn't exist", ErrorCodes::NO_FILE_IN_DATA_PART);

    size_t marks_file_size = Poco::File(marks_file_path).getSize();

    /// old version of marks with static index granularity
    if (!index_granularity_info.is_adaptive)
    {
        size_t marks_count = marks_file_size / index_granularity_info.mark_size_in_bytes;
        index_granularity.resizeWithFixedGranularity(marks_count, index_granularity_info.fixed_index_granularity); /// all the same
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
        if (index_granularity.getMarksCount() * index_granularity_info.mark_size_in_bytes != marks_file_size)
            throw Exception("Cannot read all marks from file " + marks_file_path, ErrorCodes::CANNOT_READ_ALL_DATA);
    }
    index_granularity.setInitialized();
}

void MergeTreeDataPartWide::loadIndex()
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

void MergeTreeDataPartWide::loadPartitionAndMinMaxIndex()
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

void MergeTreeDataPartWide::loadChecksums(bool require)
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

void MergeTreeDataPartWide::loadRowsCount()
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

void MergeTreeDataPartWide::loadTTLInfos()
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

// void MergeTreeDataPartWide::accumulateColumnSizes(ColumnToSize & column_to_size) const
// {
//     std::shared_lock<std::shared_mutex> part_lock(columns_lock);

//     for (const NameAndTypePair & name_type : storage.getColumns().getAllPhysical())
//     {
//         IDataType::SubstreamPath path;
//         name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
//         {
//             Poco::File bin_file(getFullPath() + IDataType::getFileNameForStream(name_type.name, substream_path) + ".bin");
//             if (bin_file.exists())
//                 column_to_size[name_type.name] += bin_file.getSize();
//         }, path);
//     }
// }

void MergeTreeDataPartWide::loadColumns(bool require)
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

        return;
    }

    is_frozen = !poco_file_path.canWrite();

    ReadBufferFromFile file = openForReading(path);
    columns.readText(file);
}

void MergeTreeDataPartWide::checkConsistency(bool require_part_metadata)
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
                    String mrk_file_name = file_name + index_granularity_info.marks_file_extension;
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
                Poco::File file(IDataType::getFileNameForStream(name_type.name, substream_path) + index_granularity_info.marks_file_extension);

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

// bool MergeTreeDataPartWide::hasColumnFiles(const String & column_name, const IDataType & type) const
// {
//     bool res = true;

//     type.enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
//     {
//         String file_name = IDataType::getFileNameForStream(column_name, substream_path);

//         auto bin_checksum = checksums.files.find(file_name + ".bin");
//         auto mrk_checksum = checksums.files.find(file_name + index_granularity_info.marks_file_extension);

//         if (bin_checksum == checksums.files.end() || mrk_checksum == checksums.files.end())
//             res = false;
//     }, {});

//     return res;
// }

}
