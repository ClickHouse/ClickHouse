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
    , index_granularity_info(storage) {}

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
    , index_granularity_info(storage) {}


ColumnSize IMergeTreeDataPart::getColumnSize(const String & column_name, const IDataType & type) const
{
    return getColumnSizeImpl(column_name, type, nullptr);
}

ColumnSize IMergeTreeDataPart::getTotalColumnsSize() const
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

IMergeTreeDataPart::~IMergeTreeDataPart()
{
    // if (on_disk && (state == State::DeleteOnDestroy || is_temp))
    // {
    //     try
    //     {
    //         std::string path = on_disk->getFullPath();

    //         Poco::File dir(path);
    //         if (!dir.exists())
    //             return;

    //         if (is_temp)
    //         {
    //             if (!startsWith(on_disk->getNameWithPrefix(), "tmp"))
    //             {
    //                 LOG_ERROR(storage.log, "~DataPart() should remove part " << path
    //                     << " but its name doesn't start with tmp. Too suspicious, keeping the part.");
    //                 return;
    //             }
    //         }

    //         dir.remove(true);
    //     }
    //     catch (...)
    //     {
    //         tryLogCurrentException(__PRETTY_FUNCTION__);
    //     }
    // }
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
        throw Exception("Data part '" + name + "is not stored on disk", ErrorCodes::LOGICAL_ERROR);
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

}
