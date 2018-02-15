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
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>

#include <common/logger_useful.h>

#define MERGE_TREE_MARK_SIZE (2 * sizeof(size_t))


namespace DB
{

namespace ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int FILE_DOESNT_EXIST;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int EXPECTED_END_OF_FILE;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int CORRUPTED_DATA;
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int UNKNOWN_FORMAT;
    extern const int UNEXPECTED_FILE_IN_DATA_PART;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
}


void MergeTreeDataPartChecksum::checkEqual(const MergeTreeDataPartChecksum & rhs, bool have_uncompressed, const String & name) const
{
    if (is_compressed && have_uncompressed)
    {
        if (!rhs.is_compressed)
            throw Exception("No uncompressed checksum for file " + name, ErrorCodes::CHECKSUM_DOESNT_MATCH);
        if (rhs.uncompressed_size != uncompressed_size)
            throw Exception("Unexpected uncompressed size of file " + name + " in data part", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
        if (rhs.uncompressed_hash != uncompressed_hash)
            throw Exception("Checksum mismatch for uncompressed file " + name + " in data part", ErrorCodes::CHECKSUM_DOESNT_MATCH);
        return;
    }
    if (rhs.file_size != file_size)
        throw Exception("Unexpected size of file " + name + " in data part", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
    if (rhs.file_hash != file_hash)
        throw Exception("Checksum mismatch for file " + name + " in data part", ErrorCodes::CHECKSUM_DOESNT_MATCH);
}

void MergeTreeDataPartChecksum::checkSize(const String & path) const
{
    Poco::File file(path);
    if (!file.exists())
        throw Exception(path + " doesn't exist", ErrorCodes::FILE_DOESNT_EXIST);
    size_t size = file.getSize();
    if (size != file_size)
        throw Exception(path + " has unexpected size: " + toString(size) + " instead of " + toString(file_size),
            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
}


void MergeTreeDataPartChecksums::checkEqual(const MergeTreeDataPartChecksums & rhs, bool have_uncompressed) const
{
    for (const auto & it : rhs.files)
    {
        const String & name = it.first;

        if (!files.count(name))
            throw Exception("Unexpected file " + name + " in data part", ErrorCodes::UNEXPECTED_FILE_IN_DATA_PART);
    }

    for (const auto & it : files)
    {
        const String & name = it.first;

        auto jt = rhs.files.find(name);
        if (jt == rhs.files.end())
            throw Exception("No file " + name + " in data part", ErrorCodes::NO_FILE_IN_DATA_PART);

        it.second.checkEqual(jt->second, have_uncompressed, name);
    }
}

void MergeTreeDataPartChecksums::checkSizes(const String & path) const
{
    for (const auto & it : files)
    {
        const String & name = it.first;
        it.second.checkSize(path + name);
    }
}

bool MergeTreeDataPartChecksums::read(ReadBuffer & in)
{
    files.clear();

    assertString("checksums format version: ", in);
    int format_version;
    readText(format_version, in);
    assertChar('\n', in);

    switch (format_version)
    {
        case 1:
            return false;
        case 2:
            return read_v2(in);
        case 3:
            return read_v3(in);
        case 4:
            return read_v4(in);
        default:
            throw Exception("Bad checksums format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT);
    }
}

bool MergeTreeDataPartChecksums::read_v2(ReadBuffer & in)
{
    size_t count;

    readText(count, in);
    assertString(" files:\n", in);

    for (size_t i = 0; i < count; ++i)
    {
        String name;
        Checksum sum;

        readString(name, in);
        assertString("\n\tsize: ", in);
        readText(sum.file_size, in);
        assertString("\n\thash: ", in);
        readText(sum.file_hash.first, in);
        assertString(" ", in);
        readText(sum.file_hash.second, in);
        assertString("\n\tcompressed: ", in);
        readText(sum.is_compressed, in);
        if (sum.is_compressed)
        {
            assertString("\n\tuncompressed size: ", in);
            readText(sum.uncompressed_size, in);
            assertString("\n\tuncompressed hash: ", in);
            readText(sum.uncompressed_hash.first, in);
            assertString(" ", in);
            readText(sum.uncompressed_hash.second, in);
        }
        assertChar('\n', in);

        files.insert(std::make_pair(name, sum));
    }

    return true;
}

bool MergeTreeDataPartChecksums::read_v3(ReadBuffer & in)
{
    size_t count;

    readVarUInt(count, in);

    for (size_t i = 0; i < count; ++i)
    {
        String name;
        Checksum sum;

        readBinary(name, in);
        readVarUInt(sum.file_size, in);
        readPODBinary(sum.file_hash, in);
        readBinary(sum.is_compressed, in);

        if (sum.is_compressed)
        {
            readVarUInt(sum.uncompressed_size, in);
            readPODBinary(sum.uncompressed_hash, in);
        }

        files.emplace(std::move(name), sum);
    }

    return true;
}

bool MergeTreeDataPartChecksums::read_v4(ReadBuffer & from)
{
    CompressedReadBuffer in{from};
    return read_v3(in);
}

void MergeTreeDataPartChecksums::write(WriteBuffer & to) const
{
    writeString("checksums format version: 4\n", to);

    CompressedWriteBuffer out{to, CompressionSettings(CompressionMethod::LZ4), 1 << 16};
    writeVarUInt(files.size(), out);

    for (const auto & it : files)
    {
        const String & name = it.first;
        const Checksum & sum = it.second;

        writeBinary(name, out);
        writeVarUInt(sum.file_size, out);
        writePODBinary(sum.file_hash, out);
        writeBinary(sum.is_compressed, out);

        if (sum.is_compressed)
        {
            writeVarUInt(sum.uncompressed_size, out);
            writePODBinary(sum.uncompressed_hash, out);
        }
    }
}

void MergeTreeDataPartChecksums::addFile(const String & file_name, size_t file_size, MergeTreeDataPartChecksum::uint128 file_hash)
{
    files[file_name] = Checksum(file_size, file_hash);
}

void MergeTreeDataPartChecksums::add(MergeTreeDataPartChecksums && rhs_checksums)
{
    for (auto & checksum : rhs_checksums.files)
        files[std::move(checksum.first)] = std::move(checksum.second);

    rhs_checksums.files.clear();
}

/// Checksum computed from the set of control sums of .bin files.
void MergeTreeDataPartChecksums::summaryDataChecksum(SipHash & hash) const
{
    /// We use fact that iteration is in deterministic (lexicographical) order.
    for (const auto & it : files)
    {
        const String & name = it.first;
        const Checksum & sum = it.second;

        if (!endsWith(name, ".bin"))
            continue;

        size_t len = name.size();
        hash.update(reinterpret_cast<const char *>(&len), sizeof(len));
        hash.update(name.data(), len);
        hash.update(reinterpret_cast<const char *>(&sum.uncompressed_size), sizeof(sum.uncompressed_size));
        hash.update(reinterpret_cast<const char *>(&sum.uncompressed_hash), sizeof(sum.uncompressed_hash));
    }
}

String MergeTreeDataPartChecksums::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

MergeTreeDataPartChecksums MergeTreeDataPartChecksums::parse(const String & s)
{
    ReadBufferFromString in(s);
    MergeTreeDataPartChecksums res;
    if (!res.read(in))
        throw Exception("Checksums format is too old", ErrorCodes::FORMAT_VERSION_TOO_OLD);
    assertEOF(in);
    return res;
}

const MergeTreeDataPartChecksums::Checksum * MergeTreeDataPart::tryGetChecksum(const String & name, const String & ext) const
{
    if (checksums.empty())
        return nullptr;

    const auto & files = checksums.files;
    const auto file_name = escapeForFileName(name) + ext;
    auto it = files.find(file_name);

    return (it == files.end()) ? nullptr : &it->second;
}

const MergeTreeDataPartChecksums::Checksum * MergeTreeDataPart::tryGetBinChecksum(const String & name) const
{
    return tryGetChecksum(name, ".bin");
}

const MergeTreeDataPartChecksums::Checksum * MergeTreeDataPart::tryGetMrkChecksum(const String & name) const
{
    return tryGetChecksum(name, ".mrk");
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

/// Returns the size of .bin file for column `name` if found, zero otherwise.
size_t MergeTreeDataPart::getColumnCompressedSize(const String & name) const
{
    const Checksum * checksum = tryGetBinChecksum(name);

    /// Probably a logic error, not sure if this can ever happen if checksums are not empty
    return checksum ? checksum->file_size : 0;
}

size_t MergeTreeDataPart::getColumnUncompressedSize(const String & name) const
{
    const Checksum * checksum = tryGetBinChecksum(name);
    return checksum ? checksum->uncompressed_size : 0;
}


size_t MergeTreeDataPart::getColumnMrkSize(const String & name) const
{
    const Checksum * checksum = tryGetMrkChecksum(name);
    return checksum ? checksum->file_size : 0;
}


/** Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
  * If no checksums are present returns the name of the first physically existing column.
  */
String MergeTreeDataPart::getColumnNameWithMinumumCompressedSize() const
{
    const auto & columns = storage.getColumnsList();
    const std::string * minimum_size_column = nullptr;
    size_t minimum_size = std::numeric_limits<size_t>::max();

    for (const auto & column : columns)
    {
        if (!hasColumnFiles(column.name))
            continue;

        const auto size = getColumnCompressedSize(column.name);
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

size_t MergeTreeDataPart::calculateTotalSize(const String & from)
{
    Poco::File cur(from);
    if (cur.isFile())
        return cur.getSize();
    std::vector<std::string> files;
    cur.list(files);
    size_t res = 0;
    for (const auto & file : files)
        res += calculateTotalSize(from + file);
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


void MergeTreeDataPart::renameAddPrefix(bool to_detached, const String & prefix) const
{
    unsigned try_no = 0;
    auto dst_name = [&, this] { return (to_detached ? "detached/" : "") + prefix + name + (try_no ? "_try" + DB::toString(try_no) : ""); };

    if (to_detached)
    {
        /** If you need to detach a part, and directory into which we want to rename it already exists,
            *  we will rename to the directory with the name to which the suffix is added in the form of "_tryN".
            * This is done only in the case of `to_detached`, because it is assumed that in this case the exact name does not matter.
            * No more than 10 attempts are made so that there are not too many junk directories left.
            */
        while (try_no < 10 && Poco::File(storage.full_path + dst_name()).exists())
        {
            LOG_WARNING(storage.log, "Directory " << dst_name() << " (to detach to) is already exist."
                " Will detach to directory with '_tryN' suffix.");
            ++try_no;
        }
    }

    renameTo(dst_name());
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

    size_in_bytes = calculateTotalSize(getFullPath());
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
            const auto checksum = tryGetBinChecksum(column.name);

            /// Should be fixed non-nullable column
            if (!checksum || !column_col->isFixedAndContiguous())
                continue;

            size_t sizeof_field = column_col->sizeOfValueIfFixed();
            rows_count = checksum->uncompressed_size / sizeof_field;

            if (checksum->uncompressed_size % sizeof_field != 0)
            {
                throw Exception(
                    "Column " + column.name + " has indivisible uncompressed size " + toString(checksum->uncompressed_size)
                    + ", sizeof " + toString(sizeof_field),
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

    for (const NameAndTypePair & name_type : storage.columns)
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
        for (const NameAndTypePair & column : storage.getColumnsList())
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

        std::optional<size_t> marks_size;
        for (const NameAndTypePair & name_type : columns)
        {
            name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
            {
                Poco::File file(IDataType::getFileNameForStream(name_type.name, substream_path) + ".mrk");

                /// Missing file is Ok for case when new column was added.
                if (file.exists())
                {
                    size_t file_size = file.getSize();

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


size_t MergeTreeDataPart::getIndexSizeInBytes() const
{
    size_t res = 0;
    for (const ColumnPtr & column : index)
        res += column->byteSize();
    return res;
}

size_t MergeTreeDataPart::getIndexSizeInAllocatedBytes() const
{
    size_t res = 0;
    for (const ColumnPtr & column : index)
        res += column->allocatedBytes();
    return res;
}

size_t MergeTreeDataPart::getTotalMrkSizeInBytes() const
{
    size_t res = 0;
    for (const NameAndTypePair & it : columns)
    {
        const Checksum * checksum = tryGetMrkChecksum(it.name);
        if (checksum)
            res += checksum->file_size;
    }
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
