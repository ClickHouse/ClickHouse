#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/CompressedStream.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromFile.h>
#include <Core/Defines.h>
#include <Common/SipHash.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Columns/ColumnNullable.h>

#include <Poco/File.h>


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
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int UNKNOWN_FORMAT;
    extern const int UNEXPECTED_FILE_IN_DATA_PART;
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

    if (format_version < 1 || format_version > 4)
        throw Exception("Bad checksums format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT);

    if (format_version == 1)
        return false;
    if (format_version == 2)
        return read_v2(in);
    if (format_version == 3)
        return read_v3(in);
    if (format_version == 4)
        return read_v4(in);

    return false;
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
        readBinary(sum.file_hash, in);
        readBinary(sum.is_compressed, in);

        if (sum.is_compressed)
        {
            readVarUInt(sum.uncompressed_size, in);
            readBinary(sum.uncompressed_hash, in);
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

    CompressedWriteBuffer out{to, CompressionMethod::LZ4, 1 << 16};
    writeVarUInt(files.size(), out);

    for (const auto & it : files)
    {
        const String & name = it.first;
        const Checksum & sum = it.second;

        writeBinary(name, out);
        writeVarUInt(sum.file_size, out);
        writeBinary(sum.file_hash, out);
        writeBinary(sum.is_compressed, out);

        if (sum.is_compressed)
        {
            writeVarUInt(sum.uncompressed_size, out);
            writeBinary(sum.uncompressed_hash, out);
        }
    }
}

void MergeTreeDataPartChecksums::addFile(const String & file_name, size_t file_size, uint128 file_hash)
{
    files[file_name] = Checksum(file_size, file_hash);
}

void MergeTreeDataPartChecksums::add(MergeTreeDataPartChecksums && rhs_checksums)
{
    for (auto & checksum : rhs_checksums.files)
        files[std::move(checksum.first)] = std::move(checksum.second);

    rhs_checksums.files.clear();
}

/// Control sum computed from the set of control sums of .bin files.
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
    String s;
    {
        WriteBufferFromString out(s);
        write(out);
    }
    return s;
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


const MergeTreeDataPartChecksums::Checksum * MergeTreeDataPart::tryGetBinChecksum(const String & name) const
{
    if (checksums.empty())
        return nullptr;

    const auto & files = checksums.files;
    const auto bin_file_name = escapeForFileName(name) + ".bin";
    auto it = files.find(bin_file_name);

    return (it == files.end()) ? nullptr : &it->second;
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


/** Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    *    If no checksums are present returns the name of the first physically existing column. */
String MergeTreeDataPart::getColumnNameWithMinumumCompressedSize() const
{
    const auto & columns = storage.getColumnsList();
    const std::string * minimum_size_column = nullptr;
    auto minimum_size = std::numeric_limits<size_t>::max();

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
        throw Exception{
            "Could not find a column of minimum size in MergeTree",
            ErrorCodes::LOGICAL_ERROR};

    return *minimum_size_column;
}


size_t MergeTreeDataPart::getExactSizeRows() const
{
    size_t rows_approx = storage.index_granularity * size;

    for (const NameAndTypePair & column : columns)
    {
        ColumnPtr column_col = column.type->createColumn();
        const auto checksum = tryGetBinChecksum(column.name);

        /// Should be fixed non-nullable column
        if (!checksum || !column_col->isFixed() || column_col->isNullable())
            continue;

        size_t sizeof_field = column_col->sizeOfField();
        size_t rows = checksum->uncompressed_size / sizeof_field;

        if (checksum->uncompressed_size % sizeof_field != 0)
        {
            throw Exception(
                "Column " + column.name + " has indivisible uncompressed size " + toString(checksum->uncompressed_size)
                + ", sizeof " + toString(sizeof_field),
                ErrorCodes::LOGICAL_ERROR);
        }

        if (!(rows_approx - storage.index_granularity < rows && rows <= rows_approx))
        {
            throw Exception("Unexpected size of column " + column.name + ": " + toString(rows) + " rows",
                            ErrorCodes::LOGICAL_ERROR);
        }

        return rows;
    }

    throw Exception("Data part doesn't contain fixed size column (even Date column)", ErrorCodes::LOGICAL_ERROR);
}


String MergeTreeDataPart::getFullPath() const
{
    return storage.full_path + (is_sharded ? ("reshard/" + DB::toString(shard_no) + "/") : "") + name + "/";
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

            if (!startsWith(name,"tmp"))
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

size_t MergeTreeDataPart::calcTotalSize(const String & from)
{
    Poco::File cur(from);
    if (cur.isFile())
        return cur.getSize();
    std::vector<std::string> files;
    cur.list(files);
    size_t res = 0;
    for (size_t i = 0; i < files.size(); ++i)
        res += calcTotalSize(from + files[i]);
    return res;
}

void MergeTreeDataPart::remove() const
{
    String from = storage.full_path + name;
    String to = storage.full_path + "tmp2_" + name;

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
        LOG_WARNING(storage.log, "Directory " << from << " (part to remove) doesn't exist or one of nested files has gone."
            " Most likely this is due to manual removing. This should be discouraged. Ignoring.");

        return;
    }

    to_dir.remove(true);
}

void MergeTreeDataPart::renameTo(const String & new_name) const
{
    String from = storage.full_path + name + "/";
    String to = storage.full_path + new_name + "/";

    Poco::File f(from);
    f.setLastModified(Poco::Timestamp::fromEpochTime(time(0)));
    f.renameTo(to);
}

void MergeTreeDataPart::renameAddPrefix(bool to_detached, const String & prefix) const
{
    unsigned try_no = 0;
    auto dst_name = [&, this] { return (to_detached ? "detached/" : "") + prefix + name + (try_no ? "_try" + DB::toString(try_no) : ""); };

    if (to_detached)
    {
        /** If you need to unhook a part, and directory into which we want to rename it already exists,
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

void MergeTreeDataPart::loadIndex()
{
    /// Size - in number of marks.
    if (!size)
    {
        if (columns.empty())
            throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        size = Poco::File(getFullPath() + escapeForFileName(columns.front().name) + ".mrk")
            .getSize() / MERGE_TREE_MARK_SIZE;
    }

    size_t key_size = storage.sort_descr.size();

    if (key_size)
    {
        index.clear();
        index.resize(key_size);

        for (size_t i = 0; i < key_size; ++i)
        {
            index[i] = storage.primary_key_data_types[i].get()->createColumn();
            index[i].get()->reserve(size);
        }

        String index_path = getFullPath() + "primary.idx";
        ReadBufferFromFile index_file(index_path,
            std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));

        for (size_t i = 0; i < size; ++i)
            for (size_t j = 0; j < key_size; ++j)
                storage.primary_key_data_types[j].get()->deserializeBinary(*index[j].get(), index_file);

        for (size_t i = 0; i < key_size; ++i)
            if (index[i].get()->size() != size)
                throw Exception("Cannot read all data from index file " + index_path
                    + "(expected size: " + toString(size) + ", read: " + toString(index[i].get()->size()) + ")",
                    ErrorCodes::CANNOT_READ_ALL_DATA);

        if (!index_file.eof())
            throw Exception("Index file " + index_path + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);
    }

    size_in_bytes = calcTotalSize(getFullPath());
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
    ReadBufferFromFile file(path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
    if (checksums.read(file))
        assertEOF(file);
}

void MergeTreeDataPart::accumulateColumnSizes(ColumnToSize & column_to_size) const
{
    Poco::ScopedReadRWLock part_lock(columns_lock);
    for (const NameAndTypePair & column : *storage.columns)
        if (Poco::File(getFullPath() + escapeForFileName(column.name) + ".bin").exists())
            column_to_size[column.name] += Poco::File(getFullPath() + escapeForFileName(column.name) + ".bin").getSize();
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

    ReadBufferFromFile file(path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
    columns.readText(file);
}

void MergeTreeDataPart::checkNotBroken(bool require_part_metadata)
{
    String path = getFullPath();

    if (!checksums.empty())
    {
        if (!storage.sort_descr.empty() && !checksums.files.count("primary.idx"))
            throw Exception("No checksum for primary.idx", ErrorCodes::NO_FILE_IN_DATA_PART);

        if (require_part_metadata)
        {
            for (const NameAndTypePair & it : columns)
            {
                String name = escapeForFileName(it.name);
                if (!checksums.files.count(name + ".mrk") ||
                    !checksums.files.count(name + ".bin"))
                    throw Exception("No .mrk or .bin file checksum for column " + name, ErrorCodes::NO_FILE_IN_DATA_PART);
            }
        }

        checksums.checkSizes(path);
    }
    else
    {
        if (!storage.sort_descr.empty())
        {
            /// Check that the primary key is not empty.
            Poco::File index_file(path + "primary.idx");

            if (!index_file.exists() || index_file.getSize() == 0)
                throw Exception("Part " + path + " is broken: primary key is empty.", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
        }

        /// Check that all marks are nonempty and have the same size.

        auto check_marks = [](const std::string & path, const NamesAndTypesList & columns, const std::string & extension)
        {
            ssize_t marks_size = -1;
            for (const NameAndTypePair & it : columns)
            {
                Poco::File marks_file(path + escapeForFileName(it.name) + extension);

                /// When you add a new column to the table, the .mrk files are not created. We will not delete anything.
                if (!marks_file.exists())
                    continue;

                if (marks_size == -1)
                {
                    marks_size = marks_file.getSize();

                    if (0 == marks_size)
                        throw Exception("Part " + path + " is broken: " + marks_file.path() + " is empty.",
                            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
                }
                else
                {
                    if (static_cast<ssize_t>(marks_file.getSize()) != marks_size)
                        throw Exception("Part " + path + " is broken: marks have different sizes.",
                            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
                }
            }
        };

        check_marks(path, columns, ".mrk");
        check_marks(path, columns, ".null.mrk");
    }
}

bool MergeTreeDataPart::hasColumnFiles(const String & column) const
{
    String prefix = getFullPath();
    String escaped_column = escapeForFileName(column);
    return Poco::File(prefix + escaped_column + ".bin").exists() &&
        Poco::File(prefix + escaped_column + ".mrk").exists();
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
        res += column->allocatedSize();
    return res;
}

}
