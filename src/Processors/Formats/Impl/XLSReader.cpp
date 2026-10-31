#include <Processors/Formats/Impl/XLSReader.h>

#include <Processors/Formats/Impl/ExcelSheetSelection.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace Excel
{

namespace
{

[[noreturn]] void throwCorrupted(std::string_view what)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "The XLS file is corrupted: {}", what);
}

UInt8 readU8(std::string_view data, size_t offset)
{
    if (offset >= data.size())
        throwCorrupted("unexpected end of a record");
    return static_cast<UInt8>(data[offset]);
}

UInt16 readU16(std::string_view data, size_t offset)
{
    if (offset + 2 > data.size())
        throwCorrupted("unexpected end of a record");
    return static_cast<UInt16>(static_cast<UInt8>(data[offset]) | (static_cast<UInt8>(data[offset + 1]) << 8));
}

UInt32 readU32(std::string_view data, size_t offset)
{
    return readU16(data, offset) | (static_cast<UInt32>(readU16(data, offset + 2)) << 16);
}

double readDouble(std::string_view data, size_t offset)
{
    UInt64 bits = readU32(data, offset) | (static_cast<UInt64>(readU32(data, offset + 4)) << 32);
    return std::bit_cast<double>(bits);
}

void appendUTF8(UInt32 code_point, String & out)
{
    if (code_point < 0x80)
        out += static_cast<char>(code_point);
    else if (code_point < 0x800)
    {
        out += static_cast<char>(0xC0 | (code_point >> 6));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
    else if (code_point < 0x10000)
    {
        out += static_cast<char>(0xE0 | (code_point >> 12));
        out += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
    else
    {
        out += static_cast<char>(0xF0 | (code_point >> 18));
        out += static_cast<char>(0x80 | ((code_point >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
}

void appendUTF16(const std::vector<UInt16> & units, String & out)
{
    for (size_t i = 0; i < units.size(); ++i)
    {
        UInt32 unit = units[i];
        if (unit >= 0xD800 && unit < 0xDC00 && i + 1 < units.size() && units[i + 1] >= 0xDC00 && units[i + 1] < 0xE000)
        {
            appendUTF8(0x10000 + ((unit - 0xD800) << 10) + (units[i + 1] - 0xDC00), out);
            ++i;
        }
        else if (unit >= 0xD800 && unit < 0xE000)
            appendUTF8(0xFFFD, out); /// A lone surrogate.
        else
            appendUTF8(unit, out);
    }
}

/// Windows-1252 differs from ISO-8859-1 in the range 0x80..0x9F.
constexpr UInt16 cp1252_high[32] = {
    0x20AC, 0xFFFD, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0xFFFD, 0x017D, 0xFFFD,
    0xFFFD, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0xFFFD, 0x017E, 0x0178};

/// Converts an 8-bit string of BIFF5 in the code page of the workbook to UTF-8.
void appendCodePageString(std::string_view bytes, UInt16 code_page, String & out)
{
    for (char c : bytes)
    {
        auto byte = static_cast<UInt8>(c);
        if (byte < 0x80)
            out += c;
        else if (code_page == 1252 || code_page == 0)
            appendUTF8(byte < 0xA0 ? cp1252_high[byte - 0x80] : byte, out);
        else if (code_page == 28591)
            appendUTF8(byte, out);
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The XLS file uses the code page {} for text, which is not supported. Only Windows-1252 and ISO-8859-1 are supported "
                "for files of Excel 5.0/95; files of Excel 97 and later use Unicode",
                code_page);
    }
}


/** The Compound File Binary format (also known as OLE2 structured storage): a file system inside a file,
  * which contains the workbook stream of an XLS file.
  * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cfb
  */
class CompoundFile
{
public:
    explicit CompoundFile(std::string_view data_);

    /// Returns the content of the first stream with one of the names (compared case-insensitively).
    std::optional<String> readStream(std::initializer_list<std::string_view> names) const;

private:
    static constexpr UInt32 END_OF_CHAIN = 0xFFFFFFFE;
    static constexpr UInt32 FREE_SECTOR = 0xFFFFFFFF;
    static constexpr UInt32 MINI_SECTOR_SIZE = 64;

    struct Entry
    {
        String name; /// Lowercase.
        UInt8 type = 0; /// 1 - storage, 2 - stream, 5 - root.
        UInt32 start_sector = 0;
        UInt64 size = 0;
    };

    std::string_view sector(UInt32 id) const;
    /// Reads a chain of sectors up to `size` bytes, or up to the end of the chain if `size` is not specified.
    String readChain(UInt32 start, std::optional<UInt64> size) const;
    String readMiniChain(UInt32 start, UInt64 size) const;

    std::string_view data;
    UInt32 sector_size = 512;
    UInt32 mini_stream_cutoff = 4096;
    std::vector<UInt32> fat;
    std::vector<UInt32> mini_fat;
    std::vector<Entry> entries;
    String mini_stream;
};

CompoundFile::CompoundFile(std::string_view data_) : data(data_)
{
    static constexpr char signature[] = "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1";
    if (data.size() < 512 || data.substr(0, 8) != std::string_view(signature, 8))
        throw Exception(ErrorCodes::INCORRECT_DATA, "The file is not an XLS file: it does not have the signature of the Compound File Binary format");

    UInt16 sector_shift = readU16(data, 30);
    if (sector_shift != 9 && sector_shift != 12)
        throwCorrupted(fmt::format("invalid sector size 2^{}", sector_shift));
    sector_size = 1u << sector_shift;
    mini_stream_cutoff = readU32(data, 56);
    UInt32 first_directory_sector = readU32(data, 48);
    UInt32 first_mini_fat_sector = readU32(data, 60);
    UInt32 first_difat_sector = readU32(data, 68);
    UInt32 num_difat_sectors = readU32(data, 72);

    /// The locations of the sectors of the file allocation table: 109 in the header, the rest in the chain of DIFAT sectors.
    std::vector<UInt32> fat_sectors;
    for (size_t i = 0; i < 109; ++i)
    {
        UInt32 id = readU32(data, 76 + i * 4);
        if (id != FREE_SECTOR)
            fat_sectors.push_back(id);
    }
    UInt32 difat_sector = first_difat_sector;
    for (UInt32 i = 0; i < num_difat_sectors && difat_sector != END_OF_CHAIN && difat_sector != FREE_SECTOR; ++i)
    {
        auto content = sector(difat_sector);
        size_t entries_per_sector = sector_size / 4 - 1;
        for (size_t j = 0; j < entries_per_sector; ++j)
        {
            UInt32 id = readU32(content, j * 4);
            if (id != FREE_SECTOR)
                fat_sectors.push_back(id);
        }
        difat_sector = readU32(content, entries_per_sector * 4);
    }

    if (fat_sectors.size() > data.size() / sector_size + 1)
        throwCorrupted("too many sectors in the file allocation table");
    fat.reserve(fat_sectors.size() * sector_size / 4);
    for (UInt32 id : fat_sectors)
    {
        auto content = sector(id);
        for (size_t j = 0; j + 4 <= content.size(); j += 4)
            fat.push_back(readU32(content, j));
    }

    String directory = readChain(first_directory_sector, {});
    for (size_t offset = 0; offset + 128 <= directory.size(); offset += 128)
    {
        std::string_view raw(directory.data() + offset, 128);
        Entry entry;
        UInt16 name_size = std::min<UInt16>(readU16(raw, 64), 64);
        for (size_t i = 0; i + 1 < name_size; i += 2)
        {
            UInt16 unit = readU16(raw, i);
            if (unit == 0)
                break;
            entry.name += unit < 0x80 ? toLowerIfAlphaASCII(static_cast<char>(unit)) : '?';
        }
        entry.type = readU8(raw, 66);
        entry.start_sector = readU32(raw, 116);
        /// The high 32 bits of the size must be zero in version 3 files, but some writers leave garbage there.
        entry.size = readU32(raw, 120);
        if (readU16(data, 26) >= 4)
            entry.size |= static_cast<UInt64>(readU32(raw, 124)) << 32;
        entries.push_back(std::move(entry));
    }

    if (entries.empty() || entries[0].type != 5)
        throwCorrupted("the root directory entry is missing");

    if (first_mini_fat_sector != END_OF_CHAIN && first_mini_fat_sector != FREE_SECTOR)
    {
        String mini_fat_data = readChain(first_mini_fat_sector, {});
        for (size_t j = 0; j + 4 <= mini_fat_data.size(); j += 4)
            mini_fat.push_back(readU32(mini_fat_data, j));
        mini_stream = readChain(entries[0].start_sector, entries[0].size);
    }
}

std::string_view CompoundFile::sector(UInt32 id) const
{
    UInt64 offset = (static_cast<UInt64>(id) + 1) * sector_size;
    if (offset >= data.size())
        throwCorrupted(fmt::format("sector {} is outside of the file", id));
    /// The last sector may be truncated.
    return data.substr(offset, std::min<UInt64>(sector_size, data.size() - offset));
}

String CompoundFile::readChain(UInt32 start, std::optional<UInt64> size) const
{
    String result;
    if (size)
    {
        if (*size > data.size())
            throwCorrupted("a stream is larger than the file");
        result.reserve(*size);
    }

    UInt32 id = start;
    size_t num_sectors = 0;
    while (id != END_OF_CHAIN && (!size || result.size() < *size))
    {
        if (id >= fat.size())
            throwCorrupted(fmt::format("invalid sector {} in a chain", id));
        if (++num_sectors > fat.size())
            throwCorrupted("a chain of sectors has a cycle");
        auto content = sector(id);
        result.append(content.data(), content.size());
        id = fat[id];
    }

    if (size)
    {
        if (result.size() < *size)
            throwCorrupted("a stream is truncated");
        result.resize(*size);
    }
    return result;
}

String CompoundFile::readMiniChain(UInt32 start, UInt64 size) const
{
    String result;
    if (size > mini_stream.size())
        throwCorrupted("a stream is larger than the mini stream");
    result.reserve(size);

    UInt32 id = start;
    size_t num_sectors = 0;
    while (result.size() < size)
    {
        if (id >= mini_fat.size())
            throwCorrupted(fmt::format("invalid mini sector {} in a chain", id));
        if (++num_sectors > mini_fat.size())
            throwCorrupted("a chain of mini sectors has a cycle");
        UInt64 offset = static_cast<UInt64>(id) * MINI_SECTOR_SIZE;
        if (offset >= mini_stream.size())
            throwCorrupted(fmt::format("mini sector {} is outside of the mini stream", id));
        result.append(mini_stream, offset, std::min<UInt64>(MINI_SECTOR_SIZE, mini_stream.size() - offset));
        id = mini_fat[id];
    }
    result.resize(size);
    return result;
}

std::optional<String> CompoundFile::readStream(std::initializer_list<std::string_view> names) const
{
    for (auto name : names)
    {
        for (const auto & entry : entries)
        {
            if (entry.type != 2 || entry.name != name)
                continue;
            if (entry.size < mini_stream_cutoff)
                return readMiniChain(entry.start_sector, entry.size);
            return readChain(entry.start_sector, entry.size);
        }
    }
    return {};
}


/// Reads data that may span a record and the following `CONTINUE` records.
class ContinuedRecordReader
{
public:
    explicit ContinuedRecordReader(std::vector<std::string_view> segments_) : segments(std::move(segments_)) {}

    UInt8 readU8()
    {
        moveToData();
        return static_cast<UInt8>(segments[segment][pos++]);
    }

    UInt16 readU16()
    {
        UInt16 low = readU8();
        return static_cast<UInt16>(low | (readU8() << 8));
    }

    UInt32 readU32()
    {
        UInt32 low = readU16();
        return low | (static_cast<UInt32>(readU16()) << 16);
    }

    void skip(size_t size)
    {
        while (size)
        {
            moveToData();
            size_t available = std::min(size, segments[segment].size() - pos);
            pos += available;
            size -= available;
        }
    }

    /// Reads `count` characters, each of 1 or 2 bytes. If the characters continue in the next `CONTINUE` record,
    /// it starts with a byte of flags that tells the size of the characters in that record.
    void readCharacters(size_t count, bool two_byte_characters, std::vector<UInt16> & out)
    {
        while (count)
        {
            if (segment >= segments.size())
                throwCorrupted("unexpected end of a string");
            size_t available_bytes = segments[segment].size() - pos;
            size_t available = two_byte_characters ? available_bytes / 2 : available_bytes;
            if (available == 0)
            {
                ++segment;
                pos = 0;
                two_byte_characters = readU8() & 1;
                continue;
            }
            size_t n = std::min(count, available);
            const char * data = segments[segment].data() + pos;
            for (size_t i = 0; i < n; ++i)
            {
                if (two_byte_characters)
                    out.push_back(static_cast<UInt16>(static_cast<UInt8>(data[2 * i]) | (static_cast<UInt8>(data[2 * i + 1]) << 8)));
                else
                    out.push_back(static_cast<UInt8>(data[i]));
            }
            pos += two_byte_characters ? 2 * n : n;
            count -= n;
        }
    }

    /// Reads a BIFF8 Unicode string: the number of characters (of 1 or 2 bytes), the flags, and optionally the formatting runs
    /// and the phonetic data, which are skipped.
    void readUnicodeString(bool one_byte_length, String & out)
    {
        size_t count = one_byte_length ? readU8() : readU16();
        UInt8 flags = readU8();
        size_t num_runs = (flags & 0x08) ? readU16() : 0;
        size_t extended_size = (flags & 0x04) ? readU32() : 0;
        units.clear();
        readCharacters(count, flags & 0x01, units);
        appendUTF16(units, out);
        skip(num_runs * 4);
        skip(extended_size);
    }

private:
    void moveToData()
    {
        while (segment < segments.size() && pos >= segments[segment].size())
        {
            ++segment;
            pos = 0;
        }
        if (segment >= segments.size())
            throwCorrupted("unexpected end of a record");
    }

    std::vector<std::string_view> segments;
    size_t segment = 0;
    size_t pos = 0;
    std::vector<UInt16> units;
};


namespace Record
{
    constexpr UInt16 FORMULA = 0x0006;
    constexpr UInt16 EOF_ = 0x000A;
    constexpr UInt16 DATEMODE = 0x0022;
    constexpr UInt16 FILEPASS = 0x002F;
    constexpr UInt16 CONTINUE = 0x003C;
    constexpr UInt16 CODEPAGE = 0x0042;
    constexpr UInt16 BOUNDSHEET = 0x0085;
    constexpr UInt16 MULRK = 0x00BD;
    constexpr UInt16 RSTRING = 0x00D6;
    constexpr UInt16 XF = 0x00E0;
    constexpr UInt16 SST = 0x00FC;
    constexpr UInt16 LABELSST = 0x00FD;
    constexpr UInt16 DIMENSIONS = 0x0200;
    constexpr UInt16 NUMBER = 0x0203;
    constexpr UInt16 LABEL = 0x0204;
    constexpr UInt16 BOOLERR = 0x0205;
    constexpr UInt16 STRING = 0x0207;
    constexpr UInt16 RK = 0x027E;
    constexpr UInt16 FORMAT = 0x041E;
    constexpr UInt16 BOF = 0x0809;
    /// The BOF records of BIFF2, BIFF3 and BIFF4.
    constexpr UInt16 BOF_BIFF2 = 0x0009;
    constexpr UInt16 BOF_BIFF3 = 0x0209;
    constexpr UInt16 BOF_BIFF4 = 0x0409;
}

bool isBOF(UInt16 type)
{
    return type == Record::BOF || type == Record::BOF_BIFF2 || type == Record::BOF_BIFF3 || type == Record::BOF_BIFF4;
}

class RecordReader
{
public:
    RecordReader(std::string_view stream_, size_t pos_) : stream(stream_), pos(pos_) {}

    bool next(UInt16 & type, std::string_view & data)
    {
        if (pos + 4 > stream.size())
            return false;
        type = readU16(stream, pos);
        UInt16 size = readU16(stream, pos + 2);
        if (pos + 4 + size > stream.size())
            throwCorrupted("a record is truncated");
        data = stream.substr(pos + 4, size);
        pos += 4 + size;
        return true;
    }

    /// Returns the record and all following `CONTINUE` records.
    std::vector<std::string_view> readContinuations(std::string_view first)
    {
        std::vector<std::string_view> segments{first};
        while (pos + 4 <= stream.size() && readU16(stream, pos) == Record::CONTINUE)
        {
            UInt16 type;
            std::string_view data;
            next(type, data);
            segments.push_back(data);
        }
        return segments;
    }

private:
    std::string_view stream;
    size_t pos;
};

double decodeRK(UInt32 rk)
{
    double value;
    if (rk & 0x02)
        value = static_cast<Int32>(rk) >> 2;
    else
        value = std::bit_cast<double>(static_cast<UInt64>(rk & 0xFFFFFFFC) << 32);
    if (rk & 0x01)
        value /= 100;
    return value;
}

String errorText(UInt8 code)
{
    switch (code)
    {
        case 0x00: return "#NULL!";
        case 0x07: return "#DIV/0!";
        case 0x0F: return "#VALUE!";
        case 0x17: return "#REF!";
        case 0x1D: return "#NAME?";
        case 0x24: return "#NUM!";
        case 0x2A: return "#N/A";
        default: return "#ERROR!";
    }
}

struct CellRecord
{
    UInt32 row;
    UInt32 column;
    Cell cell;
};


class XLSSheetReader final : public ISheetReader
{
public:
    XLSSheetReader(std::vector<CellRecord> cells_, std::optional<CellRange> dimension_, bool date1904_)
        : cells(std::move(cells_)), dimension(dimension_), date1904(date1904_)
    {
    }

    bool readRow(Row & row) override
    {
        if (pos >= cells.size())
            return false;
        row.index = cells[pos].row;
        row.cells.clear();
        while (pos < cells.size() && cells[pos].row == row.index)
        {
            row.cells.emplace_back(cells[pos].column, std::move(cells[pos].cell));
            ++pos;
        }
        return true;
    }

    std::optional<CellRange> getDimension() const override { return dimension; }
    bool isDate1904() const override { return date1904; }

private:
    std::vector<CellRecord> cells;
    size_t pos = 0;
    std::optional<CellRange> dimension;
    bool date1904;
};


class XLSWorkbook
{
public:
    explicit XLSWorkbook(std::string_view data);

    std::unique_ptr<ISheetReader> openSheet(const String & sheet);

private:
    void readGlobals();
    void readString(std::string_view data, size_t offset, bool one_byte_length, String & out) const;

    String stream;
    bool biff8 = true;
    UInt16 code_page = 1252;
    bool date1904 = false;
    std::unordered_map<UInt16, String> custom_formats;
    std::vector<UInt16> xf_formats;
    std::vector<NumberFormatKind> xf_kinds;
    std::vector<SheetInfo> sheets;
    std::vector<UInt32> sheet_offsets;
    std::vector<String> shared_strings;
};

XLSWorkbook::XLSWorkbook(std::string_view data)
{
    CompoundFile compound_file(data);
    auto workbook_stream = compound_file.readStream({"workbook", "book"});
    if (!workbook_stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The file is not an XLS file: it does not contain a workbook stream");
    stream = std::move(*workbook_stream);
    readGlobals();
}

void XLSWorkbook::readString(std::string_view data, size_t offset, bool one_byte_length, String & out) const
{
    if (offset > data.size())
        throwCorrupted("unexpected end of a record");
    if (biff8)
    {
        ContinuedRecordReader reader({data.substr(offset)});
        reader.readUnicodeString(one_byte_length, out);
    }
    else
    {
        size_t count = one_byte_length ? readU8(data, offset) : readU16(data, offset);
        size_t begin = offset + (one_byte_length ? 1 : 2);
        if (begin + count > data.size())
            throwCorrupted("unexpected end of a string");
        appendCodePageString(data.substr(begin, count), code_page, out);
    }
}

void XLSWorkbook::readGlobals()
{
    RecordReader reader(stream, 0);
    UInt16 type;
    std::string_view data;
    if (!reader.next(type, data) || !isBOF(type))
        throwCorrupted("the workbook stream does not start with a BOF record");
    if (type != Record::BOF)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "XLS files of Excel 2.x, 3.x and 4.x (BIFF2-BIFF4) are not supported");
    biff8 = readU16(data, 0) >= 0x0600;

    while (reader.next(type, data))
    {
        switch (type)
        {
            case Record::EOF_:
                break;
            case Record::FILEPASS:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Encrypted (password-protected) XLS files are not supported");
            case Record::CODEPAGE:
                code_page = readU16(data, 0);
                continue;
            case Record::DATEMODE:
                date1904 = readU16(data, 0) == 1;
                continue;
            case Record::FORMAT:
            {
                String code;
                readString(data, 2, !biff8, code);
                custom_formats[readU16(data, 0)] = std::move(code);
                continue;
            }
            case Record::XF:
                xf_formats.push_back(readU16(data, 2));
                continue;
            case Record::BOUNDSHEET:
            {
                SheetInfo info;
                info.is_visible = (readU8(data, 4) & 0x03) == 0;
                info.is_worksheet = readU8(data, 5) == 0;
                readString(data, 6, true, info.name);
                sheets.push_back(std::move(info));
                sheet_offsets.push_back(readU32(data, 0));
                continue;
            }
            case Record::SST:
            {
                ContinuedRecordReader sst(reader.readContinuations(data));
                sst.skip(4);
                UInt32 count = sst.readU32();
                /// Each string takes at least 3 bytes, so a corrupted count cannot cause a huge allocation.
                shared_strings.reserve(std::min<size_t>(count, stream.size() / 3));
                for (UInt32 i = 0; i < count; ++i)
                    sst.readUnicodeString(false, shared_strings.emplace_back());
                continue;
            }
            default:
                continue;
        }
        break;
    }

    xf_kinds.reserve(xf_formats.size());
    for (UInt16 format : xf_formats)
    {
        auto it = custom_formats.find(format);
        xf_kinds.push_back(classifyNumberFormat(format, it == custom_formats.end() ? std::string_view{} : it->second));
    }
}

std::unique_ptr<ISheetReader> XLSWorkbook::openSheet(const String & sheet)
{
    size_t sheet_index = selectSheet(sheets, sheet);

    std::vector<CellRecord> cells;
    std::optional<CellRange> dimension;

    auto format_kind = [&](UInt16 xf)
    {
        return xf < xf_kinds.size() ? xf_kinds[xf] : NumberFormatKind::Number;
    };
    auto add_number = [&](UInt32 row, UInt32 column, UInt16 xf, double value)
    {
        auto & record = cells.emplace_back(CellRecord{row, column, {}});
        record.cell.type = CellType::Number;
        record.cell.number = value;
        record.cell.format = format_kind(xf);
    };
    auto add_string = [&](UInt32 row, UInt32 column) -> String &
    {
        auto & record = cells.emplace_back(CellRecord{row, column, {}});
        record.cell.type = CellType::String;
        return record.cell.text;
    };

    /// The index of the cell with the result of a string formula, which is stored in the following `STRING` record.
    std::optional<size_t> pending_string_formula;

    RecordReader reader(stream, sheet_offsets[sheet_index]);
    UInt16 type;
    std::string_view data;
    if (!reader.next(type, data) || !isBOF(type))
        throwCorrupted(fmt::format("the sheet '{}' does not start with a BOF record", sheets[sheet_index].name));

    /// Sheets may contain embedded substreams (like charts) that have their own BOF and EOF records.
    size_t depth = 1;
    while (depth && reader.next(type, data))
    {
        if (isBOF(type))
        {
            ++depth;
            continue;
        }
        if (type == Record::EOF_)
        {
            --depth;
            continue;
        }
        if (depth > 1)
            continue;

        switch (type)
        {
            case Record::DIMENSIONS:
            {
                UInt32 first_row = data.size() >= 14 ? readU32(data, 0) : readU16(data, 0);
                UInt16 first_column = data.size() >= 14 ? readU16(data, 8) : readU16(data, 4);
                dimension = CellRange{.first_row = first_row, .first_column = first_column, .last_row = {}, .last_column = {}};
                break;
            }
            case Record::NUMBER:
                add_number(readU16(data, 0), readU16(data, 2), readU16(data, 4), readDouble(data, 6));
                break;
            case Record::RK:
                add_number(readU16(data, 0), readU16(data, 2), readU16(data, 4), decodeRK(readU32(data, 6)));
                break;
            case Record::MULRK:
            {
                UInt16 row = readU16(data, 0);
                UInt16 first_column = readU16(data, 2);
                size_t count = data.size() >= 6 ? (data.size() - 6) / 6 : 0;
                for (size_t i = 0; i < count; ++i)
                    add_number(row, first_column + static_cast<UInt32>(i), readU16(data, 4 + i * 6), decodeRK(readU32(data, 6 + i * 6)));
                break;
            }
            case Record::LABELSST:
            {
                UInt32 index = readU32(data, 6);
                if (index >= shared_strings.size())
                    throwCorrupted(fmt::format("invalid shared string index {}", index));
                add_string(readU16(data, 0), readU16(data, 2)) = shared_strings[index];
                break;
            }
            case Record::LABEL:
            case Record::RSTRING:
            {
                String & text = add_string(readU16(data, 0), readU16(data, 2));
                readString(data, 6, false, text);
                break;
            }
            case Record::BOOLERR:
            {
                auto & record = cells.emplace_back(CellRecord{readU16(data, 0), readU16(data, 2), {}});
                UInt8 value = readU8(data, 6);
                if (readU8(data, 7))
                {
                    record.cell.type = CellType::Error;
                    record.cell.text = errorText(value);
                }
                else
                {
                    record.cell.type = CellType::Bool;
                    record.cell.number = value ? 1 : 0;
                }
                break;
            }
            case Record::FORMULA:
            {
                UInt16 row = readU16(data, 0);
                UInt16 column = readU16(data, 2);
                UInt16 xf = readU16(data, 4);
                if (readU16(data, 12) != 0xFFFF)
                {
                    add_number(row, column, xf, readDouble(data, 6));
                    break;
                }
                /// The result is not a number.
                UInt8 result_type = readU8(data, 6);
                if (result_type == 0)
                {
                    add_string(row, column);
                    pending_string_formula = cells.size() - 1;
                }
                else if (result_type == 1 || result_type == 2)
                {
                    auto & record = cells.emplace_back(CellRecord{row, column, {}});
                    if (result_type == 1)
                    {
                        record.cell.type = CellType::Bool;
                        record.cell.number = readU8(data, 8) ? 1 : 0;
                    }
                    else
                    {
                        record.cell.type = CellType::Error;
                        record.cell.text = errorText(readU8(data, 8));
                    }
                }
                else if (result_type == 3)
                    add_string(row, column); /// An empty string.
                break;
            }
            case Record::STRING:
            {
                if (!pending_string_formula)
                    break;
                String & text = cells[*pending_string_formula].cell.text;
                if (biff8)
                {
                    ContinuedRecordReader string_reader(reader.readContinuations(data));
                    string_reader.readUnicodeString(false, text);
                }
                else
                    readString(data, 0, false, text);
                pending_string_formula.reset();
                break;
            }
            default:
                break;
        }
    }

    /// Cells are usually ordered, but it is not required. If a cell is present more than once, the last record wins.
    std::stable_sort(cells.begin(), cells.end(), [](const CellRecord & a, const CellRecord & b)
    {
        return a.row < b.row || (a.row == b.row && a.column < b.column);
    });
    size_t unique_size = 0;
    for (size_t i = 0; i < cells.size(); ++i)
    {
        if (unique_size && cells[unique_size - 1].row == cells[i].row && cells[unique_size - 1].column == cells[i].column)
            cells[unique_size - 1] = std::move(cells[i]);
        else if (unique_size != i)
            cells[unique_size++] = std::move(cells[i]);
        else
            ++unique_size;
    }
    cells.resize(unique_size);

    return std::make_unique<XLSSheetReader>(std::move(cells), dimension, date1904);
}

}

std::unique_ptr<ISheetReader> openXLSSheet(std::string_view data, const String & sheet)
{
    XLSWorkbook workbook(data);
    return workbook.openSheet(sheet);
}

}

}
