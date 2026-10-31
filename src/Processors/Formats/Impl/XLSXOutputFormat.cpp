#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/ExcelCommon.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/intExp10.h>

#include <zlib.h>

#include <cmath>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_COLUMNS;
    extern const int ZLIB_DEFLATE_FAILED;
}

namespace
{

/** Writes a ZIP archive to a non-seekable stream. The entries are compressed with deflate, and their sizes and checksums
  * are written in data descriptors after the data. Zip64 is not used, because some versions of Excel cannot open such files,
  * so the archive is limited to 4 GiB.
  */
class ZipStreamWriter
{
public:
    explicit ZipStreamWriter(WriteBuffer & out_) : out(out_) {}

    ~ZipStreamWriter()
    {
        if (deflate_initialized)
            deflateEnd(&zstream);
    }

    void beginEntry(const String & name)
    {
        if (in_entry)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A ZIP entry is already being written");

        current = Entry{.name = name, .offset = offset};
        writeU32(0x04034b50); /// Local file header signature.
        writeU16(20); /// Version needed to extract: 2.0 (deflate).
        writeU16(FLAGS);
        writeU16(8); /// Deflate.
        writeU16(DOS_TIME);
        writeU16(DOS_DATE);
        writeU32(0); /// The checksum and sizes are in the data descriptor.
        writeU32(0);
        writeU32(0);
        writeU16(static_cast<UInt16>(name.size()));
        writeU16(0); /// Extra field length.
        writeBytes(name.data(), name.size());

        zstream = {};
        if (deflateInit2(&zstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -MAX_WBITS, 8, Z_DEFAULT_STRATEGY) != Z_OK)
            throw Exception(ErrorCodes::ZLIB_DEFLATE_FAILED, "Cannot initialize deflate: {}", zstream.msg ? zstream.msg : "");
        deflate_initialized = true;
        crc = static_cast<UInt32>(crc32_z(0, nullptr, 0));
        in_entry = true;
    }

    void write(const char * data, size_t size)
    {
        if (!size)
            return;
        crc = static_cast<UInt32>(crc32_z(crc, reinterpret_cast<const Bytef *>(data), size));
        current.uncompressed_size += size;
        compress(data, size, Z_NO_FLUSH);
    }

    void endEntry()
    {
        compress(nullptr, 0, Z_FINISH);
        deflateEnd(&zstream);
        deflate_initialized = false;
        in_entry = false;

        current.crc = crc;
        checkSize(current.compressed_size);
        checkSize(current.uncompressed_size);
        writeU32(0x08074b50); /// Data descriptor signature.
        writeU32(current.crc);
        writeU32(static_cast<UInt32>(current.compressed_size));
        writeU32(static_cast<UInt32>(current.uncompressed_size));
        entries.push_back(std::move(current));
    }

    void finish()
    {
        UInt64 central_directory_offset = offset;
        for (const auto & entry : entries)
        {
            writeU32(0x02014b50); /// Central directory file header signature.
            writeU16(20); /// Version made by.
            writeU16(20); /// Version needed to extract.
            writeU16(FLAGS);
            writeU16(8);
            writeU16(DOS_TIME);
            writeU16(DOS_DATE);
            writeU32(entry.crc);
            writeU32(static_cast<UInt32>(entry.compressed_size));
            writeU32(static_cast<UInt32>(entry.uncompressed_size));
            writeU16(static_cast<UInt16>(entry.name.size()));
            writeU16(0); /// Extra field length.
            writeU16(0); /// Comment length.
            writeU16(0); /// Disk number.
            writeU16(0); /// Internal attributes.
            writeU32(0); /// External attributes.
            writeU32(static_cast<UInt32>(entry.offset));
            writeBytes(entry.name.data(), entry.name.size());
        }
        UInt64 central_directory_size = offset - central_directory_offset;
        checkSize(offset);

        writeU32(0x06054b50); /// End of central directory signature.
        writeU16(0);
        writeU16(0);
        writeU16(static_cast<UInt16>(entries.size()));
        writeU16(static_cast<UInt16>(entries.size()));
        writeU32(static_cast<UInt32>(central_directory_size));
        writeU32(static_cast<UInt32>(central_directory_offset));
        writeU16(0); /// Comment length.
    }

private:
    /// Bit 3: the sizes are in the data descriptor; bit 11: the names are in UTF-8.
    static constexpr UInt16 FLAGS = 0x0808;
    /// 1980-01-01 00:00:00, so that the output does not depend on the current time.
    static constexpr UInt16 DOS_TIME = 0;
    static constexpr UInt16 DOS_DATE = (0 << 9) | (1 << 5) | 1;

    struct Entry
    {
        String name;
        UInt64 offset = 0;
        UInt32 crc = 0;
        UInt64 compressed_size = 0;
        UInt64 uncompressed_size = 0;
    };

    static void checkSize(UInt64 size)
    {
        if (size > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "The XLSX output is too large: files and sheets larger than 4 GiB are not supported");
    }

    void compress(const char * data, size_t size, int flush)
    {
        zstream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(data));
        zstream.avail_in = static_cast<uInt>(size);
        while (true)
        {
            zstream.next_out = reinterpret_cast<Bytef *>(compressed.data());
            zstream.avail_out = static_cast<uInt>(compressed.size());
            int result = deflate(&zstream, flush);
            if (result != Z_OK && result != Z_STREAM_END && result != Z_BUF_ERROR)
                throw Exception(ErrorCodes::ZLIB_DEFLATE_FAILED, "Deflate failed: {}", zstream.msg ? zstream.msg : "");
            size_t produced = compressed.size() - zstream.avail_out;
            writeBytes(compressed.data(), produced);
            current.compressed_size += produced;
            if (flush == Z_FINISH ? result == Z_STREAM_END : (zstream.avail_in == 0 && zstream.avail_out != 0))
                break;
        }
    }

    void writeBytes(const char * data, size_t size)
    {
        out.write(data, size);
        offset += size;
    }

    void writeU16(UInt16 value)
    {
        writeBinaryLittleEndian(value, out);
        offset += 2;
    }

    void writeU32(UInt32 value)
    {
        writeBinaryLittleEndian(value, out);
        offset += 4;
    }

    WriteBuffer & out;
    UInt64 offset = 0;
    std::vector<Entry> entries;
    Entry current;
    bool in_entry = false;
    z_stream zstream{};
    bool deflate_initialized = false;
    UInt32 crc = 0;
    std::array<char, 65536> compressed{};
};


/// A buffer that writes into the current entry of a ZIP archive.
class ZipEntryWriteBuffer final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit ZipEntryWriteBuffer(ZipStreamWriter & zip_) : BufferWithOwnMemory<WriteBuffer>(DBMS_DEFAULT_BUFFER_SIZE), zip(zip_) {}

private:
    void nextImpl() override { zip.write(working_buffer.begin(), offset()); }

    ZipStreamWriter & zip;
};


/// Writes text for an XML element or attribute: escapes the special characters of XML, and the characters that cannot be represented
/// in XML, with the `_xHHHH_` escape sequences of Office Open XML. Invalid UTF-8 sequences are replaced with U+FFFD.
void writeXMLText(std::string_view str, WriteBuffer & out, bool escape_quote = false)
{
    auto write_escape = [&](UInt32 code)
    {
        static constexpr char hex_digits[] = "0123456789ABCDEF";
        char buf[7] = {'_', 'x', hex_digits[(code >> 12) & 0xF], hex_digits[(code >> 8) & 0xF], hex_digits[(code >> 4) & 0xF], hex_digits[code & 0xF], '_'};
        out.write(buf, sizeof(buf));
    };

    const auto * pos = reinterpret_cast<const UInt8 *>(str.data());
    const auto * end = pos + str.size();
    while (pos < end)
    {
        UInt8 c = *pos;
        if (c >= 0x80)
        {
            /// Validate a multi-byte UTF-8 sequence.
            size_t length = 0;
            UInt32 code_point = 0;
            if (c >= 0xC2 && c <= 0xDF)
            {
                length = 2;
                code_point = c & 0x1F;
            }
            else if (c >= 0xE0 && c <= 0xEF)
            {
                length = 3;
                code_point = c & 0x0F;
            }
            else if (c >= 0xF0 && c <= 0xF4)
            {
                length = 4;
                code_point = c & 0x07;
            }

            bool valid = length && pos + length <= end;
            for (size_t i = 1; valid && i < length; ++i)
            {
                if ((pos[i] & 0xC0) != 0x80)
                    valid = false;
                code_point = (code_point << 6) | (pos[i] & 0x3F);
            }
            if (valid)
                valid = !(length == 3 && (code_point < 0x800 || (code_point >= 0xD800 && code_point < 0xE000)))
                    && !(length == 4 && (code_point < 0x10000 || code_point > 0x10FFFF));

            if (!valid)
            {
                writeCString("\xEF\xBF\xBD", out);
                ++pos;
            }
            else if (code_point == 0xFFFE || code_point == 0xFFFF)
            {
                write_escape(code_point);
                pos += length;
            }
            else
            {
                out.write(reinterpret_cast<const char *>(pos), length);
                pos += length;
            }
            continue;
        }

        switch (c)
        {
            case '&': writeCString("&amp;", out); break;
            case '<': writeCString("&lt;", out); break;
            case '>': writeCString("&gt;", out); break;
            case '"':
                if (escape_quote)
                    writeCString("&quot;", out);
                else
                    out.write('"');
                break;
            /// XML parsers normalize CR characters to LF, unless they are written as character references.
            case '\r': writeCString("&#13;", out); break;
            case '\t':
            case '\n':
                out.write(static_cast<char>(c));
                break;
            case '_':
            {
                /// Escape the underscore of a literal sequence like `_x0041_`, so that it is not decoded.
                std::string_view rest(reinterpret_cast<const char *>(pos), end - pos);
                if (rest.size() >= 7 && rest[1] == 'x' && rest[6] == '_' && isHexDigit(rest[2]) && isHexDigit(rest[3]) && isHexDigit(rest[4])
                    && isHexDigit(rest[5]))
                    write_escape('_');
                else
                    out.write('_');
                break;
            }
            default:
                if (c < 0x20)
                    write_escape(c);
                else
                    out.write(static_cast<char>(c));
        }
        ++pos;
    }
}

String escapeXMLAttribute(std::string_view str)
{
    WriteBufferFromOwnString buf;
    writeXMLText(str, buf, true);
    return buf.str();
}

/// The number of code points in a valid UTF-8 string.
size_t utf8Length(std::string_view str)
{
    size_t length = 0;
    for (char c : str)
        length += (static_cast<UInt8>(c) & 0xC0) != 0x80;
    return length;
}

/// The prefix of a valid UTF-8 string with at most `length` code points.
std::string_view utf8Prefix(std::string_view str, size_t length)
{
    size_t count = 0;
    for (size_t i = 0; i < str.size(); ++i)
    {
        if ((static_cast<UInt8>(str[i]) & 0xC0) != 0x80)
        {
            if (count == length)
                return str.substr(0, i);
            ++count;
        }
    }
    return str;
}

constexpr size_t MAX_SHEET_NAME_LENGTH = 31;

void validateSheetName(const String & name)
{
    if (name.empty() || utf8Length(name) > MAX_SHEET_NAME_LENGTH || name.find_first_of("[]:*?/\\") != String::npos || name.front() == '\''
        || name.back() == '\'')
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid sheet name '{}' for the XLSX format: it must have from 1 to {} characters, must not contain any of the characters "
            "[ ] : * ? / \\, and must not start or end with an apostrophe",
            name, MAX_SHEET_NAME_LENGTH);
}

/// The styles of cells, as indices in `cellXfs` of the style sheet.
enum CellStyle : uint8_t
{
    STYLE_DEFAULT = 0,
    STYLE_HEADER = 1,
    STYLE_DATE = 2,
    STYLE_DATE_TIME = 3,
    STYLE_DATE_TIME64 = 4,
};

constexpr auto styles_xml = R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">)"
    R"(<numFmts count="3"><numFmt numFmtId="164" formatCode="yyyy-mm-dd"/><numFmt numFmtId="165" formatCode="yyyy-mm-dd hh:mm:ss"/>)"
    R"(<numFmt numFmtId="166" formatCode="yyyy-mm-dd hh:mm:ss.000"/></numFmts>)"
    R"(<fonts count="2"><font><sz val="11"/><name val="Calibri"/><family val="2"/></font>)"
    R"(<font><b/><sz val="11"/><name val="Calibri"/><family val="2"/></font></fonts>)"
    R"(<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>)"
    R"(<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>)"
    R"(<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>)"
    R"(<cellXfs count="5"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>)"
    R"(<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>)"
    R"(<xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>)"
    R"(<xf numFmtId="165" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>)"
    R"(<xf numFmtId="166" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/></cellXfs>)"
    R"(<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>)"
    R"(</styleSheet>)";

constexpr auto package_relationships_xml = R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">)"
    R"(<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>)"
    R"(</Relationships>)";


class XLSXOutputFormat final : public IOutputFormat
{
public:
    XLSXOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
        : IOutputFormat(header_, out_)
        , settings(settings_)
        , zip(out)
    {
        validateSheetName(settings.excel.output_sheet_name);

        const auto & header = getPort(PortKind::Main).getHeader();
        if (header.columns() > Excel::XLSX_MAX_COLUMNS)
            throw Exception(
                ErrorCodes::TOO_MANY_COLUMNS,
                "Too many columns for the XLSX format: {}, the maximum is {}", header.columns(), Excel::XLSX_MAX_COLUMNS);

        for (size_t i = 0; i < header.columns(); ++i)
        {
            const auto & column = header.getByPosition(i);
            auto type = removeLowCardinalityAndNullable(column.type);
            column_types.push_back(type);
            serializations.push_back(type->getDefaultSerialization());
            column_kinds.push_back(getColumnKind(type));
            column_names.push_back(Excel::columnName(static_cast<UInt32>(i)));
        }
    }

    ~XLSXOutputFormat() override
    {
        if (sheet_buffer)
            sheet_buffer->cancel();
    }

    String getName() const override { return "XLSX"; }

private:
    enum class ColumnKind : uint8_t
    {
        Number, /// Integers and decimals, written in their exact text representation.
        Float,
        Bool,
        Date,
        Date32,
        DateTime,
        DateTime64,
        String, /// String and FixedString.
        Text, /// Other types, written as strings in their text representation.
    };

    static ColumnKind getColumnKind(const DataTypePtr & type)
    {
        WhichDataType which(type);
        if (isBool(type))
            return ColumnKind::Bool;
        if (which.isIntegerOrDecimal())
            return ColumnKind::Number;
        if (which.isFloat())
            return ColumnKind::Float;
        if (which.isDate())
            return ColumnKind::Date;
        if (which.isDate32())
            return ColumnKind::Date32;
        if (which.isDateTime())
            return ColumnKind::DateTime;
        if (which.isDateTime64())
            return ColumnKind::DateTime64;
        if (which.isStringOrFixedString())
            return ColumnKind::String;
        return ColumnKind::Text;
    }

    void writePrefix() override
    {
        writeEntry("_rels/.rels", package_relationships_xml);
        writeEntry("xl/styles.xml", styles_xml);
        startSheet();
    }

    void consume(Chunk chunk) override
    {
        size_t num_rows = chunk.getNumRows();
        Columns columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();

        for (size_t row = 0; row < num_rows; ++row)
        {
            if (rows_in_sheet == Excel::XLSX_MAX_ROWS)
            {
                finishSheet();
                startSheet();
            }
            writeRow(columns, row);
        }
    }

    void writeSuffix() override
    {
        finishSheet();

        String workbook = R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">)"
            R"(<bookViews><workbookView/></bookViews><sheets>)";
        String workbook_relationships = R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">)";
        String content_types = R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">)"
            R"(<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>)"
            R"(<Default Extension="xml" ContentType="application/xml"/>)"
            R"(<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>)"
            R"(<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>)";

        for (size_t i = 1; i <= sheet_names.size(); ++i)
        {
            workbook += fmt::format(R"(<sheet name="{}" sheetId="{}" r:id="rId{}"/>)", escapeXMLAttribute(sheet_names[i - 1]), i, i);
            workbook_relationships += fmt::format(
                R"(<Relationship Id="rId{}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{}.xml"/>)",
                i, i);
            content_types += fmt::format(
                R"(<Override PartName="/xl/worksheets/sheet{}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>)",
                i);
        }
        workbook += "</sheets></workbook>";
        workbook_relationships += fmt::format(
            R"(<Relationship Id="rId{}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>)",
            sheet_names.size() + 1);
        workbook_relationships += "</Relationships>";
        content_types += "</Types>";

        writeEntry("xl/workbook.xml", workbook);
        writeEntry("xl/_rels/workbook.xml.rels", workbook_relationships);
        writeEntry("[Content_Types].xml", content_types);
        zip.finish();
    }

    void onCancel() noexcept override
    {
        if (sheet_buffer)
            sheet_buffer->cancel();
    }

    void writeEntry(const String & name, std::string_view content)
    {
        zip.beginEntry(name);
        zip.write(content.data(), content.size());
        zip.endEntry();
    }

    String makeSheetName(size_t index) const
    {
        const String & base = settings.excel.output_sheet_name;
        if (index == 1)
            return base;
        String suffix = fmt::format(" ({})", index);
        return String(utf8Prefix(base, MAX_SHEET_NAME_LENGTH - suffix.size())) + suffix;
    }

    void startSheet()
    {
        sheet_names.push_back(makeSheetName(sheet_names.size() + 1));
        zip.beginEntry(fmt::format("xl/worksheets/sheet{}.xml", sheet_names.size()));
        sheet_buffer = std::make_unique<ZipEntryWriteBuffer>(zip);
        auto & buf = *sheet_buffer;

        const auto & header = getPort(PortKind::Main).getHeader();
        bool write_header = settings.excel.output_write_header;

        writeCString(R"(<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">)", buf);

        writeCString("<sheetViews><sheetView workbookViewId=\"0\"", buf);
        if (sheet_names.size() == 1)
            writeCString(" tabSelected=\"1\"", buf);
        writeChar('>', buf);
        if (write_header)
            writeCString(R"(<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/><selection pane="bottomLeft"/>)", buf);
        writeCString("</sheetView></sheetViews><sheetFormatPr defaultRowHeight=\"15\"/>", buf);

        /// The default width of columns is too small for dates (they would be displayed as `#####`) and for long column names.
        if (header.columns())
        {
            writeCString("<cols>", buf);
            for (size_t i = 0; i < header.columns(); ++i)
            {
                size_t width = 10;
                switch (column_kinds[i])
                {
                    case ColumnKind::Date:
                    case ColumnKind::Date32: width = 12; break;
                    case ColumnKind::DateTime: width = 20; break;
                    case ColumnKind::DateTime64: width = 24; break;
                    default: break;
                }
                if (write_header)
                    width = std::max(width, utf8Length(header.getByPosition(i).name) + 2);
                width = std::min<size_t>(width, 60);
                writeString(fmt::format(R"(<col min="{0}" max="{0}" width="{1}" customWidth="1"/>)", i + 1, width), buf);
            }
            writeCString("</cols>", buf);
        }

        writeCString("<sheetData>", buf);
        rows_in_sheet = 0;

        if (write_header)
        {
            startRow();
            for (size_t i = 0; i < header.columns(); ++i)
            {
                startCell(i, STYLE_HEADER, "inlineStr");
                writeInlineString(header.getByPosition(i).name);
            }
            writeCString("</row>", buf);
        }
    }

    void finishSheet()
    {
        writeCString("</sheetData></worksheet>", *sheet_buffer);
        sheet_buffer->finalize();
        sheet_buffer.reset();
        zip.endEntry();
    }

    void startRow()
    {
        ++rows_in_sheet;
        writeCString("<row r=\"", *sheet_buffer);
        writeIntText(rows_in_sheet, *sheet_buffer);
        writeCString("\">", *sheet_buffer);
    }

    void startCell(size_t column, CellStyle style, const char * type)
    {
        auto & buf = *sheet_buffer;
        writeCString("<c r=\"", buf);
        writeString(column_names[column], buf);
        writeIntText(rows_in_sheet, buf);
        writeChar('"', buf);
        if (style != STYLE_DEFAULT)
        {
            writeCString(" s=\"", buf);
            writeIntText(static_cast<int>(style), buf);
            writeChar('"', buf);
        }
        if (type)
        {
            writeCString(" t=\"", buf);
            writeCString(type, buf);
            writeChar('"', buf);
        }
        writeChar('>', buf);
    }

    void writeInlineString(std::string_view str)
    {
        auto & buf = *sheet_buffer;
        /// Leading and trailing whitespace is not preserved without `xml:space`.
        if (!str.empty() && (isWhitespaceASCII(str.front()) || isWhitespaceASCII(str.back())))
            writeCString("<is><t xml:space=\"preserve\">", buf);
        else
            writeCString("<is><t>", buf);
        writeXMLText(str, buf);
        writeCString("</t></is></c>", buf);
    }

    void writeNumberCell(size_t column, CellStyle style, double value)
    {
        startCell(column, style, nullptr);
        writeCString("<v>", *sheet_buffer);
        writeFloatText(value, *sheet_buffer);
        writeCString("</v></c>", *sheet_buffer);
    }

    void writeStringCell(size_t column, std::string_view str)
    {
        startCell(column, STYLE_DEFAULT, "inlineStr");
        writeInlineString(str);
    }

    /// Writes a date and time that Excel cannot represent as a number as a string.
    void writeDateOrString(size_t column, CellStyle style, std::optional<double> serial, const IColumn & data, size_t row)
    {
        if (serial)
        {
            writeNumberCell(column, style, *serial);
            return;
        }
        writeTextCell(column, data, row);
    }

    void writeTextCell(size_t column, const IColumn & data, size_t row)
    {
        text.clear();
        {
            WriteBufferFromString text_buf(text);
            serializations[column]->serializeText(data, row, text_buf, settings);
            text_buf.finalize();
        }
        writeStringCell(column, text);
    }

    void writeRow(const Columns & columns, size_t row)
    {
        auto & buf = *sheet_buffer;
        startRow();
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const IColumn * data = columns[i].get();
            if (const auto * nullable = typeid_cast<const ColumnNullable *>(data))
            {
                if (nullable->isNullAt(row))
                    continue;
                data = &nullable->getNestedColumn();
            }

            switch (column_kinds[i])
            {
                case ColumnKind::Number:
                    startCell(i, STYLE_DEFAULT, nullptr);
                    writeCString("<v>", buf);
                    serializations[i]->serializeText(*data, row, buf, settings);
                    writeCString("</v></c>", buf);
                    break;
                case ColumnKind::Float:
                {
                    Float64 value = data->getFloat64(row);
                    if (std::isfinite(value))
                    {
                        startCell(i, STYLE_DEFAULT, nullptr);
                        writeCString("<v>", buf);
                        serializations[i]->serializeText(*data, row, buf, settings);
                        writeCString("</v></c>", buf);
                    }
                    else
                        writeTextCell(i, *data, row);
                    break;
                }
                case ColumnKind::Bool:
                    startCell(i, STYLE_DEFAULT, "b");
                    writeCString(data->getBool(row) ? "<v>1</v></c>" : "<v>0</v></c>", buf);
                    break;
                case ColumnKind::Date:
                    writeDateOrString(i, STYLE_DATE, Excel::localDateTimeToSerial(assert_cast<const ColumnDate &>(*data).getData()[row], 0), *data, row);
                    break;
                case ColumnKind::Date32:
                    writeDateOrString(i, STYLE_DATE, Excel::localDateTimeToSerial(assert_cast<const ColumnDate32 &>(*data).getData()[row], 0), *data, row);
                    break;
                case ColumnKind::DateTime:
                {
                    const auto & time_zone = assert_cast<const DataTypeDateTime &>(*column_types[i]).getTimeZone();
                    time_t time = assert_cast<const ColumnDateTime &>(*data).getData()[row];
                    writeDateOrString(i, STYLE_DATE_TIME, Excel::localDateTimeToSerial(time_zone.toDayNum(time).toUnderType(), secondsOfDay(time_zone, time)), *data, row);
                    break;
                }
                case ColumnKind::DateTime64:
                {
                    const auto & type = assert_cast<const DataTypeDateTime64 &>(*column_types[i]);
                    Int64 value = assert_cast<const ColumnDateTime64 &>(*data).getData()[row].value;
                    Int64 multiplier = intExp10(type.getScale());
                    Int64 seconds = value / multiplier;
                    Int64 fraction = value % multiplier;
                    if (fraction < 0)
                    {
                        fraction += multiplier;
                        --seconds;
                    }
                    const auto & time_zone = type.getTimeZone();
                    writeDateOrString(
                        i,
                        STYLE_DATE_TIME64,
                        Excel::localDateTimeToSerial(
                            time_zone.toDayNum(seconds).toUnderType(),
                            secondsOfDay(time_zone, seconds) + static_cast<double>(fraction) / static_cast<double>(multiplier)),
                        *data,
                        row);
                    break;
                }
                case ColumnKind::String:
                    writeStringCell(i, data->getDataAt(row));
                    break;
                case ColumnKind::Text:
                    writeTextCell(i, *data, row);
                    break;
            }
        }
        writeCString("</row>", buf);
    }

    static double secondsOfDay(const DateLUTImpl & time_zone, time_t time)
    {
        return time_zone.toHour(time) * 3600.0 + time_zone.toMinute(time) * 60.0 + time_zone.toSecond(time);
    }

    const FormatSettings settings;
    DataTypes column_types;
    Serializations serializations;
    std::vector<ColumnKind> column_kinds;
    std::vector<String> column_names;

    ZipStreamWriter zip;
    std::unique_ptr<ZipEntryWriteBuffer> sheet_buffer;
    std::vector<String> sheet_names;
    UInt64 rows_in_sheet = 0;
    String text;
};

}

void registerOutputFormatXLSX(FormatFactory & factory);
void registerOutputFormatXLSX(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "XLSX",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr /*format_filter_info*/)
        { return std::make_shared<XLSXOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });
    factory.markFormatHasNoAppendSupport("XLSX");
    factory.markOutputFormatNotTTYFriendly("XLSX");
    factory.setContentType("XLSX", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
}

}
