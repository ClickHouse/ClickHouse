#include <Processors/Formats/Impl/XLSXReader.h>

#if USE_MINIZIP

#include <IO/Archives/ZipArchiveReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/readFloatText.h>
#include <Processors/Formats/Impl/ExcelSheetSelection.h>
#include <Processors/Formats/Impl/XMLPullParser.h>
#include <Common/Exception.h>

#include <charconv>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace Excel
{

namespace
{

using Event = XMLPullParser::Event;

struct Relationship
{
    String type;
    String target; /// The resolved path of the target part in the archive.
};

/// The relationship types differ between the transitional and the strict variants of Office Open XML,
/// but they always end with the same suffix.
bool hasRelationshipType(const Relationship & relationship, std::string_view suffix)
{
    return relationship.type.ends_with(suffix);
}

/// Resolves the target of a relationship relative to the source part, like a relative URI.
String resolvePartPath(const String & source_part, const String & target)
{
    String path;
    if (target.starts_with('/'))
        path = target.substr(1);
    else
    {
        auto slash = source_part.rfind('/');
        path = slash == String::npos ? target : source_part.substr(0, slash + 1) + target;
    }

    std::vector<std::string_view> segments;
    std::string_view rest = path;
    while (true)
    {
        auto slash = rest.find('/');
        std::string_view segment = rest.substr(0, slash);
        if (segment == "..")
        {
            if (!segments.empty())
                segments.pop_back();
        }
        else if (!segment.empty() && segment != ".")
            segments.push_back(segment);
        if (slash == std::string_view::npos)
            break;
        rest = rest.substr(slash + 1);
    }

    String result;
    for (const auto & segment : segments)
    {
        if (!result.empty())
            result += '/';
        result += segment;
    }
    return result;
}

/// The relationships of `part` are stored in `<directory of part>/_rels/<file name of part>.rels`.
String relationshipsPath(const String & part)
{
    auto slash = part.rfind('/');
    if (slash == String::npos)
        return "_rels/" + part + ".rels";
    return part.substr(0, slash + 1) + "_rels/" + part.substr(slash + 1) + ".rels";
}

bool parseUInt(std::string_view str, UInt64 & value)
{
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);
    return ec == std::errc() && ptr == str.data() + str.size();
}

bool parseDouble(std::string_view str, double & value)
{
    ReadBufferFromMemory buf(str.data(), str.size());
    return tryReadFloatTextPrecise(value, buf) && buf.eof();
}

bool parseBoolAttribute(const String * value)
{
    return value && (*value == "1" || *value == "true");
}

/// Reads the text of a rich text element (`<si>` of the shared strings or `<is>` of an inline string):
/// the text of the `<t>` elements, including the ones in the rich text runs `<r>`, but not in the phonetic runs `<rPh>`.
void readRichText(XMLPullParser & parser, String & out)
{
    size_t target_depth = parser.depth() - 1;
    while (parser.depth() > target_depth)
    {
        auto event = parser.next();
        if (event == Event::End)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of an XLSX part");
        if (event != Event::StartElement)
            continue;
        if (parser.name() == "t")
            parser.readElementText(out);
        else if (parser.name() == "r")
            readRichText(parser, out);
        else
            parser.skipElement();
    }
}

/// Iterates over the children of the element that has just been started, calling `callback` on each child element after it is started.
/// The callback must consume the child element completely (for example, by calling `skipElement`).
template <typename Callback>
void forEachChildElement(XMLPullParser & parser, Callback && callback)
{
    size_t depth = parser.depth();
    while (parser.depth() >= depth)
    {
        auto event = parser.next();
        if (event == Event::End)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of an XLSX part");
        if (event == Event::StartElement)
        {
            callback();
            chassert(parser.depth() == depth);
        }
    }
}

/// Calls `callback` on each child element of the root element of the part.
template <typename Callback>
void forEachTopLevelElement(XMLPullParser & parser, Callback && callback)
{
    while (true)
    {
        auto event = parser.next();
        if (event == Event::End)
            return;
        if (event == Event::StartElement && parser.depth() == 1)
        {
            forEachChildElement(parser, callback);
            return;
        }
    }
}


class XLSXWorkbook
{
public:
    explicit XLSXWorkbook(std::string_view data)
        : archive(std::make_shared<ZipArchiveReader>(
              "XLSX",
              [data]() -> std::unique_ptr<SeekableReadBuffer> { return std::make_unique<ReadBufferFromMemory>(data); },
              data.size()))
    {
    }

    std::unique_ptr<ISheetReader> openSheet(const String & sheet);

private:
    std::unique_ptr<ReadBuffer> readPart(const String & path)
    {
        return archive->readFile(path, /* throw_on_not_found= */ true);
    }

    std::unordered_map<String, Relationship> readRelationships(const String & part);
    std::shared_ptr<const std::vector<String>> readSharedStrings(const String & path);
    std::vector<NumberFormatKind> readStyles(const String & path);

    std::shared_ptr<ZipArchiveReader> archive;
};


class XLSXSheetReader final : public ISheetReader
{
public:
    XLSXSheetReader(
        std::unique_ptr<ReadBuffer> in_,
        std::shared_ptr<const std::vector<String>> shared_strings_,
        std::vector<NumberFormatKind> style_kinds_,
        bool date1904_)
        : in(std::move(in_))
        , parser(*in)
        , shared_strings(std::move(shared_strings_))
        , style_kinds(std::move(style_kinds_))
        , date1904(date1904_)
    {
        findSheetData();
    }

    bool readRow(Row & row) override;
    std::optional<CellRange> getDimension() const override { return dimension; }
    bool isDate1904() const override { return date1904; }

private:
    void findSheetData();
    void readCell(Cell & cell, UInt32 & column);

    std::unique_ptr<ReadBuffer> in;
    XMLPullParser parser;
    std::shared_ptr<const std::vector<String>> shared_strings;
    std::vector<NumberFormatKind> style_kinds;
    bool date1904;

    std::optional<CellRange> dimension;
    bool finished = false;
    std::optional<UInt64> last_row_index;

    String value;
    String cell_type;
};


std::unordered_map<String, Relationship> XLSXWorkbook::readRelationships(const String & part)
{
    std::unordered_map<String, Relationship> result;
    auto path = relationshipsPath(part);
    if (!archive->fileExists(path))
        return result;

    auto in = readPart(path);
    XMLPullParser parser(*in);
    forEachTopLevelElement(parser, [&]
    {
        if (parser.name() == "Relationship")
        {
            const auto * id = parser.getAttribute("Id");
            const auto * type = parser.getAttribute("Type");
            const auto * target = parser.getAttribute("Target");
            const auto * target_mode = parser.getAttribute("TargetMode");
            if (id && type && target && !(target_mode && *target_mode == "External"))
                result[*id] = Relationship{*type, resolvePartPath(part, *target)};
        }
        parser.skipElement();
    });
    return result;
}

std::shared_ptr<const std::vector<String>> XLSXWorkbook::readSharedStrings(const String & path)
{
    auto result = std::make_shared<std::vector<String>>();
    auto in = readPart(path);
    XMLPullParser parser(*in);
    forEachTopLevelElement(parser, [&]
    {
        if (parser.name() == "si")
        {
            String & str = result->emplace_back();
            readRichText(parser, str);
            decodeOOXMLEscapes(str);
        }
        else
            parser.skipElement();
    });
    return result;
}

std::vector<NumberFormatKind> XLSXWorkbook::readStyles(const String & path)
{
    std::unordered_map<UInt64, String> custom_formats;
    std::vector<UInt64> cell_format_ids;

    auto in = readPart(path);
    XMLPullParser parser(*in);
    forEachTopLevelElement(parser, [&]
    {
        if (parser.name() == "numFmts")
        {
            forEachChildElement(parser, [&]
            {
                const auto * id = parser.getAttribute("numFmtId");
                const auto * code = parser.getAttribute("formatCode");
                UInt64 id_value;
                if (id && code && parseUInt(*id, id_value))
                    custom_formats[id_value] = *code;
                parser.skipElement();
            });
        }
        else if (parser.name() == "cellXfs")
        {
            forEachChildElement(parser, [&]
            {
                UInt64 id_value = 0;
                if (parser.name() == "xf")
                {
                    const auto * id = parser.getAttribute("numFmtId");
                    if (id && !parseUInt(*id, id_value))
                        id_value = 0;
                    cell_format_ids.push_back(id_value);
                }
                parser.skipElement();
            });
        }
        else
            parser.skipElement();
    });

    std::vector<NumberFormatKind> result;
    result.reserve(cell_format_ids.size());
    for (UInt64 id : cell_format_ids)
    {
        auto it = custom_formats.find(id);
        result.push_back(classifyNumberFormat(static_cast<UInt32>(id), it == custom_formats.end() ? std::string_view{} : it->second));
    }
    return result;
}

std::unique_ptr<ISheetReader> XLSXWorkbook::openSheet(const String & sheet)
{
    auto package_relationships = readRelationships("");
    String workbook_path;
    for (const auto & [_, relationship] : package_relationships)
        if (hasRelationshipType(relationship, "/officeDocument"))
            workbook_path = relationship.target;
    if (workbook_path.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "The file is not an XLSX workbook: the package does not have the main document");

    auto workbook_relationships = readRelationships(workbook_path);

    bool date1904 = false;
    std::vector<SheetInfo> sheets;
    std::vector<String> sheet_paths;
    {
        auto in = readPart(workbook_path);
        XMLPullParser parser(*in);
        forEachTopLevelElement(parser, [&]
        {
            if (parser.name() == "workbookPr")
            {
                date1904 = parseBoolAttribute(parser.getAttribute("date1904"));
                parser.skipElement();
            }
            else if (parser.name() == "sheets")
            {
                forEachChildElement(parser, [&]
                {
                    if (parser.name() == "sheet")
                    {
                        const auto * name = parser.getAttribute("name");
                        const auto * state = parser.getAttribute("state");
                        const auto * id = parser.getAttribute("id");
                        SheetInfo info;
                        info.name = name ? *name : "";
                        info.is_visible = !state || *state == "visible";
                        String path;
                        if (id)
                        {
                            auto it = workbook_relationships.find(*id);
                            if (it != workbook_relationships.end())
                            {
                                info.is_worksheet = hasRelationshipType(it->second, "/worksheet");
                                path = it->second.target;
                            }
                        }
                        if (path.empty())
                            info.is_worksheet = false;
                        sheets.push_back(std::move(info));
                        sheet_paths.push_back(std::move(path));
                    }
                    parser.skipElement();
                });
            }
            else
                parser.skipElement();
        });
    }

    size_t sheet_index = selectSheet(sheets, sheet);

    std::shared_ptr<const std::vector<String>> shared_strings = std::make_shared<std::vector<String>>();
    std::vector<NumberFormatKind> style_kinds;
    for (const auto & [_, relationship] : workbook_relationships)
    {
        if (hasRelationshipType(relationship, "/sharedStrings"))
            shared_strings = readSharedStrings(relationship.target);
        else if (hasRelationshipType(relationship, "/styles"))
            style_kinds = readStyles(relationship.target);
    }

    return std::make_unique<XLSXSheetReader>(readPart(sheet_paths[sheet_index]), std::move(shared_strings), std::move(style_kinds), date1904);
}


void XLSXSheetReader::findSheetData()
{
    while (true)
    {
        auto event = parser.next();
        if (event == Event::End)
        {
            finished = true;
            return;
        }
        if (event != Event::StartElement || parser.depth() != 2)
            continue;

        if (parser.name() == "sheetData")
            return;

        if (parser.name() == "dimension")
        {
            if (const auto * ref = parser.getAttribute("ref"))
            {
                /// The dimension is only a hint, so ignore it if it is malformed.
                std::optional<UInt64> row;
                std::optional<UInt32> column;
                std::string_view first = std::string_view(*ref).substr(0, ref->find(':'));
                if (parseCellReference(first, row, column) && row && column)
                    dimension = CellRange{.first_row = *row, .first_column = *column, .last_row = {}, .last_column = {}};
            }
        }
        parser.skipElement();
    }
}

void XLSXSheetReader::readCell(Cell & cell, UInt32 & column)
{
    cell.reset();

    if (const auto * ref = parser.getAttribute("r"))
    {
        std::optional<UInt64> ref_row;
        std::optional<UInt32> ref_column;
        if (!parseCellReference(*ref, ref_row, ref_column) || !ref_column)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid cell reference '{}' in an XLSX sheet", *ref);
        column = *ref_column;
    }

    const auto * type_attribute = parser.getAttribute("t");
    cell_type = type_attribute ? *type_attribute : "n";

    UInt64 style = 0;
    if (const auto * style_attribute = parser.getAttribute("s"))
        if (!parseUInt(*style_attribute, style))
            style = 0;

    bool has_value = false;
    value.clear();
    forEachChildElement(parser, [&]
    {
        if (parser.name() == "v")
        {
            parser.readElementText(value);
            has_value = true;
        }
        else if (parser.name() == "is")
        {
            readRichText(parser, value);
            has_value = true;
        }
        else
            parser.skipElement();
    });

    if (!has_value)
        return;

    if (cell_type == "s")
    {
        UInt64 index;
        if (!parseUInt(value, index) || index >= shared_strings->size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid shared string index '{}' in an XLSX sheet", value);
        cell.type = CellType::String;
        cell.text = (*shared_strings)[index];
    }
    else if (cell_type == "inlineStr" || cell_type == "str")
    {
        cell.type = CellType::String;
        cell.text = std::move(value);
        decodeOOXMLEscapes(cell.text);
    }
    else if (cell_type == "b")
    {
        cell.type = CellType::Bool;
        cell.number = (value == "1" || value == "true") ? 1 : 0;
    }
    else if (cell_type == "e")
    {
        cell.type = CellType::Error;
        cell.text = std::move(value);
    }
    else if (cell_type == "d")
    {
        /// A date in the ISO 8601 format (used by the strict variant of Office Open XML). Keep it as a string:
        /// ClickHouse parses such strings into the date and time types.
        cell.type = CellType::String;
        cell.text = std::move(value);
    }
    else
    {
        if (value.empty())
            return;
        if (!parseDouble(value, cell.number))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse the number '{}' in an XLSX sheet", value);
        cell.type = CellType::Number;
        cell.format = style < style_kinds.size() ? style_kinds[style] : NumberFormatKind::Number;
        cell.text = std::move(value);
    }
}

bool XLSXSheetReader::readRow(Row & row)
{
    while (!finished)
    {
        auto event = parser.next();
        if (event == Event::End)
        {
            finished = true;
            break;
        }
        if (event == Event::EndElement && parser.depth() == 1)
        {
            /// The end of `sheetData`.
            finished = true;
            break;
        }
        if (event != Event::StartElement)
            continue;
        if (parser.name() != "row")
        {
            parser.skipElement();
            continue;
        }

        UInt64 index = last_row_index ? *last_row_index + 1 : 0;
        if (const auto * ref = parser.getAttribute("r"))
        {
            if (!parseUInt(*ref, index) || index == 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid row number '{}' in an XLSX sheet", *ref);
            --index;
        }
        last_row_index = index;
        row.index = index;

        size_t num_cells = 0;
        UInt32 column = 0;
        forEachChildElement(parser, [&]
        {
            if (parser.name() != "c")
            {
                parser.skipElement();
                return;
            }
            if (num_cells == row.cells.size())
                row.cells.emplace_back();
            auto & [cell_column, cell] = row.cells[num_cells];
            readCell(cell, column);
            cell_column = column;
            ++column;
            ++num_cells;
        });
        row.cells.resize(num_cells);

        if (num_cells)
            return true;
    }
    return false;
}

}

std::unique_ptr<ISheetReader> openXLSXSheet(std::string_view data, const String & sheet)
{
    XLSXWorkbook workbook(data);
    return workbook.openSheet(sheet);
}

}

}

#endif
