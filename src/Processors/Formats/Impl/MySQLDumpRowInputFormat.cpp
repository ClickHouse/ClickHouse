#include "MySQLDumpRowInputFormat.h"
#include <IO/ReadHelpers.h>
#include <IO/PeekableReadBuffer.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Interpreters/MySQL/InterpretersMySQLDDLQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <Parsers/parseQuery.h>
#include <boost/algorithm/string.hpp>
#include <base/find_symbols.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int EMPTY_DATA_PASSED;
}

namespace
{
    enum MySQLQueryType
    {
        NONE,
        INSERT,
        CREATE,
    };
}

MySQLDumpRowInputFormat::MySQLDumpRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_)
    , table_name(format_settings_.mysql_dump.table_name)
    , types(header_.getDataTypes())
    , column_indexes_by_names(header_.getNamesToIndexesMap())
    , format_settings(format_settings_)
{
}


static String readTableName(ReadBuffer & in)
{
    skipWhitespaceIfAny(in);
    if (in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while parsing MySQLDump format: expected table name");

    String table_name;
    if (*in.position() == '`')
        readBackQuotedString(table_name, in);
    else
        readStringUntilWhitespace(table_name, in);
    return table_name;
}


/// Skip all data until '*/' and all data after it until '\n'
static void skipMultilineComment(ReadBuffer & in)
{
    while (!in.eof())
    {
        auto * pos = find_first_symbols<'*'>(in.position(), in.buffer().end());
        in.position() = pos;
        if (pos == in.buffer().end())
            continue;

        ++in.position();
        if (in.eof())
            break;

        if (*in.position() == '/')
        {
            ++in.position();
            break;
        }
    }

    skipToNextLineOrEOF(in);
}

/// Read all data until ';' into a string.
/// Skip all quoted strings because they can contain symbol ';'.
static String readUntilEndOfQuery(ReadBuffer & in)
{
    String result;

    while (!in.eof())
    {
        auto * pos = find_first_symbols<'\'', '"', '`', ';'>(in.position(), in.buffer().end());
        result.append(in.position(), pos - in.position());
        in.position() = pos;
        if (pos == in.buffer().end())
            continue;

        result.push_back(*in.position());

        if (*in.position() == ';')
        {
            ++in.position();
            break;
        }

        if (*in.position() == '\'')
        {
            readQuotedStringInto<false>(result, in);
            result.push_back('\'');
        }
        else if (*in.position() == '"')
        {
            readDoubleQuotedStringInto<false>(result, in);
            result.push_back('"');
        }
        else
        {
            readBackQuotedStringInto<false>(result, in);
            result.push_back('`');
        }
    }

    skipWhitespaceIfAny(in);
    return result;
}

static void skipQuery(ReadBuffer & in)
{
    readUntilEndOfQuery(in);
}


static bool skipUntilRowStartedWithOneOfKeywords(const std::unordered_set<String> & keywords, ReadBuffer & in, String * keyword_out = nullptr)
{
    while (true)
    {
        skipWhitespaceIfAny(in);

        if (in.eof())
            return false;

        if (checkString("/*", in))
            skipMultilineComment(in);
        else if (checkString("--", in))
            skipToNextLineOrEOF(in);
        else
        {
            String keyword;
            readStringUntilWhitespace(keyword, in);
            boost::to_lower(keyword);
            if (keywords.contains(keyword))
            {
                if (keyword_out)
                    *keyword_out = keyword;

                /// Note that position in the buffer is located after keyword.
                return true;
            }
            skipQuery(in);
        }
    }
}

void readUnquotedColumnName(String & name, ReadBuffer & in)
{
    name.clear();
    while (!in.eof())
    {
        char * pos = find_first_symbols<',', ')', ' '>(in.position(), in.buffer().end());
        name.append(in.position(), pos - in.position());
        in.position() = pos;

        if (in.hasPendingData())
            return;
    }
}

/// Try to read column names from a list in INSERT query.
/// Like '(x, `column name`, z)'
void tryReadColumnNames(ReadBuffer & in, std::vector<String> * column_names)
{
    skipWhitespaceIfAny(in);
    /// Check that we have the list of columns.
    if (in.eof() || *in.position() != '(')
        return;

    ++in.position();
    skipWhitespaceIfAny(in);
    bool first = true;
    while (!in.eof() && *in.position() != ')')
    {
        if (!first)
        {
            assertChar(',', in);
            skipWhitespaceIfAny(in);
        }

        first = false;

        String name;
        if (!in.eof() && *in.position() == '`')
            readBackQuotedString(name, in);
        else
            readUnquotedColumnName(name, in);

        skipWhitespaceIfAny(in);

        if (column_names)
            column_names->push_back(name);
    }

    assertChar(')', in);
}

static MySQLQueryType skipToInsertOrCreateQuery(String & table_name, ReadBuffer & in, bool skip_create_query = false)
{
    String current_table_name;
    String keyword;
    MySQLQueryType type = MySQLQueryType::NONE;
    /// In MySQL dumps INSERT queries might be replaced with REPLACE queries.
    std::unordered_set<String> keywords = {"insert", "replace"};
    if (!skip_create_query)
        keywords.insert("create");

    do
    {
        if (!skipUntilRowStartedWithOneOfKeywords(keywords, in, &keyword))
            return MySQLQueryType::NONE;

        skipWhitespaceIfAny(in);
        if (keyword != "create")
        {
            assertStringCaseInsensitive("into", in);
            type = MySQLQueryType::INSERT;
        }
        else
        {
            /// Check that it's CREATE TABLE query.
            if (!checkStringCaseInsensitive("table", in))
                continue;
            type = MySQLQueryType::CREATE;
        }

        current_table_name = readTableName(in);
        if (table_name.empty())
            table_name = current_table_name;
    } while (current_table_name != table_name);

    /// Note that now position in the buffer is located in CREATE/INSERT query after table name.
    return type;
}

static bool skipToInsertQuery(String & table_name, ReadBuffer & in)
{
    return skipToInsertOrCreateQuery(table_name, in, true) != MySQLQueryType::NONE;
}

static void skipToDataInInsertQuery(ReadBuffer & in, std::vector<String> * column_names = nullptr)
{
    tryReadColumnNames(in, column_names);
    skipWhitespaceIfAny(in);
    assertStringCaseInsensitive("values", in);
    skipWhitespaceIfAny(in);
}

static bool tryToExtractStructureFromCreateQuery(ReadBuffer & in, NamesAndTypesList & structure)
{
    /// To extract structure from CREATE query we read it all into a string and parse using MySQLParser
    /// and then extract columns names and types from MySQLParser::ASTCreateDefines

    /// Now position is located in CREATE query after table name and we need to write the beginning of CREATE query.
    String create_query_str = "CREATE TABLE _dummy " + readUntilEndOfQuery(in);
    MySQLParser::ParserCreateQuery parser;
    String error;
    const char * start = create_query_str.data();
    const char * end = create_query_str.data() + create_query_str.size();
    ASTPtr query = tryParseQuery(parser, start, end, error, false, "MySQL create query", false, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    if (!query)
        return false;

    const auto * create_query = assert_cast<MySQLParser::ASTCreateQuery *>(query.get());
    if (!create_query)
        return false;

    const auto * create_defines = create_query->columns_list->as<MySQLParser::ASTCreateDefines>();
    if (!create_defines)
        return false;

    structure = MySQLInterpreter::getColumnsList(create_defines->columns);
    return true;
}

static void skipStartOfRow(ReadBuffer & in)
{
    skipWhitespaceIfAny(in);
    assertChar('(', in);
    skipWhitespaceIfAny(in);
}

static void skipFieldDelimiter(ReadBuffer & in)
{
    skipWhitespaceIfAny(in);
    assertChar(',', in);
    skipWhitespaceIfAny(in);
}

static void skipEndOfRow(ReadBuffer & in, String & table_name)
{
    skipWhitespaceIfAny(in);
    assertChar(')', in);

    skipWhitespaceIfAny(in);
    if (!in.eof() && *in.position() == ',')
        ++in.position();

    skipWhitespaceIfAny(in);
    if (!in.eof() && *in.position() == ';')
    {
        /// ';' means end of INSERT query, skip until data from
        /// next INSERT query into the same table or until EOF.
        ++in.position();
        if (skipToInsertQuery(table_name, in))
            skipToDataInInsertQuery(in);
    }
}

static void readFirstCreateAndInsertQueries(ReadBuffer & in, String & table_name, NamesAndTypesList & structure_from_create, Names & column_names)
{
    auto type = skipToInsertOrCreateQuery(table_name, in);
    bool insert_query_present = type == MySQLQueryType::INSERT;
    if (type == MySQLQueryType::CREATE)
    {
        /// If we have CREATE query, we can extract columns names and types from it.
        if (tryToExtractStructureFromCreateQuery(in, structure_from_create))
            column_names = structure_from_create.getNames();
        skipQuery(in);
        insert_query_present = skipToInsertQuery(table_name, in);
    }

    if (!insert_query_present)
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "There is no INSERT queries{} in MySQL dump file", table_name.empty() ? "" : " for table " + table_name);

    skipToDataInInsertQuery(in, column_names.empty() ? &column_names : nullptr);
}

void MySQLDumpRowInputFormat::readPrefix()
{
    NamesAndTypesList structure_from_create;
    Names column_names;
    readFirstCreateAndInsertQueries(*in, table_name, structure_from_create, column_names);

    if (!column_names.empty() && format_settings.mysql_dump.map_column_names)
        column_mapping->addColumns(column_names, column_indexes_by_names, format_settings);
    else
        column_mapping->setupByHeader(getPort().getHeader());
}

bool MySQLDumpRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in->eof())
        return false;

    ext.read_columns.resize(types.size());

    skipStartOfRow(*in);
    for (size_t file_column = 0; file_column < column_mapping->column_indexes_for_input_fields.size(); ++file_column)
    {
        if (file_column != 0)
            skipFieldDelimiter(*in);

        const auto & column_index = column_mapping->column_indexes_for_input_fields[file_column];
        if (column_index)
            ext.read_columns[*column_index] = readField(*columns[*column_index], *column_index);
        else
            skipField();
    }
    skipEndOfRow(*in, table_name);

    column_mapping->insertDefaultsForNotSeenColumns(columns, ext.read_columns);

    /// If defaults_for_omitted_fields is set to 0, we should leave already inserted defaults.
    if (!format_settings.defaults_for_omitted_fields)
        ext.read_columns.assign(ext.read_columns.size(), true);

    return true;
}

bool MySQLDumpRowInputFormat::readField(IColumn & column, size_t column_idx)
{
    const auto & type = types[column_idx];
    const auto & serialization = serializations[column_idx];
    if (format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable())
        return SerializationNullable::deserializeTextQuotedImpl(column, *in, format_settings, serialization);

    serialization->deserializeTextQuoted(column, *in, format_settings);
    return true;
}

void MySQLDumpRowInputFormat::skipField()
{
    NullOutput out;
    readQuotedFieldInto(out, *in);
}

MySQLDumpSchemaReader::MySQLDumpSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowSchemaReader(in_, format_settings_), format_settings(format_settings_), table_name(format_settings_.mysql_dump.table_name)
{
}

NamesAndTypesList MySQLDumpSchemaReader::readSchema()
{
    NamesAndTypesList structure_from_create;
    Names names;
    readFirstCreateAndInsertQueries(in, table_name, structure_from_create, names);

    if (!structure_from_create.empty())
        return structure_from_create;

    if (!names.empty())
        setColumnNames(names);

    return IRowSchemaReader::readSchema();
}

DataTypes MySQLDumpSchemaReader::readRowAndGetDataTypes()
{
    if (in.eof())
        return {};

    skipStartOfRow(in);
    DataTypes data_types;
    String value;
    while (!in.eof() && *in.position() != ')')
    {
        if (!data_types.empty())
            skipFieldDelimiter(in);

        readQuotedField(value, in);
        auto type = determineDataTypeByEscapingRule(value, format_settings, FormatSettings::EscapingRule::Quoted);
        data_types.push_back(std::move(type));
    }
    skipEndOfRow(in, table_name);
    return data_types;
}

void registerInputFormatMySQLDump(FormatFactory & factory)
{
    factory.registerInputFormat("MySQLDump", [](
        ReadBuffer & buf,
        const Block & header,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<MySQLDumpRowInputFormat>(buf, header, params, settings);
    });

    factory.registerAdditionalInfoForSchemaCacheGetter(
        "MySQLDump", [](const FormatSettings & settings) { return "Table name: " + settings.mysql_dump.table_name; });
}

void registerMySQLSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("MySQLDump", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_shared<MySQLDumpSchemaReader>(buf, settings);
    });
}


}
