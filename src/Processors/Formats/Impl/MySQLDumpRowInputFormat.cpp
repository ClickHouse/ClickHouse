#include <Processors/Formats/Impl/MySQLDumpRowInputFormat.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <Common/assert_cast.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>
#include <IO/ReadHelpers.h>
#include <IO/PeekableReadBuffer.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/IStorage.h>

#include <Common/isValidUTF8.h>
#include <Poco/String.h>
#include <Poco/UTF8String.h>
#include <boost/algorithm/string.hpp>
#include <base/find_symbols.h>
#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int EMPTY_DATA_PASSED;
    extern const int UNKNOWN_TYPE;
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

MySQLDumpRowInputFormat::MySQLDumpRowInputFormat(ReadBuffer & in_, SharedHeader header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_)
    , table_name(format_settings_.mysql_dump.table_name)
    , types(header_->getDataTypes())
    , format_settings(format_settings_)
{
    column_indexes_by_names = getNamesToIndexesMap(getPort().getHeader());
}


static String readTableName(ReadBuffer & in)
{
    skipWhitespaceIfAny(in);
    if (in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while parsing MySQLDump format: expected table name");

    String table_name;
    if (*in.position() == '`')
        readBackQuotedStringWithSQLStyle(table_name, in);
    else if (*in.position() == '"')
        readDoubleQuotedStringWithSQLStyle(table_name, in);
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


static bool skipUntilRowStartedWithOneOfKeywords(const UnorderedSetWithMemoryTracking<String> & keywords, ReadBuffer & in, String * keyword_out = nullptr)
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

static NamesAndTypesList getColumnsList(const ASTExpressionList * columns_definition)
{
    NamesAndTypesList columns_name_and_type;
    for (const auto & declare_column_ast : columns_definition->children)
    {
        const auto & declare_column = declare_column_ast->as<MySQLParser::ASTDeclareColumn>();

        if (!declare_column || !declare_column->data_type)
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Missing type in definition of column.");

        bool is_nullable = true;
        bool is_unsigned = false;
        if (declare_column->column_options)
        {
            if (const auto * options = declare_column->column_options->as<MySQLParser::ASTDeclareOptions>())
            {
                if (options->changes.contains("is_null"))
                    is_nullable = options->changes.at("is_null")->as<ASTLiteral>()->value.safeGet<UInt64>();

                if (options->changes.contains("is_unsigned"))
                    is_unsigned = options->changes.at("is_unsigned")->as<ASTLiteral>()->value.safeGet<UInt64>();
            }
        }

        ASTPtr data_type = declare_column->data_type;
        auto * data_type_node = data_type->as<ASTDataType>();

        if (data_type_node)
        {
            String type_name_upper = Poco::toUpper(data_type_node->name);

            if (is_unsigned)
            {
                /// For example(in MySQL): CREATE TABLE test(column_name INT NOT NULL ... UNSIGNED)
                if (type_name_upper.contains("INT") && !endsWith(type_name_upper, "SIGNED")
                    && !endsWith(type_name_upper, "UNSIGNED"))
                    data_type_node->name = type_name_upper + " UNSIGNED";
            }

            /// A dump writes `SET` values as strings of comma-separated members, e.g. 'a,b'. `SET` is also a ClickHouse
            /// alias of `UInt64`, so name the type explicitly, as the `mysql` table engine reads it.
            if (type_name_upper == "SET")
            {
                data_type_node->name = "String";
                data_type_node->resetArguments();
            }

            /// Transforms MySQL ENUM's list of strings to ClickHouse string-integer pairs
            /// For example ENUM('a', 'b', 'c') -> ENUM('a'=1, 'b'=2, 'c'=3)
            /// Elements on a position further than 32767 are assigned negative values, starting with -32768.
            /// Note: Enum would be transformed to Enum8 if number of elements is less then 128, otherwise it would be transformed to Enum16.
            if (type_name_upper.contains("ENUM"))
            {
                ASTPtr arguments = data_type_node->getArguments();
                if (!arguments)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Type {} in MySQL dump requires a list of values", data_type_node->name);

                UInt16 i = 0;
                for (ASTPtr & child : arguments->children)
                {
                    auto new_child = make_intrusive<ASTFunction>();
                    new_child->name = "equals";
                    auto * literal = child->as<ASTLiteral>();
                    if (!literal || literal->value.getType() != Field::Types::String)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Values of type {} in MySQL dump must be string literals", data_type_node->name);

                    new_child->arguments = make_intrusive<ASTExpressionList>();
                    new_child->arguments->children.push_back(make_intrusive<ASTLiteral>(literal->value.safeGet<String>()));
                    new_child->arguments->children.push_back(make_intrusive<ASTLiteral>(Int16(++i)));
                    child = new_child;
                }
            }

            if (type_name_upper == "DATE")
                data_type_node->name = "Date32";
        }
        if (is_nullable)
            data_type = makeASTDataType("Nullable", data_type);

        columns_name_and_type.emplace_back(declare_column->name, DataTypeFactory::instance().get(data_type));
    }

    return columns_name_and_type;
}

static void readUnquotedColumnName(String & name, ReadBuffer & in)
{
    name.clear();
    while (!in.eof())
    {
        char * pos = find_first_symbols<',', ')', ' ', '\t', '\n', '\r', '\f', '\v'>(in.position(), in.buffer().end());
        name.append(in.position(), pos - in.position());
        in.position() = pos;

        if (in.hasPendingData())
            return;
    }
}

/// Try to read column names from a list in INSERT query.
/// Like '(x, `column name`, z)'
static void tryReadColumnNames(ReadBuffer & in, Names * column_names)
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
            readBackQuotedStringWithSQLStyle(name, in);
        else if (!in.eof() && *in.position() == '"')
            readDoubleQuotedStringWithSQLStyle(name, in);
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
    UnorderedSetWithMemoryTracking<String> keywords = {"insert", "replace"};
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
        else if (current_table_name != table_name)
        {
            /// The keyword scan splits at spaces, so it must start at a statement: skip the rest of this one.
            skipQuery(in);
        }
    } while (current_table_name != table_name);

    /// Note that now position in the buffer is located in CREATE/INSERT query after table name.
    return type;
}

static bool skipToInsertQuery(String & table_name, ReadBuffer & in)
{
    return skipToInsertOrCreateQuery(table_name, in, true) != MySQLQueryType::NONE;
}

static void skipToDataInInsertQuery(ReadBuffer & in, Names * column_names = nullptr)
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
    ASTPtr query = tryParseQuery(parser, start, end, error, false, "MySQL create query", false,
        DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, true);
    if (!query)
        return false;

    const auto * create_query = assert_cast<MySQLParser::ASTCreateQuery *>(query.get());
    if (!create_query)
        return false;

    /// `CREATE TABLE ... LIKE` has no column list, and a column list with only indexes or constraints has no columns.
    if (!create_query->columns_list)
        return false;

    const auto * create_defines = create_query->columns_list->as<MySQLParser::ASTCreateDefines>();
    if (!create_defines || !create_defines->columns)
        return false;

    structure = getColumnsList(create_defines->columns);
    return true;
}

static void checkStartOfRow(ReadBuffer & in)
{
    if (!in.eof() && *in.position() != '(')
        assertChar('(', in);
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

static void skipBinaryIntroducerIfAny(ReadBuffer & in)
{
    if (checkStringCaseInsensitive("_binary", in))
        skipWhitespaceIfAny(in);
}

static void skipRowDelimiter(ReadBuffer & in)
{
    skipWhitespaceIfAny(in);
    if (!in.eof() && *in.position() == ',')
        ++in.position();
    skipWhitespaceIfAny(in);
}

static void skipEndOfRow(ReadBuffer & in)
{
    skipWhitespaceIfAny(in);
    assertChar(')', in);
    skipRowDelimiter(in);
}

static bool prepareNextMySQLDumpRow(ReadBuffer & in, String & table_name, std::optional<Names> & next_names)
{
    next_names.reset();
    skipWhitespaceIfAny(in);
    if (!in.eof() && *in.position() == ';')
    {
        ++in.position();
        if (!skipToInsertQuery(table_name, in))
            return false;
        next_names.emplace();
        skipToDataInInsertQuery(in, &*next_names);
    }
    if (in.eof())
        return false;
    /// Check the row start without consuming it.
    checkStartOfRow(in);
    return true;
}

static void readFirstCreateAndInsertQueries(
    ReadBuffer & in,
    String & table_name,
    NamesAndTypesList & structure_from_create,
    Names & insert_column_names)
{
    auto type = skipToInsertOrCreateQuery(table_name, in);
    bool insert_query_present = type == MySQLQueryType::INSERT;
    if (type == MySQLQueryType::CREATE)
    {
        /// If we have CREATE query, we can extract columns names and types from it.
        /// `tryToExtractStructureFromCreateQuery` reads the CREATE query up to and including its `;`,
        /// so the next query must not be skipped: it can be the first INSERT query.
        tryToExtractStructureFromCreateQuery(in, structure_from_create);
        insert_query_present = skipToInsertQuery(table_name, in);
    }

    if (!insert_query_present)
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "There is no INSERT queries{} in MySQL dump file",
                        table_name.empty() ? "" : " for table " + table_name);

    /// The values are in the order of the INSERT column list, which can differ from CREATE (mysqldump omits generated columns).
    skipToDataInInsertQuery(in, &insert_column_names);
}

static void normalizeMySQLDumpColumnNames(Names & names, const Names & create_names, const BlockNameMap * header_names)
{
    /// MySQL column names are case-insensitive, so take the spelling from CREATE. A name in the header or not in CREATE stays as is.
    /// Poco decodes every invalid UTF-8 sequence to the same code point, so a name that is not valid UTF-8 is compared ignoring ASCII case only.
    auto is_valid_utf8 = [](const String & s) { return UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(s.data()), s.size()); };
    auto names_equal = [&](const String & create_name, const String & name)
    {
        if (is_valid_utf8(create_name) && is_valid_utf8(name))
            return Poco::UTF8::icompare(create_name, name) == 0;
        return Poco::icompare(create_name, name) == 0;
    };

    if (!create_names.empty())
    {
        for (auto & name : names)
        {
            if (std::ranges::find(create_names, name) != create_names.end())
                continue;
            if (header_names && header_names->find(name) != header_names->end())
                continue;
            auto it = std::ranges::find_if(create_names, [&](const String & create_name) { return names_equal(create_name, name); });
            if (it != create_names.end())
                name = *it;
        }
    }
}

static void validateMySQLDumpColumnNames(const String & table_name, const MySQLDumpColumnState & state, const Names & next_names)
{
    if (state.has_explicit_columns != !next_names.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Column list mode changed for MySQL dump table {}: expected {} {}, got {} {}",
            table_name, state.has_explicit_columns ? "explicit columns" : "no column list", state.first_names,
            next_names.empty() ? "no column list" : "explicit columns", next_names);

    UnorderedMapWithMemoryTracking<String, size_t> counts;
    for (const auto & name : state.first_names)
        ++counts[name];
    bool same_columns = state.first_names.size() == next_names.size();
    for (const auto & name : next_names)
    {
        auto it = counts.find(name);
        if (it == counts.end() || it->second == 0)
        {
            same_columns = false;
            break;
        }
        --it->second;
    }
    if (!same_columns)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Column set changed for MySQL dump table {}: expected {}, got {}", table_name, state.first_names, next_names);
}

void MySQLDumpRowInputFormat::readPrefix()
{
    NamesAndTypesList structure_from_create;
    Names column_names;
    readFirstCreateAndInsertQueries(*in, table_name, structure_from_create, column_names);
    checkStartOfRow(*in);
    column_state.create_names = structure_from_create.getNames();
    if (format_settings.mysql_dump.map_column_names)
        normalizeMySQLDumpColumnNames(column_names, column_state.create_names, &column_indexes_by_names);
    column_state.first_names = column_names;
    column_state.has_explicit_columns = !column_names.empty();
    updateColumnMapping(std::move(column_names));
}

void MySQLDumpRowInputFormat::updateColumnMapping(Names next_names)
{
    ColumnMapping next_mapping;
    if (format_settings.mysql_dump.map_column_names && (!next_names.empty() || !column_state.create_names.empty()))
        next_mapping.addColumns(next_names.empty() ? column_state.create_names : next_names, column_indexes_by_names, format_settings);
    else
        next_mapping.setupByHeader(getPort().getHeader());

    next_mapping.is_set = column_mapping->is_set;
    *column_mapping = std::move(next_mapping);
    column_state.current_names = std::move(next_names);
}

bool MySQLDumpRowInputFormat::prepareRow()
{
    std::optional<Names> next_names;
    bool has_row = prepareNextMySQLDumpRow(*in, table_name, next_names);
    if (next_names && format_settings.mysql_dump.map_column_names)
    {
        normalizeMySQLDumpColumnNames(*next_names, column_state.create_names, &column_indexes_by_names);
        validateMySQLDumpColumnNames(table_name, column_state, *next_names);
        updateColumnMapping(std::move(*next_names));
    }
    return has_row;
}

bool MySQLDumpRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!prepareRow())
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
    skipEndOfRow(*in);

    column_mapping->insertDefaultsForNotSeenColumns(columns, types, ext.read_columns);

    /// If defaults_for_omitted_fields is set to 0, we should leave already inserted defaults.
    if (!format_settings.defaults_for_omitted_fields)
        ext.read_columns.assign(ext.read_columns.size(), true);

    return true;
}

size_t MySQLDumpRowInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    while (num_rows < max_block_size && prepareRow())
    {
        if (!ValuesBlockInputFormat::skipToNextRow(in, 1, 0))
            break;
        ++num_rows;
        skipRowDelimiter(*in);
    }

    return num_rows;
}

bool MySQLDumpRowInputFormat::readField(IColumn & column, size_t column_idx)
{
    const auto & type = types[column_idx];
    const auto & serialization = serializations[column_idx];
    skipBinaryIntroducerIfAny(*in);
    if (format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type))
        return SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(column, *in, format_settings, serialization, type.get());

    serialization->deserializeTextQuoted(column, *in, format_settings);
    return true;
}

void MySQLDumpRowInputFormat::skipField()
{
    NullOutput out;
    skipBinaryIntroducerIfAny(*in);
    readQuotedFieldInto(out, *in);
}

MySQLDumpSchemaReader::MySQLDumpSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowSchemaReader(in_, format_settings_), table_name(format_settings_.mysql_dump.table_name)
{
}

NamesAndTypesList MySQLDumpSchemaReader::readSchema()
{
    NamesAndTypesList structure_from_create;
    Names names;
    readFirstCreateAndInsertQueries(in, table_name, structure_from_create, names);

    if (!structure_from_create.empty())
        return structure_from_create;

    checkStartOfRow(in);
    normalizeMySQLDumpColumnNames(names, {}, nullptr);
    column_state.first_names = names;
    column_state.has_explicit_columns = !names.empty();
    updateColumnPermutation(std::move(names));
    if (column_state.has_explicit_columns)
        setColumnNames(column_state.first_names);

    return IRowSchemaReader::readSchema();
}

void MySQLDumpSchemaReader::updateColumnPermutation(Names next_names)
{
    UnorderedMapWithMemoryTracking<String, size_t> first_positions;
    for (size_t i = 0; i < column_state.first_names.size(); ++i)
    {
        if (!first_positions.emplace(column_state.first_names[i], i).second)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Duplicate column name in MySQL dump table {}: {}", table_name, column_state.first_names[i]);
    }

    VectorWithMemoryTracking<size_t> next_permutation;
    next_permutation.reserve(next_names.size());
    for (const auto & name : next_names)
        next_permutation.push_back(first_positions.at(name));

    input_to_first = std::move(next_permutation);
    column_state.current_names = std::move(next_names);
}

bool MySQLDumpSchemaReader::prepareRow()
{
    std::optional<Names> next_names;
    bool has_row = prepareNextMySQLDumpRow(in, table_name, next_names);
    if (next_names)
    {
        normalizeMySQLDumpColumnNames(*next_names, column_state.create_names, nullptr);
        validateMySQLDumpColumnNames(table_name, column_state, *next_names);
        updateColumnPermutation(std::move(*next_names));
    }
    return has_row;
}

std::optional<DataTypes> MySQLDumpSchemaReader::readRowAndGetDataTypes()
{
    if (!prepareRow())
        return {};

    skipStartOfRow(in);
    DataTypes data_types;
    String value;
    while (!in.eof() && *in.position() != ')')
    {
        if (!data_types.empty())
            skipFieldDelimiter(in);

        skipBinaryIntroducerIfAny(in);
        readQuotedField(value, in);
        auto type = tryInferDataTypeByEscapingRule(value, format_settings, FormatSettings::EscapingRule::Quoted);
        data_types.push_back(std::move(type));
    }
    skipEndOfRow(in);
    if (column_state.has_explicit_columns)
    {
        if (data_types.size() != column_state.current_names.size() || data_types.size() != input_to_first.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Wrong number of values for MySQL dump table {}: expected {} columns {}, got {} values",
                table_name, column_state.current_names.size(), column_state.current_names, data_types.size());

        DataTypes data_types_in_first_order(data_types.size());
        for (size_t i = 0; i < data_types.size(); ++i)
            data_types_in_first_order[input_to_first[i]] = std::move(data_types[i]);
        return data_types_in_first_order;
    }
    return data_types;
}

void MySQLDumpSchemaReader::transformTypesIfNeeded(DB::DataTypePtr & type, DB::DataTypePtr & new_type)
{
    transformInferredTypesIfNeeded(type, new_type, format_settings);
}

void registerInputFormatMySQLDump(FormatFactory & factory);
void registerInputFormatMySQLDump(FormatFactory & factory)
{
    factory.registerInputFormat("MySQLDump", [](
        ReadBuffer & buf,
        const Block & header,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<MySQLDumpRowInputFormat>(buf, std::make_shared<const Block>(header), params, settings);
    });

    factory.setDocumentation("MySQLDump", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

ClickHouse supports reading MySQL [dumps](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html).

It reads all the data from `INSERT` queries belonging to a single table in the dump. 
If there is more than one table, by default it reads data from the first one.

<Note>
This format supports schema inference: if the dump contains a `CREATE` query for the specified table, the structure is inferred from it, otherwise the schema is inferred from the data of `INSERT` queries.
</Note>

## Example usage {#example-usage}

Given the following SQL dump file:

```sql title="dump.sql"
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test` (
  `x` int DEFAULT NULL,
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test` VALUES (1,NULL),(2,NULL),(3,NULL),(3,NULL),(4,NULL),(5,NULL),(6,7);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test 3` (
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test 3` VALUES (1);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test2` (
  `x` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test2` VALUES (1),(2),(3);
```

We can run the following queries:

```sql title="Query"
DESCRIBE TABLE file(dump.sql, MySQLDump) 
SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```response title="Response"
┌─name─┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ x    │ Nullable(Int32) │              │                    │         │                  │                │
└──────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql title="Query"
SELECT *
FROM file(dump.sql, MySQLDump)
SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```response title="Response"
┌─x─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

## Format settings {#format-settings}

You can specify the name of the table from which to read data from using the [`input_format_mysql_dump_table_name`](/reference/settings/formats/input-format#input_format_mysql_dump_table_name) setting.
If setting `input_format_mysql_dump_map_column_names` is set to `1` and the dump contains a `CREATE` query for specified table or column names in the `INSERT` query, the columns from the input data will map to the columns from the table by name.
Each `INSERT` may reorder the same nonempty explicit set of column names. When mapping by name, changing that set or mixing nonempty lists with statements without a column list is not supported. An empty column list is treated as no column list.
Without a parsable `CREATE`, schema inference also binds values by name and rejects those structure changes, even when `input_format_mysql_dump_map_column_names` is disabled. Disabling this setting only switches data reading to positional mapping.
When a parsable `CREATE` is available, schema inference returns its declared structure without validating subsequent data statements.

Columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/reference/settings/formats/input-format#input_format_skip_unknown_fields) is set to `1`.
)DOCS_MD"});
}

void registerMySQLSchemaReader(FormatFactory & factory);
void registerMySQLSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("MySQLDump", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_shared<MySQLDumpSchemaReader>(buf, settings);
    });

    factory.registerAdditionalInfoForSchemaCacheGetter("MySQLDump", [](const FormatSettings & settings)
    {
        auto result = getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::Quoted);
        return result + fmt::format(", table_name={}", settings.mysql_dump.table_name);
    });
}


}
