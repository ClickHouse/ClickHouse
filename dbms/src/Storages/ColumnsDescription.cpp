#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <ext/collection_cast.h>
#include <ext/map.h>

#include <boost/range/join.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_PARSE_TEXT;
}


NamesAndTypesList ColumnsDescription::getAllPhysical() const
{
    return ext::collection_cast<NamesAndTypesList>(boost::join(ordinary, materialized));
}


NamesAndTypesList ColumnsDescription::getAll() const
{
    return ext::collection_cast<NamesAndTypesList>(boost::join(ordinary, boost::join(materialized, aliases)));
}


Names ColumnsDescription::getNamesOfPhysical() const
{
    return ext::map<Names>(boost::join(ordinary, materialized), [] (const auto & it) { return it.name; });
}


NameAndTypePair ColumnsDescription::getPhysical(const String & column_name) const
{
    for (auto & it : boost::join(ordinary, materialized))
        if (it.name == column_name)
            return it;
    throw Exception("There is no column " + column_name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}


bool ColumnsDescription::hasPhysical(const String & column_name) const
{
    for (auto & it : boost::join(ordinary, materialized))
        if (it.name == column_name)
            return true;
    return false;
}


String ColumnsDescription::toString() const
{
    WriteBufferFromOwnString buf;

    writeCString("columns format version: 1\n", buf);
    writeText(ordinary.size() + materialized.size() + aliases.size(), buf);
    writeCString(" columns:\n", buf);

    const auto write_columns = [this, &buf] (const NamesAndTypesList & columns)
    {
        for (const auto & column : columns)
        {
            const auto defaults_it = defaults.find(column.name);
            const auto comments_it = comments.find(column.name);

            writeBackQuotedString(column.name, buf);
            writeChar(' ', buf);
            writeText(column.type->getName(), buf);

            const bool exist_comment = comments_it != std::end(comments);
            if (defaults_it != std::end(defaults))
            {
                writeChar('\t', buf);
                writeText(DB::toString(defaults_it->second.kind), buf);
                writeChar('\t', buf);
                writeText(queryToString(defaults_it->second.expression), buf);
            }
            else if (exist_comment)
            {
                writeChar('\t', buf);
            }

            if (exist_comment)
            {
                writeChar('\t', buf);
                writeText(comments_it->second, buf);
            }

            writeChar('\n', buf);
        }
    };

    write_columns(ordinary);
    write_columns(materialized);
    write_columns(aliases);

    return buf.str();
}

struct ParsedDefaultInfo
{
    ColumnDefaultKind default_kind;
    ASTPtr default_expr_str;
};

std::optional<ParsedDefaultInfo> parseDefaulfInfo(ReadBufferFromString & buf)
{
    if (*buf.position() == '\n')
    {
        return {};
    }

    assertChar('\t', buf);
    if (*buf.position() == '\t')
    {
        assertChar('\t', buf);
        return {};
    }

    String default_kind_str;
    readText(default_kind_str, buf);
    const auto default_kind = columnDefaultKindFromString(default_kind_str);
    assertChar('\t', buf);

    ParserExpression expr_parser;
    String default_expr_str;
    readText(default_expr_str, buf);
    const char * begin = default_expr_str.data();
    const auto end = begin + default_expr_str.size();
    ASTPtr default_expr = parseQuery(expr_parser, begin, end, "default_expression", 0);
    return ParsedDefaultInfo{default_kind, std::move(default_expr)};
}

String parseComment(ReadBufferFromString& buf)
{
    if (*buf.position() == '\n')
    {
        return {};
    }

    ParserExpression parser_expr;
    String comment;
    readText(comment, buf); // This is wrong may be
    return comment;
}

ColumnsDescription ColumnsDescription::parse(const String & str)
{
    ReadBufferFromString buf{str};

    assertString("columns format version: 1\n", buf);
    size_t count{};
    readText(count, buf);
    assertString(" columns:\n", buf);

    ParserExpression expr_parser;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    ColumnsDescription result;
    for (size_t i = 0; i < count; ++i)
    {
        String column_name;
        readBackQuotedStringWithSQLStyle(column_name, buf);
        assertChar(' ', buf);

        String type_name;
        readText(type_name, buf);
        auto type = data_type_factory.get(type_name);
        if (*buf.position() == '\n')
        {
            assertChar('\n', buf);

            result.ordinary.emplace_back(column_name, std::move(type));
            continue;
        }

        assertChar('\t', buf);

        const auto default_info = parseDefaulfInfo(buf);
        if (default_info)
        {
            const auto & default_kind = default_info->default_kind;
            const auto & default_expr = default_info->default_expr_str;
            if (ColumnDefaultKind::Default == default_kind)
                result.ordinary.emplace_back(column_name, std::move(type));
            else if (ColumnDefaultKind::Materialized == default_kind)
                result.materialized.emplace_back(column_name, std::move(type));
            else if (ColumnDefaultKind::Alias == default_kind)
                result.aliases.emplace_back(column_name, std::move(type));

            result.defaults.emplace(column_name, ColumnDefault{default_kind, default_expr});
        }

        const auto comment = parseComment(buf);
        if (!comment.empty())
        {
            result.comments.emplace(column_name, comment);
        }

        assertChar('\n', buf);
    }

    assertEOF(buf);

    return result;
}

}
