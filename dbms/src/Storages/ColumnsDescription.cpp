#include <Storages/ColumnsDescription.h>
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


NamesAndTypesList ColumnsDescription::getList() const
{
    return ext::collection_cast<NamesAndTypesList>(boost::join(ordinary, materialized));
}


Names ColumnsDescription::getNames() const
{
    return ext::map<Names>(boost::join(ordinary, materialized), [] (const auto & it) { return it.name; });
}


NameAndTypePair ColumnsDescription::get(const String & column_name) const
{
    for (auto & it : boost::join(ordinary, materialized))
        if (it.name == column_name)
            return it;
    throw Exception("There is no column " + column_name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}


bool ColumnsDescription::has(const String & column_name) const
{
    for (auto & it : boost::join(ordinary, materialized))
        if (it.name == column_name)
            return true;
    return false;
}


String ColumnsDescription::toString() const
{
    WriteBufferFromOwnString buf;

    writeString("columns format version: 1\n", buf);
    writeText(ordinary.size() + materialized.size() + aliases.size(), buf);
    writeString(" columns:\n", buf);

    const auto write_columns = [this, &buf] (const NamesAndTypesList & columns)
    {
        for (const auto & column : columns)
        {
            const auto it = defaults.find(column.name);

            writeBackQuotedString(column.name, buf);
            writeChar(' ', buf);
            writeString(column.type->getName(), buf);
            if (it == std::end(defaults))
            {
                writeChar('\n', buf);
                continue;
            }
            else
                writeChar('\t', buf);

            writeString(DB::toString(it->second.type), buf);
            writeChar('\t', buf);
            writeString(queryToString(it->second.expression), buf);
            writeChar('\n', buf);
        }
    };

    write_columns(ordinary);
    write_columns(materialized);
    write_columns(aliases);

    return buf.str();
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
        readString(type_name, buf);
        auto type = data_type_factory.get(type_name);
        if (*buf.position() == '\n')
        {
            assertChar('\n', buf);

            result.ordinary.emplace_back(column_name, std::move(type));
            continue;
        }
        assertChar('\t', buf);

        String default_type_str;
        readString(default_type_str, buf);
        const auto default_type = columnDefaultTypeFromString(default_type_str);
        assertChar('\t', buf);

        String default_expr_str;
        readText(default_expr_str, buf);
        assertChar('\n', buf);

        const char * begin = default_expr_str.data();
        const auto end = begin + default_expr_str.size();
        ASTPtr default_expr = parseQuery(expr_parser, begin, end, "default expression");

        if (ColumnDefaultType::Default == default_type)
            result.ordinary.emplace_back(column_name, std::move(type));
        else if (ColumnDefaultType::Materialized == default_type)
            result.materialized.emplace_back(column_name, std::move(type));
        else if (ColumnDefaultType::Alias == default_type)
            result.aliases.emplace_back(column_name, std::move(type));

        result.defaults.emplace(column_name, ColumnDefault{default_type, default_expr});
    }

    assertEOF(buf);

    return result;
}

}
