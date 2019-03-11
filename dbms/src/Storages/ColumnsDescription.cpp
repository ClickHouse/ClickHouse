#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
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
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>

#include <ext/collection_cast.h>
#include <ext/map.h>

#include <boost/range/join.hpp>
#include <Compression/CompressionFactory.h>

#include <optional>


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


bool ColumnsDescription::operator==(const ColumnsDescription & other) const
{
    if (ordinary != other.ordinary
        || materialized != other.materialized
        || aliases != other.aliases
        || defaults != other.defaults
        || comments != other.comments)
    {
        return false;
    }

    if (codecs.size() != other.codecs.size())
        return false;

    for (const auto & [col_name, codec_ptr] : codecs)
    {
        if (other.codecs.count(col_name) == 0)
            return false;
        if (other.codecs.at(col_name)->getCodecDesc() != codec_ptr->getCodecDesc())
            return false;
    }
    return true;
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
            const auto codec_it = codecs.find(column.name);

            writeBackQuotedString(column.name, buf);
            writeChar(' ', buf);
            writeText(column.type->getName(), buf);

            const bool exist_comment = comments_it != std::end(comments);
            const bool exist_codec = codec_it != std::end(codecs);
            if (defaults_it != std::end(defaults))
            {
                writeChar('\t', buf);
                writeText(DB::toString(defaults_it->second.kind), buf);
                writeChar('\t', buf);
                writeText(queryToString(defaults_it->second.expression), buf);
            }

            if (exist_comment)
            {
                writeChar('\t', buf);
                writeText("COMMENT ", buf);
                writeText(queryToString(ASTLiteral(Field(comments_it->second))), buf);
            }

            if (exist_codec)
            {
                writeChar('\t', buf);
                writeText("CODEC(", buf);
                writeText(codec_it->second->getCodecDesc(), buf);
                writeText(")", buf);
            }

            writeChar('\n', buf);
        }
    };

    write_columns(ordinary);
    write_columns(materialized);
    write_columns(aliases);
    return buf.str();
}

void parseColumn(ReadBufferFromString & buf, ColumnsDescription & result, const DataTypeFactory & data_type_factory)
{
    ParserColumnDeclaration column_parser(true);
    String column_line;
    readEscapedStringUntilEOL(column_line, buf);
    ASTPtr ast = parseQuery(column_parser, column_line, "column parser", 0);
    if (const ASTColumnDeclaration * col_ast = typeid_cast<const ASTColumnDeclaration *>(ast.get()))
    {
        String column_name = col_ast->name;
        auto type = data_type_factory.get(col_ast->type);

        if (col_ast->default_expression)
        {
            auto kind = columnDefaultKindFromString(col_ast->default_specifier);
            switch (kind)
            {
            case ColumnDefaultKind::Default:
                result.ordinary.emplace_back(column_name, std::move(type));
                break;
            case ColumnDefaultKind::Materialized:
                result.materialized.emplace_back(column_name, std::move(type));
                break;
            case ColumnDefaultKind::Alias:
                result.aliases.emplace_back(column_name, std::move(type));
                break;
            }

            result.defaults.emplace(column_name, ColumnDefault{kind, std::move(col_ast->default_expression)});
        }
        else
            result.ordinary.emplace_back(column_name, std::move(type));

        if (col_ast->comment)
            if (auto comment_str = typeid_cast<ASTLiteral &>(*col_ast->comment).value.get<String>(); !comment_str.empty())
                result.comments.emplace(column_name, std::move(comment_str));

        if (col_ast->codec)
        {
            auto codec = CompressionCodecFactory::instance().get(col_ast->codec, type);
            result.codecs.emplace(column_name, std::move(codec));
        }
    }
    else
        throw Exception("Cannot parse column description", ErrorCodes::CANNOT_PARSE_TEXT);
}

CompressionCodecPtr ColumnsDescription::getCodecOrDefault(const String & column_name, CompressionCodecPtr default_codec) const
{
    const auto codec = codecs.find(column_name);

    if (codec == codecs.end())
        return default_codec;

    return codec->second;
}


CompressionCodecPtr ColumnsDescription::getCodecOrDefault(const String & column_name) const
{
    return getCodecOrDefault(column_name, CompressionCodecFactory::instance().getDefaultCodec());
}

ColumnsDescription ColumnsDescription::parse(const String & str)
{
    ReadBufferFromString buf{str};

    assertString("columns format version: 1\n", buf);
    size_t count{};
    readText(count, buf);
    assertString(" columns:\n", buf);

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    ColumnsDescription result;
    for (size_t i = 0; i < count; ++i)
    {
        parseColumn(buf, result, data_type_factory);
        buf.ignore(1); /// ignore new line
    }

    assertEOF(buf);
    return result;
}

const ColumnsDescription * ColumnsDescription::loadFromContext(const Context & context, const String & db, const String & table)
{
    if (context.getSettingsRef().insert_sample_with_metadata)
    {
        if (context.isTableExist(db, table))
        {
            StoragePtr storage = context.getTable(db, table);
            return &storage->getColumns();
        }
    }

    return nullptr;
}

}
