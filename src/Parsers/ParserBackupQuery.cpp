#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    bool parseName(IParser::Pos & pos, Expected & expected, ElementType type, DatabaseAndTableName & name)
    {
        switch (type)
        {
            case ElementType::TABLE: [[fallthrough]];
            case ElementType::DICTIONARY:
            {
                return parseDatabaseAndTableName(pos, expected, name.first, name.second);
            }

            case ElementType::DATABASE:
            {
                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                name.first = getIdentifierName(ast);
                name.second.clear();
                return true;
            }

            case ElementType::TEMPORARY_TABLE:
            {
                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                name.second = getIdentifierName(ast);
                name.first.clear();
                return true;
            }

            default:
                return true;
        }
    }

    bool parsePartitions(IParser::Pos & pos, Expected & expected, ASTs & partitions)
    {
        if (!ParserKeyword{"PARTITION"}.ignore(pos, expected) && !ParserKeyword{"PARTITIONS"}.ignore(pos, expected))
            return false;

        ASTs result;
        auto parse_list_element = [&]
        {
            ASTPtr ast;
            if (!ParserPartition{}.parse(pos, ast, expected))
                return false;
            result.emplace_back(ast);
            return true;
        };
        if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
            return false;

        partitions = std::move(result);
        return true;
    }

    bool parseElement(IParser::Pos & pos, Expected & expected, Element & entry)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ElementType type;
            if (ParserKeyword{"TABLE"}.ignore(pos, expected))
                type = ElementType::TABLE;
            else if (ParserKeyword{"DICTIONARY"}.ignore(pos, expected))
                type = ElementType::DICTIONARY;
            else if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
                type = ElementType::DATABASE;
            else if (ParserKeyword{"ALL DATABASES"}.ignore(pos, expected))
                type = ElementType::ALL_DATABASES;
            else if (ParserKeyword{"TEMPORARY TABLE"}.ignore(pos, expected))
                type = ElementType::TEMPORARY_TABLE;
            else if (ParserKeyword{"ALL TEMPORARY TABLES"}.ignore(pos, expected))
                type = ElementType::ALL_TEMPORARY_TABLES;
            else if (ParserKeyword{"EVERYTHING"}.ignore(pos, expected))
                type = ElementType::EVERYTHING;
            else
                return false;

            DatabaseAndTableName name;
            if (!parseName(pos, expected, type, name))
                return false;

            ASTs partitions;
            if (type == ElementType::TABLE)
                parsePartitions(pos, expected, partitions);

            DatabaseAndTableName new_name;
            if (ParserKeyword{"AS"}.ignore(pos, expected) || ParserKeyword{"INTO"}.ignore(pos, expected))
            {
                if (!parseName(pos, expected, type, new_name))
                    return false;
            }

            if ((type == ElementType::TABLE) && partitions.empty())
                parsePartitions(pos, expected, partitions);

            entry.type = type;
            entry.name = std::move(name);
            entry.new_name = std::move(new_name);
            entry.partitions = std::move(partitions);
            return true;
        });
    }

    bool parseElements(IParser::Pos & pos, Expected & expected, std::vector<Element> & elements)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<Element> result;

            auto parse_element = [&]
            {
                Element element;
                if (parseElement(pos, expected, element))
                {
                    result.emplace_back(std::move(element));
                    return true;
                }
                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_element, false))
                return false;

            elements = std::move(result);
            return true;
        });
    }

    bool parseSettings(IParser::Pos & pos, Expected & expected, ASTPtr & settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"SETTINGS"}.ignore(pos, expected))
                return false;

            ASTPtr result;
            if (!ParserSetQuery{true}.parse(pos, result, expected))
                return false;

            settings = std::move(result);
            return true;
        });
    }
}


bool ParserBackupQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Kind kind;
    if (ParserKeyword{"BACKUP"}.ignore(pos, expected))
        kind = Kind::BACKUP;
    else if (ParserKeyword{"RESTORE"}.ignore(pos, expected))
        kind = Kind::RESTORE;
    else
        return false;

    std::vector<Element> elements;
    if (!parseElements(pos, expected, elements))
        return false;

    if (!ParserKeyword{(kind == Kind::BACKUP) ? "TO" : "FROM"}.ignore(pos, expected))
        return false;
    ASTPtr ast;
    if (!ParserStringLiteral{}.parse(pos, ast, expected))
        return false;
    String backup_name = ast->as<ASTLiteral &>().value.safeGet<String>();

    ASTPtr settings;
    parseSettings(pos, expected, settings);

    auto query = std::make_shared<ASTBackupQuery>();
    node = query;

    query->kind = kind;
    query->elements = std::move(elements);
    query->backup_name = std::move(backup_name);
    query->settings = std::move(settings);

    return true;
}

}
