#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
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

    bool parseType(IParser::Pos & pos, Expected & expected, ElementType & type, bool & name_is_in_temp_db)
    {
        name_is_in_temp_db = false;
        if (ParserKeyword{"TABLE"}.ignore(pos, expected) || ParserKeyword{"DICTIONARY"}.ignore(pos, expected))
        {
            type = ElementType::TABLE;
            return true;
        }
        if (ParserKeyword{"TEMPORARY TABLE"}.ignore(pos, expected))
        {
            type = ElementType::TABLE;
            name_is_in_temp_db = true;
            return true;
        }
        if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
        {
            type = ElementType::DATABASE;
            return true;
        }
        if (ParserKeyword{"ALL TEMPORARY TABLES"}.ignore(pos, expected))
        {
            type = ElementType::DATABASE;
            name_is_in_temp_db = true;
            return true;
        }
        if (ParserKeyword{"ALL DATABASES"}.ignore(pos, expected))
        {
            type = ElementType::ALL_DATABASES;
            return true;
        }
        return false;
    }

    bool parseTempDBFlag(IParser::Pos & pos, Expected & expected, ElementType type, bool & temp_db_flag)
    {
        temp_db_flag = false;
        switch (type)
        {
            case ElementType::TABLE:
            {
                if (ParserKeyword{"TABLE"}.ignore(pos, expected) || ParserKeyword{"DICTIONARY"}.ignore(pos, expected))
                {
                    return true;
                }
                if (ParserKeyword{"TEMPORARY TABLE"}.ignore(pos, expected))
                {
                    temp_db_flag = true;
                    return true;
                }
                return false;
            }

            case ElementType::DATABASE:
            {
                if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
                {
                    return true;
                }
                if (ParserKeyword{"ALL TEMPORARY TABLES"}.ignore(pos, expected))
                {
                    temp_db_flag = true;
                    return true;
                }
                return false;
            }

            default:
                return false;
        }
    }

    bool parseName(IParser::Pos & pos, Expected & expected, ElementType type, bool name_is_in_temp_db, DatabaseAndTableName & name)
    {
        name.first.clear();
        name.second.clear();
        switch (type)
        {
            case ElementType::TABLE:
            {
                if (name_is_in_temp_db)
                {
                    ASTPtr ast;
                    if (!ParserIdentifier{}.parse(pos, ast, expected))
                        return false;
                    name.second = getIdentifierName(ast);
                    return true;
                }
                return parseDatabaseAndTableName(pos, expected, name.first, name.second);
            }

            case ElementType::DATABASE:
            {
                if (name_is_in_temp_db)
                    return false;
                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                name.first = getIdentifierName(ast);
                return true;
            }

            default:
                return false;
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
            result.push_back(ast);
            return true;
        };
        if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
            return false;

        partitions = std::move(result);
        return true;
    }

    bool parseExceptList(IParser::Pos & pos, Expected & expected, std::set<String> & except_list)
    {
        if (!ParserKeyword{"EXCEPT"}.ignore(pos, expected))
            return false;

        std::set<String> result;
        auto parse_list_element = [&]
        {
            ASTPtr ast;
            if (!ParserIdentifier{}.parse(pos, ast, expected))
                return false;
            result.insert(getIdentifierName(ast));
            return true;
        };
        if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
            return false;

        except_list = std::move(result);
        return true;
    }

    bool parseElement(IParser::Pos & pos, Expected & expected, Element & entry)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ElementType type;
            bool name_is_in_temp_db = false;
            if (!parseType(pos, expected, type, name_is_in_temp_db))
                return false;

            DatabaseAndTableName name;
            if ((type == ElementType::TABLE) || (type == ElementType::DATABASE && !name_is_in_temp_db))
            {
                if (!parseName(pos, expected, type, name_is_in_temp_db, name))
                    return false;
            }

            bool new_name_is_in_temp_db = name_is_in_temp_db;
            DatabaseAndTableName new_name = name;
            if (ParserKeyword{"AS"}.ignore(pos, expected) || ParserKeyword{"INTO"}.ignore(pos, expected))
            {
                if (!parseTempDBFlag(pos, expected, type, new_name_is_in_temp_db))
                    new_name_is_in_temp_db = name_is_in_temp_db;

                if ((type == ElementType::TABLE) || (type == ElementType::DATABASE && !new_name_is_in_temp_db))
                {
                    if (!parseName(pos, expected, type, new_name_is_in_temp_db, new_name))
                        new_name = name;
                }
            }

            ASTs partitions;
            if (type == ElementType::TABLE)
                parsePartitions(pos, expected, partitions);

            std::set<String> except_list;
            if (type != ElementType::TABLE)
                parseExceptList(pos, expected, except_list);

            entry.type = type;
            entry.name = std::move(name);
            entry.new_name = std::move(new_name);
            entry.name_is_in_temp_db = name_is_in_temp_db;
            entry.new_name_is_in_temp_db = new_name_is_in_temp_db;
            entry.partitions = std::move(partitions);
            entry.except_list = std::move(except_list);
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

    bool parseBackupName(IParser::Pos & pos, Expected & expected, ASTPtr & backup_name)
    {
        return ParserIdentifierWithOptionalParameters{}.parse(pos, backup_name, expected);
    }

    bool parseBaseBackupSetting(IParser::Pos & pos, Expected & expected, ASTPtr & base_backup_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"base_backup"}.ignore(pos, expected)
                && ParserToken(TokenType::Equals).ignore(pos, expected)
                && parseBackupName(pos, expected, base_backup_name);
        });
    }

    bool parseSettings(IParser::Pos & pos, Expected & expected, ASTPtr & settings, ASTPtr & base_backup_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"SETTINGS"}.ignore(pos, expected))
                return false;

            ASTPtr res_settings;
            ASTPtr res_base_backup_name;

            auto parse_setting = [&]
            {
                if (!res_settings && ParserSetQuery{true}.parse(pos, res_settings, expected))
                    return true;

                if (!res_base_backup_name && parseBaseBackupSetting(pos, expected, res_base_backup_name))
                    return true;

                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_setting, false))
                return false;

            settings = std::move(res_settings);
            base_backup_name = std::move(res_base_backup_name);
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

    ASTPtr backup_name;
    if (!parseBackupName(pos, expected, backup_name))
        return false;

    ASTPtr settings;
    ASTPtr base_backup_name;
    parseSettings(pos, expected, settings, base_backup_name);

    auto query = std::make_shared<ASTBackupQuery>();
    node = query;

    query->kind = kind;
    query->elements = std::move(elements);
    query->backup_name = std::move(backup_name);
    query->base_backup_name = std::move(base_backup_name);
    query->settings = std::move(settings);

    return true;
}

}
