#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Common/assert_cast.h>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    bool parseType(IParser::Pos & pos, Expected & expected, ElementType & type, bool & is_temp_db)
    {
        is_temp_db = false;
        if (ParserKeyword{"TABLE"}.ignore(pos, expected) || ParserKeyword{"DICTIONARY"}.ignore(pos, expected))
        {
            type = ElementType::TABLE;
            return true;
        }
        if (ParserKeyword{"TEMPORARY TABLE"}.ignore(pos, expected))
        {
            type = ElementType::TABLE;
            is_temp_db = true;
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
            is_temp_db = true;
            return true;
        }
        if (ParserKeyword{"ALL DATABASES"}.ignore(pos, expected))
        {
            type = ElementType::ALL_DATABASES;
            return true;
        }
        return false;
    }

    bool parseName(IParser::Pos & pos, Expected & expected, ElementType type, bool is_temp_db, DatabaseAndTableName & name)
    {
        name.first.clear();
        name.second.clear();
        switch (type)
        {
            case ElementType::TABLE:
            {
                if (is_temp_db)
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
                if (is_temp_db)
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

    bool parseExceptList(IParser::Pos & pos, Expected & expected, bool parse_except_tables, std::set<String> & except_list)
    {
        if (!ParserKeyword{parse_except_tables ? "EXCEPT TABLES" : "EXCEPT"}.ignore(pos, expected))
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
            bool is_temp_db = false;
            if (!parseType(pos, expected, type, is_temp_db))
                return false;

            DatabaseAndTableName name;
            if ((type == ElementType::TABLE) || (type == ElementType::DATABASE && !is_temp_db))
            {
                if (!parseName(pos, expected, type, is_temp_db, name))
                    return false;
            }

            DatabaseAndTableName new_name = name;
            if (ParserKeyword{"AS"}.ignore(pos, expected))
            {
                if ((type == ElementType::TABLE) || (type == ElementType::DATABASE && !is_temp_db))
                {
                    if (!parseName(pos, expected, type, is_temp_db, new_name))
                        return false;
                }
            }

            ASTs partitions;
            if (type == ElementType::TABLE)
                parsePartitions(pos, expected, partitions);

            std::set<String> except_list;
            if ((type == ElementType::DATABASE) || (type == ElementType::ALL_DATABASES))
            {
                bool parse_except_tables = ((type == ElementType::DATABASE) && !is_temp_db);
                parseExceptList(pos, expected, parse_except_tables, except_list);
            }

            entry.type = type;
            entry.name = std::move(name);
            entry.new_name = std::move(new_name);
            entry.is_temp_db = is_temp_db;
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

    bool parseClusterHostIDs(IParser::Pos & pos, Expected & expected, ASTPtr & cluster_host_ids)
    {
        return ParserArray{}.parse(pos, cluster_host_ids, expected);
    }

    bool parseClusterHostIDsSetting(IParser::Pos & pos, Expected & expected, ASTPtr & cluster_host_ids)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"cluster_host_ids"}.ignore(pos, expected)
                && ParserToken(TokenType::Equals).ignore(pos, expected)
                && parseClusterHostIDs(pos, expected, cluster_host_ids);
        });
    }

    bool parseSettings(IParser::Pos & pos, Expected & expected, ASTPtr & settings, ASTPtr & base_backup_name, ASTPtr & cluster_host_ids)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"SETTINGS"}.ignore(pos, expected))
                return false;

            SettingsChanges settings_changes;
            ASTPtr res_base_backup_name;
            ASTPtr res_cluster_host_ids;

            auto parse_setting = [&]
            {
                if (!res_base_backup_name && parseBaseBackupSetting(pos, expected, res_base_backup_name))
                    return true;

                if (!res_cluster_host_ids && parseClusterHostIDsSetting(pos, expected, res_cluster_host_ids))
                    return true;

                SettingChange setting;
                if (ParserSetQuery::parseNameValuePair(setting, pos, expected))
                {
                    settings_changes.push_back(std::move(setting));
                    return true;
                }

                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_setting, false))
                return false;

            ASTPtr res_settings;
            if (!settings_changes.empty())
            {
                auto settings_changes_ast = std::make_shared<ASTSetQuery>();
                settings_changes_ast->changes = std::move(settings_changes);
                settings_changes_ast->is_standalone = false;
                res_settings = settings_changes_ast;
            }

            settings = std::move(res_settings);
            base_backup_name = std::move(res_base_backup_name);
            cluster_host_ids = std::move(res_cluster_host_ids);
            return true;
        });
    }

    bool parseSyncOrAsync(IParser::Pos & pos, Expected & expected, ASTPtr & settings)
    {
        bool async;
        if (ParserKeyword{"ASYNC"}.ignore(pos, expected))
            async = true;
        else if (ParserKeyword{"SYNC"}.ignore(pos, expected))
            async = false;
        else
            return false;

        SettingsChanges changes;
        if (settings)
        {
            changes = assert_cast<ASTSetQuery *>(settings.get())->changes;
        }

        boost::remove_erase_if(changes, [](const SettingChange & change) { return change.name == "async"; });
        changes.emplace_back("async", async);

        auto new_settings = std::make_shared<ASTSetQuery>();
        new_settings->changes = std::move(changes);
        new_settings->is_standalone = false;
        settings = new_settings;
        return true;
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
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

    String cluster;
    parseOnCluster(pos, expected, cluster);

    if (!ParserKeyword{(kind == Kind::BACKUP) ? "TO" : "FROM"}.ignore(pos, expected))
        return false;

    ASTPtr backup_name;
    if (!parseBackupName(pos, expected, backup_name))
        return false;

    ASTPtr settings;
    ASTPtr base_backup_name;
    ASTPtr cluster_host_ids;
    parseSettings(pos, expected, settings, base_backup_name, cluster_host_ids);
    parseSyncOrAsync(pos, expected, settings);

    auto query = std::make_shared<ASTBackupQuery>();
    node = query;

    query->kind = kind;
    query->elements = std::move(elements);
    query->cluster = std::move(cluster);
    query->backup_name = std::move(backup_name);
    query->settings = std::move(settings);
    query->base_backup_name = std::move(base_backup_name);
    query->cluster_host_ids = std::move(cluster_host_ids);

    return true;
}

}
