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

    bool parsePartitions(IParser::Pos & pos, Expected & expected, std::optional<ASTs> & partitions)
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

    bool parseExceptDatabases(IParser::Pos & pos, Expected & expected, std::set<String> & except_databases)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"EXCEPT DATABASE"}.ignore(pos, expected) && !ParserKeyword{"EXCEPT DATABASES"}.ignore(pos, expected))
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

            except_databases = std::move(result);
            return true;
        });
    }

    bool parseExceptTables(IParser::Pos & pos, Expected & expected, const std::optional<String> & database_name, std::set<DatabaseAndTableName> & except_tables)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"EXCEPT TABLE"}.ignore(pos, expected) && !ParserKeyword{"EXCEPT TABLES"}.ignore(pos, expected))
                return false;

            std::set<DatabaseAndTableName> result;
            auto parse_list_element = [&]
            {
                DatabaseAndTableName table_name;
                if (database_name)
                {
                    ASTPtr ast;
                    if (!ParserIdentifier{}.parse(pos, ast, expected))
                        return false;
                    table_name.first = *database_name;
                    table_name.second = getIdentifierName(ast);
                }
                else
                {
                    if (!parseDatabaseAndTableName(pos, expected, table_name.first, table_name.second))
                        return false;
                }

                result.emplace(std::move(table_name));
                return true;
            };
            if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
                return false;

            except_tables = std::move(result);
            return true;
        });
    }

    bool parseElement(IParser::Pos & pos, Expected & expected, bool allow_all, Element & element)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{"TABLE"}.ignore(pos, expected) || ParserKeyword{"DICTIONARY"}.ignore(pos, expected) ||
                ParserKeyword{"VIEW"}.ignore(pos, expected))
            {
                element.type = ElementType::TABLE;
                if (!parseDatabaseAndTableName(pos, expected, element.database_name, element.table_name))
                    return false;

                element.new_database_name = element.database_name;
                element.new_table_name = element.table_name;
                if (ParserKeyword("AS").ignore(pos, expected))
                {
                    if (!parseDatabaseAndTableName(pos, expected, element.new_database_name, element.new_table_name))
                        return false;
                }

                parsePartitions(pos, expected, element.partitions);
                return true;
            }

            if (ParserKeyword{"TEMPORARY TABLE"}.ignore(pos, expected))
            {
                element.type = ElementType::TEMPORARY_TABLE;

                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                element.table_name = getIdentifierName(ast);
                element.new_table_name = element.table_name;

                if (ParserKeyword("AS").ignore(pos, expected))
                {
                    ast = nullptr;
                    if (!ParserIdentifier{}.parse(pos, ast, expected))
                        return false;
                    element.new_table_name = getIdentifierName(ast);
                }

                return true;
            }

            if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
            {
                element.type = ElementType::DATABASE;

                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                element.database_name = getIdentifierName(ast);
                element.new_database_name = element.database_name;

                if (ParserKeyword("AS").ignore(pos, expected))
                {
                    ast = nullptr;
                    if (!ParserIdentifier{}.parse(pos, ast, expected))
                        return false;
                    element.new_database_name = getIdentifierName(ast);
                }

                parseExceptTables(pos, expected, element.database_name, element.except_tables);
                return true;
            }

            if (allow_all && ParserKeyword{"ALL"}.ignore(pos, expected))
            {
                element.type = ElementType::ALL;
                parseExceptDatabases(pos, expected, element.except_databases);
                parseExceptTables(pos, expected, {}, element.except_tables);
                return true;
            }

            return false;
        });
    }

    bool parseElements(IParser::Pos & pos, Expected & expected, bool allow_all, std::vector<Element> & elements)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<Element> result;

            auto parse_element = [&]
            {
                Element element;
                if (parseElement(pos, expected, allow_all, element))
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

    /// Disable "ALL" if this is a RESTORE command.
    bool allow_all = (kind == Kind::RESTORE);

    std::vector<Element> elements;
    if (!parseElements(pos, expected, allow_all, elements))
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
