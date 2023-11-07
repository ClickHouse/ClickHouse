#include <Parsers/ParserAnalyzeQuery.h>
#include <Parsers/ASTAnalyzeQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Common/assert_cast.h>
#include <boost/range/algorithm_ext/erase.hpp>

namespace DB
{

namespace
{
    bool parseSettings(IParser::Pos & pos, Expected & expected, ASTPtr & settings)
    {
        return IParserBase::wrapParseImpl(
            pos,
            [&]
            {
                if (!ParserKeyword{"SETTINGS"}.ignore(pos, expected))
                    return false;

                SettingsChanges settings_changes;
                ASTPtr res_base_backup_name;

                auto parse_setting = [&]
                {
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
                return true;
            });
    }

    bool parseSyncOrAsync(IParser::Pos & pos, Expected & expected, bool & is_async)
    {
        if (ParserKeyword{"ASYNC"}.ignore(pos, expected))
            is_async = true;
        else if (ParserKeyword{"SYNC"}.ignore(pos, expected))
            is_async = false;
        else
            return false;
        return true;
    }

    void addSyncOrAsyncToSettings(ASTPtr & settings, bool is_async)
    {
        SettingsChanges changes;
        if (settings)
        {
            changes = assert_cast<ASTSetQuery *>(settings.get())->changes;
        }

        boost::remove_erase_if(changes, [](const SettingChange & change) { return change.name == "async"; });
        changes.emplace_back("async", is_async);

        auto new_settings = std::make_shared<ASTSetQuery>();
        new_settings->changes = std::move(changes);
        new_settings->is_standalone = false;
        settings = new_settings;
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(
            pos, [&] { return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected); });
    }
}

bool ParserAnalyzeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword("ANALYZE").ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTAnalyzeQuery>();
    if (ParserKeyword("SAMPLE").ignore(pos, expected))
        query->is_full = false;
    else
        query->is_full = true;

    ParserKeyword("FULL").ignore(pos, expected);
    ParserKeyword("TABLE").ignore(pos, expected);

    /// Parse database and table
    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);
    
    /// Parse columns
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);

    if (open.ignore(pos, expected))
    {
        ParserList column_parser(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma));
        ASTPtr column_list;

        if (column_parser.parse(pos, column_list, expected))
        {
            query->column_list = std::move(column_list);
            query->children.push_back(query->column_list);
        }

        if (!close.ignore(pos, expected))
            return false;
    }

    /// Parse cluster
    String cluster;
    if (parseOnCluster(pos, expected, cluster))
        query->cluster = std::move(cluster);

    /// Parse sync or async
    bool is_async = false;
    parseSyncOrAsync(pos, expected, is_async);
    query->is_async = is_async;

    /// Parse settings
    ASTPtr settings;
    parseSettings(pos, expected, settings);

    if (settings)
    {
        query->settings = std::move(settings);
        query->children.push_back(query->settings);
    }

    addSyncOrAsyncToSettings(settings, is_async);

    node = query;
    return true;
}

}
