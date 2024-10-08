#include <Parsers/Access/ParserRowPolicyName.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <base/insertAtEnd.h>


namespace DB
{
namespace
{
    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }


    bool parseDBAndTableName(IParser::Pos & pos, Expected & expected, String & database, String & table_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            String res_database, res_table_name;

            bool wildcard = false;
            bool default_database = false;
            if (!parseDatabaseAndTableNameOrAsterisks(pos, expected, res_database, res_table_name, wildcard, default_database)
                || (res_database.empty() && res_table_name.empty() && !default_database))
                return false;

            if (res_table_name.empty())
                res_table_name = RowPolicyName::ANY_TABLE_MARK;

            /// If table is specified without DB it cannot be followed by Keyword::ON
            /// (but can be followed by Keyword::ON CLUSTER).
            /// The following code is necessary to figure out while parsing something like
            /// policy1 ON table1, policy2 ON table2
            /// that policy2 is another policy, not another table.
            auto end_pos = pos;
            if (res_database.empty() && ParserKeyword{Keyword::ON}.ignore(pos, expected))
            {
                String unused;
                if (ASTQueryWithOnCluster::parse(pos, unused, expected))
                    pos = end_pos;
                else
                    return false;
            }

            database = std::move(res_database);
            table_name = std::move(res_table_name);
            return true;
        });
    }


    bool parseOnDBAndTableName(IParser::Pos & pos, Expected & expected, String & database, String & table_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected) && parseDBAndTableName(pos, expected, database, table_name);
        });
    }


    bool parseOnDBAndTableNames(IParser::Pos & pos, Expected & expected, std::vector<std::pair<String, String>> & database_and_table_names)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::ON}.ignore(pos, expected))
                return false;

            std::vector<std::pair<String, String>> res;

            auto parse_db_and_table_name = [&]
            {
                String database, table_name;
                if (!parseDBAndTableName(pos, expected, database, table_name))
                    return false;
                res.emplace_back(std::move(database), std::move(table_name));
                return true;
            };

            if (!ParserList::parseUtil(pos, expected, parse_db_and_table_name, false))
                return false;

            database_and_table_names = std::move(res);
            return true;
         });
    }


    bool parseRowPolicyNamesAroundON(IParser::Pos & pos, Expected & expected,
                                     bool allow_multiple_short_names,
                                     bool allow_multiple_tables,
                                     bool allow_on_cluster,
                                     std::vector<RowPolicyName> & full_names,
                                     String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<String> short_names;
            if (allow_multiple_short_names)
            {
                if (!parseIdentifiersOrStringLiterals(pos, expected, short_names))
                    return false;
            }
            else
            {
                if (!parseIdentifierOrStringLiteral(pos, expected, short_names.emplace_back()))
                    return false;
            }

            String res_cluster;
            if (allow_on_cluster)
                parseOnCluster(pos, expected, res_cluster);

            std::vector<std::pair<String, String>> database_and_table_names;
            if (allow_multiple_tables && (short_names.size() == 1))
            {
                if (!parseOnDBAndTableNames(pos, expected, database_and_table_names))
                    return false;
            }
            else
            {
                String database, table_name;
                if (!parseOnDBAndTableName(pos, expected, database, table_name))
                    return false;
                database_and_table_names.emplace_back(std::move(database), std::move(table_name));
            }


            if (allow_on_cluster && res_cluster.empty())
                parseOnCluster(pos, expected, res_cluster);

            assert(!short_names.empty());
            assert(!database_and_table_names.empty());
            full_names.clear();
            for (const String & short_name : short_names)
                for (const auto & [database, table_name] : database_and_table_names)
                    full_names.push_back({short_name, database, table_name});

            cluster = std::move(res_cluster);
            return true;
        });
    }
}


bool ParserRowPolicyName::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<RowPolicyName> full_names;
    String cluster;
    if (!parseRowPolicyNamesAroundON(pos, expected, false, false, allow_on_cluster, full_names, cluster))
        return false;

    assert(full_names.size() == 1);
    auto result = std::make_shared<ASTRowPolicyName>();
    result->full_name = std::move(full_names.front());
    result->cluster = std::move(cluster);
    node = result;
    return true;
}


bool ParserRowPolicyNames::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<RowPolicyName> full_names;
    size_t num_added_names_last_time = 0;
    String cluster;

    auto parse_around_on = [&]
    {
        if (!full_names.empty())
        {
            if ((num_added_names_last_time != 1) || !cluster.empty())
                return false;
        }

        std::vector<RowPolicyName> new_full_names;
        if (!parseRowPolicyNamesAroundON(pos, expected, full_names.empty(), full_names.empty(), allow_on_cluster, new_full_names, cluster))
            return false;

        num_added_names_last_time = new_full_names.size();
        insertAtEnd(full_names, std::move(new_full_names));
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_around_on, false))
        return false;

    auto result = std::make_shared<ASTRowPolicyNames>();
    result->full_names = std::move(full_names);
    result->cluster = std::move(cluster);
    node = result;
    return true;
}

}
