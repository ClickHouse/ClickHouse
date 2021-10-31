#include <Parsers/Access/ParserRowPolicyName.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace
{
    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }


    bool parseDBAndTableName(IParser::Pos & pos, Expected & expected, String & database, String & table_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            String res_database, res_table_name;
            if (!parseDatabaseAndTableName(pos, expected, res_database, res_table_name))
                return false;

            /// If table is specified without DB it cannot be followed by "ON"
            /// (but can be followed by "ON CLUSTER").
            /// The following code is necessary to figure out while parsing something like
            /// policy1 ON table1, policy2 ON table2
            /// that policy2 is another policy, not another table.
            auto end_pos = pos;
            if (res_database.empty() && ParserKeyword{"ON"}.ignore(pos, expected))
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
            return ParserKeyword{"ON"}.ignore(pos, expected) && parseDBAndTableName(pos, expected, database, table_name);
        });
    }


    bool parseOnDBAndTableNames(IParser::Pos & pos, Expected & expected, std::vector<std::pair<String, String>> & database_and_table_names)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"ON"}.ignore(pos, expected))
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
                                     std::vector<RowPolicy::NameParts> & name_parts,
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
            name_parts.clear();
            for (const String & short_name : short_names)
                for (const auto & [database, table_name] : database_and_table_names)
                    name_parts.push_back({short_name, database, table_name});

            cluster = std::move(res_cluster);
            return true;
        });
    }
}


bool ParserRowPolicyName::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<RowPolicy::NameParts> name_parts;
    String cluster;
    if (!parseRowPolicyNamesAroundON(pos, expected, false, false, allow_on_cluster, name_parts, cluster))
        return false;

    assert(name_parts.size() == 1);
    auto result = std::make_shared<ASTRowPolicyName>();
    result->name_parts = std::move(name_parts.front());
    result->cluster = std::move(cluster);
    node = result;
    return true;
}


bool ParserRowPolicyNames::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<RowPolicy::NameParts> name_parts;
    size_t num_added_names_last_time = 0;
    String cluster;

    auto parse_around_on = [&]
    {
        if (!name_parts.empty())
        {
            if ((num_added_names_last_time != 1) || !cluster.empty())
                return false;
        }

        std::vector<RowPolicy::NameParts> new_name_parts;
        if (!parseRowPolicyNamesAroundON(pos, expected, name_parts.empty(), name_parts.empty(), allow_on_cluster, new_name_parts, cluster))
            return false;

        num_added_names_last_time = new_name_parts.size();
        boost::range::push_back(name_parts, std::move(new_name_parts));
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_around_on, false))
        return false;

    auto result = std::make_shared<ASTRowPolicyNames>();
    result->name_parts = std::move(name_parts);
    result->cluster = std::move(cluster);
    node = result;
    return true;
}

}
