#include <Parsers/ParserRowPolicyName.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>


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


    bool parseOnDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database, String & table_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"ON"}.ignore(pos, expected))
                return false;

            return parseDatabaseAndTableName(pos, expected, database, table_name);
        });
    }


    bool parseOnDatabaseAndTableName(IParser::Pos & pos, Expected & expected, std::pair<String, String> & database_and_table_name)
    {
        return parseOnDatabaseAndTableName(pos, expected, database_and_table_name.first, database_and_table_name.second);
    }


    bool parseOnDatabaseAndTableNames(IParser::Pos & pos, Expected & expected, std::vector<std::pair<String, String>> & database_and_table_names)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"ON"}.ignore(pos, expected))
                return false;

            std::vector<std::pair<String, String>> res;
            std::optional<IParser::Pos> pos_before_comma;
            do
            {
                String database, table_name;
                if (!parseDatabaseAndTableName(pos, expected, database, table_name))
                    return false;

                String unused;
                if (pos_before_comma && database.empty() && ParserKeyword{"ON"}.ignore(pos, expected)
                    && !ASTQueryWithOnCluster::parse(pos, unused, expected))
                {
                    pos = *pos_before_comma;
                    break;
                }

                res.emplace_back(std::move(database), std::move(table_name));
                pos_before_comma = pos;
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));
            database_and_table_names = std::move(res);
            return true;
        });
    }
}


bool ParserRowPolicyName::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    RowPolicy::NameParts name_parts;
    if (!parseIdentifierOrStringLiteral(pos, expected, name_parts.short_name))
        return false;

    String cluster;
    parseOnCluster(pos, expected, cluster);

    if (!parseOnDatabaseAndTableName(pos, expected, name_parts.database, name_parts.table_name))
        return false;

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto result = std::make_shared<ASTRowPolicyName>();
    result->name_parts = std::move(name_parts);
    result->cluster = std::move(cluster);
    node = result;
    return true;
}


bool ParserRowPolicyNames::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<RowPolicy::NameParts> name_parts;
    String cluster;

    do
    {
        std::vector<String> short_names;
        bool allowed_multiple_short_names = name_parts.empty();
        if (allowed_multiple_short_names)
        {
            if (!parseIdentifiersOrStringLiterals(pos, expected, short_names))
                return false;
        }
        else
        {
            if (!parseIdentifierOrStringLiteral(pos, expected, short_names.emplace_back()))
                return false;
        }

        bool allowed_on_cluster = allow_on_cluster && name_parts.empty();
        if (allowed_on_cluster)
            parseOnCluster(pos, expected, cluster);

        std::vector<std::pair<String, String>> database_and_table_names;
        bool allowed_multiple_db_and_table_names = ((name_parts.empty()) && (short_names.size() == 1));
        if (allowed_multiple_db_and_table_names)
        {
            if (!parseOnDatabaseAndTableNames(pos, expected, database_and_table_names))
                return false;
        }
        else
        {
            if (!parseOnDatabaseAndTableName(pos, expected, database_and_table_names.emplace_back()))
                return false;
        }

        allowed_on_cluster &= cluster.empty();
        if (allowed_on_cluster)
            parseOnCluster(pos, expected, cluster);

        for (const String & short_name : short_names)
            for (const auto & [database, table_name] : database_and_table_names)
                name_parts.push_back({short_name, database, table_name});

        if ((short_names.size() != 1) || (database_and_table_names.size() != 1) || !cluster.empty())
            break;
    }
    while (ParserToken{TokenType::Comma}.ignore(pos, expected));

    auto result = std::make_shared<ASTRowPolicyNames>();
    result->name_parts = std::move(name_parts);
    result->cluster = std::move(cluster);
    node = result;
    return true;
}

}
