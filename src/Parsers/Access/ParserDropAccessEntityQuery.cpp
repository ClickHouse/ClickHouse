#include <Access/IAccessStorage.h>
#include <Access/MaskingPolicy.h>
#include <Parsers/Access/ParserDropAccessEntityQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ParserRowPolicyName.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <base/range.h>


namespace DB
{
namespace
{
    bool parseEntityType(IParserBase::Pos & pos, Expected & expected, AccessEntityType & type)
    {
        for (auto i : collections::range(AccessEntityType::MAX))
        {
            const auto & type_info = AccessEntityTypeInfo::get(i);
            if (ParserKeyword::createDeprecated(type_info.name).ignore(pos, expected)
                || (!type_info.alias.empty() && ParserKeyword::createDeprecated(type_info.alias).ignore(pos, expected)))
            {
                type = i;
                return true;
            }
        }
        return false;
    }


    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }

    bool parseMaskingPolicyName(IParserBase::Pos & pos, Expected & expected, MaskingPolicyName & name, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            String short_name;
            if (!parseIdentifierOrStringLiteral(pos, expected, short_name))
                return false;

            String res_cluster;
            if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
            {
                if (ASTQueryWithOnCluster::parse(pos, res_cluster, expected))
                {
                    cluster = res_cluster;
                    if (!ParserKeyword{Keyword::ON}.ignore(pos, expected))
                        return false;
                }
            }

            String database;
            String table_name;
            if (!parseDatabaseAndTableName(pos, expected, database, table_name))
                return false;

            name.short_name = std::move(short_name);
            name.database = std::move(database);
            name.table_name = std::move(table_name);
            return true;
        });
    }
}


bool ParserDropAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::DROP}.ignore(pos, expected))
        return false;

    AccessEntityType type;
    if (!parseEntityType(pos, expected, type))
        return false;

    bool if_exists = false;
    if (ParserKeyword{Keyword::IF_EXISTS}.ignore(pos, expected))
        if_exists = true;

    Strings names;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;
    std::shared_ptr<MaskingPolicyName> masking_policy_name;
    String storage_name;
    String cluster;

    if ((type == AccessEntityType::USER) || (type == AccessEntityType::ROLE))
    {
        if (!parseUserNames(pos, expected, names, /*allow_query_parameter=*/ false))
            return false;
    }
    else if (type == AccessEntityType::ROW_POLICY)
    {
        ParserRowPolicyNames parser;
        ASTPtr ast;
        parser.allowOnCluster();
        if (!parser.parse(pos, ast, expected))
            return false;
        row_policy_names = typeid_cast<std::shared_ptr<ASTRowPolicyNames>>(ast);
        cluster = std::exchange(row_policy_names->cluster, "");
    }
    else if (type == AccessEntityType::MASKING_POLICY)
    {
        masking_policy_name = std::make_shared<MaskingPolicyName>();
        if (!parseMaskingPolicyName(pos, expected, *masking_policy_name, cluster))
            return false;
    }
    else
    {
        if (!parseIdentifiersOrStringLiterals(pos, expected, names))
            return false;
    }

    if (ParserKeyword{Keyword::FROM}.ignore(pos, expected))
        parseAccessStorageName(pos, expected, storage_name);

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = std::make_shared<ASTDropAccessEntityQuery>();
    node = query;

    query->type = type;
    query->if_exists = if_exists;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->row_policy_names = std::move(row_policy_names);
    query->masking_policy_name = std::move(masking_policy_name);
    query->storage_name = std::move(storage_name);

    return true;
}
}
