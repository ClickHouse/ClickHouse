#include <Parsers/ParserDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRowPolicyName.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseUserName.h>
#include <common/range.h>


namespace DB
{
namespace
{
    using EntityType = IAccessEntity::Type;
    using EntityTypeInfo = IAccessEntity::TypeInfo;


    bool parseEntityType(IParserBase::Pos & pos, Expected & expected, EntityType & type)
    {
        for (auto i : collections::range(EntityType::MAX))
        {
            const auto & type_info = EntityTypeInfo::get(i);
            if (ParserKeyword{type_info.name.c_str()}.ignore(pos, expected)
                || (!type_info.alias.empty() && ParserKeyword{type_info.alias.c_str()}.ignore(pos, expected)))
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
            return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserDropAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"DROP"}.ignore(pos, expected))
        return false;

    EntityType type;
    if (!parseEntityType(pos, expected, type))
        return false;

    bool if_exists = false;
    if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
        if_exists = true;

    Strings names;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;
    String cluster;

    if ((type == EntityType::USER) || (type == EntityType::ROLE))
    {
        if (!parseUserNames(pos, expected, names))
            return false;
    }
    else if (type == EntityType::ROW_POLICY)
    {
        ParserRowPolicyNames parser;
        ASTPtr ast;
        parser.allowOnCluster();
        if (!parser.parse(pos, ast, expected))
            return false;
        row_policy_names = typeid_cast<std::shared_ptr<ASTRowPolicyNames>>(ast);
        cluster = std::exchange(row_policy_names->cluster, "");
    }
    else
    {
        if (!parseIdentifiersOrStringLiterals(pos, expected, names))
            return false;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = std::make_shared<ASTDropAccessEntityQuery>();
    node = query;

    query->type = type;
    query->if_exists = if_exists;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->row_policy_names = std::move(row_policy_names);

    return true;
}
}
