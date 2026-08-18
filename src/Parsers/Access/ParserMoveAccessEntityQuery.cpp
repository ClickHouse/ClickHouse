#include <Access/IAccessStorage.h>
#include <Parsers/Access/ParserMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTMoveAccessEntityQuery.h>
#include <Parsers/Access/ParserRowPolicyName.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/StatementFactory.h>
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
}


bool ParserMoveAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::MOVE}.ignore(pos, expected))
        return false;

    AccessEntityType type = {};
    if (!parseEntityType(pos, expected, type))
        return false;

    Strings names;
    boost::intrusive_ptr<ASTRowPolicyNames> row_policy_names;
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
        row_policy_names = boost::static_pointer_cast<ASTRowPolicyNames>(ast);
        cluster = std::exchange(row_policy_names->cluster, "");
    }
    else
    {
        if (!parseIdentifiersOrStringLiterals(pos, expected, names))
            return false;
    }

    if (!ParserKeyword{Keyword::TO}.ignore(pos, expected) || !parseAccessStorageName(pos, expected, storage_name))
        return false;

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = make_intrusive<ASTMoveAccessEntityQuery>();
    node = query;

    query->type = type;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->row_policy_names = std::move(row_policy_names);
    query->storage_name = std::move(storage_name);

    return true;
}
}

namespace DB
{

REGISTER_STATEMENTS(MoveAccessEntity)
{
    factory.registerStatement("MOVE",
    {
        .description = R"(
Moves an access entity from one access storage to another. The available access storages are `local_directory`,
`memory`, `replicated`, `users_xml` (read-only) and `ldap` (read-only).
)",
        .syntax = R"(
MOVE {USER | ROLE | QUOTA | SETTINGS PROFILE | ROW POLICY} name1 [, name2, ...] TO access_storage_type
)",
        .examples = {{"Move a user to another access storage", "MOVE USER test TO local_directory;", ""}},
        .related = {"CREATE USER", "CREATE ROLE", "SHOW"},
    });
}

}
