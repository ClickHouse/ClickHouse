#include <Parsers/Access/ParserShowGrantsQuery.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>


namespace DB
{
namespace
{
    bool parseForQuery(IParser::Pos & pos, Expected & expected, ASTPtr & for_query)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::FOR}.ignore(pos, expected) || !ParserKeyword{Keyword::QUERY}.ignore(pos, expected))
                return false;
            return ParserSelectWithUnionQuery{}.parse(pos, for_query, expected);
        });
    }
}

bool ParserShowGrantsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::SHOW_GRANTS}.ignore(pos, expected))
        return false;

    boost::intrusive_ptr<ASTRolesOrUsersSet> for_roles;
    ASTPtr for_query;

    /// Tried before `FOR <roles>`, which would read `QUERY` as a user name. A user or role named `query` still parses.
    if (parseForQuery(pos, expected, for_query))
    {
        for_roles = make_intrusive<ASTRolesOrUsersSet>();
        for_roles->current_user = true;
    }
    else if (ParserKeyword{Keyword::FOR}.ignore(pos, expected))
    {
        ASTPtr for_roles_ast;
        ParserRolesOrUsersSet for_roles_p;
        for_roles_p.allowUsers().allowRoles().allowAll().allowCurrentUser();
        if (!for_roles_p.parse(pos, for_roles_ast, expected))
            return false;

        for_roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(for_roles_ast);
    }
    else
    {
        for_roles = make_intrusive<ASTRolesOrUsersSet>();
        for_roles->current_user = true;
    }

    bool with_implicit = false;
    bool final = false;

    while (!for_query)
    {
        if (!with_implicit && ParserKeyword{Keyword::WITH_IMPLICIT}.ignore(pos, expected))
        {
            with_implicit = true;
            continue;
        }

        if (!final && ParserKeyword{Keyword::FINAL}.ignore(pos, expected))
        {
            final = true;
            continue;
        }

        break;
    }

    /// The query goes last, it would read a trailing `FINAL` as its own.
    if (!for_query)
        parseForQuery(pos, expected, for_query);

    auto query = make_intrusive<ASTShowGrantsQuery>();
    query->for_roles = std::move(for_roles);
    query->with_implicit = with_implicit;
    query->final = final;
    if (for_query)
        query->set(query->for_query, for_query);
    node = query;

    return true;
}
}
