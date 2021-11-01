#include <Interpreters/JoinWhereFilterVisitor.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// return identifiers only in there's no not allowed ones
static std::vector<ASTIdentifier *> getIdentifiersStrict(ASTPtr & ast)
{
    bool skip_all = false;
    auto ident_pred = [&skip_all](const ASTPtr & node) -> bool
    {
        if (const auto * func_node = node->as<ASTFunction>(); func_node && func_node->name == "arrayJoin")
            skip_all = true;

        if (const auto * ident = node->as<ASTIdentifier>(); ident && IdentifierSemantic::getColumnName(*ident)->empty())
            skip_all = true;

        return !skip_all;
    };

    auto identifiers = getIdentifiers(ast, ident_pred);
    if (skip_all)
        return {};
    return identifiers;
}

void JoinWhereFilterMatcher::visit(const ASTFunction & func, const ASTPtr & ast, Data & data)
{
    if (func.name == "and")
        return; /// go into children

    ASTPtr ast_clone = ast->clone();
    std::vector<ASTIdentifier *> idents = getIdentifiersStrict(ast_clone);

    for (auto * ident : idents)
    {
        auto t = getTableForIdentifier(ident, data.aliases);
        if (t == JoinIdentifierPos::Right)
        {
            /// ok
        }
        else if (t == JoinIdentifierPos::Left)
        {
            return;
        }
        else
        {
            return;
        }
    }

    data.filters.push_back(std::move(ast_clone));
}

}
