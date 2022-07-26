#include <Interpreters/AddAliasToAnonymousSubqueryVisitor.h>

#include <Parsers/ASTSubquery.h>

namespace DB
{

void AddAliasToAnonymousSubqueryVisitor::visit(IAST * ast)
{
    visitImpl(ast);

    static constexpr auto dummy_subquery_name_prefix = "_subquery";
    static std::atomic_uint64_t subquery_index = 0;

    for (auto * subquery : anonymous_subqueries)
    {
        String alias;
        do
        {
            alias = dummy_subquery_name_prefix + std::to_string(++subquery_index);
        } while (aliases.contains(alias));

        subquery->setAlias(alias);
    }
}

void AddAliasToAnonymousSubqueryVisitor::visitImpl(IAST * ast)
{
    if (!ast)
        return;

    String alias = ast->tryGetAlias();
    if (!alias.empty())
        aliases.emplace(std::move(alias));
    else if (ast->as<ASTSubquery>())
        anonymous_subqueries.push_back(ast);

    for (auto & child : ast->children)
        visit(child.get());
}

}
