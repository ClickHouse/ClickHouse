#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWithAlias.h>

#include <map>

namespace DB
{
void ApplyWithGlobalVisitor::visit(ASTPtr & ast)
{
    if (ASTSelectWithUnionQuery * node_union = ast->as<ASTSelectWithUnionQuery>())
    {
        auto & first_select = node_union->list_of_selects->children[0]->as<ASTSelectQuery &>();
        ASTPtr with_expression_list = first_select.with();

        if (with_expression_list)
        {
            std::map<String, ASTPtr> exprs;
            for (auto & child : with_expression_list->children)
            {
                if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(child.get()))
                    exprs[ast_with_alias->alias] = child;
            }
            for (auto it = node_union->list_of_selects->children.begin() + 1; it != node_union->list_of_selects->children.end(); ++it)
            {
                auto & select = (*it)->as<ASTSelectQuery &>();
                auto with = select.with();
                if (with)
                {
                    std::set<String> current_names;
                    for (auto & child : with->children)
                    {
                        if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(child.get()))
                            current_names.insert(ast_with_alias->alias);
                    }
                    for (auto & with_alias : exprs)
                    {
                        if (!current_names.count(with_alias.first))
                            with->children.push_back(with_alias.second->clone());
                    }
                }
                else
                    select.setExpression(ASTSelectQuery::Expression::WITH, with_expression_list->clone());
            }
        }
    }

    for (auto & child : ast->children)
        visit(child);
}

}
