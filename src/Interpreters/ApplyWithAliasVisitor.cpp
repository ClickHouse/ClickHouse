#include <Interpreters/ApplyWithAliasVisitor.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{
void ApplyWithAliasVisitor::visit(ASTPtr & ast, const Data & data)
{
    if (auto * node_select = ast->as<ASTSelectQuery>())
    {
        std::optional<Data> new_data;
        if (auto with = node_select->with())
        {
            std::set<String> current_names;
            for (auto & child : with->children)
            {
                visit(child, new_data ? *new_data : data);
                if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(child.get()))
                {
                    if (!new_data)
                        new_data = data;
                    new_data->exprs[ast_with_alias->alias] = child;
                    current_names.insert(ast_with_alias->alias);
                }
            }
            for (const auto & with_alias : data.exprs)
            {
                if (!current_names.count(with_alias.first))
                    with->children.push_back(with_alias.second->clone());
            }
        }
        else if (!data.exprs.empty())
        {
            auto with_expression_list = std::make_shared<ASTExpressionList>();
            for (const auto & with_alias : data.exprs)
                with_expression_list->children.push_back(with_alias.second->clone());
            node_select->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list));
        }
        for (auto & child : node_select->children)
        {
            if (child != node_select->with())
                visit(child, new_data ? *new_data : data);
        }
    }
    else
        for (auto & child : ast->children)
            visit(child, data);
}

}
