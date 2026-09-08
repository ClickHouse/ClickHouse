#include <Interpreters/ApplyWithAliasVisitor.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/checkStackSize.h>


namespace DB
{

void ApplyWithAliasVisitor::visit(ASTPtr & ast, size_t max_expanded_ast_elements)
{
    visit(ast, Data{.exprs = {}, .max_expanded_ast_elements = max_expanded_ast_elements});

    /// The per-select checks bound each subtree as it is expanded, but a select is checked before its own
    /// descendants are expanded, so many small siblings can each stay under the limit while the tree they
    /// belong to ends up over it. `max_expanded_ast_elements` is documented as a bound on the whole
    /// expanded tree, so check the result once here as well.
    if (max_expanded_ast_elements)
        ast->checkSize(max_expanded_ast_elements);
}

void ApplyWithAliasVisitor::visit(ASTPtr & ast, const Data & data)
{
    checkStackSize();

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
                if (!current_names.contains(with_alias.first))
                {
                    /// Check the alias before cloning it: an alias that already grew past the limit at an
                    /// outer level must not be materialised again here.
                    if (data.max_expanded_ast_elements)
                        with_alias.second->checkSize(data.max_expanded_ast_elements);
                    with->children.push_back(with_alias.second->clone());
                }
            }
        }
        else if (!data.exprs.empty())
        {
            auto with_expression_list = make_intrusive<ASTExpressionList>();
            for (const auto & with_alias : data.exprs)
            {
                if (data.max_expanded_ast_elements)
                    with_alias.second->checkSize(data.max_expanded_ast_elements);
                with_expression_list->children.push_back(with_alias.second->clone());
            }
            node_select->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list));
        }

        if (data.max_expanded_ast_elements)
            node_select->checkSize(data.max_expanded_ast_elements);
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
