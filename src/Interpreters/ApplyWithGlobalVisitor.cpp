#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/checkStackSize.h>


namespace DB
{

/// Build: ASTTablesInSelectQuery → ASTTablesInSelectQueryElement → ASTTableExpression → ASTSubquery(inner_union)
static ASTPtr buildFromSubquery(ASTPtr inner_union)
{
    auto subquery = make_intrusive<ASTSubquery>(std::move(inner_union));

    auto table_expression = make_intrusive<ASTTableExpression>();
    table_expression->subquery = subquery;
    table_expression->children.push_back(table_expression->subquery);

    auto table_element = make_intrusive<ASTTablesInSelectQueryElement>();
    table_element->table_expression = table_expression;
    table_element->children.push_back(table_element->table_expression);

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    tables->children.push_back(std::move(table_element));

    return tables;
}

void ApplyWithGlobalVisitor::visit(ASTPtr & ast)
{
    checkStackSize();

    if (auto * node_union = ast->as<ASTSelectWithUnionQuery>())
    {
        if (node_union->list_of_selects->children.size() > 1)
        {
            if (auto * first_select = node_union->list_of_selects->children[0]->as<ASTSelectQuery>())
            {
                ASTPtr with_expression_list = first_select->with();
                if (with_expression_list)
                {
                    /// Remove WITH from first SELECT — it moves to the wrapper.
                    first_select->setExpression(ASTSelectQuery::Expression::WITH, nullptr);

                    /// Move original children into a subquery for the wrapper's FROM clause.
                    auto inner_union = make_intrusive<ASTSelectWithUnionQuery>();
                    std::swap(inner_union->list_of_selects, node_union->list_of_selects);
                    inner_union->children.push_back(inner_union->list_of_selects);
                    inner_union->union_mode = node_union->union_mode;
                    inner_union->list_of_modes = std::move(node_union->list_of_modes);
                    inner_union->set_of_modes = std::move(node_union->set_of_modes);
                    inner_union->is_normalized = node_union->is_normalized;

                    /// Build wrapper: WITH ... SELECT * FROM (inner_union)
                    auto wrapper = make_intrusive<ASTSelectQuery>();
                    wrapper->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list));

                    auto select_list = make_intrusive<ASTExpressionList>();
                    select_list->children.push_back(make_intrusive<ASTAsterisk>());
                    wrapper->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));

                    wrapper->setExpression(ASTSelectQuery::Expression::TABLES, buildFromSubquery(std::move(inner_union)));

                    /// Mutate the original node in place: replace its children with
                    /// the single wrapper SELECT. This preserves node identity so
                    /// parent named fields (e.g. ASTExplainQuery::query) stay valid,
                    /// and keeps FORMAT / INTO OUTFILE on the ASTQueryWithOutput node.
                    node_union->list_of_selects = make_intrusive<ASTExpressionList>();
                    node_union->list_of_selects->children.push_back(std::move(wrapper));
                    node_union->children.clear();
                    node_union->children.push_back(node_union->list_of_selects);
                    node_union->union_mode = SelectUnionMode::UNION_ALL;
                    node_union->list_of_modes.clear();
                    node_union->set_of_modes.clear();
                    node_union->is_normalized = true;
                }
            }
        }
    }

    /*
        * We need to visit all children recursively because the WITH statement may appear in the subquery at the nested level.
        * Behavior of `WITH ... UNION ALL ...` should be the same at the top level and inside the subquery.
        *
        * For example:
        * SELECT * FROM (WITH (SELECT ... ) AS t SELECT ... UNION ALL SELECT ...)
        *                ^^^^^^^^^^^^     should be visited     ^^^^^^^^^^^^^^^^
        * or inside `WHERE .. IN` clause:
        * SELECT * FROM ... WHERE x IN (WITH (SELECT ... ) AS t SELECT ... UNION ALL SELECT ...)
        */
    for (auto & child : ast->children)
        visit(child);
}

}
