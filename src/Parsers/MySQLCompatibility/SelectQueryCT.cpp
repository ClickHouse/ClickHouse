#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>

#include <Parsers/MySQLCompatibility/ExpressionCT.h>
#include <Parsers/MySQLCompatibility/SelectQueryCT.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTSubquery.h>

namespace MySQLCompatibility
{
bool SelectItemsListCT::setup(String & error)
{
    MySQLPtr select_item_list = getSourceNode();

    if (select_item_list == nullptr)
        return false;

    auto select_expr_path = TreePath({"selectItem", "expr"});

    auto alias_path = TreePath({"selectItem", "selectAlias"});

    if (!select_item_list->terminal_types.empty())
        has_asterisk = select_item_list->terminal_types[0] == MySQLTree::TOKEN_TYPE::MULT_OPERATOR;

    for (const auto & child : select_item_list->children)
    {
        MySQLPtr expr_node = nullptr;
        if ((expr_node = select_expr_path.descend(child)) != nullptr)
        {
            ConvPtr expr = std::make_shared<ExpressionCT>(expr_node);
            if (!expr->setup(error))
            {
                expr = nullptr;
                return false;
            }

            MySQLPtr alias_node = alias_path.descend(child);

            String alias = "";
            if (alias_node != nullptr)
            {
                MySQLPtr ident_alias = TreePath({"identifier"}).descend(alias_node);
                MySQLPtr text_alias = TreePath({"textStringLiteral"}).descend(alias_node);

                if (ident_alias != nullptr)
                {
                    if (!tryExtractIdentifier(ident_alias, alias))
                    {
                        error = "invalid identifier";
                        return false;
                    }
                }
                else if (text_alias != nullptr)
                    alias = text_alias->terminals[0];
            }

            exprs.push_back({std::move(expr), alias});
        }
    }

    return true;
}

void SelectItemsListCT::convert(CHPtr & ch_tree) const
{
    auto select_item_list = std::make_shared<DB::ASTExpressionList>();
    if (has_asterisk)
    {
        auto asterisk = std::make_shared<DB::ASTAsterisk>();
        select_item_list->children.push_back(std::move(asterisk));
    }
    for (const auto & item : exprs)
    {
        CHPtr expr_node = nullptr;
        item.expr->convert(expr_node);

        if (item.alias != "")
        {
            auto * aliased_node = dynamic_cast<DB::ASTWithAlias *>(expr_node.get());
            if (aliased_node != nullptr)
                aliased_node->setAlias(item.alias);
        }

        select_item_list->children.push_back(std::move(expr_node));
    }

    ch_tree = select_item_list;
}


bool SelectOrderByCT::setup(String & error)
{
    MySQLPtr order_list = getSourceNode();
    if (order_list == nullptr)
        return false;


    auto order_expr_path = TreePath({"orderExpression"});
    for (const auto & child : order_list->children)
    {
        MySQLPtr order_expr = nullptr;
        if ((order_expr = order_expr_path.find(child)) != nullptr)
        {
            assert(order_expr->children.size() <= 2);
            assert(order_expr->children[0]->rule_name == "expr");

            ConvPtr order_elem_ct = std::make_shared<ExpressionCT>(order_expr->children[0]);
            if (!order_elem_ct->setup(error))
            {
                order_elem_ct = nullptr;
                return false;
            }

            args.push_back({std::move(order_elem_ct), MySQLTree::TOKEN_TYPE::ASC_SYMBOL});

            if (order_expr->children.size() == 2)
                args.back().direction = order_expr->children[1]->terminal_types[0];
        }
    }

    return true;
}

void SelectOrderByCT::convert(CHPtr & ch_tree) const
{
    auto order_by_list = std::make_shared<DB::ASTExpressionList>();

    for (const auto & elem : args)
    {
        auto order_node = std::make_shared<DB::ASTOrderByElement>();
        CHPtr expr = nullptr;
        elem.expr->convert(expr);
        order_node->direction = (elem.direction == MySQLTree::TOKEN_TYPE::ASC_SYMBOL ? 1 : -1);
        order_node->children.push_back(std::move(expr));
        order_by_list->children.push_back(std::move(order_node));
    }

    ch_tree = order_by_list;
}

bool SelectLimitLengthCT::setup(String &)
{
    MySQLPtr limit_options = getSourceNode();
    if (limit_options == nullptr)
        return false;

    if (limit_options->terminals.empty())
    {
        length = std::stoi(limit_options->children[0]->terminals[0]);
        return true;
    }

    assert(limit_options->children.size() == 2);
    const String limit_type = limit_options->terminals[0];

    int first_arg = std::stoi(limit_options->children[0]->terminals[0]);
    int second_arg = std::stoi(limit_options->children[1]->terminals[0]);

    // FIXME: hacky
    if (limit_type[0] == ',')
        length = second_arg;
    else
        length = first_arg;

    return true;
}

void SelectLimitLengthCT::convert(CHPtr & ch_tree) const
{
    auto limit_length = std::make_shared<DB::ASTLiteral>(length);
    ch_tree = limit_length;
}

bool SelectLimitOffsetCT::setup(String &)
{
    MySQLPtr limit_options = getSourceNode();
    if (limit_options == nullptr)
        return false;

    if (limit_options->terminals.empty())
        return false;

    assert(limit_options->children.size() == 2);
    const String limit_type = limit_options->terminals[0];

    int first_arg = std::stoi(limit_options->children[0]->terminals[0]);
    int second_arg = std::stoi(limit_options->children[1]->terminals[0]);

    // FIXME: hacky
    if (limit_type[0] == ',')
        offset = first_arg;
    else
        offset = second_arg;

    return true;
}

void SelectLimitOffsetCT::convert(CHPtr & ch_tree) const
{
    auto limit_offset = std::make_shared<DB::ASTLiteral>(offset);
    ch_tree = limit_offset;
}

bool SelectTableCT::setup(String & error)
{
    const MySQLPtr & table_node = getSourceNode();
    auto table_path = TreePath({"singleTable", "tableRef", "qualifiedIdentifier"});
    MySQLPtr table_id;
    if ((table_id = table_path.find(table_node)) == nullptr)
    {
        error = "incorrect table";
        return false;
    }
    if (!tryExtractTableName(table_id, table, database))
    {
        error = "invalid table name";
        return false;
    }
    return true;
}

void SelectTableCT::convert(CHPtr & ch_tree) const
{
    auto table_expr = std::make_shared<DB::ASTTableExpression>();
    CHPtr table_identifier = nullptr;
    if (database == "")
        table_identifier = std::make_shared<DB::ASTTableIdentifier>(table);
    else
        table_identifier = std::make_shared<DB::ASTTableIdentifier>(database, table);

    table_expr->database_and_table_name = std::move(table_identifier);
    table_expr->children.push_back(table_expr->database_and_table_name);

    ch_tree = table_expr;
}

bool SelectSubqueryCT::setup(String & error)
{
    const MySQLPtr & subquery_node = getSourceNode();
    subquery_ct = std::make_shared<SelectQueryExprCT>(subquery_node);
    if (!subquery_ct->setup(error))
        return false;

    return true;
}

void SelectSubqueryCT::convert(CHPtr & ch_tree) const
{
    CHPtr select_query = nullptr;
    subquery_ct->convert(select_query);
    
    auto subquery = std::make_shared<DB::ASTSubquery>();
    subquery->children.push_back(std::move(select_query));

    auto table_expr = std::make_shared<DB::ASTTableExpression>();
    table_expr->subquery = std::move(subquery);
    table_expr->children.push_back(table_expr->subquery);

    ch_tree = table_expr;
}

bool SelectFromCT::setup(String & error)
{
    auto item_list_node = getSourceNode();
    if (item_list_node == nullptr)
        return false;

    items = {};

    auto expr_path = TreePath({"tableReference", "tableFactor"});
    auto table_path = TreePath({"singleTable"});
    auto subquery_path = TreePath({"derivedTable", "subquery", "queryExpression"});

    for (const auto & child : item_list_node->children)
    {
        MySQLPtr expr_node;
        if ((expr_node = expr_path.find(child)) != nullptr)
        {
            MySQLPtr result_node = nullptr;
            if ((result_node = table_path.descend(expr_node)) != nullptr)
            {
                ConvPtr table_ct = std::make_shared<SelectTableCT>(result_node);
                if (!table_ct->setup(error))
                    return false;

                items.push_back(std::move(table_ct));
            } else if ((result_node = subquery_path.find(expr_node)) != nullptr)
            {
                ConvPtr subquery_ct = std::make_shared<SelectSubqueryCT>(result_node);
                if (!subquery_ct->setup(error))
                    return false;

                items.push_back(std::move(subquery_ct));
            }
        }
    }

    return true;
}

void SelectFromCT::convert(CHPtr & ch_tree) const
{
    auto item_list = std::make_shared<DB::ASTTablesInSelectQuery>();
    for (const auto & t : items)
    {
        assert(t != nullptr);
        CHPtr table_expr;
        t->convert(table_expr);
        
        auto table_elem = std::make_shared<DB::ASTTablesInSelectQueryElement>();
        table_elem->table_expression = std::move(table_expr);
        table_elem->children.push_back(table_elem->table_expression);

        item_list->children.push_back(std::move(table_elem));
    }

    ch_tree = item_list;
}

bool SelectGroupByCT::setup(String & error)
{
    MySQLPtr group_clause = TreePath({"groupByClause"}).find(getSourceNode());

    if (group_clause == nullptr)
        return false;

    auto expr_path = TreePath({"groupingExpression", "expr"});

    for (const auto & child : group_clause->children)
    {
        MySQLPtr expr = nullptr;
        if ((expr = expr_path.find(child)) != nullptr)
        {
            ConvPtr group_elem = std::make_shared<ExpressionCT>(expr);
            if (!group_elem->setup(error))
            {
                group_elem = nullptr;
                return false;
            }
            args.push_back(std::move(group_elem));
        }
    }

    return true;
}

void SelectGroupByCT::convert(CHPtr & ch_tree) const
{
    auto expr_list = std::make_shared<DB::ASTExpressionList>();

    for (const auto & expr : args)
    {
        CHPtr node = nullptr;
        expr->convert(node);
        expr_list->children.push_back(node);
    }

    ch_tree = expr_list;
}

bool SelectQueryExprCT::setup(String & error)
{
    auto column_path = TreePath::columnPath();

    MySQLPtr query_expr = getSourceNode();

    MySQLPtr query_expr_spec = TreePath({"queryExpressionBody", "querySpecification"}).find(query_expr);

    if (query_expr_spec == nullptr)
        return false;

    // SELECT
    {
        MySQLPtr items_node = TreePath({"selectItemList"}).find(query_expr_spec);

        select_items_ct = std::make_shared<SelectItemsListCT>(items_node);
        if (!select_items_ct->setup(error))
        {
            select_items_ct = nullptr;
            return false;
        }
    }

    // FROM
    {
        MySQLPtr table_list = TreePath({"fromClause", "tableReferenceList"}).find(query_expr_spec);

        if (table_list != nullptr)
        {
            tables_ct = std::make_shared<SelectFromCT>(table_list);
            if (!tables_ct->setup(error))
            {
                tables_ct = nullptr;
                return false;
            }
        }
    }

    // ORDER BY
    {
        MySQLPtr order_list = TreePath({"orderClause", "orderList"}).find(query_expr);

        if (order_list != nullptr)
        {
            order_by_ct = std::make_shared<SelectOrderByCT>(order_list);
            if (!order_by_ct->setup(error))
            {
                order_by_ct = nullptr;
                return false;
            }
        }
    }

    // LIMIT
    {
        MySQLPtr limit_options = TreePath({"limitClause", "limitOptions"}).find(query_expr);

        if (limit_options != nullptr)
        {
            assert(limit_options->children.size() > 0 && limit_options->children.size() <= 2);
            limit_length_ct = std::make_shared<SelectLimitLengthCT>(limit_options);
            if (!limit_length_ct->setup(error))
            {
                limit_length_ct = nullptr;
                return false;
            }

            if (limit_options->children.size() == 2)
            {
                limit_offset_ct = std::make_shared<SelectLimitOffsetCT>(limit_options);
                if (!limit_offset_ct->setup(error))
                {
                    limit_offset_ct = nullptr;
                    return false;
                }
            }
        }
    }

    // WHERE
    {
        MySQLPtr where_clause = TreePath({"whereClause"}).find(query_expr_spec);

        if (where_clause != nullptr)
        {
            assert(!where_clause->children.empty());
            where_ct = std::make_shared<ExpressionCT>(where_clause->children[0]);
            if (!where_ct->setup(error))
            {
                where_ct = nullptr;
                return false;
            }
        }
    }

    // GROUP BY
    {
        MySQLPtr group_by_clause = TreePath({"groupByClause"}).find(query_expr_spec);

        if (group_by_clause != nullptr)
        {
            group_by_ct = std::make_shared<SelectGroupByCT>(group_by_clause);
            if (!group_by_ct->setup(error))
            {
                group_by_ct = nullptr;
                return false;
            }
        }
    }

    // HAVING
    {
        // TODO: if we don't have groupby, do we need this?
        MySQLPtr having_clause = TreePath({"havingClause"}).find(query_expr_spec);

        if (having_clause != nullptr)
        {
            assert(!having_clause->children.empty());
            having_ct = std::make_shared<ExpressionCT>(having_clause->children[0]);
            if (!having_ct->setup(error))
            {
                having_ct = nullptr;
                return false;
            }
        }
    }

    return true;
}

void SelectQueryExprCT::convert(CHPtr & ch_tree) const
{
    auto select_union = std::make_shared<DB::ASTSelectWithUnionQuery>();
    auto select_list = std::make_shared<DB::ASTExpressionList>();
    auto select_node = std::make_shared<DB::ASTSelectQuery>();

    // SELECT
    {
        CHPtr select_items_list = nullptr;
        select_items_ct->convert(select_items_list);
        select_node->setExpression(DB::ASTSelectQuery::Expression::SELECT, std::move(select_items_list));
    }

    // FROM
    if (tables_ct != nullptr)
    {
        CHPtr table_list;
        tables_ct->convert(table_list);
        select_node->setExpression(DB::ASTSelectQuery::Expression::TABLES, std::move(table_list));
    }

    // ORDER BY
    if (order_by_ct != nullptr)
    {
        CHPtr order_by_list = nullptr;
        order_by_ct->convert(order_by_list);
        select_node->setExpression(DB::ASTSelectQuery::Expression::ORDER_BY, std::move(order_by_list));
    }

    // LIMIT
    {
        if (limit_length_ct != nullptr)
        {
            CHPtr limit_length = nullptr;
            limit_length_ct->convert(limit_length);
            select_node->setExpression(DB::ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));
        }

        if (limit_offset_ct != nullptr)
        {
            CHPtr limit_offset = nullptr;
            limit_offset_ct->convert(limit_offset);
            select_node->setExpression(DB::ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(limit_offset));
        }
    }

    // WHERE
    if (where_ct != nullptr)
    {
        CHPtr where_node = nullptr;
        where_ct->convert(where_node);
        select_node->setExpression(DB::ASTSelectQuery::Expression::WHERE, std::move(where_node));
    }

    // GROUP BY
    if (group_by_ct != nullptr)
    {
        CHPtr group_by_node = nullptr;
        group_by_ct->convert(group_by_node);
        select_node->setExpression(DB::ASTSelectQuery::Expression::GROUP_BY, std::move(group_by_node));
    }

    // HAVING
    if (having_ct != nullptr)
    {
        CHPtr having_node = nullptr;
        having_ct->convert(having_node);
        select_node->setExpression(DB::ASTSelectQuery::Expression::HAVING, std::move(having_node));
    }

    select_list->children.push_back(std::move(select_node));
    select_union->list_of_selects = std::move(select_list);
    select_union->children.push_back(select_union->list_of_selects);

    ch_tree = select_union;
}

bool SelectQueryCT::setup(String & error)
{
    const MySQLPtr & statement_node = getSourceNode();
    MySQLPtr expr_node = TreePath({"queryExpression"}).find(statement_node);
    if (expr_node == nullptr)
    {
        error = "invalid SELECT query";
        return false;
    }

    expr_ct = std::make_shared<SelectQueryExprCT>(expr_node);
    if (!expr_ct->setup(error))
        return false;
    return true;
}

void SelectQueryCT::convert(CHPtr & ch_tree) const
{
    assert(expr_ct);
    expr_ct->convert(ch_tree);
}

}
