#include <Parsers/MySQLCompatibility/TreePath.h>

#include <Parsers/MySQLCompatibility/SelectQueryCT.h>
#include <Parsers/MySQLCompatibility/ExpressionCT.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTOrderByElement.h>

namespace MySQLCompatibility
{
bool SelectItemsListCT::setup()
{
	MySQLPtr select_item_list = _source;
	
	if (select_item_list == nullptr)
		return false;
	
	auto select_expr_path = TreePath({
			"selectItem",
			"expr",
		});
	
	for (const auto & child : select_item_list->children)
	{
		MySQLPtr expr_node = nullptr;
		if ((expr_node = select_expr_path.evaluate(child)) != nullptr)
		{
			ConvPtr expr = std::make_shared<ExpressionCT>(expr_node);
			if (!expr->setup())
			{
				expr = nullptr;
				continue; // TODO: maybe fail?
			}

			exprs.push_back(std::move(expr));
		}
	}

	return true;
}

void SelectItemsListCT::convert(CHPtr & ch_tree) const
{
	auto select_item_list = std::make_shared<DB::ASTExpressionList>();
	for (const auto & expr : exprs)
	{
		CHPtr expr_node = nullptr;
		expr->convert(expr_node);
		// auto identifier = std::make_shared<DB::ASTIdentifier>(c); // TODO: generic exprs here
		select_item_list->children.push_back(std::move(expr_node));
	}

	ch_tree = select_item_list;
}


bool SelectOrderByCT::setup()
{
	MySQLPtr order_list = _source;
	if (order_list == nullptr)
		return false;

	
	auto column_path = TreePath::columnPath();
	auto order_expr_path = TreePath({
			"orderExpression"	
		});
	auto direction_path = TreePath({
			"direction"
		});

	for (const auto & child : order_list->children)
	{
		MySQLPtr order_expr = nullptr;
		if ((order_expr = order_expr_path.evaluate(child)) != nullptr)
		{
			MySQLPtr column = nullptr;
			if ((column = column_path.evaluate(order_expr)) != nullptr && !column->terminals.empty())
			{
				args.push_back({column->terminals[0], DIRECTION::ASC});
				MySQLPtr direction = nullptr;
				if ((direction = direction_path.evaluate(child)) != nullptr && !direction->terminals.empty())
				{
					// TODO: write more pretty condition
					const String & direction_term = direction->terminals[0];
					if (direction_term[0] == 'd' || direction_term[0] == 'D')
						args.back().second = DIRECTION::DESC;
				}
			}
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
		auto identifier = std::make_shared<DB::ASTIdentifier>(elem.first); // TODO: generic exprs here
		order_node->direction = (elem.second == DIRECTION::ASC ? 1 : -1);
		order_node->children.push_back(std::move(identifier));
		order_by_list->children.push_back(std::move(order_node));
	}

	ch_tree = order_by_list;
}

bool SelectLimitLengthCT::setup()
{
	MySQLPtr limit_options = _source;
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

bool SelectLimitOffsetCT::setup()
{
	MySQLPtr limit_options = _source;
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

bool SelectTablesCT::setup()
{
	auto table_list = _source;
	if (table_list == nullptr)
		return false;

	tables = {};
	auto table_path = TreePath({
			"tableReference",
			"tableFactor",
			"singleTable",
			"tableRef",
			"qualifiedIdentifier"
		});

	for (const auto & child : table_list->children)
	{
		MySQLPtr table_and_db_node;
		if ((table_and_db_node = table_path.evaluate(child)) != nullptr)
		{
			auto identifier_path = TreePath({"pureIdentifier"});
			if (table_and_db_node->children.size() == 1)
			{
				const String & table_name = identifier_path.evaluate(
						table_and_db_node->children[0]
					)->terminals[0];
				tables.push_back({table_name, ""});
			} else
			{
				assert(table_and_db_node->children.size() == 2);
				const String & table_name = identifier_path.evaluate(
						table_and_db_node->children[1]
					)->terminals[0];
				const String & db_name = identifier_path.evaluate(
						table_and_db_node->children[0]
					)->terminals[0];

				tables.push_back({table_name, db_name});
			}
		}
	}

	return true;
}

void SelectTablesCT::convert(CHPtr & ch_tree) const
{
	auto table_list = std::make_shared<DB::ASTTablesInSelectQuery>();
	for (const auto & t : tables)
	{
		auto table_elem = std::make_shared<DB::ASTTablesInSelectQueryElement>();
		auto table_expr = std::make_shared<DB::ASTTableExpression>();

		CHPtr table_identifier = nullptr;
		if (t.database == "")
			table_identifier = std::make_shared<DB::ASTTableIdentifier>(t.table);
		else
			table_identifier = std::make_shared<DB::ASTTableIdentifier>(t.database, t.table);
		
		table_expr->database_and_table_name = std::move(table_identifier);
		table_expr->children.push_back(table_expr->database_and_table_name);
		
		table_elem->table_expression = std::move(table_expr);
		table_elem->children.push_back(table_elem->table_expression);

		table_list->children.push_back(std::move(table_elem));
	}

	ch_tree = table_list;
}

bool SelectQueryCT::setup()
{
	auto * logger = &Poco::Logger::get("AST");
	
	auto column_path = TreePath::columnPath();
	
	MySQLPtr query_expr = TreePath({
			"queryExpression"
		}).evaluate(_source);
		
	MySQLPtr query_expr_spec = TreePath({
			"queryExpressionBody",
			"querySpecification"
		}).evaluate(query_expr);

	if (query_expr_spec == nullptr)
		return false;
	
	// SELECT
	{
		MySQLPtr items_node = TreePath({
				"selectItemList"
			}).evaluate(query_expr_spec);

		select_items_ct = std::make_shared<SelectItemsListCT>(items_node);	
		if (!select_items_ct->setup())
		{
			select_items_ct = nullptr;
			return false;
		}
	}
	
	// FROM	
	{
		MySQLPtr table_list = TreePath({
				"fromClause",
				"tableReferenceList"
			}).evaluate(query_expr_spec);
		
		tables_ct = std::make_shared<SelectTablesCT>(table_list);
		if (!tables_ct->setup())
			tables_ct = nullptr;
	}

	// ORDER BY
	{
		MySQLPtr order_list = TreePath({
				"orderClause",
				"orderList"
			}).evaluate(query_expr);
		
		order_by_ct = std::make_shared<SelectOrderByCT>(order_list);
		if (!order_by_ct->setup())
			order_by_ct = nullptr;
	}

	// LIMIT
	{
		MySQLPtr limit_options = TreePath({
				"limitClause",
				"limitOptions"
			}).evaluate(query_expr);	

		limit_length_ct = std::make_shared<SelectLimitLengthCT>(limit_options);
		if (!limit_length_ct->setup())
			limit_length_ct = nullptr;

		limit_offset_ct = std::make_shared<SelectLimitOffsetCT>(limit_options);
		if (!limit_offset_ct->setup())
				limit_offset_ct = nullptr;	
	}		

	MySQLPtr where_clause = TreePath({
			"whereClause"
		}).evaluate(query_expr_spec);

	LOG_DEBUG(logger, "MySQL AST parsing succeded!");
	
	return true;
}

void SelectQueryCT::convert(CHPtr & ch_tree) const
{
	auto * logger = &Poco::Logger::get("AST");
	
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
		LOG_DEBUG(logger, "FROM!");
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
	
	select_list->children.push_back(std::move(select_node));
	select_union->list_of_selects = std::move(select_list);
	select_union->children.push_back(select_union->list_of_selects);
	
	ch_tree = select_union;

}
}
