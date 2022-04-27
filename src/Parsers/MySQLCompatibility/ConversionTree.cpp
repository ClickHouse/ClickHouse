#include <Parsers/MySQLCompatibility/ConversionTree.h>
#include <Parsers/MySQLCompatibility/TreePath.h>

// SetQueryCT
#include <Common/SettingsChanges.h>
#include <Parsers/ASTSetQuery.h>

// SimpleSelectQuery
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTOrderByElement.h>

// UseCommand
#include <Parsers/ASTUseQuery.h>

namespace MySQLCompatibility
{

// TODO: multiple key-value pairs in SET query
bool SetQueryCT::setup()
{
	MySQLPtr key_node = TreePath({
			"internalVariableName",
			"pureIdentifier"
		}).evaluate(_source);
	
	if (key_node == nullptr)
		return false;
	
	std::string key = key_node->terminals[0];
	
	MySQLPtr value_node = TreePath({
			"setExprOrDefault",
			"textStringLiteral"
		}).evaluate(_source);
	
	if (value_node == nullptr)
		return false;
	
	std::string value = value_node->terminals[0];

	_key_value_list.push_back({key, value});

	return true;
}

void SetQueryCT::convert(CHPtr & ch_tree) const
{
	auto query = std::make_shared<DB::ASTSetQuery>();
	
	DB::SettingsChanges changes;
	for (const auto & key_value : _key_value_list)
	{
		changes.push_back(DB::SettingChange{});
	
		changes.back().name = key_value.first;
		changes.back().value = key_value.second;
	}
	query->is_standalone = false;
	query->changes = std::move(changes);

	ch_tree = query;
}

bool UseCommandCT::setup()
{
	MySQLPtr db_node = TreePath({
			"useCommand",
			"identifier",
			"pureIdentifier"
	}).evaluate(_source);

	if (db_node == nullptr || db_node->terminals.empty())
		return false;
	
	database = db_node->terminals[0];

	return true;
}

void UseCommandCT::convert(CHPtr & ch_tree) const
{
	auto * logger = &Poco::Logger::get("AST");
	LOG_DEBUG(logger, "USE");
	auto query = std::make_shared<DB::ASTUseQuery>();
	query->database = database;

	ch_tree = query;
}

bool SimpleSelectQueryCT::setup()
{
	auto * logger = &Poco::Logger::get("AST");
	
	// TODO: move this helper outside
	auto column_path = TreePath({
			"expr",
			"boolPri",
			"predicate",
			"bitExpr",
			"simpleExpr",
			"columnRef",
			"fieldIdentifier",
			"pureIdentifier"
		});
	
	MySQLPtr query_expr = TreePath({
			"queryExpression"
		}).evaluate(_source);
		
	MySQLPtr query_expr_spec = TreePath({
			"queryExpressionBody",
			"querySpecification"
		}).evaluate(query_expr);

	if (query_expr_spec == nullptr)
		return false;
	
	MySQLPtr order_list = TreePath({
			"orderClause",
			"orderList"
		}).evaluate(query_expr);
	
	if (order_list != nullptr)
	{

		LOG_DEBUG(logger, "has ORDER BY");	
		this->has_order_by = true;
		
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
				LOG_DEBUG(logger, "has order_expr");
				MySQLPtr column = nullptr;
				if ((column = column_path.evaluate(order_expr)) != nullptr && !column->terminals.empty())
				{
					order_by_args.push_back({column->terminals[0], ORDER_BY_DIR::ASC});
					MySQLPtr direction = nullptr;
					if ((direction = direction_path.evaluate(child)) != nullptr && !direction->terminals.empty())
					{
						// TODO: write more pretty condition
						const String & direction_term = direction->terminals[0];
						if (direction_term[0] == 'd' || direction_term[0] == 'D')
							order_by_args.back().second = ORDER_BY_DIR::DESC;
					}
				}
			}
		}
	}

	MySQLPtr select_item_list = TreePath({
			"selectItemList"
		}).evaluate(query_expr_spec);
	
	if (select_item_list == nullptr)
		return false;
	
	columns = {};
	auto select_column_path = TreePath({
			"selectItem",
		}).append(column_path);
	
	for (const auto & child : select_item_list->children)
	{
		MySQLPtr column_node;
		if ((column_node = select_column_path.evaluate(child)) != nullptr && !column_node->terminals.empty())
			columns.push_back(column_node->terminals[0]);
	}

	
	MySQLPtr from_clause = TreePath({
			"fromClause"
		}).evaluate(query_expr_spec);

	MySQLPtr table_list = TreePath({
			"tableReferenceList"
		}).evaluate(from_clause);
	
	if (table_list == nullptr)
	{
		// FIXME
		return false;
	}

	tables = {};
	auto table_path = TreePath({
			"tableReference",
			"tableFactor",
			"singleTable",
			"tableRef",
			"pureIdentifier"
		});

	for (const auto & child : table_list->children)
	{
		MySQLPtr table_node;
		if ((table_node = table_path.evaluate(child)) != nullptr && !table_node->terminals.empty())
			tables.push_back(table_node->terminals[0]);
	}

	MySQLPtr where_clause = TreePath({
			"whereClause"
		}).evaluate(query_expr_spec);

	LOG_DEBUG(logger, "MySQL AST parsing succeded!");
	
	return true;
}

void SimpleSelectQueryCT::convert(CHPtr & ch_tree) const
{
	auto select_union = std::make_shared<DB::ASTSelectWithUnionQuery>();
	auto select_list = std::make_shared<DB::ASTExpressionList>();
	auto select_node = std::make_shared<DB::ASTSelectQuery>();

	auto select_item_list = std::make_shared<DB::ASTExpressionList>();
	for (const auto & c : columns)
	{
		auto identifier = std::make_shared<DB::ASTIdentifier>(c); // TODO: generic exprs here
		select_item_list->children.push_back(std::move(identifier));
	}

	auto table_list = std::make_shared<DB::ASTTablesInSelectQuery>();
	for (const auto & t : tables)
	{
		auto table_elem = std::make_shared<DB::ASTTablesInSelectQueryElement>();
		auto table_expr = std::make_shared<DB::ASTTableExpression>();
		auto table_identifier = std::make_shared<DB::ASTTableIdentifier>(t);
		
		table_expr->database_and_table_name = std::move(table_identifier);
		table_expr->children.push_back(table_expr->database_and_table_name);
		
		table_elem->table_expression = std::move(table_expr);
		table_elem->children.push_back(table_elem->table_expression);

		table_list->children.push_back(std::move(table_elem));
	}

	if (this->has_order_by)
	{
		auto order_by_list = std::make_shared<DB::ASTExpressionList>();

		for (const auto & elem : order_by_args)
		{
			auto order_node = std::make_shared<DB::ASTOrderByElement>();
			auto identifier = std::make_shared<DB::ASTIdentifier>(elem.first); // TODO: generic exprs here
			order_node->direction = (elem.second == ORDER_BY_DIR::ASC ? 1 : -1);
			order_node->children.push_back(std::move(identifier));
			order_by_list->children.push_back(std::move(order_node));
		}

		select_node->setExpression(DB::ASTSelectQuery::Expression::ORDER_BY, std::move(order_by_list));
	}

	
	select_node->setExpression(DB::ASTSelectQuery::Expression::SELECT, std::move(select_item_list));
	select_node->setExpression(DB::ASTSelectQuery::Expression::TABLES, std::move(table_list));

	select_list->children.push_back(std::move(select_node));
	select_union->list_of_selects = std::move(select_list);
	select_union->children.push_back(select_union->list_of_selects);
	
	ch_tree = select_union;

}
}
