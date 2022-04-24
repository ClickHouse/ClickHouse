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
	
	// CHPtr ch_key = std::make_shared<DB::ASTIdentifier>(key_value.first);
	// DB::tryGetIdentifierNameInto(ch_key, changes.back().name);
		changes.back().name = key_value.first;
		changes.back().value = key_value.second;
	}
	query->is_standalone = false;
	query->changes = std::move(changes);

	ch_tree = query;
}

bool SimpleSelectQueryCT::setup()
{
	MySQLPtr query_expr_spec = TreePath({
			"queryExpression",
			"queryExpressionBody",
			"querySpecification"
		}).evaluate(_source);
	
	if (query_expr_spec == nullptr)
		return false;
	
	MySQLPtr select_item_list = TreePath({
			"selectItemList"
		}).evaluate(query_expr_spec);
	
	if (select_item_list == nullptr)
		return false;
	
	columns = {};
	auto column_path = TreePath({
			"selectItem",
			"expr",
			"boolPri",
			"predicate",
			"bitExpr",
			"simpleExpr",
			"columnRef",
			"fieldIdentifier",
			"pureIdentifier"
		});
	
	for (const auto & child : select_item_list->children)
	{
		MySQLPtr column_node;
		if ((column_node = column_path.evaluate(child)) != nullptr && !column_node->terminals.empty())
			columns.push_back(column_node->terminals[0]);
	}

	for (const auto & c : columns)
		LOG_DEBUG(&Poco::Logger::get("AST"), "got column {}", c);
	
	MySQLPtr from_clause = TreePath({
			"fromClause"
		}).evaluate(query_expr_spec);

	MySQLPtr table_list = TreePath({
			"tableReferenceList"
		}).evaluate(from_clause);
	
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

	for (const auto & c : tables)
		LOG_DEBUG(&Poco::Logger::get("AST"), "got table {}", c);


	return true;
}

void SimpleSelectQueryCT::convert(CHPtr & ch_tree) const
{
	auto select_item_list = std::make_shared<DB::ASTExpressionList>();
	for (const auto & c : columns)
	{
		CHPtr identifier = std::make_shared<DB::ASTIdentifier>(c);
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
	
	auto select_node = std::make_shared<DB::ASTSelectQuery>();
	auto select_list = std::make_shared<DB::ASTExpressionList>();

	select_node->setExpression(DB::ASTSelectQuery::Expression::SELECT, std::move(select_item_list));
	select_node->setExpression(DB::ASTSelectQuery::Expression::TABLES, std::move(table_list));

	auto select_union = std::make_shared<DB::ASTSelectWithUnionQuery>();
	select_list->children.push_back(std::move(select_node));
	select_union->list_of_selects = std::move(select_list);
	select_union->children.push_back(select_union->list_of_selects);
	
	ch_tree = select_union;

}
}
