#include <Parsers/MySQLCompatibility/DescribeCommandCT.h>
#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

namespace MySQLCompatibility
{

// TODO: add full syntax
bool DescribeCommandCT::setup(String & error)
{
    const MySQLPtr describe_node = getSourceNode();

    MySQLPtr table_ident_node = TreePath({"tableRef", "qualifiedIdentifier"}).find(describe_node);

    if (!tryExtractTableName(table_ident_node, table_name, db_name))
    {
        error = "invalid table name";
        return false;
    }

    return true;
}

void makeDescribeCHNode(const String & table_name, const String & db_name, CHPtr & ch_tree)
{
    auto table_expr = std::make_shared<DB::ASTTableExpression>();

    CHPtr table_identifier = nullptr;
    if (db_name == "")
        table_identifier = std::make_shared<DB::ASTTableIdentifier>(table_name);
    else
        table_identifier = std::make_shared<DB::ASTTableIdentifier>(db_name, table_name);

    table_expr->database_and_table_name = std::move(table_identifier);
    table_expr->children.push_back(table_expr->database_and_table_name);

    auto query = std::make_shared<DB::ASTDescribeQuery>();
    query->table_expression = table_expr;

    ch_tree = query;
}

void DescribeCommandCT::convert(CHPtr & ch_tree) const
{
    makeDescribeCHNode(table_name, db_name, ch_tree);
}
}
