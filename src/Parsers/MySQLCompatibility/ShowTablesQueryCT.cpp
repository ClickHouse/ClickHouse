#include <Parsers/MySQLCompatibility/ShowTablesQueryCT.h>
#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>

#include <Parsers/ASTShowTablesQuery.h>

namespace MySQLCompatibility
{

// TODO: add full syntax, now only SHOW [MODE] TABLES workds
bool ShowTablesQueryCT::setup(String & error)
{
    const MySQLPtr & show_statement_node = getSourceNode();

    if (show_statement_node->terminals.size() != 2)
    {
        error = "now SHOW query works only for tables";
        return false;
    }

    if (show_statement_node->terminal_types[1] != MySQLTree::TOKEN_TYPE::TABLES_SYMBOL)
    {
        error = "now SHOW query works only for tables";
        return false;
    }


    return true;
}

void ShowTablesQueryCT::convert(CHPtr & ch_tree) const
{
    auto show_node = std::make_shared<DB::ASTShowTablesQuery>();
    ch_tree = show_node;
}
}
