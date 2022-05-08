#include <Parsers/MySQLCompatibility/DescribeCommandCT.h>
#include <Parsers/MySQLCompatibility/ShowQueryCT.h>
#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>

#include <Parsers/ASTShowTablesQuery.h>

namespace MySQLCompatibility
{

bool ShowTablesCT::setup(String & error)
{
    const MySQLPtr & show_statement_node = getSourceNode();

    MySQLPtr db_node = TreePath({"inDb"}).descend(show_statement_node);
    if (db_node != nullptr)
    {
        if (!tryExtractIdentifier(db_node, from))
        {
            error = "invalid identifier";
            return false;
        }
    }

    MySQLPtr like_node = TreePath({"likeClause"}).find(show_statement_node);
    if (like_node != nullptr)
    {
        assert(!like_node->children.empty());
        like = removeQuotes(like_node->children[0]->terminals[0]);
    }

    return true;
}
void ShowTablesCT::convert(CHPtr & ch_tree) const
{
    auto show_node = std::make_shared<DB::ASTShowTablesQuery>();

    if (from != "")
        show_node->from = from;

    if (like != "")
        show_node->like = like;

    ch_tree = show_node;
}

bool ShowColumnsCT::setup(String & error)
{
    const MySQLPtr & show_statement_node = getSourceNode();

    MySQLPtr table_node = TreePath({"tableRef"}).descend(show_statement_node);
    if (!tryExtractTableName(table_node, table_name, db_name))
    {
        error = "invalid table name";
        return false;
    }

    return true;
}

void ShowColumnsCT::convert(CHPtr & ch_tree) const
{
    makeDescribeCHNode(table_name, db_name, ch_tree);
}

// TODO: add full syntax, now only SHOW [MODE] TABLES workds
bool ShowQueryCT::setup(String & error)
{
    const MySQLPtr & show_statement_node = getSourceNode();

    if (show_statement_node->terminal_types.size() == 2)
    {
        switch (show_statement_node->terminal_types[1])
        {
            case MySQLTree::TOKEN_TYPE::TABLES_SYMBOL:
                specific_show_ct = std::make_shared<ShowTablesCT>(show_statement_node);
                break;
            default:
                error = "unsupported SHOW query";
                return false;
        }
    }
    else if (show_statement_node->terminal_types.size() == 3)
    {
        switch (show_statement_node->terminal_types[1])
        {
            case MySQLTree::TOKEN_TYPE::COLUMNS_SYMBOL:
                specific_show_ct = std::make_shared<ShowColumnsCT>(show_statement_node);
                break;
            default:
                error = "unsupported SHOW query";
                return false;
        }
    }

    if (specific_show_ct == nullptr || !specific_show_ct->setup(error))
    {
        error = "failed to process SHOW query";
        return false;
    }

    return true;
}

void ShowQueryCT::convert(CHPtr & ch_tree) const
{
    assert(specific_show_ct != nullptr);
    specific_show_ct->convert(ch_tree);
}

}
