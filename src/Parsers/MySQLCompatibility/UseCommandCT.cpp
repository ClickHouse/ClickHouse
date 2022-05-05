#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/UseCommandCT.h>

#include <Parsers/ASTUseQuery.h>

namespace MySQLCompatibility
{

bool UseCommandCT::setup(String &)
{
    MySQLPtr db_node = TreePath({"useCommand", "identifier", "pureIdentifier"}).evaluate(_source);

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
}
