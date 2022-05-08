#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>


namespace MySQLCompatibility
{

Poco::Logger * getLogger()
{
    auto * logger = &Poco::Logger::get("AST");
    return logger;
}

// TODO: maybe move to Parser overlay and make inplace for all terminals?
String removeQuotes(const String & quoted)
{
    if (quoted.size() < 2)
        return "";

    // TODO: other quotes
    if (!(quoted.front() == '\'' && quoted.back() == '\'') && !(quoted.front() == '"' && quoted.back() == '"')
        && !(quoted.front() == '`' && quoted.back() == '`'))
        return quoted;

    return quoted.substr(1, quoted.size() - 2);
}

bool tryExtractTableName(MySQLPtr node, String & table_name, String & db_name)
{
    MySQLPtr qual_ident_node = TreePath({"qualifiedIdentifier"}).find(node);

    if (qual_ident_node == nullptr)
        return false;

    if (qual_ident_node->children.size() == 1)
        return tryExtractIdentifier(qual_ident_node->children[0], table_name);
    else if (qual_ident_node->children.size() == 2)
        return tryExtractIdentifier(qual_ident_node->children[0], db_name)
            && tryExtractIdentifier(qual_ident_node->children[1], table_name);

    assert(0 && "qualified identifer must have 1 or 2 children");
    return false;
}

bool tryExtractIdentifier(MySQLPtr node, String & value)
{
    assert(node != nullptr);

    MySQLPtr pure_identifier_node = TreePath({"pureIdentifier"}).find(node);
    if (pure_identifier_node != nullptr)
    {
        value = removeQuotes(pure_identifier_node->terminals[0]);
        return true;
    }

    MySQLPtr unambiguous_keyword_node = TreePath({"identifierKeywordsUnambiguous"}).find(node);
    if (unambiguous_keyword_node != nullptr)
    {
        value = removeQuotes(unambiguous_keyword_node->terminals[0]);
        return true;
    }
    return false;
}

}
