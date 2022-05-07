#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>


namespace MySQLCompatibility
{

Poco::Logger * getLogger()
{
    auto * logger = &Poco::Logger::get("AST");
    return logger;
}

String removeQuotes(const String & quoted)
{
    if (quoted.size() < 2)
        return "";

    LOG_DEBUG(getLogger(), "got quoted? {} {} {}", quoted, quoted.front(), quoted.back());

    // TODO: other quotes
    if (!(quoted.front() == '\'' && quoted.back() == '\'') && !(quoted.front() == '"' && quoted.back() == '"')
        && !(quoted.front() == '`' && quoted.back() == '`'))
        return quoted;

    return quoted.substr(1, quoted.size() - 2);
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
