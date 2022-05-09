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
        return quoted;

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

bool tryExtractLiteral(MySQLPtr literal_node, DB::Field & field)
{
    assert(literal_node != nullptr && literal_node->rule_name == "literal");

    auto numeric_path = TreePath({"numLiteral"});
    auto text_path = TreePath({"textLiteral"});
    auto bool_path = TreePath({"boolLiteral"});
    auto null_path = TreePath({"nullLiteral"});

    MySQLPtr result = nullptr;
    if ((result = numeric_path.descend(literal_node)) != nullptr)
    {
        const MySQLPtr & numeric_node = result;
        switch (numeric_node->terminal_types[0])
        {
            case MySQLTree::TOKEN_TYPE::INT_NUMBER: {
                int val = std::stoi(numeric_node->terminals[0]);
                field = DB::Field(val);
                break;
            }
            case MySQLTree::TOKEN_TYPE::FLOAT_NUMBER:
            case MySQLTree::TOKEN_TYPE::DECIMAL_NUMBER: {
                double val = std::stod(numeric_node->terminals[0]);
                field = DB::Field(val);
                break;
            }
            default:
                return false;
        }
    }
    else if ((result = text_path.descend(literal_node)) != nullptr)
    {
        const MySQLPtr & text_literal = result;
        auto string_path = TreePath({"textStringLiteral"});

        String value = "";
        for (const auto & child : text_literal->children)
        {
            MySQLPtr text_string_node = nullptr;
            if ((text_string_node = string_path.find(child)) != nullptr)
            {
                value += removeQuotes(text_string_node->terminals[0]);
            }
        }

        field = DB::Field(value);
    }
    else if ((result = bool_path.descend(literal_node)) != nullptr)
    {
        bool value = (result->terminal_types[0] == MySQLTree::TOKEN_TYPE::TRUE_SYMBOL);
        field = DB::Field(value);
    }
    else if ((result = null_path.descend(literal_node)) != nullptr)
    {
        field = DB::Field();
    }
    else
        return false;


    return true;
}

}
