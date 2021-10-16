#include <Interpreters/ConvertStringsToEnumVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace
{

/// @note We place strings in ascending order here under the assumption it colud speed up String to Enum conversion.
String makeStringsEnum(const std::set<String> & values)
{
    String enum_string = "Enum8(";
    if (values.size() >= 255)
        enum_string = "Enum16(";

    size_t number = 1;
    for (const auto & item : values)
    {
        enum_string += "\'" + item + "\' = " + std::to_string(number++);

        if (number <= values.size())
            enum_string += ", ";
    }

    enum_string += ")";
    return enum_string;
}

void changeIfArguments(ASTPtr & first, ASTPtr & second)
{
    String first_value = first->as<ASTLiteral>()->value.get<String>();
    String second_value = second->as<ASTLiteral>()->value.get<String>();

    std::set<String> values;
    values.insert(first_value);
    values.insert(second_value);

    String enum_string = makeStringsEnum(values);
    auto enum_literal = std::make_shared<ASTLiteral>(enum_string);

    auto first_cast = makeASTFunction("CAST");
    first_cast->arguments->children.push_back(first);
    first_cast->arguments->children.push_back(enum_literal);

    auto second_cast = makeASTFunction("CAST");
    second_cast->arguments->children.push_back(second);
    second_cast->arguments->children.push_back(enum_literal);

    first = first_cast;
    second = second_cast;
}

void changeTransformArguments(ASTPtr & array_to, ASTPtr & other)
{
    std::set<String> values;

    for (const auto & item : array_to->as<ASTLiteral>()->value.get<Array>())
        values.insert(item.get<String>());
    values.insert(other->as<ASTLiteral>()->value.get<String>());

    String enum_string = makeStringsEnum(values);

    auto array_cast = makeASTFunction("CAST");
    array_cast->arguments->children.push_back(array_to);
    array_cast->arguments->children.push_back(std::make_shared<ASTLiteral>("Array(" + enum_string + ")"));
    array_to = array_cast;

    auto other_cast = makeASTFunction("CAST");
    other_cast->arguments->children.push_back(other);
    other_cast->arguments->children.push_back(std::make_shared<ASTLiteral>(enum_string));
    other = other_cast;
}

bool checkSameType(const Array & array, const String & type)
{
    for (const auto & item : array)
        if (item.getTypeName() != type)
            return false;
    return true;
}

}


bool FindUsedFunctionsMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    return !(node->as<ASTFunction>());
}

void FindUsedFunctionsMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
        visit(*func, data);
}

void FindUsedFunctionsMatcher::visit(const ASTFunction & func, Data & data)
{
    if (data.names.count(func.name) && !data.call_stack.empty())
    {
        String alias = func.tryGetAlias();
        if (!alias.empty())
        {
            data.used_functions.insert(alias);
        }
    }

    data.call_stack.push_back(func.name);

    /// Visit children with known call stack
    Visitor(data).visit(func.arguments);

    data.call_stack.pop_back();
}


bool ConvertStringsToEnumMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    return !(node->as<ASTFunction>());
}

void ConvertStringsToEnumMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
        visit(*func, data);
}

void ConvertStringsToEnumMatcher::visit(ASTFunction & function_node, Data & data)
{
    if (!function_node.arguments)
        return;

    /// We are not sure we could change the type of function result
    /// cause it is present in other function as argument
    if (data.used_functions.count(function_node.tryGetAlias()))
        return;

    if (function_node.name == "if")
    {
        if (function_node.arguments->children.size() != 2)
            return;

        const ASTLiteral * literal1 = function_node.arguments->children[1]->as<ASTLiteral>();
        const ASTLiteral * literal2 = function_node.arguments->children[2]->as<ASTLiteral>();
        if (!literal1 || !literal2)
            return;

        if (literal1->value.getTypeName() != std::string_view{"String"}
            || literal2->value.getTypeName() != std::string_view{"String"})
            return;

        changeIfArguments(function_node.arguments->children[1],
                            function_node.arguments->children[2]);
    }
    else if (function_node.name == "transform")
    {
        if (function_node.arguments->children.size() != 4)
            return;

        const ASTLiteral * literal_to = function_node.arguments->children[2]->as<ASTLiteral>();
        const ASTLiteral * literal_other = function_node.arguments->children[3]->as<ASTLiteral>();
        if (!literal_to || !literal_other)
            return;

        if (literal_to->value.getTypeName() != std::string_view{"Array"}
            || literal_other->value.getTypeName() != std::string_view{"String"})
            return;

        Array array_to = literal_to->value.get<Array>();
        if (array_to.empty())
            return;

        bool to_strings = checkSameType(array_to, "String");
        if (!to_strings)
            return;

        changeTransformArguments(function_node.arguments->children[2], function_node.arguments->children[3]);
    }
}

}

