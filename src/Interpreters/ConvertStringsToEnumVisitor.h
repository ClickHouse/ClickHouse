#pragma once

#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTExpressionList.h>
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
    String first_value = first->as<ASTLiteral>()->value.get<NearestFieldType<String>>();
    String second_value = second->as<ASTLiteral>()->value.get<NearestFieldType<String>>();

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

    for (const auto & item : array_to->as<ASTLiteral>()->value.get<NearestFieldType<Array>>())
        values.insert(item.get<NearestFieldType<String>>());
    values.insert(other->as<ASTLiteral>()->value.get<NearestFieldType<String>>());

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

struct FindUsedFunctionsMatcher
{
    using Visitor = ConstInDepthNodeVisitor<FindUsedFunctionsMatcher, true>;

    struct Data
    {
        const std::unordered_set<String> & names;
        std::unordered_set<String> & used_functions;
        std::vector<String> call_stack = {};
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !(node->as<ASTFunction>());
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>())
            visit(*func, data);
    }

    static void visit(const ASTFunction & func, Data & data)
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
};

using FindUsedFunctionsVisitor = FindUsedFunctionsMatcher::Visitor;

struct ConvertStringsToEnumMatcher
{
    struct Data
    {
        std::unordered_set<String> & used_functions;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !(node->as<ASTFunction>());
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>())
            visit(*func, data);
    }

    static void visit(ASTFunction & function_node, Data & data)
    {
        if (!function_node.arguments)
            return;

        /// We are not sure we could change the type of function result
        /// cause it is present in other fucntion as argument
        if (data.used_functions.count(function_node.tryGetAlias()))
            return;

        if (function_node.name == "if")
        {
            if (function_node.arguments->children.size() != 2)
                return;

            auto literal1 = function_node.arguments->children[1]->as<ASTLiteral>();
            auto literal2 = function_node.arguments->children[2]->as<ASTLiteral>();
            if (!literal1 || !literal2)
                return;

            if (String(literal1->value.getTypeName()) != "String" ||
                String(literal2->value.getTypeName()) != "String")
                return;

            changeIfArguments(function_node.arguments->children[1],
                              function_node.arguments->children[2]);
        }
        else if (function_node.name == "transform")
        {
            if (function_node.arguments->children.size() != 4)
                return;

            auto literal_to = function_node.arguments->children[2]->as<ASTLiteral>();
            auto literal_other = function_node.arguments->children[3]->as<ASTLiteral>();
            if (!literal_to || !literal_other)
                return;

            if (String(literal_to->value.getTypeName()) != "Array" ||
                String(literal_other->value.getTypeName()) != "String")
                return;

            Array array_to = literal_to->value.get<NearestFieldType<Array>>();
            if (array_to.size() == 0)
                return;

            bool to_strings = checkSameType(array_to, "String");
            if (!to_strings)
                return;

            changeTransformArguments(function_node.arguments->children[2],
                                     function_node.arguments->children[3]);
        }
    }
};

using ConvertStringsToEnumVisitor = InDepthNodeVisitor<ConvertStringsToEnumMatcher, true>;

}
