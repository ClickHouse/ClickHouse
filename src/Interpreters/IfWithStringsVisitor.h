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

void transformTwoStringsIntoEnum(ASTFunction & function_if_node)
{
    String first_literal = function_if_node.arguments->children[1]->as<ASTLiteral>()->value.get<NearestFieldType<String>>();
    String second_literal = function_if_node.arguments->children[2]->as<ASTLiteral>()->value.get<NearestFieldType<String>>();
    String enum_result;
    if (first_literal.compare(second_literal) < 0)
    {
        enum_result = "Enum8(\'" + first_literal + "\' = 1, \'" + second_literal + "\' = 2)";
    }
    else
    {
        enum_result = "Enum8(\'" + second_literal + "\' = 1, \'" + first_literal + "\' = 2)";
    }

    auto enum_arg = std::make_shared<ASTLiteral>(enum_result);

    auto first_cast = makeASTFunction("CAST");
    first_cast->arguments->children.push_back(function_if_node.arguments->children[1]);
    first_cast->arguments->children.push_back(enum_arg);

    auto second_cast = makeASTFunction("CAST");
    second_cast->arguments->children.push_back(function_if_node.arguments->children[2]);
    second_cast->arguments->children.push_back(enum_arg);

    function_if_node.arguments->children[1] = first_cast;
    function_if_node.arguments->children[2] = second_cast;
}

String enumForArray(const Array & arr, const Field & other)
{
    String str_enum = "Enum8(";
    if (arr.size() >= 255)
        str_enum = "Enum16(";

    size_t number = 1;
    for (const auto & item : arr)
    {
        str_enum += "\'" + item.get<NearestFieldType<String>>() + "\' = " + std::to_string(number++) + ", ";
    }

    str_enum += "\'" + other.get<NearestFieldType<String>>() + "\' = " + std::to_string(number) + ")";
    return str_enum;
}

void transformStringsArrayIntoEnum(ASTPtr & array_to, ASTPtr & other)
{
    String enum_result = enumForArray(array_to->as<ASTLiteral>()->value.get<NearestFieldType<Array>>(),
                                      other->as<ASTLiteral>()->value);

    auto array_cast = makeASTFunction("CAST");
    array_cast->arguments->children.push_back(array_to);
    array_cast->arguments->children.push_back(std::make_shared<ASTLiteral>("Array(" + enum_result + ")"));
    array_to = array_cast;

    auto other_cast = makeASTFunction("CAST");
    other_cast->arguments->children.push_back(other);
    other_cast->arguments->children.push_back(std::make_shared<ASTLiteral>(enum_result));
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


struct GettingFunctionArgumentsAliasesMatcher
{
    struct Data
    {
        std::unordered_set<String> & aliases_inside_functions;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        if (auto func = node->as<ASTFunction>())
            if (func->name == "if")
                return false;
        return true;
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>())
            visit(*func, data);
    }

    static void visit(const ASTFunction & func, Data & data)
    {
        if (func.name == "if")
        {
            String alias = func.tryGetAlias();
            if (!alias.empty())
            {
                data.aliases_inside_functions.insert(alias);
            }
        }
    }
};

using GettingFunctionArgumentsAliasesVisitor = ConstInDepthNodeVisitor<GettingFunctionArgumentsAliasesMatcher, true>;

struct FunctionOfAliasesMatcher
{
    struct Data
    {
        std::unordered_set<String> & aliases_inside_functions;
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

    static void visit(const ASTFunction & function_node, Data & data)
    {
        if (function_node.name != "if")
        {
            GettingFunctionArgumentsAliasesVisitor::Data aliases_data{data.aliases_inside_functions};
            GettingFunctionArgumentsAliasesVisitor(aliases_data).visit(function_node.arguments);
        }
    }
};

using FunctionOfAliasesVisitor = ConstInDepthNodeVisitor<FunctionOfAliasesMatcher, true>;

struct FindingIfWithStringsMatcher
{
    struct Data
    {
        std::unordered_set<String> & aliases; /// if functions as aliases inside functions
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

            if (data.aliases.count(function_node.tryGetAlias()))
                return;

            transformTwoStringsIntoEnum(function_node);
        }
        else if (function_node.name == "transform")
        {
            if (function_node.arguments->children.size() != 4)
                return;

            auto literal_from = function_node.arguments->children[1]->as<ASTLiteral>();
            auto literal_to = function_node.arguments->children[2]->as<ASTLiteral>();
            auto literal_other = function_node.arguments->children[3]->as<ASTLiteral>();

            if (!literal_from || !literal_to || !literal_other)
                return;

            if (String(literal_from->value.getTypeName()) != "Array" ||
                String(literal_to->value.getTypeName()) != "Array" ||
                String(literal_other->value.getTypeName()) != "String")
                return;

            Array array_from = literal_from->value.get<NearestFieldType<Array>>();
            Array array_to = literal_to->value.get<NearestFieldType<Array>>();

            if (array_from.size() == 0 || array_from.size() != array_to.size())
                return;

            bool from_strings = checkSameType(array_from, "String");
            if (from_strings)
                return;

            bool to_strings = checkSameType(array_to, "String");
            if (!to_strings)
                return;

            transformStringsArrayIntoEnum(
                function_node.arguments->children[2],
                function_node.arguments->children[3]);
        }
    }
};

using FindingIfWithStringsVisitor = InDepthNodeVisitor<FindingIfWithStringsMatcher, true>;

}
