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

void transformTwoStringsIntoEnum(ASTFunction * function_if_node)
{
    String first_literal = function_if_node->arguments->children[1]->as<ASTLiteral>()->value.get<NearestFieldType<String>>();
    String second_literal = function_if_node->arguments->children[2]->as<ASTLiteral>()->value.get<NearestFieldType<String>>();
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
    first_cast->arguments->children.push_back(function_if_node->arguments->children[1]);
    first_cast->arguments->children.push_back(enum_arg);

    auto second_cast = makeASTFunction("CAST");
    second_cast->arguments->children.push_back(function_if_node->arguments->children[2]);
    second_cast->arguments->children.push_back(enum_arg);

    function_if_node->arguments->children[1] = first_cast;
    function_if_node->arguments->children[2] = second_cast;
}

void transformStringsArrayIntoEnum(ASTFunction * function_transform_node)
{
    Array old_arr = function_transform_node->arguments->children[2]->as<ASTLiteral>()->value.get<NearestFieldType<Array>>();

    String enum_result = "Array(Enum8(";
    for (size_t i = 0; i < old_arr.size(); ++i)
    {
        enum_result += "\'" + old_arr[i].get<NearestFieldType<String>>() + "\' = " + std::to_string(i + 1);
        if (i != old_arr.size() - 1)
        {
            enum_result += ", ";
        }
    }

    enum_result += ", \'" + function_transform_node->arguments->children[3]->as<ASTLiteral>()->value.get<NearestFieldType<String>>()
        + "\' = " + std::to_string(old_arr.size() + 1) + "))";

    auto array_cast = makeASTFunction("CAST");
    auto enum_arg = std::make_shared<ASTLiteral>(enum_result);

    array_cast->arguments->children.push_back(function_transform_node->arguments->children[2]);
    array_cast->arguments->children.push_back(enum_arg);

    function_transform_node->arguments->children[2] = array_cast;

    String enum_result_other = enum_result.substr(6, enum_result.size() - 7);
    auto enum_arg_other = std::make_shared<ASTLiteral>(enum_result_other);
    auto other_cast = makeASTFunction("CAST");
    other_cast->arguments->children.push_back(function_transform_node->arguments->children[3]);
    other_cast->arguments->children.push_back(enum_arg_other);
    function_transform_node->arguments->children[3] = other_cast;
}

struct GettingFunctionArgumentsAliasesMatcher
{
    struct Data
    {
        std::unordered_set<String> & aliases_inside_functions;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        if (node->as<ASTFunction>())
        {
            if (node->as<ASTFunction>()->name == "if")
            {
                return false;
            }
        }
        return true;
    }
    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * function_node = ast->as<ASTFunction>())
        {
            if (function_node->name == "if")
            {
                String alias = ast->tryGetAlias();
                if (!alias.empty())
                {
                    data.aliases_inside_functions.insert(alias);
                }
            }
        }
    }
};

using GettingFunctionArgumentsAliasesVisitor = InDepthNodeVisitor<GettingFunctionArgumentsAliasesMatcher, true>;

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

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * function_node = ast->as<ASTFunction>())
        {
            if (function_node->name != "if")
            {
                GettingFunctionArgumentsAliasesVisitor::Data aliases_data{data.aliases_inside_functions};
                GettingFunctionArgumentsAliasesVisitor(aliases_data).visit(function_node->arguments);
            }
        }
    }
};

using FunctionOfAliasesVisitor = InDepthNodeVisitor<FunctionOfAliasesMatcher, true>;

struct FindingIfWithStringsMatcher
{
    struct Data
    {
        std::unordered_set<String> & if_functions_as_aliases_inside_functions;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !(node->as<ASTFunction>());
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * function_node = ast->as<ASTFunction>())
        {
            if (function_node->name == "if")
            {
                if (function_node->arguments->children[1]->as<ASTLiteral>() &&
                    function_node->arguments->children[2]->as<ASTLiteral>())
                {
                    String func_alias = ast->tryGetAlias();
                    if (!func_alias.empty())
                    {
                        if (data.if_functions_as_aliases_inside_functions.count(func_alias))
                            return;
                    }

                    if (!strcmp(function_node->arguments->children[1]->as<ASTLiteral>()->value.getTypeName(), "String")
                        && !strcmp(function_node->arguments->children[2]->as<ASTLiteral>()->value.getTypeName(), "String"))
                        transformTwoStringsIntoEnum(function_node);
                }
            }
            else if (function_node->name == "transform")
            {
                bool first_array_of_strings = false;
                bool second_array_of_strings = false;

                size_t first_size = 0;
                size_t second_size = 0;

                if (!function_node->arguments->children[1]->as<ASTLiteral>())
                    return;

                if (!function_node->arguments->children[2]->as<ASTLiteral>())
                    return;

                if (!strcmp(function_node->arguments->children[1]->as<ASTLiteral>()->value.getTypeName(), "Array"))
                {
                    first_array_of_strings = true;
                    Array array_from = function_node->arguments->children[1]->as<ASTLiteral>()->value.get<NearestFieldType<Array>>();
                    first_size = array_from.size();
                    for (size_t i = 0; i < first_size; ++i)
                    {
                        if (strcmp(array_from[i].getTypeName(), "String") != 0)
                        {
                            first_array_of_strings = false;
                            break;
                        }
                    }
                }

                if (!strcmp(function_node->arguments->children[2]->as<ASTLiteral>()->value.getTypeName(), "Array"))
                {
                    second_array_of_strings = true;
                    Array array_to = function_node->arguments->children[2]->as<ASTLiteral>()->value.get<NearestFieldType<Array>>();
                    second_size = array_to.size();
                    for (size_t i = 0; i < second_size; ++i)
                    {
                        if (strcmp(array_to[i].getTypeName(), "String") != 0)
                        {
                            first_array_of_strings = false;
                            break;
                        }
                    }
                }

                if (first_size != second_size)
                    return;

                if (first_array_of_strings)
                    return;

                if (!second_array_of_strings)
                    return;

                if (function_node->arguments->children.size() != 4)
                    return;

                if (!function_node->arguments->children[3]->as<ASTLiteral>())
                    return;

                if (!strcmp(function_node->arguments->children[3]->as<ASTLiteral>()->value.getTypeName(), "String"))
                {
                    transformStringsArrayIntoEnum(function_node);
                }
            }
        }
    }
};

using FindingIfWithStringsVisitor = InDepthNodeVisitor<FindingIfWithStringsMatcher, true>;

}
