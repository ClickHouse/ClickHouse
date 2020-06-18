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

void transformStringsIntoEnum(ASTFunction * function_if_node)
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
                        transformStringsIntoEnum(function_node);
                }
            }
        }
    }
};

using FindingIfWithStringsVisitor = InDepthNodeVisitor<FindingIfWithStringsMatcher, true>;

}
