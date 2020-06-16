#pragma once

#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>

namespace DB
{

class MonotonicityCheckMatcher
{
public:
    struct Data
    {
        const TablesWithColumns & tables;
        const Context & context;
        ASTPtr & identifier;
        bool & is_monotonous;
        bool & is_positive;
        DataTypePtr data_type = nullptr;
        bool first = true;
        bool done = false;
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        auto * ast_function = ast->as<ASTFunction>();
        if (!data.done && ast_function)
        {
            visit(*ast_function, data);
        }
    }


    static void visit(ASTFunction & ast_function, Data & data)
    {
        if (!data.is_monotonous)
            return;

        auto arguments = ast_function.arguments;
        if (arguments->children.size() != 1)
        {
            data.is_monotonous = false;
            data.done = true;
            return;
        }

        if (data.first)
            data.identifier = ast_function.arguments->children[0];

        const auto & function = FunctionFactory::instance().tryGet(ast_function.name, data.context);
        auto pos = IdentifierSemantic::chooseTable(*data.identifier->as<ASTIdentifier>(), data.tables, true);
        auto data_type_and_name = data.tables[*pos].columns.tryGetByName(data.identifier->getAliasOrColumnName());
        if (data.first)
        {
            data.data_type = data_type_and_name->type;
            data.first = false;
        }
        auto name = data_type_and_name->name;

        ColumnsWithTypeAndName args;
        args.emplace_back(data.data_type, name);
        auto function_base = function->build(args);

        auto cur_data = data.data_type;
        data.data_type = function_base->getReturnType();

        if (!function_base->hasInformationAboutMonotonicity())
        {
            data.is_monotonous = false;
            data.done = true;
            return;
        }
        auto monotonicity = function_base->getMonotonicityForRange(*cur_data, NULL, NULL);
        if (!monotonicity.is_always_monotonic)
        {
            data.is_monotonous = false;
            data.done = true;
            return;
        }

        if (!monotonicity.is_positive)
            data.is_positive = !data.is_positive;
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }

};

using MonotonicityCheckVisitor = InDepthNodeVisitor<MonotonicityCheckMatcher, false>;

class FirstLevelASTFunctionMatcher
{
public:
    struct Data
    {
        const TablesWithColumns & tables;
        const Context & context;
        ASTPtr & identifier;
        bool & is_positive;
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        auto * ast_function = ast->as<ASTFunction>();
        if (ast_function)
        {
            visit(*ast_function, data);
        }
    }


    static void visit(ASTFunction & ast_function, Data & data)
    {
        if (ast_function.children.size() != 1)
            return;

        bool is_monotonous = true;
        auto ptr = ast_function.ptr();

        MonotonicityCheckVisitor::Data monotonicity_checker_data{data.tables, data.context, data.identifier, is_monotonous, data.is_positive};
        MonotonicityCheckVisitor(monotonicity_checker_data).visit(ptr);

        if (!is_monotonous)
            data.identifier = nullptr;
    }

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        if (node->as<ASTFunction>())
            return false;

        return true;
    }
};

using FirstLevelASTFunctionVisitor = InDepthNodeVisitor<FirstLevelASTFunctionMatcher, true>;

/// Finds SELECT that can be optimized
class MonotonousOrderByData
{
public:
    using TypeToVisit = ASTSelectQuery;

    const TablesWithColumns & tables;
    const Context & context;
    bool done = false;

    void visit(ASTSelectQuery & select_query, ASTPtr &)
    {
        if (done)
            return;

        auto order_by = select_query.orderBy();

        if (!order_by)
            return;

        for (size_t i = 0; i < order_by->children.size(); ++i)
        {
            auto child = order_by->children[i];
            bool is_positive = true;
            ASTPtr identifier;
            FirstLevelASTFunctionVisitor::Data data{tables, context, identifier, is_positive};
            FirstLevelASTFunctionVisitor(data).visit(child);
            if (identifier)
            {
                auto * order_by_element = child->as<ASTOrderByElement>();
                order_by_element->children[0] = identifier;
                if (!is_positive)
                    order_by_element->direction *= -1;
            }
        }

        done = true;
    }
};

using MonotonousOrderByMatcher = OneTypeMatcher<MonotonousOrderByData>;
using MonotonousOrderByVisitor = InDepthNodeVisitor<MonotonousOrderByMatcher, true>;

}
