#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>
#include <DataTypes/FieldToDataType.h>

namespace DB
{

using Monotonicity = IFunctionBase::Monotonicity;

/// Checks from bottom to top if function composition is monotonous
class MonotonicityCheckMatcher
{
public:
    struct Data
    {
        const TablesWithColumns & tables;
        ContextPtr context;
        const std::unordered_set<String> & group_by_function_hashes;

        Monotonicity monotonicity = { .is_monotonic = true, .is_positive = true, .is_always_monotonic = true };

        ASTIdentifier * identifier = nullptr;
        DataTypePtr arg_data_type = {};

        void reject() { monotonicity.is_monotonic = false; }
        bool isRejected() const { return !monotonicity.is_monotonic; }

        bool canOptimize(const ASTFunction & ast_function) const
        {
            /// if GROUP BY contains the same function ORDER BY shouldn't be optimized
            const auto hash = ast_function.getTreeHash(/*ignore_aliases=*/ true);
            const auto key = toString(hash);
            if (group_by_function_hashes.count(key))
                return false;

            /// if ORDER BY contains aggregate function or window functions, it
            /// shouldn't be optimized
            if (ast_function.is_window_function
                || AggregateUtils::isAggregateFunction(ast_function))
            {
                return false;
            }

            return true;
        }

        bool extractIdentifierAndType(const ASTFunction & ast_function)
        {
            if (identifier)
                return true;

            identifier = ast_function.arguments->children[0]->as<ASTIdentifier>();
            if (!identifier)
                return false;

            auto pos = IdentifierSemantic::getMembership(*identifier);
            if (!pos)
                pos = IdentifierSemantic::chooseTableColumnMatch(*identifier, tables, true);
            if (!pos)
                return false;

            /// It is possible that tables list is empty.
            /// IdentifierSemantic get the position from AST, and it can be not valid to use it.
            /// Example is re-analysing a part of AST for storage Merge, see 02147_order_by_optimizations.sql
            if (*pos >= tables.size())
                return false;

            if (auto data_type_and_name = tables[*pos].columns.tryGetByName(identifier->shortName()))
            {
                arg_data_type = data_type_and_name->type;
                return true;
            }

            return false;
        }
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (const auto * ast_function = ast->as<ASTFunction>())
            visit(*ast_function, data);
    }

    static void visit(const ASTFunction & ast_function, Data & data)
    {
        if (data.isRejected())
            return;

        if (!ast_function.arguments)
        {
            data.reject();
            return;
        }

        auto arguments_size =  ast_function.arguments->children.size();

        if (arguments_size == 0 || arguments_size > 2)
        {
            data.reject();
            return;
        }
        else if (arguments_size == 2)
        {
            /// If the function has two arguments, then one of them must be a constant.
            if (!ast_function.arguments->children[0]->as<ASTLiteral>()
                && !ast_function.arguments->children[1]->as<ASTLiteral>())
            {
                data.reject();
                return;
            }
        }

        if (!data.canOptimize(ast_function))
        {
            data.reject();
            return;
        }

        const auto & function = FunctionFactory::instance().tryGet(ast_function.name, data.context);
        if (!function)
        {
            data.reject();
            return;
        }

        /// First time extract the most enclosed identifier and its data type
        if (!data.arg_data_type && !data.extractIdentifierAndType(ast_function))
        {
            data.reject();
            return;
        }

        ColumnsWithTypeAndName args;
        ColumnWithTypeAndName const_arg;

        if (arguments_size == 2)
        {
            if (ast_function.arguments->children[0]->as<ASTLiteral>())
            {
                const auto * literal = ast_function.arguments->children[0]->as<ASTLiteral>();
                const_arg = extractLiteralColumnAndTypeFromAstLiteral(literal);
                args.push_back(const_arg);
                args.emplace_back(data.arg_data_type, "tmp");
            }
            else
            {
                const auto * literal = ast_function.arguments->children[1]->as<ASTLiteral>();
                args.emplace_back(data.arg_data_type, "tmp");
                const_arg = extractLiteralColumnAndTypeFromAstLiteral(literal);
                args.push_back(const_arg);
            }
        }
        else
        {
            args.emplace_back(data.arg_data_type, "tmp");
        }

        auto function_base = function->build(args);

        if (function_base && function_base->hasInformationAboutMonotonicity())
        {
            bool is_positive = data.monotonicity.is_positive;
            data.monotonicity = function_base->getMonotonicityForRange(*data.arg_data_type, Field(), Field());

            if (!is_positive)
                data.monotonicity.is_positive = !data.monotonicity.is_positive;
            data.arg_data_type = function_base->getResultType();
        }
        else
            data.reject();
    }

    static bool needChildVisit(const ASTPtr & parent, const ASTPtr &)
    {
        /// Multi-argument functions with all but one constant arguments can be monotonic.
        if (const auto * func = typeid_cast<const ASTFunction *>(parent.get()))
            return func->arguments->children.size() <= 2;

        return true;
    }

    static ColumnWithTypeAndName extractLiteralColumnAndTypeFromAstLiteral(const ASTLiteral * literal)
    {
        ColumnWithTypeAndName result;

        result.type = applyVisitor(FieldToDataType(), literal->value);
        result.column = result.type->createColumnConst(0, literal->value);

        return result;
    }
};

using MonotonicityCheckVisitor = ConstInDepthNodeVisitor<MonotonicityCheckMatcher, false>;

}
