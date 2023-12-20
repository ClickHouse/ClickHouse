#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/applyFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>
#include <DataTypes/FieldToDataType.h>
#include <Core/Range.h>

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

        Range range = Range::createWholeUniverse();

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

        /// Monotonicity check only works for functions that contain at most two arguments and one of them must be a constant.
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

        auto function_arguments = getFunctionArguments(ast_function, data);

        auto function_base = function->build(function_arguments);

        if (function_base && function_base->hasInformationAboutMonotonicity())
        {
            bool is_positive = data.monotonicity.is_positive;
            data.monotonicity = function_base->getMonotonicityForRange(*data.arg_data_type, data.range.left, data.range.right);

            auto & key_range = data.range;

            /// If we apply function to open interval, we can get empty intervals in result.
            /// E.g. for ('2020-01-03', '2020-01-20') after applying 'toYYYYMM' we will get ('202001', '202001').
            /// To avoid this we make range left and right included.
            /// Any function that treats NULL specially is not monotonic.
            /// Thus we can safely use isNull() as an -Inf/+Inf indicator here.
            if (!key_range.left.isNull())
            {
                key_range.left = applyFunction(function_base, data.arg_data_type, key_range.left);
                key_range.left_included = true;
            }

            if (!key_range.right.isNull())
            {
                key_range.right = applyFunction(function_base, data.arg_data_type, key_range.right);
                key_range.right_included = true;
            }

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

    static ColumnsWithTypeAndName getFunctionArguments(const ASTFunction & ast_function, const Data & data)
    {
        ColumnsWithTypeAndName args;

        if (ast_function.arguments->children.size() == 2)
        {
            if (ast_function.arguments->children[0]->as<ASTLiteral>())
            {
                const auto * literal = ast_function.arguments->children[0]->as<ASTLiteral>();
                args.push_back(extractLiteralColumnAndTypeFromAstLiteral(literal));
                args.emplace_back(data.arg_data_type, "tmp");
            }
            else
            {
                const auto * literal = ast_function.arguments->children[1]->as<ASTLiteral>();
                args.emplace_back(data.arg_data_type, "tmp");
                args.push_back(extractLiteralColumnAndTypeFromAstLiteral(literal));
            }
        }
        else
        {
            args.emplace_back(data.arg_data_type, "tmp");
        }

        return args;
    }
};

using MonotonicityCheckVisitor = ConstInDepthNodeVisitor<MonotonicityCheckMatcher, false>;

}
