#include "ASTFunctionMonotonicityChecker.h"

#include <Functions/FunctionFactory.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

    void buildFunctionListImpl(const RPNBuilderTreeNode & node,
                               const KeyDescription & key_description,
                               DataTypePtr & out_key_column_type,
                               std::vector<RPNBuilderFunctionTreeNode> & function_list)
    {
        if (node.isFunction())
        {
            auto function_node = node.toFunctionNode();

            size_t arguments_size = function_node.getArgumentsSize();
            if (arguments_size > 2 || arguments_size == 0)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Monotonicity check only works for functions with 1 or 2 arguments");
            }

            function_list.push_back(function_node);

            if (arguments_size == 2)
            {
                if (function_node.getArgumentAt(0).isConstant())
                {
                    buildFunctionListImpl(function_node.getArgumentAt(1), key_description, out_key_column_type, function_list);
                }
                else if (function_node.getArgumentAt(1).isConstant())
                {
                    buildFunctionListImpl(function_node.getArgumentAt(0), key_description, out_key_column_type, function_list);
                }
                else
                {
                    // re-think below message
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "A function with {} arguments must contain at least one constant argument to be considered monotonic",
                                    arguments_size);
                }
            }
            else
            {
                buildFunctionListImpl(function_node.getArgumentAt(0), key_description, out_key_column_type, function_list);
            }
        }
        else
        {
            // assuming the column is always part of expression

            auto node_name = node.getColumnName();
            auto key_column = key_description.expression->getSampleBlock().getByName(node_name);
            out_key_column_type = key_column.type;
        }
    }

    Field applyFunctionForField(
        const FunctionBasePtr & func,
        const DataTypePtr & arg_type,
        const Field & arg_value)
    {
        ColumnsWithTypeAndName columns
            {
                { arg_type->createColumnConst(1, arg_value), arg_type, "x" },
            };

        auto col = func->execute(columns, func->getResultType(), 1);
        return (*col)[0];
    }

    FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field)
    {
        /// Fallback for fields without block reference.
        if (field.isExplicit())
            return applyFunctionForField(func, current_type, field);

        String result_name = "_" + func->getName() + "_" + toString(field.column_idx);
        const auto & columns = field.columns;
        size_t result_idx = columns->size();

        for (size_t i = 0; i < result_idx; ++i)
        {
            if ((*columns)[i].name == result_name)
                result_idx = i;
        }

        if (result_idx == columns->size())
        {
            ColumnsWithTypeAndName args{(*columns)[field.column_idx]};
            field.columns->emplace_back(ColumnWithTypeAndName {nullptr, func->getResultType(), result_name});
            (*columns)[result_idx].column = func->execute(args, (*columns)[result_idx].type, columns->front().column->size());
        }

        return {field.columns, field.row_idx, result_idx};
    }

    IFunction::Monotonicity applyMonotonicFunctionsChainToRange(
        Range key_range,
        const std::vector<FunctionBasePtr> & functions,
        DataTypePtr current_type)
    {
        // default to monotonically increasing, satisfies cases where function list is empty
        IFunction::Monotonicity monotonicity {true, true, true, true};

        for (const auto & func : functions)
        {
            /// We check the monotonicity of each function on a specific range.
            /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
            monotonicity = func->getMonotonicityForRange(*current_type.get(), key_range.left, key_range.right);

            if (!monotonicity.is_monotonic)
            {
                return {};
            }

            /// If we apply function to open interval, we can get empty intervals in result.
            /// E.g. for ('2020-01-03', '2020-01-20') after applying 'toYYYYMM' we will get ('202001', '202001').
            /// To avoid this we make range left and right included.
            /// Any function that treats NULL specially is not monotonic.
            /// Thus we can safely use isNull() as an -Inf/+Inf indicator here.
            if (!key_range.left.isNull())
            {
                key_range.left = applyFunction(func, current_type, key_range.left);
                key_range.left_included = true;
            }

            if (!key_range.right.isNull())
            {
                key_range.right = applyFunction(func, current_type, key_range.right);
                key_range.right_included = true;
            }

            current_type = func->getResultType();

            if (!monotonicity.is_positive)
                key_range.invert();
        }

        return monotonicity;
    }
}

IFunction::Monotonicity ASTFunctionMonotonicityChecker::getMonotonicityInfo(
    const KeyDescription & key_description,
    const Range & range,
    ContextPtr context
)
{
    DataTypePtr key_column_type;

    auto pkeyastclone = key_description.definition_ast->clone();

    auto result = TreeRewriter(context).analyze(pkeyastclone, key_description.expression->getRequiredColumnsWithTypes());

    auto block_with_constants = KeyCondition::getBlockWithConstants(pkeyastclone, result, context);

    auto rpn_context = RPNBuilderTreeContext(context, block_with_constants, {});

    auto rpn_node = RPNBuilderTreeNode(key_description.definition_ast.get(), rpn_context);

    std::vector<RPNBuilderFunctionTreeNode> rpn_function_list;

    buildFunctionListImpl(rpn_node, key_description, key_column_type, rpn_function_list);


    std::vector<FunctionBasePtr> functions;

    for (auto it = rpn_function_list.rbegin(); it != rpn_function_list.rend(); ++it)
    {
        auto function = *it;
        auto func_builder = FunctionFactory::instance().tryGet(function.getFunctionName(), context);

        if (!func_builder)
        {
            return {false};
        }

        ColumnsWithTypeAndName arguments;
        ColumnWithTypeAndName const_arg;
        if (function.getArgumentsSize() == 2)
        {
            if (function.getArgumentAt(0).isConstant())
            {
                const_arg = function.getArgumentAt(0).getConstantColumn();
                arguments.push_back(const_arg);
                arguments.push_back({ nullptr, key_column_type, "" });
            }
            else if (function.getArgumentAt(1).isConstant())
            {
                arguments.push_back({ nullptr, key_column_type, "" });
                const_arg = function.getArgumentAt(1).getConstantColumn();
                arguments.push_back(const_arg);
            }
        }
        else
        {
            arguments.push_back({ nullptr, key_column_type, "" });
        }
        auto func = func_builder->build(arguments);

        if (!func || !func->hasInformationAboutMonotonicity())
        {
            return {false};
        }

        functions.push_back(func);
    }

    return applyMonotonicFunctionsChainToRange(range, functions, key_column_type);
}

std::vector<RPNBuilderFunctionTreeNode> ASTFunctionMonotonicityChecker::buildFunctionList(const KeyDescription & key_description,
                                                                                          DataTypePtr & key_expr_type,
                                                                                          ContextPtr context)
{
    auto pkeyastclone = key_description.definition_ast->clone();

    auto result = TreeRewriter(context).analyze(pkeyastclone, key_description.expression->getRequiredColumnsWithTypes());

    auto block_with_constants = KeyCondition::getBlockWithConstants(pkeyastclone, result, context);

    auto rpn_context = RPNBuilderTreeContext(context, block_with_constants, {});

    auto rpn_node = RPNBuilderTreeNode(key_description.definition_ast.get(), rpn_context);

    std::vector<RPNBuilderFunctionTreeNode> function_list;

    buildFunctionListImpl(rpn_node, key_description, key_expr_type, function_list);

    return function_list;
}

}
