#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn_fwd.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPrePostProcessorUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{

constexpr char preprocessor_lambda_arg[] = "__text_index_x";
constexpr char preprocessor_column_name[] = "__text_index_column";

ASTPtr convertASTForIndexColumn(const IndexDescription & index, const ASTPtr & expression_ast, bool replace_index_column)
{
    chassert(index.column_names.size() == 1);
    chassert(index.data_types.size() == 1);
    chassert(index.expression_list_ast != nullptr);
    chassert(index.expression_list_ast->children.size() == 1);

    if (expression_ast == nullptr)
        return nullptr;

    /// Transform a preprocessor AST like `lower(val)` into `arrayMap(x -> lower(x), val)`.
    /// This is done at the AST level so that ActionsVisitor can build the DAG naturally.
    if (isArray(index.data_types.front()))
    {
        /// Firstly replace the index expression with lambda argument.
        ASTPtr new_expression = expression_ast->clone();
        replaceExpressionToIdentifier(new_expression, index.column_names.front(), preprocessor_lambda_arg);

        /// Create the array argument of arrayMap function.
        auto array_map_arg = replace_index_column
            ? make_intrusive<ASTIdentifier>(preprocessor_column_name)
            : index.expression_list_ast->children.front();

        /// Pack preprocessor expression into lambda.
        return makeASTFunction("arrayMap",
            makeASTLambda({preprocessor_lambda_arg}, std::move(new_expression)),
            array_map_arg);
    }

    if (replace_index_column)
    {
        ASTPtr new_expression = expression_ast->clone();
        replaceExpressionToIdentifier(new_expression, index.column_names.front(), preprocessor_column_name);
        return new_expression;
    }

    return expression_ast->clone();
}

ASTPtr convertASTForConstant(const IndexDescription & index, const ASTPtr & expression_ast)
{
    chassert(index.column_names.size() == 1);
    chassert(index.data_types.size() == 1);

    if (expression_ast == nullptr)
        return nullptr;

    ASTPtr body = expression_ast->clone();
    replaceExpressionToIdentifier(body, index.column_names.front(), preprocessor_column_name);
    return body;
}

/// Creates and validates an ActionsDAG for a preprocessor expression.
ActionsDAG createActionsDAGForPreprocessor(
    const NamesAndTypesList & source_columns,
    const String & source_name,
    const DataTypePtr & source_type,
    ASTPtr expression_ast)
{
    if (expression_ast == nullptr)
        return ActionsDAG();

    auto actions_dag = buildActionsDAGFromAST(expression_ast, source_columns);
    validateTransformActionsDAG(actions_dag, "preprocessor", source_name);

    const ActionsDAG::NodeRawConstPtrs & outputs = actions_dag.getOutputs();
    auto output_type = outputs.front()->result_type;
    auto nested_type = MergeTreeIndexText::getNestedDataType(output_type);
    WhichDataType which_data_type(nested_type);

    if (!which_data_type.isString() && !which_data_type.isFixedString())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression should return a column of type with base type of String or FixedString, got: {}", output_type->getName());

    auto get_array_dimensions = [](const DataTypePtr & type) -> size_t
    {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
            return array_type->getNumberOfDimensions();
        return 0;
    };

    if (get_array_dimensions(source_type) != get_array_dimensions(output_type))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must not change the array dimensions of the source column. Source type: '{}', preprocessor result type: '{}'", source_type->getName(), output_type->getName());

    return actions_dag;
}

/// Peel pure type-widening wrappers (ALIAS, CAST/_CAST/toNullable) off an ActionsDAG output node.
/// These functions widen a value to Nullable but can never synthesize NULL from a non-NULL argument
/// (CAST throws on failure rather than producing NULL), so the effective nullability of the
/// expression is the nullability of the value underneath them, not the declared outer type. Only
/// null-synthesizing functions (nullIf, if(..., NULL, ...), *OrNull, accurateCastOrNull, ...) are
/// left in place. Used both to detect null-INTRODUCING preprocessors (on a non-nullable input) and
/// null-REMOVING preprocessors (on the real source), so a widened form like
/// CAST(ifNull(str, ''), 'Nullable(String)') is classified by its inner ifNull rather than the CAST.
const ActionsDAG::Node * peelWideningWrappers(const ActionsDAG::Node * node)
{
    while (node)
    {
        if (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        {
            node = node->children.front();
            continue;
        }
        if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && !node->children.empty()
            && (node->function_base->getName() == "CAST" || node->function_base->getName() == "_CAST"
                || node->function_base->getName() == "toNullable"))
        {
            node = node->children.front();
            continue;
        }
        break;
    }
    return node;
}

/// A function is null-propagating when it relies on the default Nullable handling
/// (useDefaultImplementationForNulls() == true): the engine peels the null map off the arguments,
/// runs the function on the non-NULL values, and re-assembles a result that is NULL exactly where an
/// argument was NULL. Such a function (lower, upper, concat, substring, ...) can therefore never turn
/// a non-NULL row into NULL -- it only propagates the nullability of its arguments. Functions that
/// override this to false (nullIf, if, coalesce, ifNull, *OrNull, accurateCastOrNull, ...) manage
/// NULLs themselves and may synthesize NULL from non-NULL values. The property lives on IFunction and
/// is reachable through the FunctionToFunctionBaseAdaptor (same access pattern as KeyCondition.cpp).
bool functionIsNullPropagating(const ActionsDAG::Node * node)
{
    if (!node->function_base)
        return false;
    if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node->function_base.get()))
        return adaptor->getFunction()->useDefaultImplementationForNulls();
    return false;
}

/// True when the expression rooted at `node` can synthesize a NULL from arguments that are all
/// non-NULL -- the only shape that matters for the direct-read guard, which keys its null map on the
/// source column and is blind to NULLs the preprocessor invents. Pure type-widening wrappers
/// (CAST/_CAST/toNullable, ALIAS) widen to Nullable but never synthesize NULL, so they are peeled and
/// we recurse into their argument. Null-propagating functions (useDefaultImplementationForNulls) can
/// only pass through the nullability of their arguments, so we recurse into every argument and the
/// node itself synthesizes nothing. Any remaining node whose result is Nullable manages NULLs itself
/// (nullIf, if(..., NULL, ...), *OrNull, ...) and is treated as null-synthesizing. Constant/input
/// nodes synthesize nothing. This distinguishes e.g. lower(toNullable(str)) (propagating -> no
/// synthesis) from nullIf(str, '') (synthesizes).
bool expressionCanSynthesizeNull(const ActionsDAG::Node * node)
{
    node = peelWideningWrappers(node);
    if (!node)
        return false;

    if (node->type == ActionsDAG::ActionType::FUNCTION && functionIsNullPropagating(node))
    {
        for (const auto * child : node->children)
            if (expressionCanSynthesizeNull(child))
                return true;
        return false;
    }

    return isNullableOrLowCardinalityNullable(node->result_type);
}

/// True when the expression rooted at `node`, applied to the real (possibly Nullable) source column,
/// maps every source-NULL row to a NON-NULL value -- i.e. it strips the source nullability, so the
/// rewritten fallback predicate evaluates a source-NULL row to 0 rather than NULL and the direct-read
/// null-map wrapper must NOT reintroduce NULL for those rows. Pure type-widening wrappers
/// (CAST/_CAST/toNullable, ALIAS) do not change the actual value, so they are peeled and we recurse
/// into their argument: CAST(ifNull(str, ''), 'Nullable(String)') removes null iff its inner ifNull
/// does. A null-propagating function (useDefaultImplementationForNulls) is NULL exactly where an
/// argument is NULL, so it removes source null iff EVERY argument does -- lower(ifNull(str, ''))
/// removes null, whereas lower(toNullable(str)) does not. Any other node removes source null iff its
/// declared result is non-Nullable: ifNull / coalesce / assumeNotNull are non-Nullable (remove), while
/// a plain Nullable source or a null-synthesizing nullIf stays Nullable (does not). Mirror of
/// expressionCanSynthesizeNull() with the aggregation inverted (all arguments vs any).
bool expressionRemovesSourceNull(const ActionsDAG::Node * node)
{
    node = peelWideningWrappers(node);
    if (!node)
        return false;

    if (node->type == ActionsDAG::ActionType::FUNCTION && functionIsNullPropagating(node) && !node->children.empty())
    {
        for (const auto * child : node->children)
            if (!expressionRemovesSourceNull(child))
                return false;
        return true;
    }

    return !isNullableOrLowCardinalityNullable(node->result_type);
}

}

MergeTreeIndexTextPreprocessor::MergeTreeIndexTextPreprocessor(ASTPtr expression_ast, const IndexDescription & index_description)
    : index_column_type(index_description.data_types.front())
    /// Use source index columns to execute index and preprocessor expressions.
    , original_actions(createActionsDAGForPreprocessor(
        index_description.expression->getRequiredColumnsWithTypes(),
        index_description.column_names.front(),
        index_column_type,
        convertASTForIndexColumn(index_description, expression_ast, false)))
    /// Assume that index expression is already executed and use a placeholder column to execute preprocessor expression.
    , actions_for_index_column(createActionsDAGForPreprocessor(
        {{preprocessor_column_name, index_column_type}},
        preprocessor_column_name,
        index_column_type,
        convertASTForIndexColumn(index_description, expression_ast, true)))
    /// Take constant string and execute preprocessor expression.
    , actions_for_constant(createActionsDAGForPreprocessor(
        {{preprocessor_column_name, std::make_shared<DataTypeString>()}},
        preprocessor_column_name,
        std::make_shared<DataTypeString>(),
        convertASTForConstant(index_description, expression_ast)))
{
    if (expression_ast)
    {
        /// Detect pure case-folding preprocessors of the exact form lower(expr), lowerUTF8(expr),
        /// upper(expr), or upperUTF8(expr), where expr is the index expression itself.
        /// Nested expressions such as lower(trim(col)) are not considered pure case folding
        /// because the additional transformation would change the dictionary tokens in a way
        /// that the ILIKE case-insensitive regex can no longer match them correctly.
        const auto * func = expression_ast->as<ASTFunction>();
        if (func && func->arguments && func->arguments->children.size() == 1)
        {
            const auto & name = getFunctionCanonicalNameIfAny(func->name);
            if (name == "lower" || name == "lowerUTF8" || name == "upper" || name == "upperUTF8")
            {
                const auto & arg = func->arguments->children.front();
                is_lower_or_upper = arg->getColumnName() == index_description.column_names.front();
            }
        }

        /// actions_for_constant runs the preprocessor on a plain non-nullable String, so a Nullable
        /// output there can only come from the expression itself, not from a Nullable source.
        ///
        /// But a Nullable declared type is not enough. Two classes of expression declare Nullable yet
        /// never turn a non-NULL input into NULL: pure type-widening casts (CAST(str, 'Nullable(...)'),
        /// toNullable(str)) and null-propagating functions (lower, upper, concat, ... -- anything using
        /// the default Nullable handling), which only pass through the nullability of their arguments.
        /// Only functions that synthesize NULL from a non-NULL value (nullIf, if(..., NULL, ...),
        /// *OrNull, accurateCastOrNull, ...) matter for the direct-read guard, which is blind to NULLs
        /// the preprocessor invents. So classify the effective expression, not just the declared type:
        /// e.g. lower(toNullable(str)) is Nullable but only propagates, whereas nullIf(str, '') can
        /// synthesize (see expressionCanSynthesizeNull).
        const auto & constant_outputs = actions_for_constant.getActionsDAG().getOutputs();
        if (!constant_outputs.empty())
            introduces_null = expressionCanSynthesizeNull(constant_outputs.front());

        /// original_actions runs the preprocessor on the real (possibly Nullable) source column, so its
        /// output type is the effective post-preprocessor haystack type. When the source is Nullable but
        /// this output is not (e.g. ifNull(str, ''), coalesce(str, ''), assumeNotNull(str)), the
        /// preprocessor strips the source nullability: the rewritten fallback predicate evaluates a
        /// source-NULL row to 0 rather than NULL. The direct-read null-map wrapper keys on the source
        /// null map, so it must not reintroduce NULL for those rows (see removesNull()).
        ///
        /// As with introduces_null, the declared output type alone is not enough: a null-removing
        /// expression can be wrapped so its outer node stays Nullable yet every source-NULL row still
        /// maps to a non-NULL value. Two such shapes: a widening cast, e.g.
        /// CAST(ifNull(str, ''), 'Nullable(String)'), and a null-propagating outer function, e.g.
        /// lower(CAST(ifNull(str, ''), 'Nullable(String)')) -- lower propagates the (already non-NULL)
        /// value of its argument, so it removes source null iff the argument does. Classify the
        /// effective expression the same way introduces_null does (peel widening wrappers, recurse
        /// through null-propagating functions) rather than inspecting only the outer declared type.
        if (isNullableOrLowCardinalityNullable(index_column_type) && !original_actions.getActions().empty())
        {
            const auto & original_outputs = original_actions.getActionsDAG().getOutputs();
            if (!original_outputs.empty())
                removes_null = expressionRemovesSourceNull(original_outputs.front());
        }
    }
}

std::pair<ColumnPtr, size_t> MergeTreeIndexTextPreprocessor::processColumn(const ColumnWithTypeAndName & column, size_t start_row, size_t n_rows) const
{
    ColumnPtr index_column = column.column;
    if (actions_for_index_column.getActions().empty())
        return {index_column, start_row};

    chassert(column.type->equals(*index_column_type));
    chassert(index_column->getDataType() == column.type->getTypeId());

    /// Only copy if needed
    if (start_row != 0 || n_rows != index_column->size())
        index_column = index_column->cut(start_row, n_rows);

    return {executeUnaryExpressionActions(actions_for_index_column, index_column, index_column_type, preprocessor_column_name, n_rows), 0};
}

String MergeTreeIndexTextPreprocessor::processConstant(const String & input) const
{
    if (actions_for_constant.getActions().empty())
        return input;

    auto input_type = std::make_shared<DataTypeString>();
    ColumnPtr input_column = input_type->createColumnConst(1, Field(input));
    ColumnPtr output_column = executeUnaryExpressionActions(actions_for_constant, input_column, input_type, preprocessor_column_name, 1);
    return String{output_column->getDataAt(0)};
}

}
