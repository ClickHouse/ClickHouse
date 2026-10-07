#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

/// Collects the `parameter = value` assignments of a parameterized-view call.
///
/// Only the top-level arguments of the call are inspected, and only those that are genuine
/// `identifier = <constant expression>` assignments. The accepted argument SHAPES mirror the
/// query-tree collector in `QueryAnalyzer::resolveTableFunction`, so a positional argument, a
/// non-`equals` function or an assignment nested inside another expression binds nothing on
/// either path. An argument of any other shape is ignored, which leaves the parameter unset;
/// `ReplaceQueryParameterVisitor` then reports `UNKNOWN_QUERY_PARAMETER`, except for a
/// `Nullable` parameter, which it substitutes as `NULL`.
///
/// Value ACCEPTANCE is not identical to the analyzer's: here the value must already be a
/// literal, function call or subquery, while the analyzer resolves the value node and accepts
/// anything that folds to a constant (e.g. a `WITH` alias). That difference is pre-existing
/// and out of scope; both of its current verdicts are pinned in
/// `04648_parameterized_view_non_equals_argument.sql`.
NameToNameMap analyzeFunctionParamValues(const ASTPtr & ast, ContextPtr context)
{
    NameToNameMap parameter_values;

    const auto * view_call = ast->as<ASTFunction>();
    if (!view_call || !view_call->arguments)
        return parameter_values;

    const auto * call_arguments = view_call->arguments->as<ASTExpressionList>();
    if (!call_arguments)
        return parameter_values;

    for (const auto & argument : call_arguments->children)
    {
        /// Key on `arguments` rather than on `children`: a parametric spelling such as
        /// `equals(7)(name, 'a')` keeps its parameters in a separate child, and the analyzer
        /// accepts it, so a child-count test would reject it and re-introduce a divergence.
        const auto * assignment = argument->as<ASTFunction>();
        if (!assignment || assignment->name != "equals" || !assignment->arguments)
            continue;

        const auto * assignment_arguments = assignment->arguments->as<ASTExpressionList>();
        if (!assignment_arguments || assignment_arguments->children.size() != 2)
            continue;

        const auto * identifier = assignment_arguments->children[0]->as<ASTIdentifier>();
        if (!identifier)
            continue;

        const ASTPtr & value = assignment_arguments->children[1];
        if (!value->as<ASTLiteral>() && !value->as<ASTFunction>() && !value->as<ASTSubquery>())
            continue;

        /// Evaluate the value to a constant and serialize it with its own data type so the result
        /// is text-escaped the way `ReplaceQueryParameterVisitor` expects (it reads the value back
        /// with `deserializeTextEscaped`). This mirrors the analyzer counterpart in `QueryAnalyzer`,
        /// which likewise has no separate literal path.
        auto [field, type] = evaluateConstantExpression(value, context);
        auto column = type->createColumn();
        column->insert(field);
        WriteBufferFromOwnString buffer;
        type->getDefaultSerialization()->serializeTextEscaped(*column, 0, buffer, {});
        parameter_values[identifier->name()] = buffer.str();
    }

    return parameter_values;
}


}
