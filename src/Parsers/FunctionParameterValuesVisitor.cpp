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

class FunctionParameterValuesVisitor
{
public:
    explicit FunctionParameterValuesVisitor(NameToNameMap & parameter_values_, ContextPtr context_)
        : parameter_values(parameter_values_)
         , context(context_)
    {
    }

    void visit(const ASTPtr & ast)
    {
        if (const auto * function = ast->as<ASTFunction>())
        {
            /// A recognized `parameter = value` assignment consumes the whole subtree: the value
            /// expression must not be descended into, otherwise an inner predicate that happens to
            /// reference the same parameter name (e.g. a subquery `... WHERE p = 1`) would overwrite
            /// the collected value.
            if (visitFunction(*function))
                return;
        }
        for (const auto & child : ast->children)
            visit(child);
    }

private:
    NameToNameMap & parameter_values;
    ContextPtr context;

    /// Returns true if this function is a `parameter = value` assignment that was collected.
    bool visitFunction(const ASTFunction & parameter_function)
    {
        if (parameter_function.name != "equals" && parameter_function.children.size() != 1)
            return false;

        const auto * expression_list = parameter_function.children[0]->as<ASTExpressionList>();

        if (!expression_list || expression_list->children.size() != 2)
            return false;

        const auto * identifier = expression_list->children[0]->as<ASTIdentifier>();
        if (!identifier)
            return false;

        const ASTPtr & value = expression_list->children[1];

        /// Evaluate the value to a constant and serialize it with its own data type so the result
        /// is text-escaped the way `ReplaceQueryParameterVisitor` expects (it reads the value back
        /// with `deserializeTextEscaped`). This mirrors the analyzer counterpart in `QueryAnalyzer`,
        /// which likewise has no separate literal path.
        if (value->as<ASTLiteral>() || value->as<ASTFunction>() || value->as<ASTSubquery>())
        {
            auto [field, type] = evaluateConstantExpression(value, context);
            auto column = type->createColumn();
            column->insert(field);
            WriteBufferFromOwnString buffer;
            type->getDefaultSerialization()->serializeTextEscaped(*column, 0, buffer, {});
            parameter_values[identifier->name()] = buffer.str();
            return true;
        }

        return false;
    }
};

NameToNameMap analyzeFunctionParamValues(const ASTPtr & ast, ContextPtr context)
{
    NameToNameMap parameter_values;
    FunctionParameterValuesVisitor(parameter_values, context).visit(ast);
    return parameter_values;
}


}
