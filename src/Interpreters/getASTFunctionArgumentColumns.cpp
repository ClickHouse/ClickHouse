#include <Interpreters/getASTFunctionArgumentColumns.h>
#include <Columns/ColumnConst.h>

#include <DataTypes/FieldToDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Common/FieldVisitors.h>


namespace DB
{

NameSet getArrayJoinResultNames(const ASTSelectQuery & select_query)
{
    NameSet result_names;

    const auto & array_join_expression_list = select_query.arrayJoinExpressionList().first;
    if (!array_join_expression_list)
        return result_names;

    for (const auto & expression : array_join_expression_list->children)
        result_names.insert(expression->getAliasOrColumnName());

    return result_names;
}

std::optional<ColumnsWithTypeAndName> tryGetASTFunctionArgumentColumns(
    const ASTFunction & function, const NamesAndTypesList & source_columns, const NameSet & array_join_result_names)
{
    ColumnsWithTypeAndName arguments;

    if (!function.arguments)
        return arguments;

    arguments.reserve(function.arguments->children.size());
    for (const auto & child : function.arguments->children)
    {
        if (const auto * literal = child->as<ASTLiteral>())
        {
            auto type = applyVisitor(FieldToDataType(), literal->value);
            arguments.emplace_back(type->createColumnConst(1, literal->value), type, "");
        }
        else if (const auto * identifier = child->as<ASTIdentifier>())
        {
            /// A compound name - a table-qualified `JOIN` key such as `r.dt`, or a subcolumn such as
            /// `tuple.dt` - is not decidable here. This runs before `collectJoinedColumns`, so
            /// `source_columns` describes only the source side, and matching by the last part alone
            /// would resolve `r.dt` against an unrelated source column named `dt` and read its type.
            if (identifier->compound())
                return {};

            /// This also runs before `getArrayJoinedColumns`, so a plain name may still denote an
            /// `ARRAY JOIN` result rather than the source column it shadows: after `ARRAY JOIN arr AS dt`
            /// the runtime `dt` is an element of `arr`, whatever the type of the source `dt`.
            if (array_join_result_names.contains(identifier->name()))
                return {};

            auto name_and_type = source_columns.tryGetByName(identifier->name());
            if (!name_and_type)
                return {};
            arguments.emplace_back(ColumnPtr{}, name_and_type->type, name_and_type->name);
        }
        else
            return {};
    }

    return arguments;
}

}
