#include <Interpreters/getASTFunctionArgumentColumns.h>
#include <Columns/ColumnConst.h>

#include <DataTypes/FieldToDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/FieldVisitors.h>


namespace DB
{

std::optional<ColumnsWithTypeAndName> tryGetASTFunctionArgumentColumns(
    const ASTFunction & function, const NamesAndTypesList & source_columns)
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
            auto name_and_type = source_columns.tryGetByName(identifier->shortName());
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
