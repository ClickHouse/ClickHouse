#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

void RenameColumnData::visit(ASTIdentifier & identifier, ASTPtr &) const
{
    // TODO(ilezhankin): make proper rename
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (!identifier_column_name)
        return;

    if (*identifier_column_name == column_name)
    {
        identifier.setShortName(rename_to);
        return;
    }

    /// `j.a` is the subcolumn `a` of the column `j`.
    if (identifier.compound() && identifier.name_parts.front() == column_name)
    {
        identifier.name_parts.front() = rename_to;
        identifier.resetFullName();
    }
}

}
