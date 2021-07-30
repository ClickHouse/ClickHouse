#include <Interpreters/RenameColumnVisitor.h>
#include <Interpreters/IdentifierSemantic.h>

namespace DB
{
void RenameColumnData::visit(ASTIdentifier & identifier, ASTPtr &) const
{
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (identifier_column_name && identifier_column_name == column_name)
        identifier.name = rename_to;
}
}
