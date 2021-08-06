#include <Interpreters/RenameColumnVisitor.h>
#include <Interpreters/IdentifierSemantic.h>

namespace DB
{

void RenameColumnData::visit(ASTIdentifier & identifier, ASTPtr &) const
{
    // TODO(ilezhankin): make proper rename
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (identifier_column_name && identifier_column_name == column_name)
        identifier.setShortName(rename_to);
}

void RenameColumnsData::visit(ASTIdentifier & identifier, ASTPtr &) const
{
    // TODO(ilezhankin): make proper rename
    int index;
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (identifier_column_name)
    {
        auto it = std::find(columns_name.begin(), columns_name.end(), identifier_column_name);
        if (it != columns_name.end())
        {
            index = it - columns_name.begin();
            identifier.setShortName(renames_to.at(index));
        }
    }
}

}
