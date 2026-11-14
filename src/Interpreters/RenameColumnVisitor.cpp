#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

void RenameColumnData::visit(ASTIdentifier & identifier, ASTPtr & ast) const
{
    // TODO(ilezhankin): make proper rename
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (!identifier_column_name)
        return;

    if (identifier_column_name == column_name)
    {
        identifier.setShortName(rename_to);
    }
    else if (rename_subcolumns && identifier_column_name->starts_with(column_name + "."))
    {
        /// A compound identifier `column_name.x` is a subcolumn. One part `column_name.x` (as the key expressions built
        /// from the metadata of a data lake are written) is a subcolumn unless the table has a column with this name.
        if (identifier.compound())
        {
            if (identifier.name_parts[0] != column_name)
                return;
            auto name_parts = identifier.name_parts;
            name_parts[0] = rename_to;
            auto renamed = make_intrusive<ASTIdentifier>(std::move(name_parts));
            renamed->setAlias(identifier.tryGetAlias());
            ast = std::move(renamed);
        }
        else if (!columns || !columns->has(*identifier_column_name))
        {
            identifier.setShortName(rename_to + identifier_column_name->substr(column_name.size()));
        }
    }
}

}
