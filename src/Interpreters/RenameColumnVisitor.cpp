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
    else if (identifier_column_name->starts_with(column_name + "."))
    {
        /// `column_name.x` (written as a compound identifier, or as one part as the key expressions built from the metadata
        /// of a data lake are written) is a subcolumn unless the table has a physical column with this name, such as
        /// `MergeTree` with `share_nested_offsets = 0`, or a subcolumn of such a column.
        if (columns && columns->hasColumnOrSubcolumn(GetColumnsOptions::All, *identifier_column_name))
            return;

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
        else
        {
            identifier.setShortName(rename_to + identifier_column_name->substr(column_name.size()));
        }
    }
}

}
