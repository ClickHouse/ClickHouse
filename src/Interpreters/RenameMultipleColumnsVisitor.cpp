#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameMultipleColumnsVisitor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

void RenameMultipleColumnsData::visit(ASTIdentifier & identifier, ASTPtr &) const
{
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (identifier_column_name)
    {
        auto column_map_it = column_rename_map.find(*identifier_column_name);
        if (column_map_it != column_rename_map.end())
        {
            identifier.setShortName(column_map_it->second);
        }
    }
}

}
