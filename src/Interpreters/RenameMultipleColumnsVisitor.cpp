#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameMultipleColumnsVisitor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

void RenameMultipleColumnsData::visit(ASTIdentifier & identifier, ASTPtr &) const
{
    // TODO(ilezhankin): make proper rename
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (identifier_column_name && column_rename_map.find(*identifier_column_name) != column_rename_map.end())
    {
        String new_name = column_rename_map.at(*identifier_column_name);
        identifier.setShortName(new_name);    
    }
}

}
