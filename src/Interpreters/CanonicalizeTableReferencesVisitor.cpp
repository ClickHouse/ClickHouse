#include <Interpreters/CanonicalizeTableReferencesVisitor.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace
{

void canonicalizeTableIdentifier(ASTTableIdentifier & identifier, const ContextPtr & context)
{
    /// Only two-part identifiers are unambiguous catalog references; short ones may be
    /// query-local (WITH RECURSIVE aliases, temporary tables) and must keep their spelling.
    if (identifier.isParam() || identifier.name_parts.size() != 2)
        return;
    if (identifier.name_parts[0].spelling.empty() || identifier.name_parts[1].spelling.empty())
        return;

    StorageID resolved = DatabaseCatalog::instance().resolveStorageIDNames(identifier.getTableId(), context);
    if (resolved.database_name == identifier.name_parts[0].spelling && resolved.table_name == identifier.name_parts[1].spelling)
        return;

    /// The resolved spelling is exact; pin it so later in-process resolution does not re-fold it.
    identifier.name_parts[0] = IdentifierPart{resolved.database_name, IdentifierPartQuote::DoubleQuoted};
    identifier.name_parts[1] = IdentifierPart{resolved.table_name, IdentifierPartQuote::DoubleQuoted};
    identifier.full_name = resolved.database_name + "." + resolved.table_name;
}

}

void CanonicalizeTableReferencesVisitor::visit(IAST & ast, const ContextPtr & context)
{
    if (auto * identifier = ast.as<ASTTableIdentifier>())
    {
        canonicalizeTableIdentifier(*identifier, context);
        return;
    }

    for (auto & child : ast.children)
        visit(*child, context);
}

}
