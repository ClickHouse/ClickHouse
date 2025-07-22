#pragma once

#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct Map;

/// Add additional_table_filters setting to select queries if it is not set
class EnsureAdditionalTableInSelectsSettingsVisitor
{
public:
    explicit EnsureAdditionalTableInSelectsSettingsVisitor(const Map & additional_table_filters);

    void visit(ASTPtr & ast);

private:
    const Map & additional_table_filters;

    void visitSelectQuery(ASTPtr & ast);
    void visitChildren(ASTPtr & ast);
};

}
