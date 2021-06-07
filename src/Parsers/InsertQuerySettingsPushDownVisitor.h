#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectQuery;
struct SettingChange;
class SettingsChanges;

/// Pushdown SETTINGS clause to the INSERT from the SELECT query:
/// (since SETTINGS after SELECT will be parsed by the SELECT parser.)
///
/// NOTE: INSERT ... SELECT ... FORMAT Null SETTINGS max_insert_threads=10 works even w/o push down,
/// since ParserInsertQuery does not use ParserQueryWithOutput.
class InsertQuerySettingsPushDownMatcher
{
public:
    using Visitor = InDepthNodeVisitor<InsertQuerySettingsPushDownMatcher, true>;

    struct Data
    {
        ASTPtr & insert_settings_ast;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);
    static void visit(ASTPtr & ast, Data & data);

private:
    static void visit(ASTSelectQuery &, ASTPtr &, Data &);
};

using InsertQuerySettingsPushDownVisitor = InsertQuerySettingsPushDownMatcher::Visitor;

}
