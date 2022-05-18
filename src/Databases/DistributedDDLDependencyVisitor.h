#pragma once
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;
class ASTFunctionWithKeyValueArguments;
class ASTStorage;
class ASTRenameQuery;
class ASTQueryWithTableAndOutput;
class ASTTableIdentifier;
class ASTSystemQuery;

/// Contains also databases with no table name and dictionaries
using TableNamesSet = std::unordered_set<QualifiedTableName>;

TableNamesSet getDependenciesSetFromQuery(ContextMutablePtr global_context, const ASTPtr & ast);

class DistributedDDLDependencyVisitor
{
public:
    struct Data
    {
        String default_database;
        TableNamesSet dependencies;
        ContextMutablePtr global_context;
        ASTPtr query;
    };

    using Visitor = ConstInDepthNodeVisitor<DistributedDDLDependencyVisitor, true>;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

private:
    static void visit(const ASTFunction & function, Data & data);
    static void visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data);
    static void visit(const ASTStorage & storage, Data & data);
    static void visit(const ASTRenameQuery & rename_query, Data & data); // rename_query->elements contains Tables "from" and "to"
    static void visit(const ASTQueryWithTableAndOutput & table_and_output_query, Data & data); // table_and_output_query->getTable() contains full_name from ASTIdentifier, tryParseFromString QualifiedTableName
    static void visit(const ASTTableIdentifier & table_identifier, Data & data); // table_identifier->getTable() returns ASTIdentifier, ->name() returns full_name, tryParseFromString QualifiedTableName
    static void visit(const ASTSystemQuery & system_query, Data & data); // system_query->getTable() contains full_name from ASTIdentifier, tryParseFromString QualifiedTableName

    static void extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx);
};

using DistributedDDLDependenciesVisitor = DistributedDDLDependencyVisitor::Visitor;

}
