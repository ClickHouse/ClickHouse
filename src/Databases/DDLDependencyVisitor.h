#pragma once
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;
class ASTFunctionWithKeyValueArguments;


class DDLDependencyVisitor
{
public:
    struct Data
    {
        using TableNamesSet = std::set<QualifiedTableName>;
        String default_database;
        TableNamesSet dependencies;
        ContextPtr global_context;
        ASTPtr create_query;
    };

    using Visitor = ConstInDepthNodeVisitor<DDLDependencyVisitor, true>;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

private:
    static void visit(const ASTFunction & function, Data & data);
    static void visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data);

    static void extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx);
};

using TableLoadingDependenciesVisitor = DDLDependencyVisitor::Visitor;

}
