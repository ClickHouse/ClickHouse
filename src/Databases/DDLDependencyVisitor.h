#pragma once
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;


class DDLDependencyVisitor
{
public:
    struct Data
    {
        using TableDependencies = std::vector<QualifiedTableName>;
        String default_database;
        TableDependencies dependencies;
    };

    using Visitor = ConstInDepthNodeVisitor<DDLDependencyVisitor, true>;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

private:
    static void visit(const ASTFunction & function, Data & data);

    static void extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx);
};

using TableLoadingDependenciesVisitor = DDLDependencyVisitor::Visitor;

}
