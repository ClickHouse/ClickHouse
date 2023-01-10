#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <memory>


namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Changes a create query to a form which is appropriate or suitable for saving in a backup.
/// Also extracts a replicated table's shared ID from the create query if this is a create query for a replicated table.
/// `replicated_table_shared_id` can be null if you don't need that.
void adjustCreateQueryForBackup(ASTPtr ast, const ContextPtr & global_context, std::optional<String> * replicated_table_shared_id);

/// Visits ASTCreateQuery and changes it to a form which is appropriate or suitable for saving in a backup.
class DDLAdjustingForBackupVisitor
{
public:
    struct Data
    {
        ASTPtr create_query;
        ContextPtr global_context;
        std::optional<String> * replicated_table_shared_id = nullptr;
    };

    using Visitor = InDepthNodeVisitor<DDLAdjustingForBackupVisitor, false>;

    static bool needChildVisit(const ASTPtr & ast, const ASTPtr & child);
    static void visit(ASTPtr ast, const Data & data);
};

}
