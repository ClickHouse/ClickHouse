#pragma once

#include <Interpreters/IInterpreter.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class Context;
class MergeTreeData;

/// the real `ALTER TABLE ... ADD INDEX` or `ADD PROJECTION` validation, shared with `EXPLAIN WHATIF`
/// so the two cannot drift apart
void checkHypotheticalObjectIsAddable(
    const MergeTreeData & merge_tree,
    const StorageMetadataPtr & metadata,
    const ASTPtr & declaration,
    const ContextPtr & context);

class InterpreterHypotheticalObjectQuery : public IInterpreter, WithContext
{
public:
    InterpreterHypotheticalObjectQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithContext(context_)
        , query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
