#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

class ASTFunction;

class RewriteMapElementConstKeyToShardData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;

    StorageSnapshotPtr storage_snapshot;
};

using RewriteMapElementConstKeyToShardMatcher = OneTypeMatcher<RewriteMapElementConstKeyToShardData>;
using RewriteMapElementConstKeyToShardVisitor = InDepthNodeVisitor<RewriteMapElementConstKeyToShardMatcher, true>;

}
