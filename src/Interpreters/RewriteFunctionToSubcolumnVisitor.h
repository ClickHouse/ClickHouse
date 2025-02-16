#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class ASTFunction;

/// Rewrites functions to subcolumns, if possible, to reduce amount of read data.
/// E.g. 'length(arr)' -> 'arr.size0', 'col IS NULL' -> 'col.null'
class RewriteFunctionToSubcolumnData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;

    StorageMetadataPtr metadata_snapshot;
};

using RewriteFunctionToSubcolumnMatcher = OneTypeMatcher<RewriteFunctionToSubcolumnData>;
using RewriteFunctionToSubcolumnVisitor = InDepthNodeVisitor<RewriteFunctionToSubcolumnMatcher, true>;

}
