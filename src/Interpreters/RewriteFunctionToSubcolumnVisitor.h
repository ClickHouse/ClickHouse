#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class ASTFunction;
class ASTIdentifier;

/// Collects info about identifiers to select columns to optimize to subcolumns.
class RewriteFunctionToSubcolumnFirstPassMatcher
{
public:
    struct Data
    {
        explicit Data(StorageMetadataPtr metadata_snapshot_) : metadata_snapshot(std::move(metadata_snapshot_)) {}

        StorageMetadataPtr metadata_snapshot;
        std::unordered_map<String, UInt64> indentifiers_count;
        std::unordered_map<String, UInt64> optimized_identifiers_count;
    };

    static void visit(const ASTPtr & ast, Data & data);
    static void visit(const ASTFunction & function, Data & data);
    static bool needChildVisit(ASTPtr & , ASTPtr &) { return true; }
};

using RewriteFunctionToSubcolumnFirstPassVisitor = InDepthNodeVisitor<RewriteFunctionToSubcolumnFirstPassMatcher, true>;

/// Rewrites functions to subcolumns, if possible, to reduce amount of read data.
/// E.g. 'length(arr)' -> 'arr.size0', 'col IS NULL' -> 'col.null'
class RewriteFunctionToSubcolumnSecondPassData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;

    RewriteFunctionToSubcolumnSecondPassData(StorageMetadataPtr metadata_snapshot_, NameSet identifiers_to_optimize_)
        : metadata_snapshot(std::move(metadata_snapshot_)), identifiers_to_optimize(std::move(identifiers_to_optimize_))
    {
    }

    StorageMetadataPtr metadata_snapshot;
    NameSet identifiers_to_optimize;
};

using RewriteFunctionToSubcolumnSecondPassMatcher = OneTypeMatcher<RewriteFunctionToSubcolumnSecondPassData>;
using RewriteFunctionToSubcolumnSecondPassVisitor = InDepthNodeVisitor<RewriteFunctionToSubcolumnSecondPassMatcher, true>;

}
