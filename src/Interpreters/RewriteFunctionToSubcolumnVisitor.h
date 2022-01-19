#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class ASTFunction;

/// Collect all identifiers that cannot be optimized to subcolumns.
/// If we can optimize some function to suncolumn, but we have to read
/// its argument's identifier as a whole column in another part of query,
/// we won't optimize, because it doesn't make sense.
class FindIdentifiersForbiddenToReplaceToSubcolumnsMatcher
{
public:
    struct Data
    {
        IdentifierNameSet forbidden_identifiers;
    };

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr &);
};

using FindIdentifiersForbiddenToReplaceToSubcolumnsVisitor = InDepthNodeVisitor<FindIdentifiersForbiddenToReplaceToSubcolumnsMatcher, true>;

/// Rewrites functions to subcolumns, if possible, to reduce amount of read data.
/// E.g. 'length(arr)' -> 'arr.size0', 'col IS NULL' -> 'col.null'
class RewriteFunctionToSubcolumnData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;

    StorageMetadataPtr metadata_snapshot;
    IdentifierNameSet forbidden_identifiers;
};

using RewriteFunctionToSubcolumnMatcher = OneTypeMatcher<RewriteFunctionToSubcolumnData>;
using RewriteFunctionToSubcolumnVisitor = InDepthNodeVisitor<RewriteFunctionToSubcolumnMatcher, true>;

}
