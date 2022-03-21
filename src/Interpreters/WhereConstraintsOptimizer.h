#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTSelectQuery;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Optimizer that can remove useless parts of conditions
/// in WHERE clause according to table constraints.
class WhereConstraintsOptimizer final
{
public:
    WhereConstraintsOptimizer(
        ASTSelectQuery * select_query,
        const StorageMetadataPtr & metadata_snapshot,
        bool optimize_append_index_);

    void perform();

private:
    ASTSelectQuery * select_query;
    const StorageMetadataPtr & metadata_snapshot;
    bool optimize_append_index;
};

}
