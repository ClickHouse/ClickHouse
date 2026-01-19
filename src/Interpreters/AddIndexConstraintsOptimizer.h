#pragma once

#include <memory>

namespace DB
{

class CNFQuery;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Optimizer that extracts constraints that
/// depends only on columns of primary key
/// and tries to add function 'indexHint' to
/// WHERE clause, which reduces amount of read data.
class AddIndexConstraintsOptimizer final
{
public:
    explicit AddIndexConstraintsOptimizer(const StorageMetadataPtr & metadata_snapshot);

    void perform(CNFQuery & cnf_query);

private:
    const StorageMetadataPtr & metadata_snapshot;
};

}
