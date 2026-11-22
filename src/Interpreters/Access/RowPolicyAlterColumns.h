#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <memory>

namespace DB
{

class AlterCommands;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// throws if the alter drops or renames a column a row policy of the table reads, or modifies it so that the policy breaks
void checkRowPoliciesBeforeAlter(
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    const AlterCommands & commands,
    bool share_nested_offsets,
    const ContextPtr & context);

}
