#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class IStorage;
struct StorageID;

/// `storage` itself or a storage it reads through (see IStorage::getUnderlyingStorages, also the target of a
/// materialized view or a `Buffer`, the sources of a `Merge` table and the storage of a table function) whose merges
/// blend rows, or nullptr. Throws if the user of `context` cannot see a table which `storage` can read through,
/// whether the table exists or not.
ConstStoragePtr findBlendingEngine(const IStorage & storage, const ContextPtr & context);

/// Throws if a row policy for `table_id` applies to `storage` while it reads a blending engine,
/// unless `allow_suspicious_row_policies_with_blending_engines` is set or `replaced`, the table which had
/// the name before, read a blending engine too. Resolves the storages only if such a policy exists.
void checkNoRowPolicyForBlendingEngine(
    const IStorage & storage,
    const StorageID & table_id,
    bool include_database_policies,
    const ContextPtr & context,
    const IStorage * replaced = nullptr);

}
