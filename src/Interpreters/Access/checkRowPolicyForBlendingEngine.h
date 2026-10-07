#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class IStorage;
struct StorageID;

/// `storage` itself or a storage it reads through (see IStorage::getUnderlyingStorages) whose merges blend rows, or nullptr
const IStorage * findBlendingEngine(const IStorage & storage);

/// Throws if a row policy for `table_id` applies to `storage` while it reads a blending engine,
/// unless `allow_suspicious_row_policies_with_blending_engines` is set
void checkNoRowPolicyForBlendingEngine(const IStorage & storage, const StorageID & table_id, bool include_database_policies, const ContextPtr & context);

}
