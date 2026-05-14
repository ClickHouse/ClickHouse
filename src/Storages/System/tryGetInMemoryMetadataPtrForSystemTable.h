#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

/// Returns `nullptr` when `getInMemoryMetadataPtr` throws.
///
/// Intended exclusively for `system.*` tables that iterate user storages and may encounter a
/// dangling `Alias` whose target table was dropped (which surfaces as `UNKNOWN_TABLE`).
///
/// Note, not an IStorage interface, since system tables are the only user.
StorageMetadataPtr tryGetInMemoryMetadataPtrForSystemTable(const StoragePtr & storage, ContextPtr context);

}
