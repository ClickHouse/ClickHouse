#pragma once

#include <Access/EnabledRowPolicies.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class IStorage;

/// SELECT row policies of `storage` combined with those of every storage its rows come from
/// (see IStorage::getUnderlyingStorages). Returns nullptr when no policy applies.
RowPolicyFilterPtr getRowPolicyFilterForStorage(const IStorage & storage, const ContextPtr & context);

/// Same, but nullptr also when the combined filter is always true, i.e. when nothing has to be filtered.
RowPolicyFilterPtr getEffectiveRowPolicyFilter(const IStorage & storage, const ContextPtr & context);

/// The same two, for a table that the query may have reached through a read-only `Overlay` facade:
/// `as_written_id` is the id exactly as written in the query. Reading through a facade requires the
/// grant on both the facade name and the source it resolves to, so the SELECT row policies of both
/// names apply (a row must pass both): for a plain source table `storage` is the source and the
/// facade's policies are combined in; for a parameterized view the synthesized storage keeps the
/// facade name and carries the id of the underlying source view, whose policies are combined in
/// instead. When `as_written_id` does not name a facade these are the plain overloads above.
RowPolicyFilterPtr getRowPolicyFilterForStorage(const IStorage & storage, const StorageID & as_written_id, const ContextPtr & context);
RowPolicyFilterPtr getEffectiveRowPolicyFilter(const IStorage & storage, const StorageID & as_written_id, const ContextPtr & context);

}
