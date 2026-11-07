#pragma once

#include <Access/EnabledRowPolicies.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IStorage;

/// SELECT row policies of `storage` combined with those of every storage its rows come from
/// (see IStorage::getUnderlyingStorages). Returns nullptr when no policy applies.
RowPolicyFilterPtr getRowPolicyFilterForStorage(const IStorage & storage, const ContextPtr & context, RowPolicyFilterPtr * own_filter = nullptr);

/// Same, but nullptr also when the combined filter is always true, i.e. when nothing has to be filtered.
/// If `own_filter` is set, it receives the policies of `storage` itself exactly as they were combined into
/// the result (policies can change concurrently, so a second lookup may return different ones).
RowPolicyFilterPtr getEffectiveRowPolicyFilter(const IStorage & storage, const ContextPtr & context, RowPolicyFilterPtr * own_filter = nullptr);

}
