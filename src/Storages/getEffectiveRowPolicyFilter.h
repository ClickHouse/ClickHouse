#pragma once

#include <Access/EnabledRowPolicies.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class IStorage;

/// SELECT row policies of `storage` combined with those of every storage its rows come from
/// (see IStorage::getUnderlyingStorages). Returns nullptr when no policy applies.
RowPolicyFilterPtr getRowPolicyFilterForStorage(const IStorage & storage, const ContextPtr & context);

/// Same, but nullptr also when the combined filter is always true, i.e. when nothing has to be filtered.
RowPolicyFilterPtr getEffectiveRowPolicyFilter(const IStorage & storage, const ContextPtr & context);

/// Throws `BAD_ARGUMENTS` if the row policy filter of `storage` contains a column matcher, e.g. `t.*`. A qualified
/// matcher over a `Tuple` column of the storage, e.g. `tup.*`, expands into the elements of the column and is allowed.
void checkRowPolicyFilterMatchersAgainstStorage(const ASTPtr & filter, const IStorage & storage, const ContextPtr & context);

}
