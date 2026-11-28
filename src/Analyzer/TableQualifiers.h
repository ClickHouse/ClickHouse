#pragma once

#include <base/types.h>

#include <Core/Block_fwd.h>
#include <IO/WriteBuffer.h>

#include <unordered_set>

namespace DB
{

/// Erase the numeric index of every analyzer-generated table qualifier in a rendered column name, rewriting
/// `__tableN.<tail>` to `__table.<tail>`.
///
/// `createUniqueAliasesIfNecessary` gives every table expression an alias `__tableN`, and
/// `GlobalPlannerContext::buildColumnIdentifier` renders a column of that table as `__tableN.<backQuoteIfNeed(column)>`.
/// The numbering is per query tree and restarts at 1 whenever a tree is rebuilt (`buildQueryTreeForShard` does exactly
/// that), so the same column is `__table1.x` in one build and `__tableK.x` in another while the rest of the name is
/// identical. Erasing just the index makes the two comparable and keeps everything that identifies the column - above
/// all the column name itself.
///
/// `genuine_tails` is the closed set of column-name tails collected structurally from the query tree (see
/// `CollectGenuineQualifierTailsVisitor`). When it is given, a `__tableN.` whose tail is not in it is user text - a
/// `'__table1.'` string constant, a lambda argument named `__table1.` - and is left untouched. Pass `nullptr` where
/// the query tree is not available and normalizing such text cannot cause harm on its own, e.g. for a hash that also
/// mixes in each node's type and each constant's value, which keep two such names apart regardless.
String normalizeGeneratedTableQualifiers(const String & name, const std::unordered_set<String> * genuine_tails = nullptr);

/// Write a column name that a query plan step carries into a build-independent cache key.
///
/// A name alone is both too weak and too strong. `normalizeGeneratedTableQualifiers` erases the qualifier
/// index on purpose, so `__table1.id` and `__table2.id` - the same column name from two different join
/// inputs - normalize to one string; and a rendered name can differ between plan builds for the same
/// column once the shard rewrite inlines an alias. So where the step's input header resolves the column,
/// its POSITION in that header is written and the name is left out: the position is structural, equal in
/// both builds, and unique within the header. A name the header does not resolve (a derived aggregate
/// label, say) gets a sentinel position and its normalized text.
void writeCacheKeyColumnName(const String & name, const Block * input_header, WriteBuffer & out);


}
