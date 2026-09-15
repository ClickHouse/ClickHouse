#pragma once

#include <DataTypes/IDataType.h>

#include <functional>


namespace DB
{

/** Recursive walks over a data type and the types nested in it.
  *
  * Every walk here is written in terms of `IDataType::getChildren` and `IDataType::cloneWithChildren`,
  * so a data type becomes visible to all of them at once by implementing those two methods, and the
  * traversal order is decided in one place instead of once per walk.
  */

/// Applies `callback` to `type` and to every type below it, in pre-order - the root first.
void forEachInTypeTree(const IDataType & type, const std::function<void(const IDataType &)> & callback);

/// True when `type` itself or any type below it satisfies `predicate`.
/// Stops at the first match without visiting the rest of the tree.
bool anyInTypeTree(const IDataType & type, const std::function<bool(const IDataType &)> & predicate);

/// What to do with a customization - a custom name such as `Point` or `SimpleAggregateFunction(...)`,
/// and/or a custom serialization - that `IDataTypeCustomName::rederiveFor` could not carry over to the
/// rewritten children. A customization is observable: it is what `toTypeName`, `DESCRIBE` and the
/// binary type encoding report, and it decides how the column is read back, so a type that quietly
/// loses one announces something it is not.
enum class CustomizationPolicy : uint8_t
{
    /// Leave that subtree exactly as it was and keep rewriting the rest of the tree.
    Keep,
    /// Abort the whole rewrite and return nullptr, for a caller that cannot use a partial result.
    Refuse,
};

/// Called for every node once its children have been rewritten. Returns the replacement for the node,
/// the node itself when there is nothing to replace, or nullptr to abort the whole rewrite.
using TypeTreeRewriteFn = std::function<DataTypePtr(const DataTypePtr &)>;

/// Rebuilds `type` bottom-up.
///
/// A subtree in which nothing changed keeps its original type object, so its customizations survive
/// without consulting `policy` at all; only a subtree that actually moved is rebuilt through
/// `IDataType::cloneWithChildren`, and only then can a customization need re-deriving.
///
/// Returns nullptr when `callback` aborted, or when `policy` is `Refuse` and a customization could not
/// follow the rewrite. With `Keep` the result is never nullptr unless `callback` made it so.
DataTypePtr rewriteTypeTree(const DataTypePtr & type, const TypeTreeRewriteFn & callback, CustomizationPolicy policy);

}
