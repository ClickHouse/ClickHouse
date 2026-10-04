#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/** Returns true if from_type can be safely cast to to_type.
  *
  * "Safely" means every value of from_type is representable in to_type, including NULL: a target that
  * cannot hold a NULL is not a safe destination for a source that can produce one.
  *
  * Examples:
  * From type UInt8 to type UInt16 returns true.
  * From type UInt16 to type UInt8 returns false.
  * From type String to type LowCardinality(String) returns true.
  * From type LowCardinality(String) to type String returns true.
  * From type String to type UInt8 returns false.
  */
bool canBeSafelyCast(const DataTypePtr & from_type, const DataTypePtr & to_type);

/// Whether a value of this type is converted to a `Field` that no longer tells which type the
/// value actually has. A `Variant` flattens a row to the `Field` of its active alternative
/// (`ColumnVariant::operator []`) and `DataTypeVariant::equals` allows several aggregate-state
/// alternatives that are compatible by state representation; `Dynamic` and `JSON` similarly store
/// values of types that are not fixed by the column type. Two values on different alternatives can
/// then produce equal `Field`s although the alternative itself is a part of the value and is
/// observable (e.g. by `variantType`).
///
/// Only these three types are checked, not `IDataType::hasDynamicSubcolumns`: the latter is also
/// true for a plain `Map`, which merely exposes the `m.keys` and `m.values` virtual subcolumns while
/// the type of every value it holds is still fixed by the declared `Map(K, V)`.
bool typeCanHideTheValueType(const IDataType & type);

}
