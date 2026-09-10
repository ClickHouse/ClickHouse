#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <string_view>

namespace DB
{

/** `equals` treats the trailing zero bytes of a `FixedString` as padding, so
  * `toFixedString('V0', 3) = toFixedString('V0', 4)` and `toFixedString('V0', 3) = 'V0\0'`. Plain
  * `String` against plain `String` stays length-sensitive: `'V0' != 'V0\0'`.
  *
  * The rule is equality of the values with their trailing zero bytes removed, so removing them from
  * both operands and comparing normally gives the same answer.
  *
  * A value alone cannot say which rule applies. `Field` has no `FixedString` type, and
  * `CAST(FixedString AS String)` removes the padding from only the operand it converts. The declared
  * types are the only place the rule is visible, which is what these functions expose.
  *
  * Callers: `FunctionComparison`, which compares an `Array`, a `Map` or a `Tuple` through a cast to
  * the common type of its operands; the `arrayIndex.h` functions (`has`, `indexOf`, `countEqual`,
  * `indexOfAssumeSorted`); and `MergeTreeIndexBloomFilter`, which must agree with them or it prunes
  * granules holding rows the function would match.
  */

/// Whether comparing values of these two types ignores trailing zero bytes. Recurses into `Tuple`,
/// `Array` and `Map`, so a `FixedString` nested at any depth is compared the same way as a
/// top-level one.
bool zeroPaddedStringComparison(const DataTypePtr & left, const DataTypePtr & right);

/// Whether a search constant of this type is subject to the rule, and so has no single canonical
/// spelling among the values it matches. A skip index that stores one hash or one term per stored
/// value cannot probe all of them, so it must decline rather than prune a matching granule.
/// Recurses into `Array` for the constant of `has`/`hasAny`/`hasAll`.
bool zeroPaddedStringConstant(const DataTypePtr & type);

/// A copy of `field` with the trailing zero padding removed from every `FixedString` value it
/// carries, recursing into `Array`. `String = FixedString(N)` ignores that padding, so the search
/// terms of a skip index have to be taken from the value without it.
Field stripFixedStringPaddingForTerms(const Field & field, const DataTypePtr & type);

/// The type whose values become the terms or hashes of a skip index: for an array-typed indexed
/// column, its element type. `Nullable` and `LowCardinality` wrappers are removed.
DataTypePtr indexedElementType(const DataTypePtr & type);

/// The canonical form of a value under that rule.
inline std::string_view stripTrailingZeros(std::string_view value)
{
    size_t size = value.size();
    while (size && value[size - 1] == '\0')
        --size;
    return value.substr(0, size);
}

/// Rewrites every `String` value reachable in `column` into its canonical form, recursing through
/// `Const`, `Nullable`, `Tuple`, `Array` and `Map` — matching `zeroPaddedStringComparison`.
/// Apply to both operands after a cast to their common type, which strips all trailing '\0' from
/// FixedString but leaves String untouched.
ColumnPtr stripTrailingZerosInStrings(const ColumnPtr & column, const DataTypePtr & type);

/// As above, for the elements of an array column: `hasAny`/`hasAll` compare elements of their two
/// array arguments, so the rule applies one level inside each.
ColumnPtr stripTrailingZerosInArrayElements(const ColumnPtr & column, const DataTypePtr & element_type);

}
