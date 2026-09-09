#pragma once

#include <Columns/IColumn.h>
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
  * Callers: the `arrayIndex.h` functions (`has`, `indexOf`, `countEqual`, `indexOfAssumeSorted`),
  * and `MergeTreeIndexBloomFilter`, which must agree with them or it prunes granules holding rows
  * the function would match.
  */

/// Whether comparing values of these two types ignores trailing zero bytes. Recurses into `Tuple`,
/// which `equals` decomposes element-wise, but not into `Array` or `Map`, which `equals` compares
/// through a lossy cast and so does not apply the rule to.
bool zeroPaddedStringComparison(const DataTypePtr & left, const DataTypePtr & right);

/// The canonical form of a value under that rule.
inline std::string_view stripTrailingZeros(std::string_view value)
{
    size_t size = value.size();
    while (size && value[size - 1] == '\0')
        --size;
    return value.substr(0, size);
}

/// Rewrites every `String` value reachable in `column` into its canonical form, recursing through
/// `Const`, `Nullable` and `Tuple` — matching `zeroPaddedStringComparison`.
/// Apply to both operands after a cast to their common type, which strips all trailing '\0' from
/// FixedString but leaves String untouched.
ColumnPtr stripTrailingZerosInStrings(const ColumnPtr & column, const DataTypePtr & type);

/// As above, for the elements of an array column: `hasAny`/`hasAll` compare elements of their two
/// array arguments, so the rule applies one level inside each.
ColumnPtr stripTrailingZerosInArrayElements(const ColumnPtr & column, const DataTypePtr & element_type);

}
