#pragma once

#include <Common/StringValueFilter.h>

#include <optional>

namespace DB
{

class ActionsDAG;
class Field;

/** Extracts filters on individual string values from substring search conditions
  * (`like`, `position`, `startsWith`, `endsWith`, `equals`) on `String` and `Nullable(String)`
  * columns which are conjuncts of the given filter expression (typically PREWHERE).
  *
  * Only conditions that never match an empty string are extracted, so that a reader may replace
  * non-matching values with empty strings during the scan (see `StringValueFilter`). This is only
  * correct if the filter expression is guaranteed to be applied to the read rows afterwards.
  *
  * `row_level_filter` is the row policy expression that the readers evaluate on the same scanned
  * columns before the filter expression, if any: a column that it reads is never filtered during
  * the scan, because it would observe the substituted empty strings.
  *
  * Returns nullptr if there are no suitable conditions.
  */
StringValueFiltersPtr extractStringValueFilters(
    const ActionsDAG & filter_dag, const String & filter_column_name, const ActionsDAG * row_level_filter = nullptr);

/// Whether the filter expression rejects every row in which the `String` column `column_name` is empty,
/// i.e. one of its conjuncts is `s != ''`, `notEmpty(s)`, `s > ''` or a condition that `extractStringValueFilters` extracts.
bool filterRejectsEmptyString(const ActionsDAG & filter_dag, const String & filter_column_name, const String & column_name);
/// Same for a filter DAG whose only output is the filter.
bool filterRejectsEmptyString(const ActionsDAG & filter_dag, const String & column_name);

/// Whether a LIKE pattern contains fixed substrings that can be used as a string value filter.
bool likePatternHasStringValueFilterConditions(const String & pattern);

/// Evaluates the result of a comparison of `position(...)` with a constant assuming that `position`
/// returned 0 (i.e. the needle is not contained in the value). If the result is false, the condition
/// rejects all values that do not contain the needle, so the needle can be used as a filter.
/// Returns std::nullopt for unsupported comparison functions or constant types.
std::optional<bool> evaluatePositionComparisonAtZero(const String & function_name, const Field & constant, bool position_is_left_argument);

}
