#pragma once

#include <Common/StringValueFilter.h>

namespace DB
{

class ActionsDAG;

/** Extracts filters on individual string values from substring search conditions
  * (`like`, `position`, `startsWith`, `endsWith`, `equals`) on `String` and `Nullable(String)`
  * columns which are conjuncts of the given filter expression (typically PREWHERE).
  *
  * Only conditions that never match an empty string are extracted, so that a reader may replace
  * non-matching values with empty strings during the scan (see `StringValueFilter`). This is only
  * correct if the filter expression is guaranteed to be applied to the read rows afterwards.
  *
  * Returns nullptr if there are no suitable conditions.
  */
StringValueFiltersPtr extractStringValueFilters(const ActionsDAG & filter_dag, const String & filter_column_name);

}
