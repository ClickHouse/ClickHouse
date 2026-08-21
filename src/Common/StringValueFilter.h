#pragma once

#include <base/types.h>

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

namespace DB
{

class CaseSensitiveStringSearcher;

/** A filter on individual string values, extracted from substring search conditions
  * (`like`, `position`, `startsWith`, `endsWith`, `equals`) of a PREWHERE expression.
  *
  * It is used to optimize reading: when a value does not pass the filter, the reader is allowed
  * to produce an empty string instead of the value, avoiding a copy of its data. This is correct because:
  * - the conditions the filter was built from never match an empty string (all needles are non-empty);
  * - the condition is a conjunct of the PREWHERE expression, and the PREWHERE filter is guaranteed
  *   to be applied to the read rows, so every row with a non-matching value is filtered out,
  *   and replacing its value with an empty string cannot change the query result.
  *
  * A single filter object is shared by all readers of a column within one query. It collects
  * statistics of how selective it is and disables itself when the fraction of matching values
  * is too high for the check to pay off (the readers then simply read all values as usual).
  */
class StringValueFilter
{
public:
    struct Condition
    {
        enum class Type : UInt8
        {
            Substring,  /// value contains the needle
            Prefix,     /// value starts with the needle
            Suffix,     /// value ends with the needle
            Equals,     /// value equals the needle
        };

        Type type;
        String needle;
    };

    /// All needles must be non-empty, so that an empty string never matches the filter.
    explicit StringValueFilter(std::vector<Condition> conditions_);
    ~StringValueFilter();

    /// Returns false if the filter turned out to be non-selective and should not be applied anymore.
    bool isEnabled() const { return !disabled.load(std::memory_order_relaxed); }

    /// Returns true if the value matches all conditions.
    bool match(const char * data, size_t size) const;
    bool match(const UInt8 * data, size_t size) const { return match(reinterpret_cast<const char *>(data), size); }

    /// Account a batch of checked values, update profile events and decide whether the filter
    /// is selective enough to keep using it. `replaced` is the number of values that did not
    /// match, `replaced_bytes` is the total size of their data.
    void updateStats(size_t checked, size_t replaced, size_t replaced_bytes) const;

    const std::vector<Condition> & getConditions() const { return conditions; }

private:
    /// After this many values are checked, the filter is disabled if less than half of them were replaced.
    static constexpr size_t MIN_VALUES_TO_EVALUATE_SELECTIVITY = 65536;

    const std::vector<Condition> conditions;
    /// Searchers for Substring conditions (empty entries for other types).
    /// They reference the needles stored in `conditions`, which are never modified.
    std::vector<std::unique_ptr<CaseSensitiveStringSearcher>> searchers;

    mutable std::atomic<size_t> values_checked{0};
    mutable std::atomic<size_t> values_replaced{0};
    mutable std::atomic<bool> disabled{false};
};

using StringValueFilterPtr = std::shared_ptr<const StringValueFilter>;

/// Filters for multiple columns, keyed by the column name.
using StringValueFilters = std::unordered_map<String, StringValueFilterPtr>;
using StringValueFiltersPtr = std::shared_ptr<const StringValueFilters>;

}
