#include <Common/StringValueFilter.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringSearcher.h>

#include <cstring>

namespace ProfileEvents
{
    extern const Event StringValueFilterValuesChecked;
    extern const Event StringValueFilterValuesReplaced;
    extern const Event StringValueFilterBytesSkipped;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StringValueFilter::StringValueFilter(std::vector<Condition> conditions_)
    : conditions(std::move(conditions_))
{
    if (conditions.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StringValueFilter must have at least one condition");

    searchers.resize(conditions.size());
    for (size_t i = 0; i < conditions.size(); ++i)
    {
        if (conditions[i].needle.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "StringValueFilter needle must be non-empty");

        if (conditions[i].type == Condition::Type::Substring)
        {
            searchers[i] = std::make_unique<CaseSensitiveStringSearcher>(conditions[i].needle.data(), conditions[i].needle.size());

            if (bulk_scan_condition == SIZE_MAX || conditions[i].needle.size() > conditions[bulk_scan_condition].needle.size())
                bulk_scan_condition = i;
        }
    }
}

StringValueFilter::~StringValueFilter() = default;

void StringValueFilter::findBulkScanMatches(
    const char * buffer,
    size_t buffer_original_start,
    const UInt64 * offsets,
    size_t first_value,
    size_t last_value,
    std::vector<size_t> & matches) const
{
    const auto & searcher = *searchers[bulk_scan_condition];
    const size_t needle_size = conditions[bulk_scan_condition].needle.size();

    const char * end = buffer + (offsets[last_value - 1] - buffer_original_start);
    const char * pos = buffer;
    size_t candidate = first_value;

    while (pos < end)
    {
        const auto * found = searcher.search(reinterpret_cast<const UInt8 *>(pos), reinterpret_cast<const UInt8 *>(end));
        if (found == reinterpret_cast<const UInt8 *>(end))
            break;

        size_t found_original = buffer_original_start + (reinterpret_cast<const char *>(found) - buffer);

        /// The value containing the beginning of the occurrence.
        while (offsets[candidate] <= found_original)
            ++candidate;

        if (found_original + needle_size <= offsets[candidate])
        {
            matches.push_back(candidate);
            /// Continue from the end of this value: more occurrences in it are redundant.
            pos = buffer + (offsets[candidate] - buffer_original_start);
        }
        else
        {
            /// The occurrence crosses a boundary between values: not a match.
            pos = reinterpret_cast<const char *>(found) + 1;
        }
    }
}


bool StringValueFilter::matchImpl(const char * data, size_t size, size_t skip_condition) const
{
    for (size_t i = 0; i < conditions.size(); ++i)
    {
        if (i == skip_condition)
            continue;

        const auto & condition = conditions[i];
        const auto & needle = condition.needle;

        if (size < needle.size())
            return false;

        switch (condition.type)
        {
            case Condition::Type::Substring:
            {
                const auto * begin = reinterpret_cast<const UInt8 *>(data);
                const auto * end = begin + size;
                if (searchers[i]->search(begin, end) == end)
                    return false;
                break;
            }
            case Condition::Type::Prefix:
            {
                if (memcmp(data, needle.data(), needle.size()) != 0)
                    return false;
                break;
            }
            case Condition::Type::Suffix:
            {
                if (memcmp(data + size - needle.size(), needle.data(), needle.size()) != 0)
                    return false;
                break;
            }
            case Condition::Type::Equals:
            {
                if (size != needle.size() || memcmp(data, needle.data(), needle.size()) != 0)
                    return false;
                break;
            }
        }
    }

    return true;
}

bool StringValueFilter::match(const char * data, size_t size) const
{
    return matchImpl(data, size, SIZE_MAX);
}

bool StringValueFilter::matchOtherConditions(const char * data, size_t size) const
{
    return matchImpl(data, size, bulk_scan_condition);
}

void StringValueFilter::updateStats(size_t checked, size_t replaced, size_t replaced_bytes) const
{
    if (!checked)
        return;

    ProfileEvents::increment(ProfileEvents::StringValueFilterValuesChecked, checked);
    ProfileEvents::increment(ProfileEvents::StringValueFilterValuesReplaced, replaced);
    ProfileEvents::increment(ProfileEvents::StringValueFilterBytesSkipped, replaced_bytes);

    size_t total_checked = values_checked.fetch_add(checked, std::memory_order_relaxed) + checked;
    size_t total_replaced = values_replaced.fetch_add(replaced, std::memory_order_relaxed) + replaced;

    /// If most of the values match the filter, checking them does not pay off:
    /// the query will process almost all values anyway, and we would only add
    /// the cost of an extra search per value.
    if (total_checked >= MIN_VALUES_TO_EVALUATE_SELECTIVITY && total_replaced * 2 < total_checked)
        disabled.store(true, std::memory_order_relaxed);
}

}
