#include <DataTypes/EnumValues.h>
#include <boost/algorithm/string.hpp>
#include <base/sort.h>
#include <Core/Field.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int EMPTY_DATA_PASSED;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
}

template <typename T>
EnumValues<T>::EnumValues(const Values & values_)
    : values(values_)
{
    if (values.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "DataTypeEnum enumeration cannot be empty");

    /// Sort values by numeric value (for getValues() compatibility)
    ::sort(std::begin(values), std::end(values), [](const auto & left, const auto & right)
    {
        return left.second < right.second;
    });

    buildLookupStructures();
}

template <typename T>
EnumValues<T>::~EnumValues() = default;

template <typename T>
void EnumValues<T>::buildLookupStructures()
{
    const size_t n = values.size();

    /// Build name-sorted index for binary search on names
    name_sorted_index.resize(n);
    for (size_t i = 0; i < n; ++i)
        name_sorted_index[i] = static_cast<uint16_t>(i);

    ::sort(name_sorted_index.begin(), name_sorted_index.end(), [this](uint16_t a, uint16_t b)
    {
        return values[a].first < values[b].first;
    });

    /// Check for duplicate names
    for (size_t i = 1; i < n; ++i)
    {
        if (values[name_sorted_index[i - 1]].first == values[name_sorted_index[i]].first)
        {
            const auto & dup_name = values[name_sorted_index[i]].first;
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate names in enum: '{}' = {} and {}",
                    dup_name,
                    toString(values[name_sorted_index[i - 1]].second),
                    toString(values[name_sorted_index[i]].second));
        }
    }

    /// Check for duplicate values (values are already sorted by value)
    for (size_t i = 1; i < n; ++i)
    {
        if (values[i - 1].second == values[i].second)
        {
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate values in enum: '{}' = {} and '{}'",
                    values[i].first, toString(values[i].second), values[i - 1].first);
        }
    }

    /// Find min/max values
    min_value = values.front().second;
    max_value = values.back().second;

    /// Decide strategy for value-to-name lookup
    /// Cast to Int32 first to avoid signed overflow when computing range (e.g., 127 - (-128) for Enum8)
    size_t range = static_cast<size_t>(static_cast<Int32>(max_value) - static_cast<Int32>(min_value)) + 1;

    if constexpr (sizeof(T) == 1)
    {
        /// Enum8: always use direct lookup (max 256 bytes)
        use_direct_value_lookup = true;
    }
    else
    {
        /// Enum16: use direct lookup if range is small enough
        use_direct_value_lookup = (range <= DIRECT_LOOKUP_THRESHOLD);
    }

    if (use_direct_value_lookup)
    {
        /// Build direct lookup array
        value_to_index.resize(range, INVALID_INDEX);
        for (size_t i = 0; i < n; ++i)
        {
            /// Cast to Int32 first to avoid signed overflow (e.g., 1 - (-128) for Enum8)
            size_t idx = static_cast<size_t>(static_cast<Int32>(values[i].second) - static_cast<Int32>(min_value));
            value_to_index[idx] = static_cast<uint16_t>(i);
        }
    }
    else
    {
        /// Build value-sorted index for binary search
        /// Since values are already sorted by value, the index is just 0, 1, 2, ...
        value_sorted_index.resize(n);
        for (size_t i = 0; i < n; ++i)
            value_sorted_index[i] = static_cast<uint16_t>(i);
    }
}

template <typename T>
bool EnumValues<T>::hasValue(T value) const
{
    if (use_direct_value_lookup)
    {
        if (value < min_value || value > max_value)
            return false;
        /// Cast to Int32 first to avoid signed overflow
        size_t idx = static_cast<size_t>(static_cast<Int32>(value) - static_cast<Int32>(min_value));
        return value_to_index[idx] != INVALID_INDEX;
    }
    else
    {
        /// Binary search on values (values are sorted by value)
        size_t lo = 0;
        size_t hi = values.size();
        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            T mid_val = values[mid].second;
            if (mid_val < value)
                lo = mid + 1;
            else if (mid_val > value)
                hi = mid;
            else
                return true;
        }
        return false;
    }
}

template <typename T>
std::string_view EnumValues<T>::getNameForValue(T value) const
{
    std::string_view result;
    if (!getNameForValue(value, result))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unexpected value {} in enum", toString(value));
    return result;
}

template <typename T>
bool EnumValues<T>::getNameForValue(T value, std::string_view & result) const
{
    if (use_direct_value_lookup)
    {
        if (value < min_value || value > max_value)
            return false;
        /// Cast to Int32 first to avoid signed overflow
        size_t arr_idx = static_cast<size_t>(static_cast<Int32>(value) - static_cast<Int32>(min_value));
        uint16_t idx = value_to_index[arr_idx];
        if (idx == INVALID_INDEX)
            return false;
        result = values[idx].first;
        return true;
    }
    else
    {
        /// Binary search on values (values are sorted by value)
        size_t lo = 0;
        size_t hi = values.size();
        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            T mid_val = values[mid].second;
            if (mid_val < value)
                lo = mid + 1;
            else if (mid_val > value)
                hi = mid;
            else
            {
                result = values[mid].first;
                return true;
            }
        }
        return false;
    }
}

template <typename T>
T EnumValues<T>::getValue(std::string_view field_name) const
{
    T x;
    if (tryGetValue(x, field_name))
        return x;

    auto hints = this->getHints(std::string{field_name});
    auto hints_string = !hints.empty() ? ", maybe you meant: " + toString(hints) : "";
    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown element '{}' for enum{}", field_name, hints_string);
}

template <typename T>
bool EnumValues<T>::tryGetValue(T & x, std::string_view field_name) const
{
    /// Binary search on name-sorted index
    size_t lo = 0;
    size_t hi = name_sorted_index.size();
    while (lo < hi)
    {
        size_t mid = lo + (hi - lo) / 2;
        std::string_view mid_name = values[name_sorted_index[mid]].first;

        int cmp = mid_name.compare(field_name);
        if (cmp < 0)
            lo = mid + 1;
        else if (cmp > 0)
            hi = mid;
        else
        {
            x = values[name_sorted_index[mid]].second;
            return true;
        }
    }

    /// Fallback: try parsing as numeric value
    if (tryParse(x, field_name.data(), field_name.size()) && hasValue(x))
        return true;

    return false;
}

template <typename T>
Names EnumValues<T>::getAllRegisteredNames() const
{
    Names result;
    result.reserve(values.size());
    for (const auto & value : values)
        result.emplace_back(value.first);
    return result;
}

template <typename T>
std::unordered_set<String> EnumValues<T>::getSetOfAllNames(bool to_lower) const
{
    std::unordered_set<String> result;
    for (const auto & value : values)
        result.insert(to_lower ? boost::algorithm::to_lower_copy(value.first) : value.first);
    return result;
}

template <typename T>
std::unordered_set<T> EnumValues<T>::getSetOfAllValues() const
{
    std::unordered_set<T> result;
    for (const auto & value : values)
        result.insert(value.second);
    return result;
}

template <typename T>
template <typename TValues>
bool EnumValues<T>::containsAll(const TValues & rhs_values) const
{
    auto check = [&](const auto & value)
    {
        /// Try to find by name using binary search
        T found_value;
        if (tryGetValue(found_value, value.first))
        {
            /// If we have this name, it should have the same value
            return found_value == value.second;
        }
        /// If we don't have this name, check if the value exists
        return hasValue(static_cast<T>(value.second));
    };

    return std::all_of(rhs_values.begin(), rhs_values.end(), check);
}

template <typename T>
size_t EnumValues<T>::memoryUsage() const
{
    size_t total = sizeof(*this);

    /// values vector
    total += values.capacity() * sizeof(Value);
    for (const auto & v : values)
        total += v.first.capacity();

    /// name_sorted_index
    total += name_sorted_index.capacity() * sizeof(uint16_t);

    /// value_to_index or value_sorted_index
    if (use_direct_value_lookup)
        total += value_to_index.capacity() * sizeof(uint16_t);
    else
        total += value_sorted_index.capacity() * sizeof(uint16_t);

    return total;
}

template bool EnumValues<Int8>::containsAll<EnumValues<Int8>::Values>(const EnumValues<Int8>::Values & rhs_values) const;
template bool EnumValues<Int8>::containsAll<EnumValues<Int16>::Values>(const EnumValues<Int16>::Values & rhs_values) const;
template bool EnumValues<Int16>::containsAll<EnumValues<Int8>::Values>(const EnumValues<Int8>::Values & rhs_values) const;
template bool EnumValues<Int16>::containsAll<EnumValues<Int16>::Values>(const EnumValues<Int16>::Values & rhs_values) const;

template class EnumValues<Int8>;
template class EnumValues<Int16>;

}
