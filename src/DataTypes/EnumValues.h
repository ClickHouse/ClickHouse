#pragma once

#include <unordered_set>
#include <string>
#include <string_view>
#include <Core/Names.h>
#include <Common/NamePrompter.h>


namespace DB
{

/// Compact enum storage with efficient lookups.
/// - Strings stored in values vector (sorted by value for compatibility)
/// - Name-to-value: binary search on sorted name index (O(log N))
/// - Value-to-name: direct array lookup (O(1)) for small ranges, binary search for large ranges
template <typename T>
class EnumValues : public IHints<>
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;

private:
    /// Original values sorted by numeric value (for getValues() compatibility)
    Values values;

    /// Index into values, sorted by name (for binary search on names)
    std::vector<uint16_t> name_sorted_index;

    /// Value-to-name lookup strategy
    /// For Enum8: always direct (max 256 bytes)
    /// For Enum16: direct if range <= 4096, else binary search
    bool use_direct_value_lookup = true;

    /// Direct lookup array: value_to_index[value - min_value] = index into values
    /// Only used when use_direct_value_lookup = true
    T min_value{};
    T max_value{};
    std::vector<uint16_t> value_to_index;

    /// Sorted indices for binary search on values (used when use_direct_value_lookup = false)
    /// value_sorted_index[i] = index into values, sorted by value
    std::vector<uint16_t> value_sorted_index;

    static constexpr uint16_t INVALID_INDEX = 65535;
    static constexpr size_t DIRECT_LOOKUP_THRESHOLD = 4096;

    void buildLookupStructures();

    /// Binary search helpers
    std::string_view getNameAt(size_t idx) const { return values[idx].first; }
    T getValueAt(size_t idx) const { return values[idx].second; }

public:
    explicit EnumValues(const Values & values_);
    ~EnumValues() override;

    const Values & getValues() const { return values; }

    /// Check if value exists in enum
    bool hasValue(T value) const;

    /// Get name for value, throws if not found
    std::string_view getNameForValue(T value) const;

    /// Get name for value, returns false if not found
    bool getNameForValue(T value, std::string_view & result) const;

    /// Get value for name, throws if not found
    T getValue(std::string_view field_name) const;

    /// Get value for name, returns false if not found
    bool tryGetValue(T & x, std::string_view field_name) const;

    template <typename TValues>
    bool containsAll(const TValues & rhs_values) const;

    Names getAllRegisteredNames() const override;

    std::unordered_set<String> getSetOfAllNames(bool to_lower) const;

    std::unordered_set<T> getSetOfAllValues() const;

    /// Memory usage estimation
    size_t memoryUsage() const;
};

}
