#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Common/HashTable/HashMap.h>


namespace DB::Benchmark
{

/// Type alias for enum values (name, numeric value pairs)
template <typename T>
using EnumValue = std::pair<std::string, T>;

template <typename T>
using EnumValues = std::vector<EnumValue<T>>;


// ============================================================================
// Implementation 1: Current Implementation (baseline)
// Uses: vector<pair<string, T>> + HashMap<string_view, T> + unordered_map<T, string_view>
// ============================================================================

template <typename T>
class EnumStorageCurrent
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;

private:
    class NameToValueMap : public HashMap<std::string_view, T, StringViewHash> {};
    using ValueToNameMap = std::unordered_map<T, std::string_view>;

    Values values;
    std::unique_ptr<NameToValueMap> name_to_value_map;
    ValueToNameMap value_to_name_map;

    void fillMaps()
    {
        for (const auto & name_and_value : values)
        {
            name_to_value_map->insert({std::string_view{name_and_value.first}, name_and_value.second});
            value_to_name_map.insert({name_and_value.second, std::string_view{name_and_value.first}});
        }
    }

public:
    explicit EnumStorageCurrent(const Values & values_)
        : values(values_)
        , name_to_value_map(std::make_unique<NameToValueMap>())
    {
        std::sort(values.begin(), values.end(), [](const auto & a, const auto & b) {
            return a.second < b.second;
        });
        fillMaps();
    }

    const Values & getValues() const { return values; }

    std::optional<T> tryGetValue(std::string_view name) const
    {
        if (auto it = name_to_value_map->find(name); it != name_to_value_map->end())
            return it->getMapped();
        return std::nullopt;
    }

    std::optional<std::string_view> tryGetName(T value) const
    {
        if (auto it = value_to_name_map.find(value); it != value_to_name_map.end())
            return it->second;
        return std::nullopt;
    }

    size_t size() const { return values.size(); }

    /// Estimate memory usage
    size_t memoryUsage() const
    {
        size_t total = sizeof(*this);
        // Values vector
        total += values.capacity() * sizeof(Value);
        for (const auto & v : values)
            total += v.first.capacity();
        // name_to_value_map (rough estimate)
        total += name_to_value_map->getBufferSizeInBytes();
        // value_to_name_map (rough estimate: bucket count * pointer + entries)
        total += value_to_name_map.bucket_count() * sizeof(void*);
        total += value_to_name_map.size() * (sizeof(T) + sizeof(std::string_view) + sizeof(void*) * 2);
        return total;
    }
};


// ============================================================================
// Implementation 2: Current with HashMap for value→name
// Uses: vector<pair<string, T>> + HashMap<string_view, T> + HashMap<T, string_view>
// ============================================================================

template <typename T>
class EnumStorageHashMapBoth
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;

private:
    class NameToValueMap : public HashMap<std::string_view, T, StringViewHash> {};
    class ValueToNameMap : public HashMap<T, std::string_view> {};

    Values values;
    std::unique_ptr<NameToValueMap> name_to_value_map;
    std::unique_ptr<ValueToNameMap> value_to_name_map;

    void fillMaps()
    {
        for (const auto & name_and_value : values)
        {
            name_to_value_map->insert({std::string_view{name_and_value.first}, name_and_value.second});
            value_to_name_map->insert({name_and_value.second, std::string_view{name_and_value.first}});
        }
    }

public:
    explicit EnumStorageHashMapBoth(const Values & values_)
        : values(values_)
        , name_to_value_map(std::make_unique<NameToValueMap>())
        , value_to_name_map(std::make_unique<ValueToNameMap>())
    {
        std::sort(values.begin(), values.end(), [](const auto & a, const auto & b) {
            return a.second < b.second;
        });
        fillMaps();
    }

    const Values & getValues() const { return values; }

    std::optional<T> tryGetValue(std::string_view name) const
    {
        if (auto it = name_to_value_map->find(name); it != name_to_value_map->end())
            return it->getMapped();
        return std::nullopt;
    }

    std::optional<std::string_view> tryGetName(T value) const
    {
        if (auto it = value_to_name_map->find(value); it != value_to_name_map->end())
            return it->getMapped();
        return std::nullopt;
    }

    size_t size() const { return values.size(); }

    size_t memoryUsage() const
    {
        size_t total = sizeof(*this);
        total += values.capacity() * sizeof(Value);
        for (const auto & v : values)
            total += v.first.capacity();
        total += name_to_value_map->getBufferSizeInBytes();
        total += value_to_name_map->getBufferSizeInBytes();
        return total;
    }
};


// ============================================================================
// Implementation 3: Compact Sorted Array
// Uses: offsets[] + concatenated strings (sorted) + values[] + sorted index for reverse lookup
// ============================================================================

template <typename T>
class EnumStorageCompactSorted
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;

private:
    // Strings sorted lexicographically
    std::vector<uint32_t> offsets;     // offsets[i] = start of string i, offsets[N] = total length
    std::string string_data;            // concatenated strings
    std::vector<T> values_by_name;      // values_by_name[i] = value for i-th string (sorted by name)

    // For reverse lookup (value → name): indices sorted by value
    std::vector<uint16_t> indices_by_value;  // indices into the name-sorted arrays, sorted by their values

    std::string_view getStringAt(size_t idx) const
    {
        return std::string_view(string_data.data() + offsets[idx], offsets[idx + 1] - offsets[idx]);
    }

public:
    explicit EnumStorageCompactSorted(const Values & input_values)
    {
        if (input_values.empty())
            return;

        // Sort by name lexicographically
        Values sorted_by_name = input_values;
        std::sort(sorted_by_name.begin(), sorted_by_name.end(), [](const auto & a, const auto & b) {
            return a.first < b.first;
        });

        size_t n = sorted_by_name.size();
        offsets.reserve(n + 1);
        values_by_name.reserve(n);
        indices_by_value.resize(n);

        // Build offsets and concatenated string
        uint32_t offset = 0;
        for (const auto & [name, value] : sorted_by_name)
        {
            offsets.push_back(offset);
            string_data += name;
            offset += static_cast<uint32_t>(name.size());
            values_by_name.push_back(value);
        }
        offsets.push_back(offset);

        // Build indices sorted by value for reverse lookup
        for (size_t i = 0; i < n; ++i)
            indices_by_value[i] = static_cast<uint16_t>(i);

        std::sort(indices_by_value.begin(), indices_by_value.end(), [this](uint16_t a, uint16_t b) {
            return values_by_name[a] < values_by_name[b];
        });
    }

    std::optional<T> tryGetValue(std::string_view name) const
    {
        if (offsets.size() <= 1)
            return std::nullopt;

        size_t n = offsets.size() - 1;
        size_t lo = 0, hi = n;

        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            std::string_view mid_str = getStringAt(mid);

            if (mid_str < name)
                lo = mid + 1;
            else if (mid_str > name)
                hi = mid;
            else
                return values_by_name[mid];
        }
        return std::nullopt;
    }

    std::optional<std::string_view> tryGetName(T value) const
    {
        if (indices_by_value.empty())
            return std::nullopt;

        // Binary search on values using indices_by_value
        size_t lo = 0, hi = indices_by_value.size();

        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            T mid_value = values_by_name[indices_by_value[mid]];

            if (mid_value < value)
                lo = mid + 1;
            else if (mid_value > value)
                hi = mid;
            else
                return getStringAt(indices_by_value[mid]);
        }
        return std::nullopt;
    }

    size_t size() const { return offsets.empty() ? 0 : offsets.size() - 1; }

    size_t memoryUsage() const
    {
        size_t total = sizeof(*this);
        total += offsets.capacity() * sizeof(uint32_t);
        total += string_data.capacity();
        total += values_by_name.capacity() * sizeof(T);
        total += indices_by_value.capacity() * sizeof(uint16_t);
        return total;
    }

    /// For compatibility - reconstruct values vector
    Values getValues() const
    {
        Values result;
        result.reserve(size());
        for (size_t i = 0; i < size(); ++i)
            result.emplace_back(std::string(getStringAt(i)), values_by_name[i]);

        // Sort by value to match original behavior
        std::sort(result.begin(), result.end(), [](const auto & a, const auto & b) {
            return a.second < b.second;
        });
        return result;
    }
};


// ============================================================================
// Implementation 4: Compact with Direct Array (Enum8 only)
// Uses: Same as #3 but with direct 256-entry array for O(1) value→name lookup
// ============================================================================

class EnumStorageCompactDirectArray
{
public:
    using T = int8_t;
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;

private:
    // Strings sorted lexicographically
    std::vector<uint16_t> offsets;      // offsets[i] = start of string i
    std::string string_data;             // concatenated strings
    std::vector<T> values_by_name;       // values_by_name[i] = value for i-th string

    // Direct array for O(1) reverse lookup: index 0-255 maps to value -128 to 127
    // value_to_index[value + 128] = index into name arrays, or 255 if not present
    std::array<uint8_t, 256> value_to_index;

    static constexpr uint8_t INVALID_INDEX = 255;

    std::string_view getStringAt(size_t idx) const
    {
        return std::string_view(string_data.data() + offsets[idx], offsets[idx + 1] - offsets[idx]);
    }

public:
    explicit EnumStorageCompactDirectArray(const Values & input_values)
    {
        value_to_index.fill(INVALID_INDEX);

        if (input_values.empty())
            return;

        // Sort by name lexicographically
        Values sorted_by_name = input_values;
        std::sort(sorted_by_name.begin(), sorted_by_name.end(), [](const auto & a, const auto & b) {
            return a.first < b.first;
        });

        size_t n = sorted_by_name.size();
        offsets.reserve(n + 1);
        values_by_name.reserve(n);

        // Build offsets and concatenated string
        uint16_t offset = 0;
        for (size_t i = 0; i < n; ++i)
        {
            const auto & [name, value] = sorted_by_name[i];
            offsets.push_back(offset);
            string_data += name;
            offset += static_cast<uint16_t>(name.size());
            values_by_name.push_back(value);

            // Fill direct lookup array
            value_to_index[static_cast<uint8_t>(value)] = static_cast<uint8_t>(i);
        }
        offsets.push_back(offset);
    }

    std::optional<T> tryGetValue(std::string_view name) const
    {
        if (offsets.size() <= 1)
            return std::nullopt;

        size_t n = offsets.size() - 1;
        size_t lo = 0, hi = n;

        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            std::string_view mid_str = getStringAt(mid);

            if (mid_str < name)
                lo = mid + 1;
            else if (mid_str > name)
                hi = mid;
            else
                return values_by_name[mid];
        }
        return std::nullopt;
    }

    std::optional<std::string_view> tryGetName(T value) const
    {
        uint8_t idx = value_to_index[static_cast<uint8_t>(value)];
        if (idx == INVALID_INDEX)
            return std::nullopt;
        return getStringAt(idx);
    }

    size_t size() const { return offsets.empty() ? 0 : offsets.size() - 1; }

    size_t memoryUsage() const
    {
        size_t total = sizeof(*this);
        total += offsets.capacity() * sizeof(uint16_t);
        total += string_data.capacity();
        total += values_by_name.capacity() * sizeof(T);
        // value_to_index is inline in the struct
        return total;
    }

    Values getValues() const
    {
        Values result;
        result.reserve(size());
        for (size_t i = 0; i < size(); ++i)
            result.emplace_back(std::string(getStringAt(i)), values_by_name[i]);

        std::sort(result.begin(), result.end(), [](const auto & a, const auto & b) {
            return a.second < b.second;
        });
        return result;
    }
};


// ============================================================================
// Implementation 5: Compact with Dynamic Direct Array
// Uses: Same as #4 but allocates only [min_value..max_value] range
// More memory efficient when enum values don't span full int8_t range
// ============================================================================

template <typename T>
class EnumStorageCompactDirectDynamic
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;

private:
    // Strings sorted lexicographically
    std::vector<uint16_t> offsets;       // offsets[i] = start of string i
    std::string string_data;              // concatenated strings
    std::vector<T> values_by_name;        // values_by_name[i] = value for i-th string

    // Dynamic array for O(1) reverse lookup
    // value_to_index[value - min_value] = index into name arrays
    std::vector<uint8_t> value_to_index;
    T min_value = 0;
    T max_value = 0;

    static constexpr uint8_t INVALID_INDEX = 255;

    std::string_view getStringAt(size_t idx) const
    {
        return std::string_view(string_data.data() + offsets[idx], offsets[idx + 1] - offsets[idx]);
    }

public:
    explicit EnumStorageCompactDirectDynamic(const Values & input_values)
    {
        if (input_values.empty())
            return;

        // Find min/max values
        min_value = input_values[0].second;
        max_value = input_values[0].second;
        for (const auto & [name, value] : input_values)
        {
            if (value < min_value) min_value = value;
            if (value > max_value) max_value = value;
        }

        // Allocate only the range we need
        size_t range_size = static_cast<size_t>(max_value - min_value + 1);
        value_to_index.resize(range_size, INVALID_INDEX);

        // Sort by name lexicographically
        Values sorted_by_name = input_values;
        std::sort(sorted_by_name.begin(), sorted_by_name.end(), [](const auto & a, const auto & b) {
            return a.first < b.first;
        });

        size_t n = sorted_by_name.size();
        offsets.reserve(n + 1);
        values_by_name.reserve(n);

        // Build offsets and concatenated string
        uint16_t offset = 0;
        for (size_t i = 0; i < n; ++i)
        {
            const auto & [name, value] = sorted_by_name[i];
            offsets.push_back(offset);
            string_data += name;
            offset += static_cast<uint16_t>(name.size());
            values_by_name.push_back(value);

            // Fill direct lookup array (offset by min_value)
            value_to_index[static_cast<size_t>(value - min_value)] = static_cast<uint8_t>(i);
        }
        offsets.push_back(offset);
    }

    std::optional<T> tryGetValue(std::string_view name) const
    {
        if (offsets.size() <= 1)
            return std::nullopt;

        size_t n = offsets.size() - 1;
        size_t lo = 0, hi = n;

        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            std::string_view mid_str = getStringAt(mid);

            if (mid_str < name)
                lo = mid + 1;
            else if (mid_str > name)
                hi = mid;
            else
                return values_by_name[mid];
        }
        return std::nullopt;
    }

    std::optional<std::string_view> tryGetName(T value) const
    {
        // Fast bounds check - return nullopt for values outside our range
        if (value < min_value || value > max_value)
            return std::nullopt;

        uint8_t idx = value_to_index[static_cast<size_t>(value - min_value)];
        if (idx == INVALID_INDEX)
            return std::nullopt;
        return getStringAt(idx);
    }

    size_t size() const { return offsets.empty() ? 0 : offsets.size() - 1; }

    T getMinValue() const { return min_value; }
    T getMaxValue() const { return max_value; }
    size_t getRangeSize() const { return value_to_index.size(); }

    size_t memoryUsage() const
    {
        size_t total = sizeof(*this);
        total += offsets.capacity() * sizeof(uint16_t);
        total += string_data.capacity();
        total += values_by_name.capacity() * sizeof(T);
        total += value_to_index.capacity() * sizeof(uint8_t);
        return total;
    }

    Values getValues() const
    {
        Values result;
        result.reserve(size());
        for (size_t i = 0; i < size(); ++i)
            result.emplace_back(std::string(getStringAt(i)), values_by_name[i]);

        std::sort(result.begin(), result.end(), [](const auto & a, const auto & b) {
            return a.second < b.second;
        });
        return result;
    }
};


// ============================================================================
// Implementation 6: Minimal Compact (no reverse lookup structure)
// Uses: offsets[] + strings + values[], linear scan for value→name
// Useful baseline to see overhead of lookup structures
// ============================================================================

template <typename T>
class EnumStorageMinimalCompact
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;

private:
    std::vector<uint32_t> offsets;
    std::string string_data;
    std::vector<T> values_by_name;

    std::string_view getStringAt(size_t idx) const
    {
        return std::string_view(string_data.data() + offsets[idx], offsets[idx + 1] - offsets[idx]);
    }

public:
    explicit EnumStorageMinimalCompact(const Values & input_values)
    {
        if (input_values.empty())
            return;

        Values sorted_by_name = input_values;
        std::sort(sorted_by_name.begin(), sorted_by_name.end(), [](const auto & a, const auto & b) {
            return a.first < b.first;
        });

        size_t n = sorted_by_name.size();
        offsets.reserve(n + 1);
        values_by_name.reserve(n);

        uint32_t offset = 0;
        for (const auto & [name, value] : sorted_by_name)
        {
            offsets.push_back(offset);
            string_data += name;
            offset += static_cast<uint32_t>(name.size());
            values_by_name.push_back(value);
        }
        offsets.push_back(offset);
    }

    std::optional<T> tryGetValue(std::string_view name) const
    {
        if (offsets.size() <= 1)
            return std::nullopt;

        size_t n = offsets.size() - 1;
        size_t lo = 0, hi = n;

        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            std::string_view mid_str = getStringAt(mid);

            if (mid_str < name)
                lo = mid + 1;
            else if (mid_str > name)
                hi = mid;
            else
                return values_by_name[mid];
        }
        return std::nullopt;
    }

    std::optional<std::string_view> tryGetName(T value) const
    {
        // Linear scan - O(n)
        for (size_t i = 0; i < values_by_name.size(); ++i)
        {
            if (values_by_name[i] == value)
                return getStringAt(i);
        }
        return std::nullopt;
    }

    size_t size() const { return offsets.empty() ? 0 : offsets.size() - 1; }

    size_t memoryUsage() const
    {
        size_t total = sizeof(*this);
        total += offsets.capacity() * sizeof(uint32_t);
        total += string_data.capacity();
        total += values_by_name.capacity() * sizeof(T);
        return total;
    }

    Values getValues() const
    {
        Values result;
        result.reserve(size());
        for (size_t i = 0; i < size(); ++i)
            result.emplace_back(std::string(getStringAt(i)), values_by_name[i]);

        std::sort(result.begin(), result.end(), [](const auto & a, const auto & b) {
            return a.second < b.second;
        });
        return result;
    }
};


// ============================================================================
// Test Data Generators
// ============================================================================

/// Generate random string of given length
inline std::string generateRandomString(size_t length, uint32_t seed)
{
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    std::string result;
    result.reserve(length);

    uint32_t state = seed;
    for (size_t i = 0; i < length; ++i)
    {
        state = state * 1103515245 + 12345;  // LCG
        result += charset[(state >> 16) % (sizeof(charset) - 1)];
    }
    return result;
}

/// Generate enum values with random names
template <typename T>
EnumValues<T> generateRandomEnum(size_t count, size_t min_name_len, size_t max_name_len, uint32_t seed = 42)
{
    EnumValues<T> values;
    values.reserve(count);

    uint32_t state = seed;
    for (size_t i = 0; i < count; ++i)
    {
        state = state * 1103515245 + 12345;
        size_t len = min_name_len + (state % (max_name_len - min_name_len + 1));
        std::string name = generateRandomString(len, state + static_cast<uint32_t>(i));

        // Ensure unique names by appending index if needed
        name += "_" + std::to_string(i);

        T value = static_cast<T>(i);
        values.emplace_back(std::move(name), value);
    }
    return values;
}

/// Generate realistic status enum
template <typename T>
EnumValues<T> generateStatusEnum()
{
    return {
        {"success", static_cast<T>(0)},
        {"error", static_cast<T>(1)},
        {"pending", static_cast<T>(2)},
        {"cancelled", static_cast<T>(3)},
        {"timeout", static_cast<T>(4)},
        {"unknown", static_cast<T>(5)},
        {"in_progress", static_cast<T>(6)},
        {"completed", static_cast<T>(7)},
        {"failed", static_cast<T>(8)},
        {"retrying", static_cast<T>(9)}
    };
}

/// Generate country code enum (2-letter codes)
template <typename T>
EnumValues<T> generateCountryEnum()
{
    return {
        {"US", static_cast<T>(1)}, {"GB", static_cast<T>(2)}, {"DE", static_cast<T>(3)},
        {"FR", static_cast<T>(4)}, {"JP", static_cast<T>(5)}, {"CN", static_cast<T>(6)},
        {"IN", static_cast<T>(7)}, {"BR", static_cast<T>(8)}, {"RU", static_cast<T>(9)},
        {"CA", static_cast<T>(10)}, {"AU", static_cast<T>(11)}, {"IT", static_cast<T>(12)},
        {"ES", static_cast<T>(13)}, {"MX", static_cast<T>(14)}, {"KR", static_cast<T>(15)},
        {"NL", static_cast<T>(16)}, {"SE", static_cast<T>(17)}, {"CH", static_cast<T>(18)},
        {"PL", static_cast<T>(19)}, {"BE", static_cast<T>(20)}, {"AT", static_cast<T>(21)},
        {"NO", static_cast<T>(22)}, {"DK", static_cast<T>(23)}, {"FI", static_cast<T>(24)},
        {"IE", static_cast<T>(25)}, {"PT", static_cast<T>(26)}, {"CZ", static_cast<T>(27)},
        {"GR", static_cast<T>(28)}, {"IL", static_cast<T>(29)}, {"SG", static_cast<T>(30)},
        {"HK", static_cast<T>(31)}, {"NZ", static_cast<T>(32)}, {"ZA", static_cast<T>(33)},
        {"AR", static_cast<T>(34)}, {"CL", static_cast<T>(35)}, {"CO", static_cast<T>(36)},
        {"TH", static_cast<T>(37)}, {"MY", static_cast<T>(38)}, {"PH", static_cast<T>(39)},
        {"ID", static_cast<T>(40)}, {"VN", static_cast<T>(41)}, {"EG", static_cast<T>(42)},
        {"NG", static_cast<T>(43)}, {"PK", static_cast<T>(44)}, {"BD", static_cast<T>(45)},
        {"TR", static_cast<T>(46)}, {"SA", static_cast<T>(47)}, {"AE", static_cast<T>(48)},
        {"UA", static_cast<T>(49)}, {"RO", static_cast<T>(50)}
    };
}

/// Generate category enum with longer names
template <typename T>
EnumValues<T> generateCategoryEnum()
{
    return {
        {"electronics_and_computers", static_cast<T>(1)},
        {"clothing_and_apparel", static_cast<T>(2)},
        {"home_and_garden", static_cast<T>(3)},
        {"sports_and_outdoors", static_cast<T>(4)},
        {"books_and_media", static_cast<T>(5)},
        {"health_and_beauty", static_cast<T>(6)},
        {"toys_and_games", static_cast<T>(7)},
        {"automotive_and_industrial", static_cast<T>(8)},
        {"food_and_beverages", static_cast<T>(9)},
        {"office_and_school_supplies", static_cast<T>(10)},
        {"pet_supplies_and_accessories", static_cast<T>(11)},
        {"jewelry_and_watches", static_cast<T>(12)},
        {"musical_instruments", static_cast<T>(13)},
        {"arts_crafts_and_sewing", static_cast<T>(14)},
        {"baby_products_and_nursery", static_cast<T>(15)}
    };
}

}  // namespace DB::Benchmark
