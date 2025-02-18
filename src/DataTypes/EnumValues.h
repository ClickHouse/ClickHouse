#pragma once

#include <unordered_map>
#include <Common/HashTable/HashMap.h>
#include <Common/NamePrompter.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename T>
class EnumValues : public IHints<>
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;
    using NameToValueMap = HashMap<StringRef, T, StringRefHash>;
    using ValueToNameMap = std::unordered_map<T, StringRef>;

private:
    Values values;
    NameToValueMap name_to_value_map;
    ValueToNameMap value_to_name_map;

    void fillMaps();

public:
    explicit EnumValues(const Values & values_);

    const Values & getValues() const { return values; }

    auto findByValue(const T & value) const
    {
        auto it = value_to_name_map.find(value);
        if (it == value_to_name_map.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected value {} in enum", toString(value));

        return it;
    }

    bool hasValue(const T & value) const
    {
        return value_to_name_map.contains(value);
    }

    /// throws exception if value is not valid
    const StringRef & getNameForValue(const T & value) const
    {
        return findByValue(value)->second;
    }

    /// returns false if value is not valid
    bool getNameForValue(const T & value, StringRef & result) const
    {
        const auto it = value_to_name_map.find(value);
        if (it == value_to_name_map.end())
            return false;

        result = it->second;
        return true;
    }

    T getValue(StringRef field_name) const;
    bool tryGetValue(T & x, StringRef field_name) const;

    template <typename TValues>
    bool containsAll(const TValues & rhs_values) const
    {
        auto check = [&](const auto & value)
        {
            auto it = name_to_value_map.find(value.first);
            /// If we don't have this name, than we have to be sure,
            /// that this value exists in enum
            if (it == name_to_value_map.end())
                return value_to_name_map.count(value.second) > 0;

            /// If we have this name, than it should have the same value
            return it->value.second == value.second;
        };

        return std::all_of(rhs_values.begin(), rhs_values.end(), check);
    }

    Names getAllRegisteredNames() const override;

    std::unordered_set<String> getSetOfAllNames(bool to_lower) const;

    std::unordered_set<T> getSetOfAllValues() const;
};

}
