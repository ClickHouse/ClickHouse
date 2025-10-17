#pragma once

#include <unordered_map>
#include <unordered_set>
#include <string>
#include <Core/Names.h>
#include <base/StringRef.h>
#include <Common/NamePrompter.h>


namespace DB
{

template <typename T>
class EnumValues : public IHints<>
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;

private:
    class NameToValueMap;
    using ValueToNameMap = std::unordered_map<T, StringRef>;

    Values values;
    std::unique_ptr<NameToValueMap> name_to_value_map;
    ValueToNameMap value_to_name_map;

    void fillMaps();

public:
    explicit EnumValues(const Values & values_);
    ~EnumValues() override;

    const Values & getValues() const { return values; }

    ValueToNameMap::const_iterator findByValue(const T & value) const;

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
    bool containsAll(const TValues & rhs_values) const;

    Names getAllRegisteredNames() const override;

    std::unordered_set<String> getSetOfAllNames(bool to_lower) const;

    std::unordered_set<T> getSetOfAllValues() const;
};

}
