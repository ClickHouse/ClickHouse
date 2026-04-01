#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <string_view>
#include <vector>
#include <Core/Names.h>
#include <Common/NamePrompter.h>


namespace DB
{

template <typename T>
class EnumValues : public IHints<>
{
public:
    using Value = std::pair<std::string, T>;
    using Values = std::vector<Value>;
    /// `TemporaryAdd` is only for intermediate `ADD ENUM VALUES` state:
    /// values stay in parser order and duplicate numeric placeholders are allowed
    /// until `mergeEnumTypes` remaps and validates the final enum values.
    enum class ValidationMode
    {
        Normal,
        TemporaryAdd,
    };

private:
    class NameToValueMap;
    using ValueToNameMap = std::unordered_map<T, std::string_view>;

    Values values;
    std::unique_ptr<NameToValueMap> name_to_value_map;
    ValueToNameMap value_to_name_map;

    void fillMaps(ValidationMode validation_mode);

public:
    explicit EnumValues(const Values & values_, ValidationMode validation_mode = ValidationMode::Normal);
    ~EnumValues() override;

    const Values & getValues() const { return values; }

    ValueToNameMap::const_iterator findByValue(const T & value) const;

    bool hasValue(const T & value) const
    {
        return value_to_name_map.contains(value);
    }

    /// throws exception if value is not valid
    std::string_view getNameForValue(const T & value) const
    {
        return findByValue(value)->second;
    }

    /// returns false if value is not valid
    bool getNameForValue(const T & value, std::string_view & result) const
    {
        const auto it = value_to_name_map.find(value);
        if (it == value_to_name_map.end())
            return false;

        result = it->second;
        return true;
    }

    T getValue(std::string_view field_name) const;
    bool tryGetValue(T & x, std::string_view field_name) const;

    template <typename TValues>
    bool containsAll(const TValues & rhs_values) const;

    Names getAllRegisteredNames() const override;

    std::unordered_set<String> getSetOfAllNames(bool to_lower) const;

    std::unordered_set<T> getSetOfAllValues() const;
};

}
