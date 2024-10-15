#pragma once

#include <Common/Exception.h>
#include <Common/NamePrompter.h>
#include <base/types.h>
#include <Poco/String.h>

#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** If stored objects may have several names (aliases)
  * this interface may be helpful
  * template parameter is available as Value
  */
template <typename ValueType>
class IFactoryWithAliases : public IHints<2>
{
protected:
    using Value = ValueType;

    String getAliasToOrName(const String & name) const
    {
        if (aliases.contains(name))
            return aliases.at(name);
        if (String name_lowercase = Poco::toLower(name); case_insensitive_aliases.contains(name_lowercase))
            return case_insensitive_aliases.at(name_lowercase);
        return name;
    }

    std::unordered_map<String, String> case_insensitive_name_mapping;

public:
    /// For compatibility with SQL, it's possible to specify that certain function name is case insensitive.
    enum Case
    {
        Sensitive,
        Insensitive
    };

    /** Register additional name for value
      * real_name have to be already registered.
      */
    void registerAlias(const String & alias_name, const String & real_name, Case case_sensitiveness = Sensitive)
    {
        const auto & creator_map = getMap();
        const auto & case_insensitive_creator_map = getCaseInsensitiveMap();

        String real_name_lowercase = Poco::toLower(real_name);
        if (!creator_map.contains(real_name) && !case_insensitive_creator_map.contains(real_name_lowercase))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{}: can't create alias '{}', the real name '{}' is not registered",
                getFactoryName(),
                alias_name,
                real_name);

        registerAliasUnchecked(alias_name, real_name, case_sensitiveness);
    }

    /// We need sure the real_name exactly exists when call the function directly.
    void registerAliasUnchecked(const String & alias_name, const String & real_name, Case case_sensitiveness = Sensitive)
    {
        String alias_name_lowercase = Poco::toLower(alias_name);
        const String factory_name = getFactoryName();

        if (case_sensitiveness == Insensitive)
        {
            if (!case_insensitive_aliases.emplace(alias_name_lowercase, real_name).second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: case insensitive alias name '{}' is not unique", factory_name, alias_name);
            case_insensitive_name_mapping[alias_name_lowercase] = real_name;
        }

        if (!aliases.emplace(alias_name, real_name).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: alias name '{}' is not unique", factory_name, alias_name);
    }


    std::vector<String> getAllRegisteredNames() const override
    {
        std::vector<String> result;
        auto getter = [](const auto & pair) { return pair.first; };
        std::transform(getMap().begin(), getMap().end(), std::back_inserter(result), getter);
        std::transform(aliases.begin(), aliases.end(), std::back_inserter(result), getter);
        return result;
    }

    bool isCaseInsensitive(const String & name) const
    {
        String name_lowercase = Poco::toLower(name);
        return getCaseInsensitiveMap().contains(name_lowercase) || case_insensitive_aliases.contains(name_lowercase);
    }

    const String & aliasTo(const String & name) const
    {
        if (auto it = aliases.find(name); it != aliases.end())
            return it->second;
        if (auto jt = case_insensitive_aliases.find(Poco::toLower(name)); jt != case_insensitive_aliases.end())
            return jt->second;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: name '{}' is not alias", getFactoryName(), name);
    }

    bool isAlias(const String & name) const { return aliases.contains(name) || case_insensitive_aliases.contains(name); }

    bool hasNameOrAlias(const String & name) const
    {
        return getMap().contains(name) || getCaseInsensitiveMap().contains(name) || isAlias(name);
    }

    /// Return the canonical name (the name used in registration) if it's different from `name`.
    const String & getCanonicalNameIfAny(const String & name) const
    {
        auto it = case_insensitive_name_mapping.find(Poco::toLower(name));
        if (it != case_insensitive_name_mapping.end())
            return it->second;
        return name;
    }

    ~IFactoryWithAliases() override = default;

private:
    using InnerMap = std::unordered_map<String, Value>; // name -> creator
    using AliasMap = std::unordered_map<String, String>; // alias -> original name

    virtual const InnerMap & getMap() const = 0;
    virtual const InnerMap & getCaseInsensitiveMap() const = 0;
    virtual String getFactoryName() const = 0;

    /// Alias map to data_types from previous two maps
    AliasMap aliases;

    /// Case insensitive aliases
    AliasMap case_insensitive_aliases;
};

}
