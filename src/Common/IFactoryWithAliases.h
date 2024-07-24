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

/// A base class which provides case-sensitive and case-insensitive aliases for case-sensitive and case-insensitive
/// names (e.g. function names) in the derived class.
template <typename ValueType>
class IFactoryWithAliases : public IHints<2>
{
protected:
    using Value = ValueType;

    String getAliasToOrName(const String & name) const
    {
        if (aliases.contains(name))
            return aliases.at(name);
        else if (String name_lowercase = Poco::toLower(name); case_insensitive_aliases.contains(name_lowercase))
            return case_insensitive_aliases.at(name_lowercase);
        else
            return name;
    }

    std::unordered_map<String, String> case_insensitive_name_mapping;

public:
    /// For compatibility with SQL, it's possible to specify that certain function name is case insensitive.
    enum class Case
    {
        Sensitive,
        Insensitive
    };

    /// Register additional name for value. real_name must already be registered.
    void registerAlias(const String & alias_name, const String & real_name, Case case_sensitiveness = Case::Sensitive)
    {
        const auto & original_name_map = getOriginalNameMap();
        const auto & case_insensitive_original_name_map = getOriginalCaseInsensitiveNameMap();

        String real_name_lowercase = Poco::toLower(real_name);
        if (!original_name_map.contains(real_name) && !case_insensitive_original_name_map.contains(real_name_lowercase))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: can't create alias '{}', the real name '{}' is not registered", getFactoryName(), alias_name, real_name);

        registerAliasUnchecked(alias_name, real_name, case_sensitiveness);
    }

    /// Note: Please make sure the real_name is already registered when calling this function directly.
    void registerAliasUnchecked(const String & alias_name, const String & real_name, Case case_sensitiveness = Case::Sensitive)
    {
        String alias_name_lowercase = Poco::toLower(alias_name);
        const String & factory_name = getFactoryName();

        if (case_sensitiveness == Case::Insensitive)
        {
            if (!case_insensitive_aliases.emplace(alias_name_lowercase, real_name).second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: case insensitive alias name '{}' is not unique", factory_name, alias_name);
            case_insensitive_name_mapping[alias_name_lowercase] = real_name;
        }

        /// Note: case-insensitive aliases are registered in aliases and case_insensitive_aliases
        if (!aliases.emplace(alias_name, real_name).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: alias name '{}' is not unique", factory_name, alias_name);
    }


    std::vector<String> getAllRegisteredNames() const override
    {
        std::vector<String> result;
        auto getter = [](const auto & pair) { return pair.first; };
        std::transform(getOriginalNameMap().begin(), getOriginalNameMap().end(), std::back_inserter(result), getter);
        std::transform(aliases.begin(), aliases.end(), std::back_inserter(result), getter);
        return result;
    }

    bool isCaseInsensitive(const String & name) const
    {
        String name_lowercase = Poco::toLower(name);
        return getOriginalCaseInsensitiveNameMap().contains(name_lowercase) || case_insensitive_aliases.contains(name_lowercase);
    }

    const String & aliasTo(const String & name) const
    {
        if (auto it = aliases.find(name); it != aliases.end())
            return it->second;
        else if (auto jt = case_insensitive_aliases.find(Poco::toLower(name)); jt != case_insensitive_aliases.end())
            return jt->second;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: name '{}' is not alias", getFactoryName(), name);
    }

    bool isAlias(const String & name) const
    {
        return aliases.contains(name) || case_insensitive_aliases.contains(name);
    }

    bool isNameOrAlias(const String & name) const
    {
        return getOriginalNameMap().contains(name) || getOriginalCaseInsensitiveNameMap().contains(name) || isAlias(name);
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

protected:
    using OriginalNameMap = std::unordered_map<String, ValueType>; /// original name -> creator/documentation/properties/etc.

private:
    /// Derived classes must implement this:
    virtual const OriginalNameMap & getOriginalNameMap() const = 0;
    virtual const OriginalNameMap & getOriginalCaseInsensitiveNameMap() const = 0;
    virtual String getFactoryName() const = 0;

    using AliasNameMap = std::unordered_map<String, String>; /// alias name -> original name
    AliasNameMap aliases;
    AliasNameMap case_insensitive_aliases;
};

}
