#pragma once

#include <string>
#include <Core/UatraitsFast/uatraits-fast.h>
#include <common/StringRef.h>
#include "Actions.h"


namespace detail
{
    template <UATraits::Result::VersionFields field>
    struct VersionFieldTags
    {
        using Action = ActionSetVersion;
    };

    template <UATraits::Result::UIntFields field>
    struct UIntFieldTags
    {
        using Action = ActionSetUInt;
    };

    template <UATraits::Result::BoolFields field>
    struct BoolFieldTags
    {
        using Action = ActionSetBool;
    };

    template <UATraits::Result::StringRefFields field>
    struct StringRefFieldTags
    {
        using Action = ActionSetStringRef;
        static constexpr bool NeedsSourceCaching = false; /// Если true, то это поле может ссылаться на исходную строку (например, в подстановке).
    };

    template <>
    struct StringRefFieldTags<UATraits::Result::DeviceName>
    {
        using Action = SimpleMatchingActionSetStringRef;
        static constexpr bool NeedsSourceCaching = true;
    };

    template <>
    struct StringRefFieldTags<UATraits::Result::DeviceModel>
    {
        using Action = SimpleMatchingActionSetStringRef;
        static constexpr bool NeedsSourceCaching = true;
    };

    /// XXXAddActionHelpers

    /// IgnoredFields
    template <UATraits::Result::IgnoredFields field, class Strings, class Actions>
    struct IgnoredFieldAddActionHelperImpl
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            if (name == UATraits::Result::ignoredFieldNames()[field])
            {
                return true; /// we're done here
            }
            else
            {
                return IgnoredFieldAddActionHelperImpl<static_cast<UATraits::Result::IgnoredFields>(field + 1), Strings, Actions>::addAction(strings, actions, name, value);
            }
        }
    };

    template <class Strings, class Actions>
    struct IgnoredFieldAddActionHelperImpl<UATraits::Result::IgnoredFieldsCount, Strings, Actions>
    {
        inline static bool addAction(Strings & /*strings*/, Actions & /*actions*/, const std::string & /*name*/, const std::string & /*value*/)
        {
            return false; /// action not found
        }
    };

    template <class Strings, class Actions>
    struct IgnoredFieldAddActionHelper
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            return IgnoredFieldAddActionHelperImpl<static_cast<UATraits::Result::IgnoredFields>(0), Strings, Actions>::addAction(strings, actions, name, value);
        }
    };

    /// VersionFields
    template <UATraits::Result::VersionFields field, class Strings, class Actions>
    struct VersionFieldAddActionHelperImpl
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            if (name == UATraits::Result::versionFieldNames()[field])
            {
                strings.push_back(value);
                actions.emplace_back(std::make_shared<typename VersionFieldTags<field>::Action>
                        (
                            field, StringRef(strings.back())
                        ));
                return true; /// we're done here
            }
            else
            {
                return VersionFieldAddActionHelperImpl<static_cast<UATraits::Result::VersionFields>(field + 1), Strings, Actions>::addAction(strings, actions, name, value);
            }
        }
    };

    template <class Strings, class Actions>
    struct VersionFieldAddActionHelperImpl<UATraits::Result::VersionFieldsCount, Strings, Actions>
    {
        inline static bool addAction(Strings & /*strings*/, Actions & /*actions*/, const std::string & /*name*/, const std::string & /*value*/)
        {
            return false; /// action not found
        }
    };

    template <class Strings, class Actions>
    struct VersionFieldAddActionHelper
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            return VersionFieldAddActionHelperImpl<static_cast<UATraits::Result::VersionFields>(0), Strings, Actions>::addAction(strings, actions, name, value);
        }
    };

    /// StringRefFields
    template <UATraits::Result::StringRefFields field, class Strings, class Actions>
    struct StringRefFieldAddActionHelperImpl
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            if (name == UATraits::Result::stringRefFieldNames()[field])
            {
                strings.push_back(value);
                actions.emplace_back(std::make_shared<typename StringRefFieldTags<field>::Action>
                        (
                            field, StringRef(strings.back())
                        ));
                return true; /// we're done here
            }
            else
            {
                return StringRefFieldAddActionHelperImpl<static_cast<UATraits::Result::StringRefFields>(field + 1), Strings, Actions>::addAction(strings, actions, name, value);
            }
        }
    };

    template <class Strings, class Actions>
    struct StringRefFieldAddActionHelperImpl<UATraits::Result::StringRefFieldsCount, Strings, Actions>
    {
        inline static bool addAction(Strings & /*strings*/, Actions & /*actions*/, const std::string & /*name*/, const std::string & /*value*/)
        {
            return false; /// action not found
        }
    };

    template <class Strings, class Actions>
    struct StringRefFieldAddActionHelper
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            return StringRefFieldAddActionHelperImpl<static_cast<UATraits::Result::StringRefFields>(0), Strings, Actions>::addAction(strings, actions, name, value);
        }
    };

    /// BoolFields
    template <UATraits::Result::BoolFields field, class Strings, class Actions>
    struct BoolFieldAddActionHelperImpl
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            if (name == UATraits::Result::boolFieldNames()[field])
            {
                actions.emplace_back(std::make_shared<typename BoolFieldTags<field>::Action>
                        (
                            field, value == "false" ? false : true
                        ));
                return true; /// we're done here
            }
            else
            {
                return BoolFieldAddActionHelperImpl<static_cast<UATraits::Result::BoolFields>(field + 1), Strings, Actions>::addAction(strings, actions, name, value);
            }
        }
    };

    template <class Strings, class Actions>
    struct BoolFieldAddActionHelperImpl<UATraits::Result::BoolFieldsCount, Strings, Actions>
    {
        inline static bool addAction(Strings & /*strings*/, Actions & /*actions*/, const std::string & /*name*/, const std::string & /*value*/)
        {
            return false; /// action not found
        }
    };

    template <class Strings, class Actions>
    struct BoolFieldAddActionHelper
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            return BoolFieldAddActionHelperImpl<static_cast<UATraits::Result::BoolFields>(0), Strings, Actions>::addAction(strings, actions, name, value);
        }
    };

    /// UIntFields
    template <UATraits::Result::UIntFields field, class Strings, class Actions>
    struct UIntFieldAddActionHelperImpl
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            if (name == UATraits::Result::uIntFieldNames()[field])
            {
                actions.emplace_back(std::make_shared<typename UIntFieldTags<field>::Action>
                        (
                            field, Poco::NumberParser::parseUnsigned(value)
                        ));
                return true; /// we're done here
            }
            else
            {
                return UIntFieldAddActionHelperImpl<static_cast<UATraits::Result::UIntFields>(field + 1), Strings, Actions>::addAction(strings, actions, name, value);
            }
        }
    };

    template <class Strings, class Actions>
    struct UIntFieldAddActionHelperImpl<UATraits::Result::UIntFieldsCount, Strings, Actions>
    {
        inline static bool addAction(Strings & /*strings*/, Actions & /*actions*/, const std::string & /*name*/, const std::string & /*value*/)
        {
            return false; /// action not found
        }
    };

    template <class Strings, class Actions>
    struct UIntFieldAddActionHelper
    {
        inline static bool addAction(Strings & strings, Actions & actions, const std::string & name, const std::string & value)
        {
            return UIntFieldAddActionHelperImpl<static_cast<UATraits::Result::UIntFields>(0), Strings, Actions>::addAction(strings, actions, name, value);
        }
    };

    /// CacheSourceHelper

    template <UATraits::SourceType source_type, UATraits::Result::StringRefFields field>
    struct CacheSourceHelperImpl
    {
        inline static void cacheSource(UATraits::Result & result, const StringRef & user_agent_replace, const StringRef & user_agent_match)
        {
            if (StringRefFieldTags<field>::NeedsSourceCaching)
            {
                std::string & user_agent_cache = source_type == UATraits::SourceUserAgent ? result.user_agent_cache : result.x_operamini_phone_ua_cache;
                if (user_agent_cache.size() != user_agent_replace.size)
                {
                    /// Сохраним строку.
                    user_agent_cache = user_agent_replace.toString();
                }

                /// Поправим ссылки.
                if (user_agent_match.data <= result.string_ref_fields[field].data
                    && user_agent_match.data + user_agent_match.size >= result.string_ref_fields[field].data + result.string_ref_fields[field].size)
                {
                    result.string_ref_fields[field].data += user_agent_cache.data() - user_agent_match.data;
                }
            }
            CacheSourceHelperImpl<source_type, static_cast<UATraits::Result::StringRefFields>(field + 1)>::cacheSource(result, user_agent_replace, user_agent_match);
        }
    };

    template <UATraits::SourceType source_type>
    struct CacheSourceHelperImpl<source_type, UATraits::Result::StringRefFieldsCount>
    {
        inline static void cacheSource(UATraits::Result & /*result*/, const StringRef & /*user_agent_replace*/, const StringRef & /*user_agent_match*/)
        {
        }
    };

    template <UATraits::SourceType source_type>
    struct CacheSourceHelper
    {
        /// Сохранить user_agent_replace в result, если в полях StringRef используется user_agent_match, и поправить ссылки.
        inline static void cacheSource(UATraits::Result & result, const StringRef & user_agent_replace, const StringRef & user_agent_match)
        {
            return CacheSourceHelperImpl<source_type, static_cast<UATraits::Result::StringRefFields>(0)>::cacheSource(result, user_agent_replace, user_agent_match);
        }

        /// Сохранить user_agent в result, если он используется в полях StringRef, и поправить ссылки.
        inline static void cacheSource(UATraits::Result & result, const StringRef & user_agent)
        {
            cacheSource(result, user_agent, user_agent);
        }
    };

    /// FixReferencesHelper

    template <UATraits::Result::StringRefFields field>
    struct FixReferencesHelperImpl
    {
        inline static void fixReferences(UATraits::Result & result, const UATraits::Result & right)
        {
            if (StringRefFieldTags<field>::NeedsSourceCaching)
            {
                if (right.user_agent_cache.data() <= result.string_ref_fields[field].data
                    && right.user_agent_cache.data() + right.user_agent_cache.size() >= result.string_ref_fields[field].data + result.string_ref_fields[field].size)
                {
                    result.string_ref_fields[field].data += result.user_agent_cache.data() - right.user_agent_cache.data();
                }
                else if (right.x_operamini_phone_ua_cache.data() <= result.string_ref_fields[field].data
                    && right.x_operamini_phone_ua_cache.data() + right.x_operamini_phone_ua_cache.size() >= result.string_ref_fields[field].data + result.string_ref_fields[field].size)
                {
                    result.string_ref_fields[field].data += result.x_operamini_phone_ua_cache.data() - right.x_operamini_phone_ua_cache.data();
                }
            }
            FixReferencesHelperImpl<static_cast<UATraits::Result::StringRefFields>(field + 1)>::fixReferences(result, right);
        }
    };

    template <>
    struct FixReferencesHelperImpl<UATraits::Result::StringRefFieldsCount>
    {
        inline static void fixReferences(UATraits::Result & /*result*/, const UATraits::Result & /*right*/)
        {
        }
    };

    struct FixReferencesHelper
    {
        inline static void fixReferences(UATraits::Result & result, const UATraits::Result & right)
        {
            FixReferencesHelperImpl<static_cast<UATraits::Result::StringRefFields>(0)>::fixReferences(result, right);
        }
    };
}
