#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/getMultipleKeysFromConfig.h>

#include <unordered_set>
#include <base/StringRef.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace
{
    String concatKeyAndSubKey(const String & key, const String & subkey)
    {
        // Copied from Poco::Util::ConfigurationView::translateKey():
        String result = key;
        if (!result.empty() && !subkey.empty() && subkey[0] != '[')
            result += '.';
        result += subkey;
        return result;
    }

    bool isSameConfigurationImpl(const Poco::Util::AbstractConfiguration & left, const String & left_key,
                                 const Poco::Util::AbstractConfiguration & right, const String & right_key,
                                 const std::unordered_set<std::string_view> * ignore_keys)
    {
        if (&left == &right && left_key == right_key)
            return true;

        /// Get the subkeys of the left and right configurations.
        Poco::Util::AbstractConfiguration::Keys left_subkeys;
        Poco::Util::AbstractConfiguration::Keys right_subkeys;
        left.keys(left_key, left_subkeys);
        right.keys(right_key, right_subkeys);

        if (ignore_keys)
        {
            std::erase_if(left_subkeys, [&](const String & key) { return ignore_keys->contains(key); });
            std::erase_if(right_subkeys, [&](const String & key) { return ignore_keys->contains(key); });

#if defined(DEBUG_OR_SANITIZER_BUILD)
            /// Compound `ignore_keys` are not yet implemented.
            for (const auto & ignore_key : *ignore_keys)
                chassert(ignore_key.find('.') == std::string_view::npos);
#endif
        }

        /// Check that the right configuration has the same set of subkeys as the left configuration.
        if (left_subkeys.size() != right_subkeys.size())
            return false;

        if (left_subkeys.empty())
        {
            if (left.hasProperty(left_key))
            {
                return right.hasProperty(right_key) && (left.getRawString(left_key) == right.getRawString(right_key));
            }

            return !right.hasProperty(right_key);
        }

        /// Go through all the subkeys and compare corresponding parts of the configurations.
        std::unordered_set<std::string_view> left_subkeys_set{left_subkeys.begin(), left_subkeys.end()};
        for (const auto & subkey : right_subkeys)
        {
            if (!left_subkeys_set.contains(subkey))
                return false;

            if (!isSameConfigurationImpl(left, concatKeyAndSubKey(left_key, subkey), right, concatKeyAndSubKey(right_key, subkey), nullptr))
                return false;
        }
        return true;
    }
}


bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left, const Poco::Util::AbstractConfiguration & right)
{
    return isSameConfiguration(left, String(), right, String());
}

bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left, const Poco::Util::AbstractConfiguration & right, const String & key)
{
    return isSameConfiguration(left, key, right, key);
}

bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left, const String & left_key,
                         const Poco::Util::AbstractConfiguration & right, const String & right_key,
                         const std::unordered_set<std::string_view> & ignore_keys)
{
    const auto * ignore_keys_ptr = !ignore_keys.empty() ? &ignore_keys : nullptr;
    return isSameConfigurationImpl(left, left_key, right, right_key, ignore_keys_ptr);
}

bool isSameConfigurationWithMultipleKeys(const Poco::Util::AbstractConfiguration & left, const Poco::Util::AbstractConfiguration & right, const String & root, const String & name)
{
    if (&left == &right)
        return true;

    auto left_multiple_keys = getMultipleKeysFromConfig(left, root, name);
    auto right_multiple_keys = getMultipleKeysFromConfig(right, root, name);
    if (left_multiple_keys.size() != right_multiple_keys.size())
        return false;

    for (auto & key : left_multiple_keys)
        if (!isSameConfiguration(left, right, concatKeyAndSubKey(root, key)))
            return false;

    return true;
}

}
