#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/getMultipleKeysFromConfig.h>

#include <unordered_set>
#include <common/StringRef.h>
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
    };
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
                         const Poco::Util::AbstractConfiguration & right, const String & right_key)
{
    if (&left == &right && left_key == right_key)
        return true;

    /// Config may have multiple keys with one name. For example:
    /// <yandex>
    ///     <some_key>...</some_key>
    ///     <some_key>...</some_key>
    /// </yandex>
    /// In this case we need to compare all of them.
    auto left_multiple_keys = getMultipleKeysFromConfig(left, "", left_key);
    auto right_multiple_keys = getMultipleKeysFromConfig(right, "", right_key);
    if (left_multiple_keys.size() != right_multiple_keys.size())
        return false;

    for (size_t i = 0; i != left_multiple_keys.size(); ++i)
    {
        if (left_multiple_keys[i] != right_multiple_keys[i])
            return false;

        bool has_property = left.hasProperty(left_multiple_keys[i]);
        if (has_property != right.hasProperty(right_multiple_keys[i]))
            return false;
        if (has_property)
        {
            /// The left and right configurations contains values so we can compare them.
            if (left.getRawString(left_multiple_keys[i]) != right.getRawString(right_multiple_keys[i]))
                return false;
        }

        /// Get the subkeys of the left and right configurations.
        Poco::Util::AbstractConfiguration::Keys subkeys;
        left.keys(left_multiple_keys[i], subkeys);

        {
            /// Check that the right configuration has the same set of subkeys as the left configuration.
            Poco::Util::AbstractConfiguration::Keys right_subkeys;
            right.keys(right_multiple_keys[i], right_subkeys);
            std::unordered_set<StringRef> left_subkeys{subkeys.begin(), subkeys.end()};
            if ((left_subkeys.size() != right_subkeys.size()) || (left_subkeys.size() != subkeys.size()))
                return false;
            for (const auto & right_subkey : right_subkeys)
                if (!left_subkeys.count(right_subkey))
                    return false;
        }

        /// Go through all the subkeys and compare corresponding parts of the configurations.
        for (const auto & subkey : subkeys)
            if (!isSameConfiguration(left, concatKeyAndSubKey(left_multiple_keys[i], subkey), right, concatKeyAndSubKey(right_multiple_keys[i], subkey)))
                return false;
    }

    return true;
}

}
