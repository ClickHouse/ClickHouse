#include <Common/Config/ConfigurationWithUsageTracking.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Poco skips the leading dots of a key (see `XMLConfiguration::findNode`), and the code reading
/// a configuration usually forms a key as `config_prefix + ".name"`, which gives ".name" when the
/// prefix is empty. Bring such keys to a single form to be able to compare them.
String normalizeKey(const String & key)
{
    size_t pos = key.find_first_not_of('.');
    if (pos == String::npos)
        return {};
    return key.substr(pos);
}

/// `Poco::Util::AbstractConfiguration::keys` escapes the dots inside a name, because a dot is the
/// separator of the path. Such a name appears when a disk is defined in a query as
/// `disk(..., a.b = 1)`: it is a single element named `a.b`, not a section `a` with an element `b`.
/// The escaping is an implementation detail of the path syntax, and it is removed before reporting.
String unescapeDots(const String & key)
{
    String result;
    result.reserve(key.size());
    for (size_t i = 0; i < key.size(); ++i)
    {
        if (key[i] == '\\' && i + 1 < key.size() && key[i + 1] == '.')
            continue;
        result += key[i];
    }
    return result;
}

}

ConfigurationWithUsageTracking::ConfigurationWithUsageTracking(const Poco::Util::AbstractConfiguration & config_)
    : config(config_)
{
}

ConfigurationWithUsageTracking::~ConfigurationWithUsageTracking() = default;

bool ConfigurationWithUsageTracking::getRaw(const std::string & key, std::string & value) const
{
    markAsUsed(key);

    /// A missing key is reported by returning false, while `getRawString` throws.
    if (!config.has(key))
        return false;

    value = config.getRawString(key);
    return true;
}

void ConfigurationWithUsageTracking::setRaw(const std::string & key, const std::string &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot modify a read-only configuration, key: {}", key);
}

void ConfigurationWithUsageTracking::enumerate(const std::string & key, Keys & range) const
{
    config.keys(key, range);
}

void ConfigurationWithUsageTracking::markAsUsed(const String & key) const
{
    std::lock_guard lock(mutex);
    used_keys.insert(normalizeKey(key));
}

bool ConfigurationWithUsageTracking::isUsed(const String & key) const
{
    std::lock_guard lock(mutex);
    return used_keys.contains(normalizeKey(key));
}

Strings ConfigurationWithUsageTracking::getUnusedKeys(const String & prefix) const
{
    Strings result;
    collectUnusedKeys(prefix, "", result);
    return result;
}

void ConfigurationWithUsageTracking::collectUnusedKeys(
    const String & prefix, const String & relative_key, Strings & result) const
{
    String key;
    if (relative_key.empty())
        key = prefix;
    else if (prefix.empty())
        key = relative_key;
    else
        key = prefix + "." + relative_key;

    Keys children;
    config.keys(key, children);

    /// Only the leaves carry values, and only they can be reported: an intermediate node is read
    /// as a section (with `has`), which says nothing about the keys inside it.
    if (children.empty())
    {
        if (!relative_key.empty() && !isUsed(key))
            result.push_back(unescapeDots(relative_key));
        return;
    }

    for (const auto & child : children)
        collectUnusedKeys(prefix, relative_key.empty() ? child : relative_key + "." + child, result);
}

}
