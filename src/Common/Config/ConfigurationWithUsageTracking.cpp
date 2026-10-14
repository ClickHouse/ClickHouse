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

/// The position of the last component of `key` (the name of the element inside its section),
/// skipping the escaped dots (see `unescapeDots`).
size_t getNamePosition(const String & key)
{
    for (size_t pos = key.size(); pos > 0; --pos)
    {
        if (key[pos - 1] == '.' && (pos < 2 || key[pos - 2] != '\\'))
            return pos;
    }
    return 0;
}

/// The key of the section containing `key`.
String getParentKey(const String & key)
{
    const size_t pos = getNamePosition(key);
    return pos == 0 ? String{} : key.substr(0, pos - 1);
}

/// Whether `key` (normalized) may be read by the code that enumerated its section and picked
/// the elements by a pattern of their names. Such code looks up the pattern as well, so it is known from `used`:
/// - the bare name of a repeated element (see `getKeysFromConfig` of an `encrypted` disk) matches
///   the name itself and its repetitions (`key`, `key[1]`, ...), but not `key_typo`;
/// - a prefix looked up with `getNamePrefixKey` (see `getHTTPHeaders`) matches every name starting with it
///   (`header`, `header_x`, ...).
bool matchesNameReadInSection(const String & key, const std::unordered_set<String> & used)
{
    const String parent = getParentKey(key);
    const std::string_view name = std::string_view(key).substr(getNamePosition(key));
    for (const auto & used_key : used)
    {
        const size_t used_name_pos = getNamePosition(used_key);
        if (used_name_pos == used_key.size())
            continue;
        if (getParentKey(used_key) != parent)
            continue;
        std::string_view used_name = std::string_view(used_key).substr(used_name_pos);
        if (used_name.ends_with(ConfigurationWithUsageTracking::name_prefix_suffix))
        {
            used_name.remove_suffix(1);
            if (name.starts_with(used_name))
                return true;
        }
        else if (name.starts_with(used_name) && (name.size() == used_name.size() || name[used_name.size()] == '['))
            return true;
    }
    return false;
}

}

String ConfigurationWithUsageTracking::unescapeDots(const String & key)
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

ConfigurationWithUsageTracking::ConfigurationWithUsageTracking(const Poco::Util::AbstractConfiguration & config_)
    : config(config_)
{
    config.duplicate();
}

ConfigurationWithUsageTracking::~ConfigurationWithUsageTracking()
{
    config.release();
}

bool ConfigurationWithUsageTracking::getRaw(const std::string & key, std::string & value) const
{
    const bool present = config.has(key);
    {
        std::lock_guard lock(mutex);
        String normalized_key = normalizeKey(key);
        if (present)
            usage.present.insert(normalized_key);
        usage.used.insert(std::move(normalized_key));
    }

    /// A missing key is reported by returning false, while `getRawString` throws.
    if (!present)
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

    std::lock_guard lock(mutex);
    String normalized_key = normalizeKey(key);
    for (const auto & child : range)
        usage.present.insert(normalized_key.empty() ? child : normalized_key + "." + child);
    usage.enumerated.insert(std::move(normalized_key));
}

void ConfigurationWithUsageTracking::markAsUsed(const String & key) const
{
    std::lock_guard lock(mutex);
    usage.used.insert(normalizeKey(key));
}

ConfigurationWithUsageTracking::Usage ConfigurationWithUsageTracking::getUsage() const
{
    std::lock_guard lock(mutex);
    return usage;
}

bool ConfigurationWithUsageTracking::isUsed(const String & key) const
{
    std::lock_guard lock(mutex);
    return usage.used.contains(normalizeKey(key));
}

Strings ConfigurationWithUsageTracking::getUnusedKeys(const String & prefix) const
{
    Strings result;
    collectUnusedKeys(prefix, "", nullptr, result, nullptr);
    return result;
}

Strings ConfigurationWithUsageTracking::getUnknownKeys(const String & prefix, const Usage & previous, Strings * deferred_sections) const
{
    Strings result;
    collectUnusedKeys(prefix, "", &previous, result, deferred_sections);
    return result;
}

void ConfigurationWithUsageTracking::collectUnusedKeys(
    const String & prefix, const String & relative_key, const Usage * previous, Strings & result, Strings * deferred_sections) const
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
    /// The parent of `key` is an enumerated section of the previous configuration.
    bool in_enumerated_section = false;
    String normalized_key;
    if (previous && !relative_key.empty())
    {
        normalized_key = normalizeKey(key);
        in_enumerated_section = previous->enumerated.contains(getParentKey(normalized_key));
    }

    if (children.empty())
    {
        /// A leaf of an enumerated section is not judged only if the enumerating code may pick it
        /// by a pattern of its name. Any other leaf there is unknown as well, otherwise an element
        /// with a typo next to the keys of an `encrypted` disk would let a reload be applied partially.
        if (!relative_key.empty() && !isUsed(key)
            && !(in_enumerated_section && matchesNameReadInSection(normalized_key, previous->used)))
            result.push_back(unescapeDots(relative_key));
        return;
    }

    if (previous && !relative_key.empty() && !previous->present.contains(normalizeKey(key)) && (in_enumerated_section || isUsed(key)))
    {
        if (deferred_sections)
            deferred_sections->push_back(relative_key);
        return;
    }

    for (const auto & child : children)
        collectUnusedKeys(prefix, relative_key.empty() ? child : relative_key + "." + child, previous, result, deferred_sections);
}

}
