#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Common/FieldVisitorToString.h>
#include <magic_enum.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace NamedCollectionConfiguration
{

void setOverridable(Poco::Util::AbstractConfiguration & config, const std::string & path, bool value);

bool hasConfigValue(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path)
{
    return config.has(path);
}

template <typename T> T getConfigValue(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path)
{
    return getConfigValueOrDefault<T>(config, path);
}

template <typename T> T getConfigValueOrDefault(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const T * default_value)
{
    if (!config.has(path))
    {
        if (!default_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}`", path);
        return *default_value;
    }

    try
    {
        if constexpr (std::is_same_v<T, String>)
            return config.getString(path);
        else if constexpr (std::is_same_v<T, UInt64>)
            return config.getUInt64(path);
        else if constexpr (std::is_same_v<T, Int64>)
            return config.getInt64(path);
        else if constexpr (std::is_same_v<T, Float64>)
            return config.getDouble(path);
        else if constexpr (std::is_same_v<T, bool>)
            return config.getBool(path);
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Unsupported type in getConfigValueOrDefault(). "
                "Supported types are String, UInt64, Int64, Float64, bool");
    }
    catch (const Poco::SyntaxException &)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot extract {} from {}",
            magic_enum::enum_name(Field::TypeToEnum<NearestFieldType<T>>::value),
            path);
    }
}

template <typename T>
void setConfigValue(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const T & value,
    bool update,
    const std::optional<bool> is_overridable)
{
    if (!update && config.has(path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key `{}` already exists", path);

    if constexpr (std::is_same_v<T, String>)
        config.setString(path, value);
    else if constexpr (std::is_same_v<T, UInt64>)
        config.setUInt64(path, value);
    else if constexpr (std::is_same_v<T, Int64>)
        config.setInt64(path, value);
    else if constexpr (std::is_same_v<T, Float64>)
        config.setDouble(path, value);
    else if constexpr (std::is_same_v<T, bool>)
        config.setBool(path, value);
    else
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Unsupported type in setConfigValue(). "
            "Supported types are String, UInt64, Int64, Float64, bool");
    if (is_overridable)
        setOverridable(config, path, *is_overridable);
}

template <typename T> void copyConfigValue(
    const Poco::Util::AbstractConfiguration & from_config,
    const std::string & from_path,
    Poco::Util::AbstractConfiguration & to_config,
    const std::string & to_path)
{
    if (!from_config.has(from_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}`", from_path);

    if (to_config.has(to_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key `{}` already exists", to_path);

    if constexpr (std::is_same_v<T, String>)
        to_config.setString(to_path, from_config.getString(from_path));
    else if constexpr (std::is_same_v<T, UInt64>)
        to_config.setUInt64(to_path, from_config.getUInt64(from_path));
    else if constexpr (std::is_same_v<T, Int64>)
        to_config.setInt64(to_path, from_config.getInt64(from_path));
    else if constexpr (std::is_same_v<T, Float64>)
        to_config.setDouble(to_path, from_config.getDouble(from_path));
    else
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Unsupported type in copyConfigValue(). "
            "Supported types are String, UInt64, Int64, Float64");
    const auto overridable = isOverridable(from_config, from_path);
    if (overridable)
        setOverridable(to_config, to_path, *overridable);
}

void removeConfigValue(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path)
{
    if (!config.has(path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}`", path);
    config.remove(path);
}

ConfigurationPtr createEmptyConfiguration(const std::string & root_name)
{
    using DocumentPtr = Poco::AutoPtr<Poco::XML::Document>;
    using ElementPtr = Poco::AutoPtr<Poco::XML::Element>;

    DocumentPtr xml_document(new Poco::XML::Document());
    ElementPtr root_element(xml_document->createElement(root_name));
    xml_document->appendChild(root_element);

    ConfigurationPtr config(new Poco::Util::XMLConfiguration(xml_document));
    return config;
}

ConfigurationPtr createConfiguration(
    const std::string & root_name, const SettingsChanges & settings, const std::unordered_map<String, bool> & overridability)
{
    namespace Configuration = NamedCollectionConfiguration;

    auto config = Configuration::createEmptyConfiguration(root_name);
    for (const auto & [name, value] : settings)
    {
        Configuration::setConfigValue<String>(*config, name, convertFieldToString(value));
        auto ovalue = overridability.find(name);
        if (ovalue != overridability.end())
            Configuration::setOverridable(*config, name, ovalue->second);
    }

    return config;
}

void listKeys(
    const Poco::Util::AbstractConfiguration & config,
    std::queue<std::string> enumerate_paths,
    std::set<std::string, std::less<>> & result,
    ssize_t depth)
{
    if (enumerate_paths.empty())
        enumerate_paths.push("");

    const bool do_finish = depth == 0;
    if (depth >= 0)
        --depth;

    auto initial_paths = std::move(enumerate_paths);
    enumerate_paths = {};
    while (!initial_paths.empty())
    {
        auto path = initial_paths.front();
        initial_paths.pop();

        Poco::Util::AbstractConfiguration::Keys keys;
        if (path.empty())
            config.keys(keys);
        else
            config.keys(path, keys);

        if (keys.empty())
        {
            result.insert(path);
        }
        else if (do_finish)
        {
            for (const auto & key : keys)
                result.emplace(path.empty() ? key : path + '.' + key);
        }
        else
        {
            for (const auto & key : keys)
                enumerate_paths.emplace(path.empty() ? key : path + '.' + key);
        }
    }

    if (enumerate_paths.empty())
        return;

    listKeys(config, enumerate_paths, result, depth);
}

std::optional<bool> isOverridable(const Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    // XPath syntax to access path's attribute 'overridable'
    // e.g. <url overridable=1>...</url>
    std::string overridable_path = path + "[@overridable]";
    if (config.has(overridable_path))
        return config.getBool(overridable_path);
    return {};
}

void setOverridable(Poco::Util::AbstractConfiguration & config, const std::string & path, const bool value)
{
    std::string overridable_path = path + "[@overridable]";
    config.setBool(overridable_path, value);
}

template String getConfigValue<String>(const Poco::Util::AbstractConfiguration & config,
                                       const std::string & path);
template UInt64 getConfigValue<UInt64>(const Poco::Util::AbstractConfiguration & config,
                                       const std::string & path);
template Int64 getConfigValue<Int64>(const Poco::Util::AbstractConfiguration & config,
                                     const std::string & path);
template Float64 getConfigValue<Float64>(const Poco::Util::AbstractConfiguration & config,
                                         const std::string & path);
template bool getConfigValue<bool>(const Poco::Util::AbstractConfiguration & config,
                                   const std::string & path);

template String getConfigValueOrDefault<String>(const Poco::Util::AbstractConfiguration & config,
                                                const std::string & path, const String * default_value);
template UInt64 getConfigValueOrDefault<UInt64>(const Poco::Util::AbstractConfiguration & config,
                                                const std::string & path, const UInt64 * default_value);
template Int64 getConfigValueOrDefault<Int64>(const Poco::Util::AbstractConfiguration & config,
                                              const std::string & path, const Int64 * default_value);
template Float64 getConfigValueOrDefault<Float64>(const Poco::Util::AbstractConfiguration & config,
                                                  const std::string & path, const Float64 * default_value);
template bool getConfigValueOrDefault<bool>(const Poco::Util::AbstractConfiguration & config,
                                            const std::string & path, const bool * default_value);

template void setConfigValue<String>(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const String & value,
    bool update,
    const std::optional<bool> is_overridable);
template void setConfigValue<UInt64>(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const UInt64 & value,
    bool update,
    const std::optional<bool> is_overridable);
template void setConfigValue<Int64>(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const Int64 & value,
    bool update,
    const std::optional<bool> is_overridable);
template void setConfigValue<Float64>(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const Float64 & value,
    bool update,
    const std::optional<bool> is_overridable);
template void setConfigValue<bool>(
    Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    const bool & value,
    bool update,
    const std::optional<bool> is_overridable);

template void copyConfigValue<String>(const Poco::Util::AbstractConfiguration & from_config, const std::string & from_path,
                                      Poco::Util::AbstractConfiguration & to_config, const std::string & to_path);
template void copyConfigValue<UInt64>(const Poco::Util::AbstractConfiguration & from_config, const std::string & from_path,
                                      Poco::Util::AbstractConfiguration & to_config, const std::string & to_path);
template void copyConfigValue<Int64>(const Poco::Util::AbstractConfiguration & from_config, const std::string & from_path,
                                     Poco::Util::AbstractConfiguration & to_config, const std::string & to_path);
template void copyConfigValue<Float64>(const Poco::Util::AbstractConfiguration & from_config, const std::string & from_path,
                                       Poco::Util::AbstractConfiguration & to_config, const std::string & to_path);
template void copyConfigValue<bool>(const Poco::Util::AbstractConfiguration & from_config, const std::string & from_path,
                                    Poco::Util::AbstractConfiguration & to_config, const std::string & to_path);
}

}
