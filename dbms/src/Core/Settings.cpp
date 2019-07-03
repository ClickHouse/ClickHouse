#include "Settings.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <string.h>
#include <boost/program_options/options_description.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_PROFILE;
    extern const int NO_ELEMENTS_IN_CONFIG;
}


IMPLEMENT_SETTINGS_COLLECTION(Settings, LIST_OF_SETTINGS)


/** Set the settings from the profile (in the server configuration, many settings can be listed in one profile).
    * The profile can also be set using the `set` functions, like the `profile` setting.
    */
void Settings::setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config)
{
    String elem = "profiles." + profile_name;

    if (!config.has(elem))
        throw Exception("There is no profile '" + profile_name + "' in configuration file.", ErrorCodes::THERE_IS_NO_PROFILE);

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(elem, config_keys);

    for (const std::string & key : config_keys)
    {
        if (key == "constraints")
            continue;
        if (key == "profile")   /// Inheritance of one profile from another.
            setProfile(config.getString(elem + "." + key), config);
        else
            set(key, config.getString(elem + "." + key));
    }
}

void Settings::loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(path))
        throw Exception("There is no path '" + path + "' in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(path, config_keys);

    for (const std::string & key : config_keys)
    {
        set(key, config.getString(path + "." + key));
    }
}

void Settings::dumpToArrayColumns(IColumn * column_names_, IColumn * column_values_, bool changed_only)
{
    /// Convert ptr and make simple check
    auto column_names = (column_names_) ? &typeid_cast<ColumnArray &>(*column_names_) : nullptr;
    auto column_values = (column_values_) ? &typeid_cast<ColumnArray &>(*column_values_) : nullptr;

    size_t size = 0;

    for (const auto & setting : *this)
    {
        if (!changed_only || setting.isChanged())
        {
            if (column_names)
            {
                StringRef name = setting.getName();
                column_names->getData().insertData(name.data, name.size);
            }
            if (column_values)
                column_values->getData().insert(setting.getValueAsString());
            ++size;
        }
    }

    if (column_names)
    {
        auto & offsets = column_names->getOffsets();
        offsets.push_back(offsets.back() + size);
    }

    /// Nested columns case
    bool the_same_offsets = column_names && column_values && column_names->getOffsetsPtr() == column_values->getOffsetsPtr();

    if (column_values && !the_same_offsets)
    {
        auto & offsets = column_values->getOffsets();
        offsets.push_back(offsets.back() + size);
    }
}

void Settings::addProgramOptions(boost::program_options::options_description & options)
{
    for (size_t index = 0; index != Settings::size(); ++index)
    {
        auto on_program_option
            = boost::function1<void, const std::string &>([this, index](const std::string & value) { set(index, value); });
        options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
            Settings::getName(index).data,
            boost::program_options::value<std::string>()->composing()->notifier(on_program_option),
            Settings::getDescription(index).data)));
    }
}
}
