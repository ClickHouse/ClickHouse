#pragma once

#include <Core/Types_fwd.h>

#include <boost/program_options.hpp>

namespace DB
{

template <typename T>
void addProgramOptionAsMultitoken(T &cmd_settings, boost::program_options::options_description & options, std::string_view name, const typename T::SettingFieldRef & field)
{
    auto on_program_option = boost::function1<void, const Strings &>([&cmd_settings, name](const Strings & values) { cmd_settings.set(name, values.back()); });
    if (field.getTypeName() == "Bool")
    {
        options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
                name.data(), boost::program_options::value<Strings>()->multitoken()->composing()->implicit_value(std::vector<std::string>{"1"}, "1")->notifier(on_program_option), field.getDescription())));
    }
    else
    {
        options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
                name.data(), boost::program_options::value<Strings>()->multitoken()->composing()->notifier(on_program_option), field.getDescription())));
    }
}

template <typename T>
void addProgramOptionsAsMultitokens(T &cmd_settings, boost::program_options::options_description & options)
{
    const auto & settings_to_aliases = T::Traits::settingsToAliases();
    for (const auto & field : cmd_settings.all())
    {
        std::string_view name = field.getName();
        addProgramOptionAsMultitoken(cmd_settings, options, name, field);

        if (auto it = settings_to_aliases.find(name); it != settings_to_aliases.end())
            for (const auto alias : it->second)
                addProgramOptionAsMultitoken(cmd_settings, options, alias, field);
    }
}

/// Adds program options to set the settings from a command line.
/// (Don't forget to call notify() on the `variables_map` after parsing it!)
template <typename T>
void addProgramOption(T & cmd_settings, boost::program_options::options_description & options, std::string_view name, const typename T::SettingFieldRef & field)
{
    auto on_program_option = boost::function1<void, const std::string &>([&cmd_settings, name](const std::string & value) { cmd_settings.set(name, value); });
    if (field.getTypeName() == "Bool")
    {
        options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
                name.data(), boost::program_options::value<std::string>()->composing()->implicit_value("1")->notifier(on_program_option), field.getDescription()))); // NOLINT
    }
    else
    {
        options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
                name.data(), boost::program_options::value<std::string>()->composing()->notifier(on_program_option), field.getDescription()))); // NOLINT
    }
}

template <typename T>
void addProgramOptions(T &cmd_settings, boost::program_options::options_description & options)
{
    const auto & settings_to_aliases = T::Traits::settingsToAliases();
    for (const auto & field : cmd_settings.all())
    {
        std::string_view name = field.getName();
        addProgramOption(cmd_settings, options, name, field);

        if (auto it = settings_to_aliases.find(name); it != settings_to_aliases.end())
            for (const auto alias : it->second)
                addProgramOption(cmd_settings, options, alias, field);
    }
}


}
