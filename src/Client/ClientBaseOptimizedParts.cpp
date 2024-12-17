#include <Client/ClientApplicationBase.h>
#include <Core/BaseSettingsProgramOptions.h>

namespace DB
{

/**
 * Program options parsing is very slow in debug builds and it affects .sh tests
 * causing them to timeout sporadically.
 * It seems impossible to enable optimizations for a single function (only to disable them), so
 * instead we extract the code to a separate source file and compile it with different options.
 */
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNRECOGNIZED_ARGUMENTS;
}

namespace
{

/// Define transparent hash to we can use
/// std::string_view with the containers
struct TransparentStringHash
{
    using is_transparent = void;
    size_t operator()(std::string_view txt) const
    {
        return std::hash<std::string_view>{}(txt);
    }
};

/*
 * This functor is used to parse command line arguments and replace dashes with underscores,
 * allowing options to be specified using either dashes or underscores.
 */
class OptionsAliasParser
{
public:
    explicit OptionsAliasParser(const boost::program_options::options_description& options)
    {
        options_names.reserve(options.options().size());
        for (const auto& option : options.options())
            options_names.insert(option->long_name());
    }

    /*
     * Parses arguments by replacing dashes with underscores, and matches the resulting name with known options
     * Implements boost::program_options::ext_parser logic
     */
    std::pair<std::string, std::string> operator()(const std::string & token) const
    {
        if (!token.starts_with("--"))
            return {};
        std::string arg = token.substr(2);

        // divide token by '=' to separate key and value if options style=long_allow_adjacent
        auto pos_eq = arg.find('=');
        std::string key = arg.substr(0, pos_eq);

        if (options_names.contains(key))
            // option does not require any changes, because it is already correct
            return {};

        std::replace(key.begin(), key.end(), '-', '_');
        if (!options_names.contains(key))
            // after replacing '-' with '_' argument is still unknown
            return {};

        std::string value;
        if (pos_eq != std::string::npos && pos_eq < arg.size())
            value = arg.substr(pos_eq + 1);

        return {key, value};
    }

private:
    std::unordered_set<std::string> options_names;
};

}

void ClientApplicationBase::parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments)
{
    if (allow_repeated_settings)
        cmd_settings.addToProgramOptionsAsMultitokens(options_description.main_description.value());
    else
        cmd_settings.addToProgramOptions(options_description.main_description.value());

    if (allow_merge_tree_settings)
    {
        /// Add merge tree settings manually, because names of some settings
        /// may clash. Query settings have higher priority and we just
        /// skip ambiguous merge tree settings.
        auto & main_options = options_description.main_description.value();

        std::unordered_set<std::string, TransparentStringHash, std::equal_to<>> main_option_names;
        for (const auto & option : main_options.options())
            main_option_names.insert(option->long_name());

        for (const auto & setting : cmd_merge_tree_settings.all())
        {
            const auto add_setting = [&](const std::string_view name)
            {
                if (auto it = main_option_names.find(name); it != main_option_names.end())
                    return;

                if (allow_repeated_settings)
                    addProgramOptionAsMultitoken(cmd_merge_tree_settings, main_options, name, setting);
                else
                    addProgramOption(cmd_merge_tree_settings, main_options, name, setting);
            };

            const auto & setting_name = setting.getName();

            add_setting(setting_name);

            const auto & settings_to_aliases = MergeTreeSettings::Traits::settingsToAliases();
            if (auto it = settings_to_aliases.find(setting_name); it != settings_to_aliases.end())
            {
                for (const auto alias : it->second)
                {
                    add_setting(alias);
                }
            }
        }
    }

    /// Parse main commandline options.
    auto parser = po::command_line_parser(arguments)
                      .options(options_description.main_description.value())
                      .extra_parser(OptionsAliasParser(options_description.main_description.value()))
                      .allow_unregistered();
    po::parsed_options parsed = parser.run();

    /// Check unrecognized options without positional options.
    auto unrecognized_options = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::exclude_positional);
    if (!unrecognized_options.empty())
    {
        auto hints = this->getHints(unrecognized_options[0]);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized option '{}'. Maybe you meant {}",
                            unrecognized_options[0], toString(hints));

        throw Exception(ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized option '{}'", unrecognized_options[0]);
    }

    /// Check positional options.
    for (const auto & op : parsed.options)
    {
        if (!op.unregistered && op.string_key.empty() && !op.original_tokens[0].starts_with("--")
            && !op.original_tokens[0].empty() && !op.value.empty())
        {
            /// Two special cases for better usability:
            /// - if the option contains a whitespace, it might be a query: clickhouse "SELECT 1"
            /// These are relevant for interactive usage - user-friendly, but questionable in general.
            /// In case of ambiguity or for scripts, prefer using proper options.

            const auto & token = op.original_tokens[0];
            po::variable_value value(boost::any(op.value), false);

            const char * option;
            if (token.contains(' '))
                option = "query";
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Positional option `{}` is not supported.", token);

            if (!options.emplace(option, value).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Positional option `{}` is not supported.", token);
        }
    }

    po::store(parsed, options);
}

}
