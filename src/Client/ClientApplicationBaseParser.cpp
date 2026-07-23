#include <Client/ClientApplicationBase.h>
#include <Client/ClientApplicationBaseParser.h>

#include <filesystem>
#include <vector>
#include <string>
#include <utility>


namespace po = boost::program_options;


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

void ClientApplicationBase::parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments)
{
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
        /// Skip all options after empty `--`. These are processed separately into the Application configuration.
        if (op.string_key.empty() && op.original_tokens[0].starts_with("--"))
            break;

        if (!op.unregistered && op.string_key.empty() && !op.original_tokens[0].starts_with("--")
            && !op.original_tokens[0].empty() && !op.value.empty())
        {
            /// Two special cases for better usability:
            /// - if the option contains a whitespace, it might be a query: clickhouse "SELECT 1"
            /// - if the option is a filesystem file, then it's likely a queries file (clickhouse repro.sql)
            /// These are relevant for interactive usage - user-friendly, but questionable in general.
            /// In case of ambiguity or for scripts, prefer using proper options.

            const auto & token = op.original_tokens[0];
            po::variable_value value(boost::any(op.value), false);

            const char * option;
            std::error_code ec;
            if (token.contains(' '))
                option = "query";
            else if (std::filesystem::is_regular_file(std::filesystem::path{token}, ec))
                option = "queries-file";
            else if (token.contains('/') || token.contains('.'))
                /// The argument looks like a file path (contains `/` or `.`) but doesn't exist on disk.
                /// Give a clear "no such file" error rather than the generic "positional option is not supported"
                /// which is confusing when the user meant to pass a file, e.g.:
                ///     $ clickhouse local /tmp/aaa.rep
                ///     Positional option `/tmp/aaa.rep` is not supported.
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such file: {}", token);
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Positional option `{}` is not supported.", token);

            if (!options.emplace(option, value).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Positional option `{}` is not supported.", token);
        }
    }

    po::store(parsed, options);
}

OptionsAliasParser::OptionsAliasParser(const boost::program_options::options_description & options)
{
    options_names.reserve(options.options().size());
    for (const auto & option : options.options())
        options_names.insert(option->long_name());
}

std::pair<std::string, std::string> OptionsAliasParser::operator()(const std::string & token) const
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

}
