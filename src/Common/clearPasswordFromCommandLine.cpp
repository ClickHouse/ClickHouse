#include <cstring>
#include <string_view>
#include <Common/clearPasswordFromCommandLine.h>

using namespace std::literals;

void clearPasswordFromCommandLine(int argc, char ** argv)
{
    /// Options of the programs calling this function whose values are credentials.
    static constexpr std::string_view secret_options[]
        = {"--password"sv, "--jwt"sv, "--ssh-key-passphrase"sv, "--one-time-password"sv, "--oauth-client-secret"sv, "--identity"sv};

    /// Backwards, so that an option is recognized before a preceding `--password` without a value zeroes it.
    for (int arg = argc - 1; arg >= 1; --arg)
    {
        const std::string_view argument = argv[arg];
        for (const auto option : secret_options)
        {
            if (argument == option)
            {
                /// clickhouse-client also accepts `--option = value`.
                const int value = (arg + 2 < argc && argv[arg + 1] == "="sv) ? arg + 2 : arg + 1;
                if (value < argc)
                    memset(argv[value], 0, strlen(argv[value]));
                break;
            }
            if (argument.size() > option.size() && argument.starts_with(option)
                && (argument[option.size()] == '=' || argument[option.size()] == ':'))
            {
                memset(argv[arg] + option.size() + 1, 0, argument.size() - option.size() - 1);
                break;
            }
        }
    }
}
