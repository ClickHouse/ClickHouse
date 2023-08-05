#include <Common/ShellCommandSettings.h>

#include <magic_enum.hpp>
#include <Poco/String.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ExternalCommandStderrReaction parseExternalCommandStderrReaction(const std::string & config)
{
    auto reaction = magic_enum::enum_cast<ExternalCommandStderrReaction>(Poco::toUpper(config));
    if (!reaction)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown stderr_reaction: {}. Possible values are 'none', 'log' and 'throw'", config);

    return *reaction;
}

ExternalCommandErrorExitReaction parseExternalCommandErrorExitReaction(const std::string & config)
{
    auto reaction = magic_enum::enum_cast<ExternalCommandErrorExitReaction>(Poco::toUpper(config));
    if (!reaction)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Unknown error_exit_reaction: {}. Possible values are 'none', 'log_first' and 'log_last'", config);

    return *reaction;
}

}
