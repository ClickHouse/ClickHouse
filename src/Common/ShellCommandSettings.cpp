#include <Poco/String.h>
#include <Common/Exception.h>
#include <Common/ShellCommandSettings.h>
#include <magic_enum.hpp>

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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown stderr reaction: {}. Possible values are 'none', 'log' and 'throw'", config);

    return *reaction;
}

}
