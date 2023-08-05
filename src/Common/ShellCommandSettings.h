#pragma once

#include <string>

namespace DB
{

enum class ExternalCommandStderrReaction
{
    NONE, /// Do nothing.
    LOG, /// Try to log all outputs of stderr from the external command.
    THROW /// Throw exception when the external command outputs something to its stderr.
};

ExternalCommandStderrReaction parseExternalCommandStderrReaction(const std::string & config);

enum class ExternalCommandErrorExitReaction
{
    NONE, /// Do nothing.
    LOG_FIRST, /// Try to log first 1_KiB outputs of stderr from the external command.
    LOG_LAST /// Same as above, but log last 1_KiB outputs.
};

ExternalCommandErrorExitReaction parseExternalCommandErrorExitReaction(const std::string & config);

}
