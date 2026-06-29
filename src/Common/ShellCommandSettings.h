#pragma once

#include <string>

namespace DB
{

enum class ExternalCommandStderrReaction : uint8_t
{
    NONE, /// Do nothing.
    LOG, /// Try to log all outputs of stderr from the external command immediately.
    LOG_FIRST, /// Try to log first 1_KiB outputs of stderr from the external command after exit.
    LOG_LAST, /// Same as above, but log last 1_KiB outputs.
    THROW /// Immediately throw exception when the external command outputs something to its stderr.
};

ExternalCommandStderrReaction parseExternalCommandStderrReaction(const std::string & config);

}
