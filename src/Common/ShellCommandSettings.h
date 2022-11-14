#pragma once

#include <string>

namespace DB
{

enum class ExternalCommandStderrReaction
{
    NONE, /// Do nothing.
    LOG,  /// Try to log all outputs of stderr from the external command.
    THROW /// Throw exception when the external command outputs something to its stderr.
};

ExternalCommandStderrReaction parseExternalCommandStderrReaction(const std::string & config);

}
