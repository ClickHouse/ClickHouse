#pragma once

#include <Parsers/ASTAlterQuery.h>

namespace DB
{

/// TODO: Make normal interfaces
using ExecuteCommands = std::vector<const ASTAlterCommand *>;

}
