#pragma once

#include "../BSON/Document.h"
#include "AggregateHandler.h"
#include "FindHandler.h"

namespace DB
{
namespace MongoDB
{

BSON::Document::Ptr handleIsMaster();

BSON::Document::Ptr handleBuildInfo();

BSON::Document::Ptr handleGetParameter(Command::Ptr command);

BSON::Document::Ptr handlePing();

BSON::Document::Ptr handleGetLog(Command::Ptr command);

BSON::Document::Ptr handleUnknownCommand(Command::Ptr command);

BSON::Document::Ptr handleAtlasCLI(Command::Ptr command);

BSON::Document::Ptr handleError(const std::string & err_what);

}
}
