#pragma once
#include <stdexcept>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Common/CurrentThread.h>
#include "../BSON/Binary.h"
#include "../BSON/Document.h"
#include "../BSON/Element.h"
#include "Commands.h"

namespace DB
{
namespace MongoDB
{

struct FindCommand
{
    using Ptr = Poco::SharedPtr<FindCommand>;

    FindCommand() = default;

    std::string db_name;
    std::string collection_name;

    std::optional<BSON::Document::Ptr> filter;
    std::optional<BSON::Document::Ptr> sort;
    std::optional<BSON::Document::Ptr> projection;
    std::optional<Int32> limit;
    // TODO support other options
};

FindCommand parseFindCommand(Command::Ptr command);
BSON::Document::Ptr handleFind(Command::Ptr command, ContextMutablePtr context);

}
}
