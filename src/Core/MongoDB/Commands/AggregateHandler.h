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

BSON::Document::Ptr handleAggregate(const Command::Ptr command, ContextMutablePtr context);

}
}
