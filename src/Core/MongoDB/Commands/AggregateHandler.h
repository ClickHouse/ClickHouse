#pragma once


#include <stdexcept>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Common/CurrentThread.h>
#include "../Binary.h"
#include "../Document.h"
#include "../Element.h"
#include "Commands.h"

namespace DB
{
namespace MongoDB
{

BSON::Document::Ptr handleAggregate(const Command::Ptr command, ContextMutablePtr context);

}
} // namespace DB::MongoDB
