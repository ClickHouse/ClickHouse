#pragma once

/// `setCallbackContext` is called by the ClickHouse client/local startup
/// to bind the line-reader callbacks (in RustylineCallbacks.cpp) to the
/// current Suggest store, query Context and word-break configuration.
/// Kept out of `RustylineCallbacks.h` so the latter stays self-contained
/// for the cxx-build standalone compile.

#include <Client/LineReader.h>

namespace DB
{
class Context;
}

namespace DB::rustyline
{

void setCallbackContext(::DB::LineReader::Suggest * suggest,
                        const ::DB::Context * context,
                        const char * word_break_characters,
                        bool rainbow_parentheses,
                        bool embedded_mode);

}
