#pragma once
#include <config.h>
#include <string>

#if USE_BREAKPAD

#include <client/linux/handler/exception_handler.h>
#include <client/linux/handler/minidump_descriptor.h>

namespace Minidump2Core { int generate(const char * minidump_path, const char * coredump_path); }

#endif

namespace Poco { namespace Util { class LayeredConfiguration; } }

/// Generate minidump
namespace MinidumpWriter
{
bool useMinidump();
void initialize(Poco::Util::LayeredConfiguration & config);
void installMinidumpHandler(int);
}
