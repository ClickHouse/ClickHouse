#pragma once
#include <string>
#include <config.h>
#include <csignal>

namespace Poco
{
namespace Util
{
    class LayeredConfiguration;
}
}

/// Generate minidump
namespace MinidumpWriter
{
void initialize(Poco::Util::LayeredConfiguration & config);
void onFault(int sig, const siginfo_t * info, void * context);
}
