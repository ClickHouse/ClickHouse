#pragma once
#include <string>
#include <config.h>
#include <bits/types/siginfo_t.h>

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
void signalHandler(int sig, siginfo_t * info, void * context);
}
