#pragma once
#include <Core/Types.h>

namespace DB
{

/// A base class for stateless table engines configurations.
struct StatelessTableEngineConfiguration
{
    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";
};

}
