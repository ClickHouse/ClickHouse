#pragma once

#include <Core/Field.h>

namespace DB
{

/// A left-closed and right-open interval representing the preimage of a function.
struct FieldInterval
{
    Field first;
    Field second;
};

}
