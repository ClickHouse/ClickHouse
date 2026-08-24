#pragma once

#include <vector>

#include <Common/VectorWithMemoryTracking.h>
#include <Core/ValueWithType.h>


namespace DB
{

using ValuesWithType = VectorWithMemoryTracking<ValueWithType>;

}
