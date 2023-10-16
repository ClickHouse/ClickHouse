#pragma once

#include <Common/IntervalKind.h>
#include <base/types.h>

#include <vector>

namespace DB
{
namespace ProtonConsts
{
const String RESERVED_EMIT_VERSION = "emit_version()";

/// Default periodic interval
const std::pair<Int64, IntervalKind> DEFAULT_PERIODIC_INTERVAL = {2, IntervalKind::Second};
}
}
