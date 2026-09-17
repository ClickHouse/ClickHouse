#pragma once

#include <Access/Common/UDTAccessTarget.h>
#include <Interpreters/Context_fwd.h>

#include <span>

namespace DB::UDT
{

/// Checks each distinct stable database/type identity exactly once at a metadata-operation boundary.
/// Existing-table reads deliberately do not call this API.
void checkUsageAccess(const ContextPtr & context, std::span<const AccessTarget> targets);

}
