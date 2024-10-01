#pragma once

/// Get number of CPU cores without hyper-threading.
/// The calculation respects possible cgroups limits.
unsigned getNumberOfPhysicalCPUCores();
