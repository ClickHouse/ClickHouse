#pragma once

/// Get the number of CPU cores to use. Depending on the machine size we choose
/// between the number of physical and logical cores.
/// Also under cgroups we respect possible cgroups limits.
unsigned getNumberOfCPUCoresToUse();
