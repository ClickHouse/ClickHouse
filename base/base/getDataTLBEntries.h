#pragma once

#include <cstddef>

/// Number of 4 KiB page translations the last level data TLB of the current CPU holds. Read from CPUID
/// on x86_64 (Intel leaf 0x18, AMD leaf 0x80000006). On aarch64 Linux the core is identified from
/// `MIDR_EL1` in sysfs and looked up among known Arm Neoverse cores, because the TLB geometry is not
/// exposed there; Darwin exposes it neither. Defaults to 1024 if probing fails. Computed once and cached.
size_t getDataTLBEntries();
