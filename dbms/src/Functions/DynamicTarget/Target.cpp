#include "Target.h"

namespace DB::DynamicTarget
{

bool IsArchSupported(TargetArch arch)
{
    // TODO(dakovalkov): use cpuid
    return arch != TargetArch::AVX512;
}

} // namespace DB::DynamicTarget