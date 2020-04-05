#include <Functions/TargetSpecific.h>

namespace DB
{

bool IsArchSupported(TargetArch arch)
{
    // TODO(dakovalkov): use cpuid
    return arch != TargetArch::AVX512;
}

} // namespace DB
