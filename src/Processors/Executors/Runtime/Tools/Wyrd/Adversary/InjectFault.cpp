#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/InjectFault.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/Access.h>

namespace DB::Runtime::Wyrd
{

void injectFault(const char *, std::source_location)
{
    if (auto * current = adversary())
        current->threadFault();
}

bool spuriousFailure(std::source_location)
{
    if (auto * current = adversary())
        return current->threadSpuriousFailure();
    return false;
}

}
