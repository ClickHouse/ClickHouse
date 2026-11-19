#include <Common/Wyrd/Strategies/Adversary/InjectFault.h>
#include <Common/Wyrd/Strategies/Adversary/Access.h>

namespace WYRD_NAMESPACE(DB::Wyrd)
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
