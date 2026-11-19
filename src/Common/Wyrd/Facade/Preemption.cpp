#include <Common/Wyrd/Facade/Preemption.h>

#if defined(WYRD_SIMULATION)
#    include <Common/Wyrd/Strategies/Simulation/InjectFault.h>
#elif defined(WYRD_ADVERSARY)
#    include <Common/Wyrd/Strategies/Adversary/InjectFault.h>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_SIMULATION)

void preemptionPoint(std::source_location location)
{
    injectFault("preemptionPoint", location);
}

#elif defined(WYRD_ADVERSARY)

void preemptionPoint(std::source_location location)
{
    injectFault("preemptionPoint", location);
}

#else

void preemptionPoint(std::source_location)
{
}

#endif

}
