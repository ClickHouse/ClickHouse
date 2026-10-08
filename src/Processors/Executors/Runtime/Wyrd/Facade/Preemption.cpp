#include <Processors/Executors/Runtime/Wyrd/Facade/Preemption.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/InjectFault.h>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/InjectFault.h>
#endif

namespace DB::Runtime::Wyrd
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
