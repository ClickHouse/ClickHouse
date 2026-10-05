#include <Processors/Executors/Runtime/Wyrd/Facade/Preemption.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Simulation/InjectFault.h>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Adversary/InjectFault.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

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
