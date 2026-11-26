#include <Common/Wyrd/Facade/Preemption.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Strategies/Simulation/InjectFault.h>
#    include <Common/Wyrd/Strategies/Adversary/InjectFault.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

static void preemptionPoint(std::source_location location)
{
    injectFault("preemptionPoint", location);
}

}

namespace AdversaryMode
{

static void preemptionPoint(std::source_location location)
{
    injectFault("preemptionPoint", location);
}

}

void preemptionPoint(std::source_location location)
{
    if (getMode() == Mode::Simulation)
        SimulationMode::preemptionPoint(location);
    else
        AdversaryMode::preemptionPoint(location);
}

#else

void preemptionPoint(std::source_location)
{
}

#endif

}
