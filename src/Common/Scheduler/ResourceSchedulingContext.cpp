#include <Common/Scheduler/ResourceSchedulingContext.h>

namespace DB
{

ResourceSchedulingContext & ResourceSchedulingContext::anonymous()
{
    // Leaked intentionally: it must outlive every leaf whose destructor erases from it.
    static ResourceSchedulingContext * instance = new ResourceSchedulingContext(0, 1.0, 1.0, 0.0, 0.0, 0.0, 0);
    return *instance;
}

}
