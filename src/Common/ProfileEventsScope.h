#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>

namespace DB
{

class ScopedProfileEvents : private boost::noncopyable
{
private:
    bool attached = false;
    ProfileEvents::Counters * performance_counters_scope;
public:
    explicit ScopedProfileEvents(ProfileEvents::Counters * performance_counters_scope_);

    ~ScopedProfileEvents();
};


}

