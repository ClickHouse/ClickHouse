#include <IO/Resource/FairPolicy.h>

#include <IO/SchedulerNodeFactory.h>

namespace DB
{

void registerFairPolicy(SchedulerNodeFactory & factory)
{
    factory.registerMethod<FairPolicy>("fair");
}

}
