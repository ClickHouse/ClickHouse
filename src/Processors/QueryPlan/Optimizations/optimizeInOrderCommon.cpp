
#include <Processors/QueryPlan/Optimizations/optimizeInOrderCommon.h>


namespace DB::QueryPlanOptimizations
{

Poco::Logger * getLogger()
{
    static Poco::Logger & logger = Poco::Logger::get("QueryPlanOptimizations");
    return &logger;
}

}
