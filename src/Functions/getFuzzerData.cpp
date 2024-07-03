#ifdef FUZZING_MODE
#include <Functions/getFuzzerData.h>

namespace DB
{

REGISTER_FUNCTION(GetFuzzerData)
{
    factory.registerFunction<FunctionGetFuzzerData>();
}

}
#endif
