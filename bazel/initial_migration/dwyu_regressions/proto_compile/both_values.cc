#include "dwyu_proto/left/value.pb.h"
#include "dwyu_proto/right/value.pb.h"

long long protoCompileBothValues(
    const dwyu::proto_compile::left::Value & left,
    const dwyu::proto_compile::right::Value & right)
{
    return static_cast<long long>(left.left()) + right.right();
}
