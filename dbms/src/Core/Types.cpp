#include <Core/Types.h>

namespace DB
{

template struct Decimal<Int32>;
template struct Decimal<Int64>;
template struct Decimal<Int128>;

}
