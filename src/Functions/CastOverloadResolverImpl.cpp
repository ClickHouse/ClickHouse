#include <Functions/CastOverloadResolverImpl.h>

namespace DB
{

template class CastOverloadResolverImpl<CastType::nonAccurate, false, CastOverloadName, CastName>;
template class CastOverloadResolverImpl<CastType::accurate, false, CastOverloadName, CastName>;
template class CastOverloadResolverImpl<CastType::accurateOrNull, false, CastOverloadName, CastName>;

}
