#include <Functions/CastOverloadResolverImpl.h>

namespace DB
{

template class CastOverloadResolverImpl<CastType::nonAccurate, true, CastInternalOverloadName, CastInternalName>;
template class CastOverloadResolverImpl<CastType::accurate, true, CastInternalOverloadName, CastInternalName>;
template class CastOverloadResolverImpl<CastType::accurateOrNull, true, CastInternalOverloadName, CastInternalName>;

}
