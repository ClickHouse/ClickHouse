#include <Functions/CastOverloadResolverImpl.h>

namespace DB
{

template class CastOverloadResolverImpl<CastType::nonAccurate, false>;
template class CastOverloadResolverImpl<CastType::accurate, false>;
template class CastOverloadResolverImpl<CastType::accurateOrNull, false>;

}
