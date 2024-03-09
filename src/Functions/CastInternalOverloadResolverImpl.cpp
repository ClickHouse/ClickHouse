#include <Functions/CastOverloadResolverImpl.h>

namespace DB
{

template class CastOverloadResolverImpl<CastType::nonAccurate, true>;
template class CastOverloadResolverImpl<CastType::accurate, true>;
template class CastOverloadResolverImpl<CastType::accurateOrNull, true>;

}
