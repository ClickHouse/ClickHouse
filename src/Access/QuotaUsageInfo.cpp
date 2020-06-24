#include <Access/QuotaUsageInfo.h>
#include <boost/range/algorithm/fill.hpp>


namespace DB
{
QuotaUsageInfo::QuotaUsageInfo() : quota_id(UUID(UInt128(0)))
{
}


QuotaUsageInfo::Interval::Interval()
{
    boost::range::fill(used, 0);
    boost::range::fill(max, 0);
}
}
