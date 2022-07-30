#include <IO/WriteHelpers.h>
#include <Common/NamePrompter.h>

namespace DB::detail
{
void appendHintsMessageImpl(String & message, const std::vector<String> & hints)
{
    if (hints.empty())
    {
        return;
    }

    message += ". Maybe you meant: " + toString(hints);
}
}
