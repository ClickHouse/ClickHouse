#include <IO/WriteHelpers.h>
#include <Common/NamePrompter.h>

namespace DB
{

void appendHintsMessage(String & message, const std::vector<String> & hints)
{
    if (hints.empty())
    {
        return;
    }

    message += ". Maybe you meant: " + toString(hints);
}

}
