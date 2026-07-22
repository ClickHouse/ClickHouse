#include <IO/WriteHelpers.h>
#include <Common/NamePrompter.h>

namespace DB
{

String getHintsErrorMessageSuffix(const std::vector<String> & hints)
{
    if (hints.empty())
        return {};

    return ". Maybe you meant: " + toString(hints);
}

void appendHintsMessage(String & message, const std::vector<String> & hints)
{
    message += getHintsErrorMessageSuffix(hints);
}

}
