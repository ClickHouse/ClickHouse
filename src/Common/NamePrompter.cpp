#include <IO/WriteHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/NamePrompter.h>

namespace DB
{

void checkPromptingNotCancelled()
{
    CurrentThread::checkIfNotCancelled();
}

String getHintsErrorMessageSuffix(const VectorWithMemoryTracking<String> & hints)
{
    if (hints.empty())
        return {};

    return ". Maybe you meant: " + toString(hints);
}

void appendHintsMessage(String & message, const VectorWithMemoryTracking<String> & hints)
{
    message += getHintsErrorMessageSuffix(hints);
}

}
