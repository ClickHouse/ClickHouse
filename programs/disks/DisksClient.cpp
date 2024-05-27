#include "DisksClient.h"
#include <Client/ClientBase.h>
#include <Client/ReplxxLineReader.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/HelpFormatter.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/EventNotifier.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/filesystemHelpers.h>

#include <Disks/registerDisks.h>

#include <Formats/registerFormats.h>
#include <Common/TerminalSize.h>

namespace DB
{
std::vector<String> split(const String & text, const String & delimiters)
{
    std::vector<String> arguments;
    auto prev = text.begin();
    auto pos = std::find_if(text.begin(), text.end(), [&](char x) { return delimiters.contains(x); });
    while (pos != text.end())
    {
        if (pos > prev)
        {
            arguments.push_back({prev, pos});
        }
        prev = ++pos;
        pos = std::find_if(prev, text.end(), [&](char x) { return delimiters.contains(x); });
    }
    if (pos > prev)
    {
        arguments.push_back({prev, text.end()});
    }
    return arguments;
    }
}
