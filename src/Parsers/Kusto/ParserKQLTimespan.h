#pragma once

#include <base/types.h>

#include <string>

namespace DB
{
class ParserKQLTimespan
{
public:
    static std::string compose(Int64 ticks);
};
}
