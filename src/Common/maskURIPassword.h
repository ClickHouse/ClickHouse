#pragma once

#include <Common/re2.h>


namespace DB
{

inline bool maskURIPassword(std::string * uri)
{
    return RE2::Replace(uri, R"(([^:]+://[^:]*):([^@]*)@(.*))", "\\1:[HIDDEN]@\\3");
}

}
