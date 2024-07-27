#pragma once

#include <re2/re2.h>


namespace DB
{

inline bool maskURIPassword(std::string * uri)
{
    return RE2::Replace(uri, R"(([^:]+://[^:]*):([^@]*)@(.*))", "\\1:[HIDDEN]@\\3");
}

}
