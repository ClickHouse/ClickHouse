#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

struct ASTLSQueryIDAndQueryNames
{
    static constexpr auto ID = "LSQuery";
    static constexpr auto Query = "LS";
};

using ASTLSQuery = ASTQueryWithOutputImpl<ASTLSQueryIDAndQueryNames>;

}

