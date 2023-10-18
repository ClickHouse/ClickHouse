#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

struct ASTShowMergesIDAndQueryName
{
    static constexpr auto ID = "ShowMergesQuery";
    static constexpr auto Query = "SHOW MERGES";
};

using ASTShowMergesQuery = ASTQueryWithOutputImpl<ASTShowMergesIDAndQueryName>;

}
