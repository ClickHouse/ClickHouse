#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

struct ASTShowEngineAndQueryNames
{
    static constexpr auto ID = "ShowEngineQuery";
    static constexpr auto Query = "SHOW ENGINES";
};

using ASTShowEnginesQuery = ASTQueryWithOutputImpl<ASTShowEngineAndQueryNames>;

}
