#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

struct ASTShowUserProcessesIDAndQueryNames
{
    static constexpr auto ID = "ShowUserProcesses";
    static constexpr auto Query = "SHOW USER PROCESSES";
};

using ASTShowUserProcessesQuery = ASTQueryWithOutputImpl<ASTShowUserProcessesIDAndQueryNames>;

}
