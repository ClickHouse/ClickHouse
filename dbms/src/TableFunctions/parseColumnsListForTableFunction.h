#pragma once

#include <Core/Block.h>


namespace DB
{

void parseColumnsList(const std::string & structure, Block & sample_block, const Context & context);

}

