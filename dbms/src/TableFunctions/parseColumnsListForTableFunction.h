#pragma once

#include <Core/Block.h>


namespace DB
{
/*Parses a common argument for table functions such as table structure given in string*/
void parseColumnsListFromString(const std::string & structure, Block & sample_block, const Context & context);

}

