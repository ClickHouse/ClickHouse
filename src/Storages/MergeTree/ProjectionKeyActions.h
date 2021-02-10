#pragma once

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class Block;

class ProjectionKeyActions
{
public:
    bool add(ASTPtr & node, const std::string & node_name, Block & key_block);
    std::map<NameAndTypePair, ASTPtr> func_map;
    std::map<std::string, std::string> name_map;
};

}
