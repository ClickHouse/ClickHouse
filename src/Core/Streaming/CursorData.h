#pragma once

#include <Core/Streaming/CursorTree.h>

namespace DB
{

struct CursorData
{
    CursorTreeNodePtr tree;
    std::optional<String> keeper_key;
};

using CursorDataMap = std::map<String, CursorData>;

void readBinary(CursorDataMap & data_map, ReadBuffer & buf);
void writeBinary(const CursorDataMap & data_map, WriteBuffer & buf);

}
