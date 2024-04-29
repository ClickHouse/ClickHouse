#pragma once

#include <map>

#include <base/types.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <Core/Streaming/CursorTree_fwd.h>

namespace DB
{

struct CursorData
{
    CursorTreeNodePtr tree;
    std::optional<String> keeper_key;
};

/// full storage name -> CursorData
using CursorDataMap = std::map<String, CursorData>;

void readBinary(CursorDataMap & data_map, ReadBuffer & buf);
void writeBinary(const CursorDataMap & data_map, WriteBuffer & buf);

}
