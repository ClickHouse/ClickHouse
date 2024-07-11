#pragma once

#include <Core/Streaming/CursorData.h>

namespace DB
{

class CursorMerger
{
public:
    void add(const CursorDataMap & data_map);

    bool isEmpty() const;
    bool hasSome() const;

    CursorDataMap finalize();

private:
    CursorDataMap merged_data_map;
};

}
