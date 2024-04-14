#pragma once

#include <Core/Streaming/CursorTree.h>

namespace DB
{

/// TODO
class ICursor
{
public:
    // explicit ICursor(CursorTree tree_);
    virtual ~ICursor() = default;

protected:
    // CursorTree tree;
};

}
