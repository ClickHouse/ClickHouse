#include <Processors/CursorInfo.h>

namespace DB
{

CursorInfo::CursorInfo(CursorDataMap cursors_) : cursors(std::move(cursors_))
{
}

}
