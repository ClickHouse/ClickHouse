#include <Core/Streaming/ICursor.h>

namespace DB
{

ICursor::ICursor(CursorTree tree_) : tree{std::move(tree_)}
{
}

}
