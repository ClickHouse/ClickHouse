#include <Core/Block.h>
#include <Core/ShuffleCursor.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void ShuffleCursor::reset(const Block & block, IColumnPermutation * perm)
{
    if (block.getColumns().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty column list in block");
    reset(block.getColumns(), block, block.getColumns()[0]->size(), perm);
}

void ShuffleCursor::reset(const Columns & columns, const Block &, UInt64 num_rows, IColumnPermutation * perm)
{
    all_columns.clear();

    size_t num_columns = columns.size();

    for (size_t j = 0; j < num_columns; ++j)
        all_columns.push_back(columns[j].get());

    pos = 0;
    rows = num_rows;
    permutation = perm;
}


}
