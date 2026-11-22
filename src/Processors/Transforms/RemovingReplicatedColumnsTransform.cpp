#include <Processors/Transforms/RemovingReplicatedColumnsTransform.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>


namespace DB
{

RemovingReplicatedColumnsTransform::RemovingReplicatedColumnsTransform(SharedHeader header, bool keep_strings_)
    : ISimpleTransform(header, std::make_shared<const Block>(materializeBlock(*header)), false)
    , keep_strings(keep_strings_)
{
}

void RemovingReplicatedColumnsTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & col : columns)
    {
        const auto * replicated = typeid_cast<const ColumnReplicated *>(col.get());
        if (keep_strings && replicated && typeid_cast<const ColumnString *>(replicated->getNestedColumn().get()))
            continue;
        col = col->convertToFullColumnIfReplicated();
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
