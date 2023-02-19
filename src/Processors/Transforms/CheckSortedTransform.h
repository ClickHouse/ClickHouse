#pragma once
#include <Processors/ISimpleTransform.h>
#include <Core/SortDescription.h>
#include <Columns/IColumn.h>

namespace DB
{
/// Streams checks that flow of blocks is sorted in the sort_description order
/// Othrewise throws exception in readImpl function.
class CheckSortedTransform : public ISimpleTransform
{
public:
    CheckSortedTransform(const Block & header, const SortDescription & sort_description);

    String getName() const override { return "CheckSortedTransform"; }


protected:
    void transform(Chunk & chunk) override;

private:
    SortDescriptionWithPositions sort_description_map;
    Columns last_row;
};
}
