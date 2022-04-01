#pragma once
#include <Processors/ISimpleTransform.h>
#include <Core/SortDescription.h>
#include <Columns/IColumn.h>

namespace DB
{
using SortDescriptionsWithPositions = std::vector<SortColumnDescription>;

/// Streams checks that flow of blocks is sorted in the sort_description order
/// Othrewise throws exception in readImpl function.
class CheckSortedTransform : public ISimpleTransform
{
public:
    CheckSortedTransform(Block header_, const SortDescription & sort_description_);

    String getName() const override { return "CheckSortedTransform"; }


protected:
    void transform(Chunk & chunk) override;

private:
    Block header;
    SortDescriptionsWithPositions sort_description_map;
    Columns last_row;

    /// Just checks, that all sort_descriptions has column_number
    static SortDescriptionsWithPositions addPositionsToSortDescriptions(const SortDescription & sort_description);
};
}
