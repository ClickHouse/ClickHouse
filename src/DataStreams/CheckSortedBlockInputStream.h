#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Core/SortDescription.h>
#include <Columns/IColumn.h>

namespace DB
{
using SortDescriptionsWithPositions = std::vector<SortColumnDescription>;

/// Streams checks that flow of blocks is sorted in the sort_description order
/// Othrewise throws exception in readImpl function.
class CheckSortedBlockInputStream : public IBlockInputStream
{
public:
    CheckSortedBlockInputStream(
        const BlockInputStreamPtr & input_,
        const SortDescription & sort_description_);

    String getName() const override { return "CheckingSorted"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    Block header;
    SortDescriptionsWithPositions sort_description_map;
    Columns last_row;

private:
    /// Just checks, that all sort_descriptions has column_number
    SortDescriptionsWithPositions addPositionsToSortDescriptions(const SortDescription & sort_description);
};
}
