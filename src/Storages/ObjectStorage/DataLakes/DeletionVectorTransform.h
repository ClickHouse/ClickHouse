#pragma once
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace roaring
{
class Roaring;
}

namespace DB
{

class DeletionVectorTransform final : public DB::ISimpleTransform
{
public:
    using ExcludedRows = DB::DataLakeObjectMetadata::ExcludedRows;
    using ExcludedRowsPtr = DB::DataLakeObjectMetadata::ExcludedRowsPtr;

    /// Skips the rows in `rows_`, or, if `rows_are_selection_`, skips all rows except the rows in `rows_`.
    DeletionVectorTransform(
        const DB::SharedHeader & header_,
        ExcludedRowsPtr rows_,
        bool rows_are_selection_ = false);

    String getName() const override { return "DeletionVectorTransform"; }

    void transform(DB::Chunk & chunk) override;

    static void transform(DB::Chunk & chunk, const ExcludedRows & rows, bool rows_are_selection = false);

private:
    ExcludedRowsPtr rows;
    bool rows_are_selection;
};
}
