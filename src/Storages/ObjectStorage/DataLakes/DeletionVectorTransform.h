#pragma once
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace DB
{

class DeletionVectorTransform final : public DB::ISimpleTransform
{
public:
    using ExcludedRows = DB::DataLakeObjectMetadata::ExcludedRows;
    using ExcludedRowsPtr = DB::DataLakeObjectMetadata::ExcludedRowsPtr;

    DeletionVectorTransform(
        const DB::SharedHeader & header_,
        ExcludedRowsPtr excluded_rows_);

    String getName() const override { return "DeletionVectorTransform"; }

    void transform(DB::Chunk & chunk) override;

    /// Drops the rows of `chunk` whose row numbers belong to the deletion bitmap.
    static void transform(DB::Chunk & chunk, const ExcludedRows & excluded_rows);

private:
    const ExcludedRowsPtr excluded_rows;
};
}
