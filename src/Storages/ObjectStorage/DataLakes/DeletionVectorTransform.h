#pragma once
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace roaring
{
class Roaring;
}

namespace DB
{

class DeletionVectorTransform : public DB::ISimpleTransform
{
public:
    using ExcludedRows = DB::DataLakeObjectMetadata::ExcludedRows;
    using ExcludedRowsPtr = DB::DataLakeObjectMetadata::ExcludedRowsPtr;

    DeletionVectorTransform(
        const DB::SharedHeader & header_,
        ExcludedRowsPtr excluded_rows_);

    String getName() const override { return "DeletionVectorTransform"; }

    void transform(DB::Chunk & chunk) override;

    static void transform(DB::Chunk & chunk, const ExcludedRows & excluded_rows);

private:
    ExcludedRowsPtr excluded_rows;
};
}
