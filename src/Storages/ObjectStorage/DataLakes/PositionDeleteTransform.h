#pragma once
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace roaring
{
class Roaring;
}

namespace DB
{

class PositionDeleteTransform : public DB::ISimpleTransform
{
public:
    using ExcludedRows = DB::DataLakeObjectMetadata::ExcludedRows;
    using ExcludedRowsPtr = DB::DataLakeObjectMetadata::ExcludedRowsPtr;

    PositionDeleteTransform(
        const DB::SharedHeader & header_,
        ExcludedRowsPtr excluded_rows_);

    String getName() const override { return "PositionDeleteTransform"; }

    void transform(DB::Chunk & chunk) override;

private:
    ExcludedRowsPtr excluded_rows;
};
}
