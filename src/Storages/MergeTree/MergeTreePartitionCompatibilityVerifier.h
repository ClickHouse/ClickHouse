#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Core/Field.h>

namespace DB
{

/*
 * Asserts dst partition exp is monotonic on the source global min_max idx Range.
 * If it is, applies dst partition exp and asserts min_max partition ids are the same.
 *
 * Otherwise, throws exception.
 * */

class MergeTreePartitionCompatibilityVerifier
{
public:
    using DataPart = IMergeTreeDataPart;
    using DataPartPtr = std::shared_ptr<const DataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;

    struct SourceTableInfo
    {
        const MergeTreeData & storage;
        const Field & min_idx;
        const Field & max_idx;
    };

    static void verify(const SourceTableInfo & source_table_info, const StorageMetadataPtr & destination_table_metadata, ContextPtr context);

private:
    static bool isDestinationPartitionExpressionMonotonicallyIncreasing(
        const SourceTableInfo & source_table_info,
        const StorageMetadataPtr & destination_table_metadata,
        ContextPtr context
    );

    static void validatePartitionIds(
        const SourceTableInfo & source_table_info,
        const StorageMetadataPtr & metadata,
        ContextPtr context
    );
};

}
