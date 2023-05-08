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

    /*
     * // TODO IMPROVE DOCS
     * parameters a bit confusing, but here's the breakdown:
     *  storage - storage containing source table info
     *  min/max idx - source global min_max idx
     *  metadata - destination table metadata
     * */
    static void verify(const MergeTreeData & storage, Field min_idx, Field max_idx, const StorageMetadataPtr & metadata, ContextPtr context);

private:
    static bool isDestinationPartitionExpressionMonotonicallyIncreasing(Field min,
                                                          Field max,
                                                          const StorageMetadataPtr & metadata,
                                                          ContextPtr context);

    static void validatePartitionIds(
        const MergeTreeData & storage,
        Field min,
        Field max,
        const StorageMetadataPtr & metadata,
        ContextPtr context
    );
};

}
