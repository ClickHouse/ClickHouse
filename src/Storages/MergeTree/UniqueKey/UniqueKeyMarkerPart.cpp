#include <Storages/MergeTree/UniqueKey/UniqueKeyMarkerPart.h>

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>

#include <Common/Exception.h>
#include <base/defines.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

namespace DB::UniqueKeyTxn
{

MarkerPartHandle createMarkerPart(
    MergeTreeData & data,
    const String & partition_id,
    Int64 block_number,
    const MergeTreePartition & partition,
    const UniqueKeyManifest & meta)
{
    /// `BAD_ARGUMENTS` not `LOGICAL_ERROR`: the latter aborts Debug builds, so
    /// gtests cannot `EXPECT_THROW` on it. Negative block numbers never come
    /// from the allocator, but fail loudly rather than stage `all_-1_-1_0`.
    if (block_number < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "createMarkerPart: block_number must be non-negative, got {}", block_number);

    chassert(meta.is_marker);

    /// Built via `createEmptyPart`, so the marker is byte-identical to a stock
    /// empty cover part (`all_<bn>_<bn>_0`, level 0, 0 rows) — only
    /// `unique_key.txt` distinguishes it.
    MergeTreePartInfo new_part_info{partition_id, block_number, block_number, /*level=*/0};
    String new_part_name = new_part_info.getPartNameAndCheckFormat(data.format_version);

    auto metadata_snapshot = data.getInMemoryMetadataPtr(data.getContext(), /*bypass_metadata_cache=*/false);
    auto [new_data_part, tmp_dir_holder] =
        data.createEmptyPart(new_part_info, partition, new_part_name, metadata_snapshot, /*txn=*/NO_TRANSACTION_PTR);

    /// Callers staging for the publish lock pass `creation_csn = INVALID_CSN`;
    /// the commit driver rewrites the manifest with the real csn at the
    /// linearization point. Durability ordering is owned by `UniqueKeyManifest::write`.
    UniqueKeyManifest::write(new_data_part->getDataPartStorage(), meta);

    /// Mirror the lightweight projection into the in-memory cache so the
    /// freshly-built marker doesn't look legacy to same-process consumers
    /// (the lazy disk read only runs at csn-seed). The commit driver
    /// overwrites this with the real csn.
    new_data_part->setUniqueKeyMeta(UniqueKeyPartMeta{meta.creation_csn, meta.is_marker});

    return MarkerPartHandle{std::move(new_data_part), std::move(tmp_dir_holder)};
}

}
