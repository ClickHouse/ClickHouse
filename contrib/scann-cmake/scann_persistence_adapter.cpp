#include <scann_persistence_adapter.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include <scann/hashes/asymmetric_hashing2/serialization.h>
#include <scann/tree_x_hybrid/tree_ah_hybrid_residual.h>
#include <scann/utils/types.h>

namespace research_scann
{

StatusOr<SingleMachineFactoryOptions> TreeAHHybridResidualPersistenceAdapter::extractFactoryOptions(
    TreeAHHybridResidual & searcher)
{
    SCANN_ASSIGN_OR_RETURN(
        auto opts,
        searcher.SingleMachineSearcherBase<float>::ExtractSingleMachineFactoryOptions());
    opts.datapoints_by_token
        = std::make_shared<std::vector<std::vector<DatapointIndex>>>(searcher.datapoints_by_token_);
    opts.serialized_partitioner = std::make_shared<SerializedPartitioner>();
    searcher.query_tokenizer_->CopyToProto(opts.serialized_partitioner.get());

    if (searcher.asymmetric_queryer_)
    {
        opts.ah_codebook = std::make_shared<CentersForAllSubspaces>();
        *opts.ah_codebook = asymmetric_hashing2::DatasetSpanToCentersProto(
            searcher.asymmetric_queryer_->model()->centers(),
            searcher.asymmetric_queryer_->quantization_scheme());
    }
    return opts;
}

Status TreeAHHybridResidualPersistenceAdapter::streamHashedDataset(
    const TreeAHHybridResidual & searcher,
    bool secondary,
    const std::function<Status(ConstSpan<uint8_t>)> & consumer)
{
    SCANN_ASSIGN_OR_RETURN(const DatapointIndex dataset_size, searcher.DatasetSize());
    SCANN_RET_CHECK_EQ(searcher.leaf_searchers_.size(), searcher.datapoints_by_token_.size());

    std::vector<std::pair<DatapointIndex, DatapointIndex>> locations(
        dataset_size, std::make_pair(kInvalidDatapointIndex, kInvalidDatapointIndex));
    std::vector<bool> seen(dataset_size, false);
    for (size_t leaf : Seq(searcher.datapoints_by_token_.size()))
    {
        SCANN_RET_CHECK(searcher.leaf_searchers_[leaf]);
        const auto & packed = searcher.leaf_searchers_[leaf]->packed_dataset();
        SCANN_RET_CHECK_EQ(packed.num_datapoints, searcher.datapoints_by_token_[leaf].size());
        for (const auto [inner_idx, global_idx] : Enumerate(searcher.datapoints_by_token_[leaf]))
        {
            SCANN_RET_CHECK_LT(global_idx, dataset_size);
            if (seen[global_idx] == secondary)
            {
                locations[global_idx]
                    = {static_cast<DatapointIndex>(leaf), static_cast<DatapointIndex>(inner_idx)};
            }
            seen[global_idx] = true;
        }
    }

    const size_t hash_size = searcher.asymmetric_queryer_ ? searcher.asymmetric_queryer_->num_blocks() : 0;
    SCANN_RET_CHECK_GT(hash_size, 0);
    std::vector<uint8_t> unpacked(hash_size, 0);
    for (size_t global_idx : Seq(dataset_size))
    {
        const auto [leaf, inner_idx] = locations[global_idx];
        if (leaf == kInvalidDatapointIndex)
        {
            SCANN_RET_CHECK(secondary);
            std::fill(unpacked.begin(), unpacked.end(), 0);
        }
        else
        {
            const auto & packed = searcher.leaf_searchers_[leaf]->packed_dataset();
            SCANN_RET_CHECK_EQ(packed.num_blocks, hash_size);
            const size_t block_offset = inner_idx & 0x0f;
            const size_t offset = block_offset + (inner_idx & ~31) * hash_size / 2;
            SCANN_RET_CHECK_LT(offset + (hash_size - 1) * 16, packed.bit_packed_data.size());
            for (size_t block : Seq(hash_size))
            {
                const uint8_t value = packed.bit_packed_data[offset + block * 16];
                unpacked[block] = (inner_idx & 0x10) ? value >> 4 : value & 0x0f;
            }
        }
        SCANN_RETURN_IF_ERROR(consumer(MakeConstSpan(unpacked)));
    }
    return OkStatus();
}

bool TreeAHHybridResidualPersistenceAdapter::hasSecondaryHashedDataset(const TreeAHHybridResidual & searcher)
{
    return !searcher.datapoints_by_token_disjoint_;
}

}
