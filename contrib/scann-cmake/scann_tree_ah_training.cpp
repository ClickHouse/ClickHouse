#include <scann_tree_ah_training.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include <absl/random/distributions.h>
#include <absl/strings/str_cat.h>
#include <scann/hashes/internal/asymmetric_hashing_impl.h>
#include <scann/oss_wrappers/scann_random.h>
#include <scann/oss_wrappers/scann_status.h>
#include <scann/projection/chunking_projection.h>
#include <scann/projection/projection_factory.h>
#include <scann/utils/gmm_utils.h>
#include <scann/utils/parallel_for.h>

namespace research_scann
{
namespace
{

constexpr size_t min_residual_sample_size = 100000;
constexpr size_t max_residual_sample_size = 2000000;
constexpr size_t residual_blocks_per_batch = 64;
constexpr uint32_t residual_sample_seed = 2023;

Status validateConfig(const AsymmetricHasherConfig & config)
{
    if (config.quantization_scheme() != AsymmetricHasherConfig::PRODUCT)
        return InvalidArgumentError("Tree-AH residual training requires PRODUCT quantization.");

    if (config.projection().projection_type() != ProjectionConfig::CHUNK)
        return InvalidArgumentError("Direct block-major residual training requires a CHUNK projection.");

    if (config.num_clusters_per_block() < 1 || config.num_clusters_per_block() > 256)
    {
        return InvalidArgumentError(absl::StrCat(
            "num_clusters_per_block must be between 1 and 256, not ",
            config.num_clusters_per_block(), "."));
    }

    if (config.max_clustering_iterations() < 1)
    {
        return InvalidArgumentError(absl::StrCat(
            "max_clustering_iterations must be strictly positive, not ",
            config.max_clustering_iterations(), "."));
    }

    if (!(config.clustering_convergence_tolerance() > 0))
    {
        return InvalidArgumentError(absl::StrCat(
            "clustering_convergence_tolerance must be strictly positive, not ",
            config.clustering_convergence_tolerance(), "."));
    }

    if (config.sampling_fraction() <= 0.0f || config.sampling_fraction() > 1.0f)
    {
        return InvalidArgumentError(absl::StrCat(
            "sampling_fraction must be strictly positive and <= 1.0, not ",
            config.sampling_fraction(), "."));
    }

    if (config.max_sample_size() < 1)
    {
        return InvalidArgumentError(absl::StrCat(
            "max_sample_size must be strictly positive, not ",
            config.max_sample_size(), "."));
    }

    return OkStatus();
}

std::vector<DatapointIndex> selectResidualSample(size_t dataset_size)
{
    /// Keep the same deterministic reservoir sample that was previously
    /// materialized as a complete row-major residual dataset.
    const size_t sample_size = std::min(
        std::clamp(dataset_size / 2, min_residual_sample_size, max_residual_sample_size),
        dataset_size);
    std::vector<DatapointIndex> sample_indices(sample_size);

    std::mt19937 generator(residual_sample_seed);
    std::uniform_int_distribution<uint64_t> distribution;
    for (size_t i = 0; i < dataset_size; ++i)
    {
        if (i < sample_size)
        {
            sample_indices[i] = static_cast<DatapointIndex>(i);
        }
        else
        {
            const uint64_t candidate = distribution(
                generator,
                std::uniform_int_distribution<uint64_t>::param_type(0, i));
            if (candidate < sample_size)
                sample_indices[candidate] = static_cast<DatapointIndex>(i);
        }
    }
    return sample_indices;
}

std::vector<DatapointIndex> selectAHTrainingSample(
    const AsymmetricHasherConfig & config,
    size_t residual_sample_size)
{
    /// Compose ScaNN's ordinary AH sampling with the residual sample before
    /// materialization, preserving the selected rows and their order.
    const double sampling_fraction = config.has_expected_sample_size()
        ? std::min(
            1.0,
            static_cast<double>(config.expected_sample_size())
                / static_cast<double>(residual_sample_size))
        : config.sampling_fraction();

    MTRandom generator(kDeterministicSeed * (config.sampling_seed() + 1));
    std::vector<DatapointIndex> sample;
    for (DatapointIndex i = 0; i < residual_sample_size; ++i)
    {
        if (absl::Uniform<double>(generator, 0, 1.0) < sampling_fraction)
            sample.push_back(i);
    }

    if (sample.size() > static_cast<size_t>(config.max_sample_size()))
    {
        std::shuffle(sample.begin(), sample.end(), generator);
        sample.resize(config.max_sample_size());
        std::sort(sample.begin(), sample.end());
    }
    return sample;
}

StatusOr<std::vector<uint32_t>> buildTokensByDatapoint(
    size_t dataset_size,
    ConstSpan<std::vector<DatapointIndex>> datapoints_by_token)
{
    std::vector<uint32_t> tokens_by_datapoint(dataset_size);
    for (uint32_t token : Seq(datapoints_by_token.size()))
    {
        for (DatapointIndex datapoint : datapoints_by_token[token])
        {
            SCANN_RET_CHECK_LT(datapoint, dataset_size);
            tokens_by_datapoint[datapoint] = token;
        }
    }
    return tokens_by_datapoint;
}

StatusOr<std::vector<DenseDataset<float>>> buildBlockMajorResidualBatch(
    const DenseDataset<float> & dataset,
    const KMeansTreeLikePartitioner<float> & partitioner,
    ConstSpan<uint32_t> tokens_by_datapoint,
    const ChunkingProjection<float> & projector,
    ConstSpan<DatapointIndex> sampled_datapoints,
    size_t first_block,
    size_t block_end,
    ThreadPool * parallelization_pool)
{
    SCANN_RET_CHECK_EQ(tokens_by_datapoint.size(), dataset.size());
    SCANN_RET_CHECK_LT(first_block, block_end);

    auto project_residual = [&](size_t sample_position, std::vector<std::vector<float>> * storage) -> Status
    {
        const DatapointIndex datapoint = sampled_datapoints[sample_position];
        SCANN_RET_CHECK_LT(datapoint, dataset.size());
        const uint32_t token = tokens_by_datapoint[datapoint];
        SCANN_ASSIGN_OR_RETURN(auto residual, partitioner.ResidualizeToFloat(dataset[datapoint], token));
        SCANN_RETURN_IF_ERROR(VerifyAllFinite(residual.values()));

        ChunkedDatapoint<float> chunked;
        SCANN_RETURN_IF_ERROR(projector.ProjectInput(residual.ToPtr(), &chunked));
        SCANN_RET_CHECK_LE(block_end, chunked.size());
        SCANN_RET_CHECK_EQ(block_end - first_block, storage->size());
        for (size_t local_block : Seq(storage->size()))
        {
            const auto projected = chunked[first_block + local_block].values_span();
            auto & block_storage = (*storage)[local_block];
            SCANN_RET_CHECK_EQ(block_storage.size(), sampled_datapoints.size() * projected.size());
            std::copy(
                projected.begin(),
                projected.end(),
                block_storage.begin() + sample_position * projected.size());
        }
        return OkStatus();
    };

    SCANN_RET_CHECK(!sampled_datapoints.empty());
    const DatapointIndex first_datapoint = sampled_datapoints[0];
    SCANN_RET_CHECK_LT(first_datapoint, dataset.size());
    SCANN_ASSIGN_OR_RETURN(
        auto first_residual,
        partitioner.ResidualizeToFloat(dataset[first_datapoint], tokens_by_datapoint[first_datapoint]));
    SCANN_RETURN_IF_ERROR(VerifyAllFinite(first_residual.values()));
    ChunkedDatapoint<float> first_chunked;
    SCANN_RETURN_IF_ERROR(projector.ProjectInput(first_residual.ToPtr(), &first_chunked));
    SCANN_RET_CHECK_LE(block_end, first_chunked.size());

    std::vector<std::vector<float>> block_storage(block_end - first_block);
    for (size_t local_block : Seq(block_storage.size()))
    {
        const auto projected = first_chunked[first_block + local_block].values_span();
        block_storage[local_block].resize(sampled_datapoints.size() * projected.size());
        std::copy(projected.begin(), projected.end(), block_storage[local_block].begin());
    }

    /// All vectors have their final sizes before parallel work starts. Each
    /// worker writes only the range belonging to its sample position.
    SCANN_RETURN_IF_ERROR(ParallelForWithStatus<1>(
        Seq(size_t{1}, sampled_datapoints.size()),
        parallelization_pool,
        [&](size_t sample_position) { return project_residual(sample_position, &block_storage); }));

    std::vector<DenseDataset<float>> result;
    result.reserve(block_storage.size());
    for (auto & block : block_storage)
        result.emplace_back(std::move(block), sampled_datapoints.size());
    return std::move(result);
}

Status trainBlockCodebooks(
    std::vector<DenseDataset<float>> block_datasets,
    const AsymmetricHasherConfig & config,
    GmmUtils & gmm,
    size_t first_block,
    std::vector<DenseDataset<double>> & all_centers)
{
    SCANN_RET_CHECK_LE(first_block + block_datasets.size(), all_centers.size());
    for (size_t local_block : Seq(block_datasets.size()))
    {
        const size_t block = first_block + local_block;
        /// Keep every future block in Float32 and materialize only the current
        /// block in the double representation required by k-means.
        DenseDataset<double> training_dataset;
        block_datasets[local_block].ConvertType(&training_dataset);
        block_datasets[local_block].clear();
        block_datasets[local_block].ShrinkToFit();

        DenseDataset<double> centers;
        std::vector<std::vector<DatapointIndex>> subpartitions;
        SCANN_RETURN_IF_ERROR(gmm.ComputeKmeansClustering(
            training_dataset,
            config.num_clusters_per_block(),
            &centers,
            {.final_partitions = &subpartitions}));

        for (size_t center_index : IndicesOf(centers))
        {
            SCANN_RETURN_IF_ERROR(VerifyAllFinite(centers[center_index].values_span()));
            if (!config.use_norm_biasing_correction())
                continue;

            SCANN_ASSIGN_OR_RETURN(
                const double correction,
                asymmetric_hashing_internal::ComputeNormBiasCorrection(
                    training_dataset, centers[center_index], subpartitions[center_index]));
            SCANN_RET_CHECK(std::isfinite(correction)) << correction;
            for (double & value : centers.mutable_data(center_index))
                value *= correction;
        }

        std::vector<uint32_t> centers_permutation(centers.size());
        std::iota(centers_permutation.begin(), centers_permutation.end(), 0U);
        std::stable_sort(
            centers_permutation.begin(),
            centers_permutation.end(),
            [&subpartitions](uint32_t lhs, uint32_t rhs)
            {
                return subpartitions[lhs].size() > subpartitions[rhs].size();
            });

        constexpr size_t assumed_cache_line_size = 64;
        constexpr size_t floats_per_cache_line = assumed_cache_line_size / sizeof(float);
        const uint64_t cache_lines_per_row =
            std::max(size_t{1}, centers.size() / floats_per_cache_line);
        const size_t num_rotate = ((block / 2) % cache_lines_per_row) * floats_per_cache_line;
        std::rotate(
            centers_permutation.begin(),
            centers_permutation.begin() + num_rotate,
            centers_permutation.end());
        if (block & 1)
            std::reverse(centers_permutation.begin(), centers_permutation.end());

        for (uint32_t center : centers_permutation)
            all_centers[block].AppendOrDie(centers[center], "");
    }
    return OkStatus();
}

}

StatusOr<shared_ptr<const asymmetric_hashing2::Model<float>>> TrainTreeAHResidualModel(
    const DenseDataset<float> & dataset,
    const KMeansTreeLikePartitioner<float> & partitioner,
    ConstSpan<std::vector<DatapointIndex>> datapoints_by_token,
    const AsymmetricHasherConfig & config,
    shared_ptr<const DistanceMeasure> quantization_distance,
    shared_ptr<ThreadPool> parallelization_pool)
{
    if (dataset.empty())
        return InvalidArgumentError("Cannot train AH on an empty dataset.");
    SCANN_RETURN_IF_ERROR(validateConfig(config));

    auto residual_sample = selectResidualSample(dataset.size());
    const auto ah_sample_positions = selectAHTrainingSample(config, residual_sample.size());
    if (ah_sample_positions.size() < static_cast<size_t>(config.num_clusters_per_block()))
    {
        return InvalidArgumentError(absl::StrCat(
            "Number of clusters per block (",
            config.num_clusters_per_block(),
            ") is greater than asymmetric hashing training data size (",
            ah_sample_positions.size(), ")."));
    }

    std::vector<DatapointIndex> sampled_datapoints;
    sampled_datapoints.reserve(ah_sample_positions.size());
    for (DatapointIndex position : ah_sample_positions)
    {
        SCANN_RET_CHECK_LT(position, residual_sample.size());
        sampled_datapoints.push_back(residual_sample[position]);
    }
    residual_sample.clear();
    residual_sample.shrink_to_fit();

    SCANN_ASSIGN_OR_RETURN(auto projector, ChunkingProjectionFactory<float>(config.projection()));
    auto shared_projector = shared_ptr<const ChunkingProjection<float>>(std::move(projector));
    SCANN_ASSIGN_OR_RETURN(auto tokens_by_datapoint, buildTokensByDatapoint(dataset.size(), datapoints_by_token));

    GmmUtils::Options gmm_options;
    gmm_options.seed = config.clustering_seed();
    gmm_options.max_iterations = config.max_clustering_iterations();
    gmm_options.epsilon = config.clustering_convergence_tolerance();
    gmm_options.parallelization_pool = parallelization_pool;
    gmm_options.partition_assignment_type = GmmUtils::Options::UNBALANCED_FLOAT32;
    GmmUtils gmm(std::move(quantization_distance), std::move(gmm_options));

    const size_t num_blocks = shared_projector->num_blocks();
    std::vector<DenseDataset<double>> double_centers(num_blocks);
    for (size_t first_block = 0; first_block < num_blocks; first_block += residual_blocks_per_batch)
    {
        const size_t block_end = std::min(first_block + residual_blocks_per_batch, num_blocks);
        SCANN_ASSIGN_OR_RETURN(
            auto block_datasets,
            buildBlockMajorResidualBatch(
                dataset,
                partitioner,
                tokens_by_datapoint,
                *shared_projector,
                sampled_datapoints,
                first_block,
                block_end,
                parallelization_pool.get()));
        SCANN_RETURN_IF_ERROR(
            trainBlockCodebooks(std::move(block_datasets), config, gmm, first_block, double_centers));
    }

    tokens_by_datapoint.clear();
    tokens_by_datapoint.shrink_to_fit();
    sampled_datapoints.clear();
    sampled_datapoints.shrink_to_fit();

    auto centers = asymmetric_hashing_internal::ConvertCentersIfNecessary<float>(std::move(double_centers));
    SCANN_ASSIGN_OR_RETURN(
        auto model,
        asymmetric_hashing2::Model<float>::FromCenters(std::move(centers), config.quantization_scheme()));
    model->SetProjection(std::move(shared_projector));
    return shared_ptr<const asymmetric_hashing2::Model<float>>(std::move(model));
}

}
