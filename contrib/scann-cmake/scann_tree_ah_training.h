#pragma once

#include <memory>
#include <vector>

#include <scann/data_format/dataset.h>
#include <scann/distance_measures/distance_measure_base.h>
#include <scann/hashes/asymmetric_hashing2/training_model.h>
#include <scann/oss_wrappers/scann_threadpool.h>
#include <scann/partitioning/kmeans_tree_like_partitioner.h>
#include <scann/proto/hash.pb.h>
#include <scann/utils/common.h>
#include <scann/utils/types.h>

namespace research_scann
{

StatusOr<shared_ptr<const asymmetric_hashing2::Model<float>>> TrainTreeAHResidualModel(
    const DenseDataset<float> & dataset,
    const KMeansTreeLikePartitioner<float> & partitioner,
    ConstSpan<std::vector<DatapointIndex>> datapoints_by_token,
    const AsymmetricHasherConfig & config,
    shared_ptr<const DistanceMeasure> quantization_distance,
    shared_ptr<ThreadPool> parallelization_pool);

}
