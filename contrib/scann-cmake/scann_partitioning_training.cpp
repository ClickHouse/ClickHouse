#include <cstdint>
#include <utility>
#include <vector>

#include <scann/data_format/datapoint.h>
#include <scann/data_format/dataset.h>
#include <scann/distance_measures/distance_measure_base.h>
#include <scann/oss_wrappers/scann_status.h>
#include <scann/trees/kmeans_tree/kmeans_tree.h>
#include <scann/trees/kmeans_tree/kmeans_tree_node.h>
#include <scann/trees/kmeans_tree/training_options.h>
#include <scann/utils/datapoint_utils.h>
#include <scann/utils/types.h>

namespace research_scann
{

Status KMeansTree::TrainWithSubset(
    const Dataset & training_data,
    std::vector<DatapointIndex> training_subset,
    const DistanceMeasure & training_distance,
    int32_t k_per_level,
    KMeansTreeTrainingOptions * training_options)
{
    SCANN_RET_CHECK(training_options);
    SCANN_RET_CHECK(!training_subset.empty());

    Status status = root_.Train(
        training_data,
        std::move(training_subset),
        training_distance,
        k_per_level,
        0,
        training_options);
    if (!status.ok())
        return status;

    if (root_.IsLeaf())
    {
        Datapoint<double> root_center;
        SCANN_RETURN_IF_ERROR(training_data.MeanByDimension(root_.indices_, &root_center));
        Datapoint<float> root_center_float;
        MaybeConvertDatapoint(root_center.ToPtr(), &root_center_float);
        root_.float_centers_.AppendOrDie(root_center_float.ToPtr());

        root_.children_ = std::vector<KMeansTreeNode>(1);
        root_.children_[0].Reset();
        root_.children_[0].indices_ = root_.indices_;
    }

    n_tokens_ = root_.NumberLeaves(0);
    root_.PopulateCurNodeCenters();
    learned_spilling_type_ = training_options->learned_spilling_type;
    max_spill_centers_ = training_options->max_spill_centers;
    root_.CreateFixedPointCenters();
    CheckIfFlat();
    return status;
}

}
