#pragma once

template <bool kHasIndices, bool kIsSquaredL2, typename DatasetViewT,
          typename IndexT, typename ResultElemT, typename CallbackT>
SCANN_OUTLINE void OneToManyInt8FloatImpl(
    const float * __restrict__ query,
    DatasetViewT dataset_view,
    const float * __restrict__ inv_multipliers_for_squared_l2,
    const IndexT * indices,
    MutableSpan<ResultElemT> result,
    CallbackT callback)
{
    const DimensionIndex dims = dataset_view.dimensionality();
    if (result.empty() || dims == 0)
        return;

    const size_t datapoint_bytes = sizeof(int8_t) * dims;

    for (size_t j : Seq(result.size()))
    {
        const size_t idx = kHasIndices ? indices[j] : one_to_many_low_level::GetDatapointIndex(result, j);
        const int8_t * database = dataset_view.GetPtr(idx);

        float distance = 0.0f;
        if constexpr (kIsSquaredL2)
        {
            for (DimensionIndex i : Seq(dims))
            {
                const float difference = query[i] - static_cast<float>(database[i]) * inv_multipliers_for_squared_l2[i];
                distance += difference * difference;
            }
        }
        else
        {
            for (DimensionIndex i : Seq(dims))
                distance -= query[i] * static_cast<float>(database[i]);
        }

        InvokeCallback(callback, j, distance, datapoint_bytes, database);
    }
}
