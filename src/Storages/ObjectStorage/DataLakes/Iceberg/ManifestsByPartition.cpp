#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestsByPartition.h>

namespace DB::Iceberg
{

namespace
{

using SinglePartition = std::vector<std::optional<String>>;

std::optional<SinglePartition> getSinglePartition(const PartitionFieldSummaries & summaries)
{
    SinglePartition result;
    for (const auto & field : summaries)
    {
        if (field.contains_nan)
            return std::nullopt;
        if (field.contains_null && !field.lower_bound && !field.upper_bound)
            result.emplace_back();
        else if (!field.contains_null && field.lower_bound && field.lower_bound == field.upper_bound)
            result.emplace_back(*field.lower_bound);
        else
            return std::nullopt;
    }
    return result;
}

std::optional<SinglePartition> getSinglePartition(const Row & partition_value, const DataTypes & partition_types)
{
    ManifestPartitionSummary summary(partition_types);
    summary.add(partition_value);
    SinglePartition result;
    for (const auto & field : summary.getFields())
    {
        if (field.contains_nan || field.bounds_unavailable)
            return std::nullopt;
        if (field.contains_null)
        {
            result.emplace_back();
            continue;
        }
        auto bytes = dumpFieldToBytes(*field.lower_bound, field.type);
        result.emplace_back(String(bytes.begin(), bytes.end()));
    }
    return result;
}

}

ManifestsByPartition::ManifestsByPartition(
    const ManifestFileCacheKeys & parent_manifests, Int32 partition_spec_id, const DataTypes & partition_types_)
    : partition_types(partition_types_)
{
    for (auto it = parent_manifests.rbegin(); it != parent_manifests.rend(); ++it)
    {
        if (it->content_type != ManifestFileContentType::DATA || it->partition_spec_id != partition_spec_id)
            continue;
        if (auto partition = getSinglePartition(it->partition_summaries))
            manifests[std::move(*partition)].push_back(*it);
    }
}

const ManifestFileCacheKeys & ManifestsByPartition::get(const Row & partition_value) const
{
    static const ManifestFileCacheKeys no_manifests;
    const auto partition = getSinglePartition(partition_value, partition_types);
    if (!partition)
        return no_manifests;
    auto it = manifests.find(*partition);
    return it == manifests.end() ? no_manifests : it->second;
}

}

#endif
