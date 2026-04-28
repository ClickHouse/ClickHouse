#include "config.h"

#include <Storages/MergeTree/ANNIndex/IANNIndexSearcher.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>

#include <Common/Exception.h>

#if USE_DISKANN
#include <Storages/MergeTree/ANNIndex/DiskANNIndexSearcherAdapter.h>
#include <filesystem>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

IANNIndexSearcherPtr createANNIndexSearcher(
    const ANNIndexShapeFingerprint & shape,
    const std::string & index_directory,
    const IANNSearchDefaults & defaults)
{
#if USE_DISKANN
    if (shape.algorithm == "diskann")
    {
        namespace fs = std::filesystem;
        const auto index_prefix
            = (fs::path(index_directory) / DiskANNArtifactNames::INDEX_PREFIX_BASENAME).string();

        const auto * disk_defaults = dynamic_cast<const DiskANNSearchDefaults *>(&defaults);
        if (!disk_defaults)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "createANNIndexSearcher: algorithm `diskann` received defaults of a different "
                "concrete type; `dynamic_cast` to `DiskANNSearchDefaults` failed");

        const auto metric = static_cast<DiskANNMetric>(shape.metric);
        auto disk_searcher = std::make_shared<DiskANNDiskIndexSearcher>(
            shape.dim,
            metric,
            index_prefix,
            disk_defaults->options);

        return std::make_shared<DiskANNIndexSearcherAdapter>(std::move(disk_searcher), metric, shape.dim);
    }
#else
    (void)index_directory;
    (void)defaults;
#endif

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "ANN index algorithm `{}` is not supported", shape.algorithm);
}

}
