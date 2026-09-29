#pragma once

#include <cstdint>
#include <functional>

#include <scann/base/single_machine_factory_options.h>
#include <scann/utils/common.h>

namespace research_scann
{

class TreeAHHybridResidual;

class TreeAHHybridResidualPersistenceAdapter
{
public:
    static StatusOr<SingleMachineFactoryOptions> extractFactoryOptions(TreeAHHybridResidual & searcher);

    static Status streamHashedDataset(
        const TreeAHHybridResidual & searcher,
        bool secondary,
        const std::function<Status(ConstSpan<uint8_t>)> & consumer);

    static bool hasSecondaryHashedDataset(const TreeAHHybridResidual & searcher);
};

}
