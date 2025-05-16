#include <Models/NullModel.h>

namespace DB
{

NullModel::~NullModel() = default;

void NullModel::fit(const FeatureMatrix&, const Targets&)
{
    // Nothing to do.
}

void NullModel::fit(const Features&, const Target&)
{
    // Nothing to do.
}

Targets NullModel::predict(const FeatureMatrix&)
{
    // Nothing to do.
    return Targets{};
}

void NullModel::setHyperParameters(const HyperParameters&)
{
    // Nothing to do.
}

}
