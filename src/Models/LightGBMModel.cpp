#include <Models/LightGBMModel.h>

namespace DB
{

LightGBMModel::~LightGBMModel() = default;

void LightGBMModel::fit(const FeatureMatrix&, Targets)
{
    // Nothing to do.
}

void LightGBMModel::fit(const Features&, Target)
{
    // Nothing to do.
}

Targets LightGBMModel::predict(const FeatureMatrix&)
{
    // Nothing to do.
    return Targets{};
}

void LightGBMModel::setHyperParameters(const HyperParameters&)
{
    // Nothing to do.
}

}
