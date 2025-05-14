#include <Models/LightGBMModel.h>

#include <LightGBM/c_api.h>

namespace DB
{

LightGBMModel::~LightGBMModel() = default;

void LightGBMModel::fit(const FeatureMatrix&, Targets)
{

}

void LightGBMModel::fit(const Features&, Target)
{

}

Targets LightGBMModel::predict(const FeatureMatrix&)
{

    return Targets{};
}

void LightGBMModel::setHyperParameters(const HyperParameters&)
{

}

}
