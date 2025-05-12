#include <Models/XGBoostModel.h>

namespace DB
{

XGBoostModel::~XGBoostModel() = default;

void XGBoostModel::fit(const FeatureMatrix&, Targets)
{

}

void XGBoostModel::fit(const Features&, Target)
{

}

Targets XGBoostModel::predict(const FeatureMatrix&)
{

    return Targets{};
}

void XGBoostModel::setHyperParameters(const HyperParameters&)
{

}

}
