#pragma once

#include <Models/IModel.h>

namespace DB
{

/// Light GBM Boosting model.
class LightGBMModel final : public IModel
{
public:
    LightGBMModel();
    ~LightGBMModel() override;

    void fit(const FeatureMatrix& batch, const Targets& targets) override;
    void fit(const Features& features, const Target& target)     override;
    Targets predict(const FeatureMatrix& features)               override;

protected:
    void setHyperParameters(const HyperParameters& hyperparameters) override;

private:
    // Forwarded from "LightGBM/c_api.h"
    using BoosterHandle = void*;
    using DatasetHandle = void*;

    BoosterHandle booster = nullptr;
    DatasetHandle dataset = nullptr;

    std::size_t n_features = 0;
    std::string hps_str;

    /// Helper functions
    static std::vector<double> flatten(const FeatureMatrix& m);
    void initFeatureDim(std::size_t d);
    void initDataset(const std::vector<double>& flat, const Targets& y);
};

}
