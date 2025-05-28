#pragma once

#include <Models/IModel.h>

namespace DB
{

/// Extreme Gradient Boosting model.
class XGBoostModel : public IModel {
public:
    XGBoostModel();
    ~XGBoostModel() override;

    void fit(const FeatureMatrix& batch, const Targets& targets) override;

    void fit(const Features& features, const Target& target) override;

    Targets predict(const FeatureMatrix& features) override;

protected:
    void setHyperParameters(const HyperParameters& hyperparameters) override;

private:
    /// Forwarded from "xgboost/c_api.h"
    using BoosterHandle = void*;
    using DMatrixHandle = void*;

    BoosterHandle booster = nullptr;
    DMatrixHandle dtrain = nullptr;

    std::size_t n_features = 0;
    HyperParameters hps;

    std::vector<float> train_storage;   // owns training matrix

    /// Helper functions
    static std::vector<float> flatten(const FeatureMatrix& m);
    std::string paramString() const;
    void initFeatureDim(std::size_t d);
    void buildDMatrix(const FeatureMatrix& X, const Targets& y);
};

}
