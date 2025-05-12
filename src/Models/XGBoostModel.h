#pragma once

#include <Models/IModel.h>

namespace DB
{

/// Extreme Gradient Boosting model.
class XGBoostModel : public IModel {
public:
    ~XGBoostModel() override;

    void fit(const FeatureMatrix& batch, Targets targets) override;

    void fit(const Features& features, Target target) override;

    Targets predict(const FeatureMatrix& features) override;

private:
    void setHyperParameters(const HyperParameters& hyperparameters) override;
};

}
