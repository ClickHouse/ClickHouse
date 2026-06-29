#pragma once

#include <Models/IModel.h>

namespace DB
{

/// Null model. Does nothing.
class NullModel : public IModel {
public:
    ~NullModel() override;

    void fit(const FeatureMatrix& batch, const Targets& targets) override;

    void fit(const Features& features, const Target& target) override;

    Targets predict(const FeatureMatrix& features) override;

private:
    void setHyperParameters(const HyperParameters& hyperparameters) override;
};

}
