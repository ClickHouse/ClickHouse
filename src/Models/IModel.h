#pragma once

#include <Models/Model_fwd.h>

namespace DB
{

class IModel {
public:
    virtual ~IModel() = default;

    /// Perform one boosting iteration over the entire batch.
    virtual void fit(const FeatureMatrix& batch, Targets targets) = 0;

    // Perform one boosting iteratio nwith a single sample.
    virtual void fit(const Features& features, Target target) = 0;

    // Predict targets based on the input features
    virtual Targets predict(const FeatureMatrix& features) = 0;

protected:
    // Models can be created only via `createModel` function
    IModel() = default;

    /// Sets model's hyperparameters,
    /// e.g. {"learning_rate":"0.1", "max_depth":"6", ...}.
    /// Called right after the creatation of model in `createModel`
    virtual void setHyperParameters(const HyperParameters& hyperparameters) = 0;

private:
    friend ModelPtr createModel(const String& algorithm, const HyperParameters& hyperparamers);
};

}
