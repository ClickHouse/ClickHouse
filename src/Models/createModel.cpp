#include <Models/createModel.h>

#include <Common/Exception.h>

#include <Models/NullModel.h>
#include <Models/XGBoostModel.h>
#include <Models/LightGBMModel.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MODEL_NOT_FOUND;
}

ModelPtr createModel(const String& algorithm, const HyperParameters& hyperparamers)
{
    ModelPtr model;

    if (algorithm == "null")
    {
        model = std::make_shared<NullModel>();
    }
    else if (algorithm == "xgboost")
    {
        model = std::make_shared<XGBoostModel>();
    }
    else if (algorithm == "lightgbm")
    {
        model = std::make_shared<LightGBMModel>();
    }
    else // TODO: add catboost
    {
        throw Exception(ErrorCodes::MODEL_NOT_FOUND, "Unknown model algorithm: {}", algorithm);
    }

    model->setHyperParameters(hyperparamers);
    return model;
}

}
