#include <Models/createModel.h>

#include "config.h"

#include <Common/Exception.h>

#include <Models/NullModel.h>
#if USE_XGBOOST
#include <Models/XGBoostModel.h>
#endif
#if USE_LIGHTGBM
#include <Models/LightGBMModel.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int MODEL_NOT_FOUND;
    extern const int SUPPORT_IS_DISABLED;
}

ModelPtr createModel(const String & algorithm, const HyperParameters & hyperparamers)
{
    ModelPtr model;

    if (algorithm == "null")
    {
        model = std::make_shared<NullModel>();
    }
    else if (algorithm == "xgboost")
    {
#if USE_XGBOOST
        model = std::make_shared<XGBoostModel>();
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse was built without XGBoost support");
#endif
    }
    else if (algorithm == "lightgbm")
    {
#if USE_LIGHTGBM
        model = std::make_shared<LightGBMModel>();
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse was built without LightGBM support");
#endif
    }
    else // TODO: add catboost
    {
        throw Exception(ErrorCodes::MODEL_NOT_FOUND, "Unknown model algorithm: {}", algorithm);
    }

    model->setHyperParameters(hyperparamers);
    return model;
}

}
