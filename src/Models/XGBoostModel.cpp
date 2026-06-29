#include <Models/XGBoostModel.h>

#include <Common/Exception.h>

#include <xgboost/c_api.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int XGBOOST_ERROR;
}

namespace
{

void throwOnError(int status, const char * where)
{
    if (status != 0)
        throw Exception(
            ErrorCodes::XGBOOST_ERROR,
            "{} failed: {}",
            where,
            XGBGetLastError());
}

} // namespace

XGBoostModel::XGBoostModel() = default;

XGBoostModel::~XGBoostModel()
{
    if (booster)
        XGBoosterFree(booster);
    if (dtrain)
        XGDMatrixFree(dtrain);
}

void XGBoostModel::fit(const FeatureMatrix& batch, const Targets& targets)
{
    if (batch.empty())
        throw Exception(
            ErrorCodes::XGBOOST_ERROR,
            "Empty training batch");

    if (batch.size() != targets.size())
        throw Exception(
            ErrorCodes::XGBOOST_ERROR,
            "Features Dimension ({}) ≠ Target Dimension ({})",
            batch.size(),
            targets.size());

    initFeatureDim(batch[0].size());

    if (!dtrain)
        buildDMatrix(batch, targets);

    if (!booster)
    {
        const DMatrixHandle dmats[1] = { dtrain };
        throwOnError(XGBoosterCreate(dmats, 1, &booster), "XGBoosterCreate");

        for (const auto & [key, value] : hps)
            throwOnError(
                XGBoosterSetParam(booster, key.c_str(), value.c_str()),
                "XGBoosterSetParam");
    }

    int n_iter = 100;
    if (auto it = hps.find("num_iterations"); it != hps.end())
        n_iter = std::stoi(it->second);
    if (auto it = hps.find("n_estimators"); it != hps.end())
        n_iter = std::stoi(it->second);

    for (int i = 0; i < n_iter; ++i)
        throwOnError(
            XGBoosterUpdateOneIter(booster, i, dtrain),
            "XGBoosterUpdateOneIter");
}

void XGBoostModel::fit(const Features& features, const Target& target)
{
    fit(FeatureMatrix{features}, Targets{target});
}

Targets XGBoostModel::predict(const FeatureMatrix&)
{

    return Targets{};
}

void XGBoostModel::setHyperParameters(const HyperParameters& hyperparameters)
{
    if (booster || dtrain)
        throw Exception(
            ErrorCodes::XGBOOST_ERROR,
            "setHyperParameters must be called before training");
    hps = hyperparameters;
}

std::vector<float> XGBoostModel::flatten(const FeatureMatrix& m)
{
    const std::size_t rows = m.size();
    const std::size_t cols = m[0].size();
    std::vector<float> out(rows * cols);

    for (std::size_t r = 0; r < rows; ++r)
        std::transform(
            m[r].begin(), m[r].end(),
            out.begin() + r * cols,
            [](double v) { return static_cast<float>(v); });

    return out;
}

void XGBoostModel::initFeatureDim(std::size_t d)
{
    if (!n_features)
        n_features = d;
    else if (d != n_features)
        throw Exception(
            ErrorCodes::XGBOOST_ERROR,
            "Inconsistent feature dimensionality: expected {}, got {}",
            n_features,
            d);
}

void XGBoostModel::buildDMatrix(const FeatureMatrix& X, const Targets& y)
{
    train_storage = flatten(X);

    throwOnError(XGDMatrixCreateFromMat(
                     train_storage.data(),
                     static_cast<bst_ulong>(X.size()),
                     static_cast<bst_ulong>(n_features),
                     std::numeric_limits<float>::quiet_NaN(),
                     &dtrain),
                 "XGDMatrixCreateFromMat(train)");

    std::vector<float> y_float(y.begin(), y.end());
    throwOnError(XGDMatrixSetFloatInfo(
                     dtrain, "label",
                     y_float.data(),
                     static_cast<bst_ulong>(y_float.size())),
                 "XGDMatrixSetFloatInfo(label)");
}

}
