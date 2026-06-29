#include "LightGBMModel.h"

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <sstream>
#include <algorithm>

#include <LightGBM/c_api.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIGHTGBM_ERROR;
}

namespace
{

void throwOnError(int status, const char * where)
{
    if (status != 0)
        throw Exception(
            ErrorCodes::LIGHTGBM_ERROR,
            "{} failed: {}",
            where,
            LGBM_GetLastError());
}

} // namespace

// TODO: add num_iterations parsing
LightGBMModel::LightGBMModel()
    : booster{nullptr}
    , dataset{nullptr}
    , n_features{0}
{}

LightGBMModel::~LightGBMModel()
{
    if (booster)
        LGBM_BoosterFree(booster);

    if (dataset)
        LGBM_DatasetFree(dataset);
}

void LightGBMModel::setHyperParameters(const HyperParameters& hyperparameters)
{
    if (booster || dataset)
        throw Exception(
            ErrorCodes::LIGHTGBM_ERROR,
            "setHyperParameters must be called before training starts");

    for (const auto& [key, value]: hyperparameters) {
        if (!hps_str.empty()) {
            hps_str += ' ';
        }
        hps_str += fmt::format("{}={}", key, value);
    }
}

void LightGBMModel::fit(const FeatureMatrix& batch, const Targets& targets)
{
    if (batch.empty() || batch[0].empty())
        throw Exception(
            ErrorCodes::LIGHTGBM_ERROR,
            "fit: Cannot train model on an empty dataset");

    if (batch.size() != targets.size())
        throw Exception(
            ErrorCodes::LIGHTGBM_ERROR,
            "fit: Invalid dimensions. Feature dimension ({}) ≠ Target dimension ({})",
            batch.size(),
            targets.size());

    initFeatureDim(batch[0].size());
    std::vector<double> flat = flatten(batch);

    if (!dataset)
        initDataset(flat, targets);

    int is_finished = 0;
    throwOnError(LGBM_BoosterUpdateOneIter(booster, &is_finished),
                "LGBM_BoosterUpdateOneIter");
}

void LightGBMModel::fit(const Features& features, const Target& target)
{
    fit(FeatureMatrix{features}, Targets{target});
}

Targets LightGBMModel::predict(const FeatureMatrix& features)
{
    if (!booster)
        throw Exception(
            ErrorCodes::LIGHTGBM_ERROR,
            "predict: model has not been trained yet");

    if (features.empty()) return {};

    if (features[0].size() != n_features)
        throw Exception(
            ErrorCodes::LIGHTGBM_ERROR,
            "predict: feature dimension mismatch (expected {}, got {})",
            n_features,
            features[0].size());

    std::vector<double> flat = flatten(features);
    std::vector<double> predictions(features.size());
    int64_t out_len = 0;

    throwOnError(
        LGBM_BoosterPredictForMat(
            booster,
            flat.data(),
            C_API_DTYPE_FLOAT64,
            static_cast<int32_t>(features.size()),
            static_cast<int32_t>(n_features),
            1,
            C_API_PREDICT_NORMAL,
            0,
            -1,
            hps_str.c_str(),
            &out_len,
            predictions.data()
        ),
        "LGBM_BoosterPredictForMat");

    return predictions;
}


std::vector<double> LightGBMModel::flatten(const FeatureMatrix& m)
{
    std::size_t rows = m.size();
    std::size_t cols = m[0].size();
    std::vector<double> out(rows * cols);

    for (std::size_t r = 0; r < rows; ++r)
        std::copy(m[r].begin(), m[r].end(), out.begin() + r * cols);

    return out;
}

void LightGBMModel::initFeatureDim(std::size_t d)
{
    if (!n_features)
        n_features = d;
    else if (d != n_features)
        throw Exception(
            ErrorCodes::LIGHTGBM_ERROR,
            "Inconsistent feature dimensionality: expected {}, got {}",
            n_features,
            d);
}

void LightGBMModel::initDataset(const std::vector<double>& flat,
                                const Targets& y)
{
    throwOnError(
        LGBM_DatasetCreateFromMat(
            flat.data(),
            C_API_DTYPE_FLOAT64,
            static_cast<int32_t>(y.size()),
            static_cast<int32_t>(n_features),
            1,
            hps_str.c_str(),
            nullptr,
            &dataset),
        "LGBM_DatasetCreateFromMat");

    throwOnError(
        LGBM_DatasetSetField(
            dataset,
            "init_score",
            const_cast<double*>(y.data()),
            static_cast<int32_t>(y.size()),
            C_API_DTYPE_FLOAT64),
        "LGBM_DatasetSetField(label)");

    throwOnError(
        LGBM_BoosterCreate(
            dataset,
            hps_str.c_str(),
            &booster),
        "LGBM_BoosterCreate");
}

}
