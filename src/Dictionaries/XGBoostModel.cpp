#include <Dictionaries/XGBoostModel.h>

#if USE_XGBOOST

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Common/Exception.h>
#include <Common/scope_guard_safe.h>

#include <base/types.h>
#include <xgboost/c_api.h>

#include <fmt/format.h>
#include <Poco/JSON/Object.h>

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <limits>
#include <string_view>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int XGBOOST_ERROR;
}

namespace
{

/// `XGBOOST_ERROR` is reserved for failures reported by the XGBoost library itself.
inline void throwOnError(int err, std::string_view call)
{
    if (err != 0)
        throw Exception(ErrorCodes::XGBOOST_ERROR, "XGBoost call {} failed: {}", call, XGBGetLastError());
}
}

XGBoostModel::XGBoostModel(const HyperParameters & hyper_parameters)
    : hps(hyper_parameters)
{
}

XGBoostModel::~XGBoostModel()
{
    if (booster)
        XGBoosterFree(booster);
    if (dmatrix)
        XGDMatrixFree(dmatrix);
}

void XGBoostModel::throwIfTypeIsInvalid(const ColumnWithTypeAndName & col)
{
    auto type = col.type;
    WhichDataType which(type->getTypeId());
    if (!which.isNativeNumber() || which.isEnum())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "XGBoost only accepts numerical types. The column {} has type {}",
            col.column->getName(),
            type->getName());
    }
}

void XGBoostModel::startTraining(const Block & header, const String & target_column_)
{
    /// Record the training schema: `target_column_` is the label, every other column of `header` a feature.
    if (!header.has(target_column_))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Target column '{}' is not present in the training data", target_column_);

    target_column = target_column_;

    feature_columns.clear();
    for (const auto & column : header.getColumnsWithTypeAndName())
        if (column.name != target_column)
            feature_columns.push_back(column.name);

    if (feature_columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No feature columns for training (target column is '{}')", target_column);

    n_features = feature_columns.size();
}

void XGBoostModel::addTrainingData(const Block & batch)
{
    if (batch.rows() == 0)
        return;

    VectorWithMemoryTracking<const IColumn *> feature_cols;
    feature_cols.reserve(n_features);
    for (const auto & name : feature_columns)
    {
        const auto & col_with_type_and_name = batch.getByName(name);
        const auto * icol = col_with_type_and_name.column.get();
        feature_cols.push_back(icol);

        throwIfTypeIsInvalid(col_with_type_and_name);
    }

    const IColumn * label_col{nullptr};
    {
        const auto & col_with_type_and_name = batch.getByName(target_column);
        label_col = col_with_type_and_name.column.get();
        throwIfTypeIsInvalid(col_with_type_and_name);
    }
    chassert(label_col);

    flattened_features.reserve(flattened_features.size() + (batch.rows() * n_features));
    labels.reserve(labels.size() + batch.rows());

    // Transforms from the Block into a flattened vector, stores the tuples row-wise
    for (std::size_t r = 0; r < batch.rows(); ++r)
    {
        for (std::size_t c = 0; c < n_features; ++c)
            flattened_features.push_back(static_cast<float>(feature_cols[c]->getFloat64(r)));
        labels.push_back(static_cast<float>(label_col->getFloat64(r)));
    }

    ingested_rows += batch.rows();
}

void XGBoostModel::finalizeTraining()
{
    if (ingested_rows == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No training data was provided");

    // Validate the parameters provided by the user
    const auto params = sanitizeTrainingParams(hps);

    chassert(labels.size() == ingested_rows);
    chassert(flattened_features.size() == ingested_rows * n_features);

    throwOnError(
        XGDMatrixCreateFromMat(flattened_features.data(), ingested_rows, n_features, std::numeric_limits<float>::quiet_NaN(), &dmatrix),
        "XGDMatrixCreateFromMat");

    // Create the model
    throwOnError(XGBoosterCreate(&dmatrix, 1, &booster), "XGBoosterCreate");

    // Apply already sanitized params into the model
    for (const auto & [key, value] : params)
    {
        throwOnError(
            XGBoosterSetParam(booster, key.c_str(), value.c_str()), fmt::format("XGBoosterSetParam({} = {})", key, value));
    }

    // Set the label for each row
    throwOnError(XGDMatrixSetFloatInfo(dmatrix, "label", labels.data(), ingested_rows), "XGDMatrixSetFloatInfo");

    // Train the model
    for (int i = 0; i < num_iterations; ++i)
    {
        throwOnError(XGBoosterUpdateOneIter(booster, i, dmatrix), "XGBoosterUpdateOneIter");
    }

    /// Release ingestion resources; the booster is self-contained from here on.
    XGDMatrixFree(dmatrix);
    dmatrix = nullptr;

    flattened_features.clear();
    flattened_features.shrink_to_fit();
    labels.clear();
    labels.shrink_to_fit();
}

ColumnPtr XGBoostModel::predict(const Block & batch, const PredictParameters & params)
{
    if (batch.columns() != n_features)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} features, got {}", n_features, batch.columns());
    }

    const std::size_t rows = batch.rows();
    if (rows == 0)
        return ColumnFloat64::create();

    VectorWithMemoryTracking<const IColumn *> feature_cols;
    feature_cols.reserve(n_features);
    for (const auto & name : feature_columns)
        feature_cols.push_back(batch.getByName(name).column.get());

    VectorWithMemoryTracking<float> features;

    features.reserve(rows * n_features);

    for (std::size_t r = 0; r < rows; ++r)
    {
        for (std::size_t c = 0; c < n_features; ++c)
            features.push_back(static_cast<float>(feature_cols[c]->getFloat64(r)));
    }

    DMatrixHandle predict_dmatrix{nullptr};
    SCOPE_EXIT({
        if (predict_dmatrix)
        {
            XGDMatrixFree(predict_dmatrix);
        }
    });

    throwOnError(
        XGDMatrixCreateFromMat(features.data(), rows, n_features, std::numeric_limits<float>::quiet_NaN(), &predict_dmatrix),
        "XGDMatrixCreateFromMat");

    auto result = ColumnFloat64::create(rows);

    {
        std::lock_guard lock(predict_mutex);

        /* Shape of output prediction */
        bst_ulong const * out_shape{nullptr};
        /* Dimension of output prediction */
        bst_ulong out_dim{0};
        /* Pointer to a thread local contiguous array, assigned in prediction function. */
        float const * out_result{nullptr};

        String config = sanitizePredictParams(params);

        throwOnError(
            XGBoosterPredictFromDMatrix(booster, predict_dmatrix, config.c_str(), &out_shape, &out_dim, &out_result),
            "XGBoosterPredictFromDMatrix");

        if (out_dim != 1 || out_shape[0] != rows)
            throw Exception(
                ErrorCodes::XGBOOST_ERROR,
                "XGBoost returned a {}-dimensional prediction result for {} row(s), expected one value per row",
                out_dim,
                rows);

        auto & data = result->getData();
        for (std::size_t i = 0; i < rows; ++i)
            data[i] = static_cast<Float64>(out_result[i]);
    }

    return result;
}

UnorderedMapWithMemoryTracking<String, String> XGBoostModel::sanitizeTrainingParams(const HyperParameters & params)
{
    UnorderedMapWithMemoryTracking<String, String> sanitized;

    static const std::unordered_set<String> allowed_keys{ // STYLE_CHECK_ALLOW_STD_CONTAINERS
        "booster",
        "objective",
        "seed",
        "verbosity",
        "nthread",
        "eta",
        "gamma",
        "max_depth",
        "min_child_weight",
        "max_delta_step",
        "subsample",
        "sampling_method",
        "colsample_bytree",
        "colsample_bylevel",
        "colsample_bynode",
        "lambda",
        "alpha",
        "tree_method",
        "scale_pos_weight",
        "grow_policy",
        "max_leaves",
        "max_bin",
        "num_parallel_tree",
        "num_iterations"};

    for (const auto & [key, value] : params)
    {
        if (!allowed_keys.contains(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown or forbidden training parameter '{}'", key);

        /// Disable multiclass objectives
        if (key == "objective" && value.starts_with("multi:"))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Objective '{}' is not supported: multiclass training requires the 'num_class' parameter, which an XGBoost "
                "dictionary does not accept, because it predicts exactly one Float64 per row. Use a regression objective, or "
                "'binary:logistic' for two-class classification",
                value);

        // If we found num_iterations, record this value and do not add it to the final map
        if (key == "num_iterations")
        {
            int parsed_iterations = 0;
            if (!tryParse(parsed_iterations, value) || parsed_iterations <= 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter 'num_iterations' must be a positive integer, got '{}'", value);
            num_iterations = parsed_iterations;
        }
        else
        {
            sanitized.emplace(key, value);
        }
    }

    return sanitized;
}

String XGBoostModel::sanitizePredictParams(const PredictParameters & params)
{
    // Default parameters
    Poco::JSON::Object config;
    config.set("type", 0);
    config.set("iteration_begin", 0);
    config.set("iteration_end", 0);
    config.set("strict_shape", false);
    config.set("training", false);

    static const std::unordered_set<String> allowed_keys{ // STYLE_CHECK_ALLOW_STD_CONTAINERS
        "type", "iteration_begin", "iteration_end"};

    /// Fetch upper limit for `iteration_begin` and `iteration_end`.
    int boosted_rounds = 0;
    throwOnError(XGBoosterBoostedRounds(booster, &boosted_rounds), "XGBoosterBoostedRounds");

    for (const auto & [key, value] : params)
    {
        if (!allowed_keys.contains(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown or forbidden prediction parameter '{}'", key);

        if (key == "type" && value != 0 && value != 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unsupported prediction 'type' {}. Only 0 (value) and 1 (margin) are supported, "
                "because predictXGBoost returns a single Float64 per row",
                value);

        if ((key == "iteration_begin" || key == "iteration_end") && (value < 0 || value > boosted_rounds))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Prediction parameter '{}' is {}, but the model has {} boosting round(s), so it must be between 0 and {}",
                key,
                value,
                boosted_rounds,
                boosted_rounds);

        config.set(key, value);
    }

    /// `Poco::JSON::Object::stringify` requires a `std::ostream`
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    config.stringify(oss);
    return oss.str();
}
}

#endif
