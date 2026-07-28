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
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
extern const int XGBOOST_ERROR;
}

namespace
{

inline void throwOnError(int err)
{
    if (err != 0)
    {
        std::string what{XGBGetLastError()};
        throw Exception(ErrorCodes::XGBOOST_ERROR, "Error: {}", what);
    }
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
            ErrorCodes::XGBOOST_ERROR,
            "XGBoost only accepts numerical types. The column {} has type {}",
            col.column->getName(),
            type->getName());
    }
}

void XGBoostModel::setSchema(const Block & header, const String & target_column_)
{
    if (!header.has(target_column_))
        throw Exception(ErrorCodes::XGBOOST_ERROR, "Target column '{}' is not present in the training data", target_column_);

    target_column = target_column_;

    feature_columns.clear();
    for (const auto & column : header.getColumnsWithTypeAndName())
        if (column.name != target_column)
            feature_columns.push_back(column.name);

    if (feature_columns.empty())
        throw Exception(ErrorCodes::XGBOOST_ERROR, "No feature columns for training (target column is '{}')", target_column);

    n_features = feature_columns.size();
}

void XGBoostModel::startTraining(const Block & header, const String & target_column_)
{
    setSchema(header, target_column_);
}

void XGBoostModel::loadFromFile(const Block & header, const String & target_column_, const String & path)
{
    setSchema(header, target_column_);

    /// An empty booster to load the persisted model into.
    throwOnError(XGBoosterCreate(nullptr, 0, &booster));
    throwOnError(XGBoosterLoadModel(booster, path.c_str()));

    /// The declared feature key must match the model that was trained: otherwise prediction would silently
    /// bind the wrong columns.
    bst_ulong loaded_features = 0;
    throwOnError(XGBoosterGetNumFeature(booster, &loaded_features));
    if (loaded_features != n_features)
        throw Exception(
            ErrorCodes::XGBOOST_ERROR,
            "Loaded XGBoost model from '{}' expects {} feature(s), but the dictionary declares {}",
            path,
            loaded_features,
            n_features);
}

void XGBoostModel::saveToFile(const String & path) const
{
    if (!booster)
        throw Exception(ErrorCodes::XGBOOST_ERROR, "Cannot save an XGBoost model that has not been trained");

    throwOnError(XGBoosterSaveModel(booster, path.c_str()));
}

void XGBoostModel::addTrainingData(const Block & batch)
{
    const std::size_t rows = batch.rows();
    if (rows == 0)
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

    flattened_features.reserve(ingested_rows + (rows * n_features));
    labels.reserve(ingested_rows + rows);

    // Transforms from the Block into a flattened vector, stores the tuples row-wise
    for (std::size_t r = 0; r < rows; ++r)
    {
        for (std::size_t c = 0; c < n_features; ++c)
            flattened_features.push_back(static_cast<float>(feature_cols[c]->getFloat64(r)));
        labels.push_back(static_cast<float>(label_col->getFloat64(r)));
    }

    ingested_rows += rows;
}

void XGBoostModel::finalizeTraining()
{
    if (ingested_rows == 0)
        throw Exception(ErrorCodes::XGBOOST_ERROR, "No training data was provided");


    chassert(labels.size() == ingested_rows);
    chassert(flattened_features.size() == ingested_rows * n_features);

    throwOnError(
        XGDMatrixCreateFromMat(flattened_features.data(), ingested_rows, n_features, std::numeric_limits<float>::quiet_NaN(), &dmatrix));

    // Create the model
    throwOnError(XGBoosterCreate(&dmatrix, 1, &booster));

    // Set the label for each row
    throwOnError(XGDMatrixSetFloatInfo(dmatrix, "label", labels.data(), ingested_rows));

    // Validate the parameters provided by the user
    const auto params = sanitizeTrainingParams(hps);

    for (const auto & [key, value] : params)
    {
        throwOnError(XGBoosterSetParam(booster, key.c_str(), value.c_str()));
    }

    // Train the model
    for (int i = 0; i < num_iterations; ++i)
    {
        throwOnError(XGBoosterUpdateOneIter(booster, i, dmatrix));
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
        throw Exception(ErrorCodes::XGBOOST_ERROR, "Expected {} features, got {}", n_features, batch.columns());
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

    throwOnError(XGDMatrixCreateFromMat(features.data(), rows, n_features, std::numeric_limits<float>::quiet_NaN(), &predict_dmatrix));

    auto result = ColumnFloat64::create();

    {
        std::lock_guard lock(predict_mutex);

        /* Shape of output prediction */
        bst_ulong const * out_shape{nullptr};
        /* Dimension of output prediction */
        bst_ulong out_dim{0};
        /* Pointer to a thread local contiguous array, assigned in prediction function. */
        float const * out_result{nullptr};

        String config = sanitizePredictParams(params);

        throwOnError(XGBoosterPredictFromDMatrix(booster, predict_dmatrix, config.c_str(), &out_shape, &out_dim, &out_result));

        size_t out_len = 1;
        for (uint64_t i = 0; i < out_dim; ++i)
            out_len *= out_shape[i];

        // Should have predicted the number of inputted rows
        chassert(rows == out_len);

        auto & data = result->getData();
        data.resize(out_len);
        for (std::size_t i = 0; i < out_len; ++i)
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
        "eval_metric",
        "seed",
        "verbosity",
        "nthread",
        "eta",
        "learning_rate",
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
        "reg_lambda",
        "alpha",
        "reg_alpha",
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
            throw Exception(ErrorCodes::XGBOOST_ERROR, "Unknown or forbidden training parameter '{}'", key);

        // If we found num_iterations, record this value and do not add it to the final map
        if (key == "num_iterations")
        {
            int parsed_iterations = 0;
            if (!tryParse(parsed_iterations, value) || parsed_iterations <= 0)
                throw Exception(ErrorCodes::XGBOOST_ERROR, "Parameter 'num_iterations' must be a positive integer, got '{}'", value);
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
        "type", "iteration_begin", "iteration_end", "strict_shape", "ntree_limit"};

    for (const auto & [key, value] : params)
    {
        if (!allowed_keys.contains(key))
            throw Exception(ErrorCodes::XGBOOST_ERROR, "Unknown or forbidden prediction parameter '{}'", key);

        if (key == "type" && value != 0 && value != 1)
            throw Exception(
                ErrorCodes::XGBOOST_ERROR,
                "Unsupported prediction 'type' {}. Only 0 (value) and 1 (margin) are supported, "
                "because predictXGBoost returns a single Float64 per row",
                value);

        /// `strict_shape` is a boolean in XGBoost's config; the rest are integers.
        if (key == "strict_shape")
            config.set(key, value != 0);
        else
            config.set(key, value);
    }

    /// `Poco::JSON::Object::stringify` requires a `std::ostream`
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    config.stringify(oss);
    return oss.str();
}
}

#endif
