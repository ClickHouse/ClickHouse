#include <Dictionaries/XGBoostModel.h>

#if USE_XGBOOST

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
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

/// Forwards a message of the XGBoost library to the server log instead of `stderr`, where XGBoost writes by
/// default. A message carries no separate level: it is formatted as `[HH:MM:SS] LEVEL: file:line: text`, and the
/// lines XGBoost prints regardless of `verbosity` (such as its timing monitor) have no level at all.
void logXGBoostMessage(const char * raw_message)
{
    static const LoggerPtr log = getLogger("XGBoost");

    std::string_view message(raw_message);

    /// The server log has its own timestamp.
    if (message.starts_with('['))
        if (const auto end = message.find("] "); end != std::string_view::npos)
            message.remove_prefix(end + 2);

    while (message.ends_with('\n'))
        message.remove_suffix(1);

    if (message.empty())
        return;

    auto consume_prefix = [&](std::string_view prefix)
    {
        if (!message.starts_with(prefix))
            return false;
        message.remove_prefix(prefix.size());
        return true;
    };

    if (consume_prefix("WARNING: "))
        LOG_WARNING(log, "{}", message);
    else if (consume_prefix("INFO: "))
        LOG_INFO(log, "{}", message);
    else if (consume_prefix("DEBUG: "))
        LOG_DEBUG(log, "{}", message);
    else
        LOG_INFO(log, "{}", message);
}

/// XGBoost keeps the log callback per thread, so it is registered on every thread that calls into the library
/// before the call, rather than once. XGBoost is built without OpenMP, so it does not log from threads of its own.
void registerLogCallback()
{
    throwOnError(XGBRegisterLogCallback(&logXGBoostMessage), "XGBRegisterLogCallback");
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
    if (!which.isNativeNumber())
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
    /// Validate the parameters provided by the user before the caller reads the source, so that a mistake in the
    /// layout definition is reported without paying for a scan of the whole training table.
    training_params = sanitizeTrainingParams(hps);

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

    chassert(labels.size() == ingested_rows);
    chassert(flattened_features.size() == ingested_rows * n_features);

    registerLogCallback();

    throwOnError(
        XGDMatrixCreateFromMat(flattened_features.data(), ingested_rows, n_features, std::numeric_limits<float>::quiet_NaN(), &dmatrix),
        "XGDMatrixCreateFromMat");

    // Create the model
    throwOnError(XGBoosterCreate(&dmatrix, 1, &booster), "XGBoosterCreate");

    // Apply the params sanitized in `startTraining` into the model
    for (const auto & [key, value] : training_params)
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

    /// A `Nullable` feature is read through its nested column, and a NULL becomes NaN, the missing-value
    /// marker the matrix is created with below.
    VectorWithMemoryTracking<const IColumn *> feature_cols;
    VectorWithMemoryTracking<const NullMap *> null_maps;
    feature_cols.reserve(n_features);
    null_maps.reserve(n_features);
    for (const auto & name : feature_columns)
    {
        const IColumn * column = batch.getByName(name).column.get();
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
        {
            feature_cols.push_back(&nullable->getNestedColumn());
            null_maps.push_back(&nullable->getNullMapData());
        }
        else
        {
            feature_cols.push_back(column);
            null_maps.push_back(nullptr);
        }
    }

    VectorWithMemoryTracking<float> features;

    features.reserve(rows * n_features);

    for (std::size_t r = 0; r < rows; ++r)
    {
        for (std::size_t c = 0; c < n_features; ++c)
        {
            if (null_maps[c] && (*null_maps[c])[r])
                features.push_back(std::numeric_limits<float>::quiet_NaN());
            else
                features.push_back(static_cast<float>(feature_cols[c]->getFloat64(r)));
        }
    }

    registerLogCallback();

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
            /// Parsed as `UInt64`, so that a positive integer too large for the `int` iteration index of
            /// `XGBoosterUpdateOneIter` is reported as out of range rather than as not an integer.
            UInt64 parsed_iterations = 0;
            if (!tryParse(parsed_iterations, value) || parsed_iterations == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter 'num_iterations' must be a positive integer, got '{}'", value);

            constexpr auto max_iterations = static_cast<UInt64>(std::numeric_limits<int>::max());
            if (parsed_iterations > max_iterations)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Parameter 'num_iterations' is {}, but the maximum is {}",
                    parsed_iterations,
                    max_iterations);

            num_iterations = static_cast<int>(parsed_iterations);
        }
        else
        {
            sanitized.emplace(key, value);
        }
    }

    return sanitized;
}

void XGBoostModel::validatePredictParams(const PredictParameters & params)
{
    static const std::unordered_set<String> allowed_keys{ // STYLE_CHECK_ALLOW_STD_CONTAINERS
        "type", "iteration_begin", "iteration_end"};

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

        if ((key == "iteration_begin" || key == "iteration_end") && value < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prediction parameter '{}' is {}, but it must not be negative", key, value);
    }
}

String XGBoostModel::sanitizePredictParams(const PredictParameters & params)
{
    validatePredictParams(params);

    // Default parameters
    Poco::JSON::Object config;
    config.set("type", 0);
    config.set("iteration_begin", 0);
    config.set("iteration_end", 0);
    config.set("strict_shape", false);
    config.set("training", false);

    /// Fetch upper limit for `iteration_begin` and `iteration_end`, which needs the trained model.
    int boosted_rounds = 0;
    throwOnError(XGBoosterBoostedRounds(booster, &boosted_rounds), "XGBoosterBoostedRounds");

    for (const auto & [key, value] : params)
    {
        if ((key == "iteration_begin" || key == "iteration_end") && value > boosted_rounds)
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
