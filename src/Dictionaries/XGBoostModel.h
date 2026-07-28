#pragma once

#include "config.h"

#if USE_XGBOOST

#include <Columns/IColumn_fwd.h>
#include <Core/Block_fwd.h>
#include <base/types.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <mutex>

namespace DB
{

struct ColumnWithTypeAndName;

/// Training hyperparameters as parameter name -> value, assembled from the dictionary layout config
using HyperParameters = MapWithMemoryTracking<String, String>;
/// Prediction parameters
using PredictParameters = MapWithMemoryTracking<String, Int64>;


/// Extreme Gradient Boosting model, driven directly by XGBoostDictionary.
///
/// Training follows a streaming lifecycle so that datasets that do not fit in memory can be fed batch
/// by batch:
///
///     XGBoostModel(hyper_parameters) -> startTraining -> addTrainingData* -> finalizeTraining -> predict*
///
/// NOTE: batches are currently staged in memory.
class XGBoostModel
{
public:
    explicit XGBoostModel(const HyperParameters & hyper_parameters);
    ~XGBoostModel();

    /// Begin training. `header` is a sample block describing the input schema (structure only, no rows);
    /// `target_column` names the label column - every other column in `header` is treated as a feature.
    void startTraining(const Block & header, const String & target_column);

    /// Feed one batch of training rows. Called repeatedly, once per source block.
    void addTrainingData(const Block & batch);

    /// Build/train the model. After this the model is ready to predict.
    void finalizeTraining();

    /// Load a previously trained model from `path` (as written by `saveToFile`), skipping training.
    /// `header`/`target_column` describe the expected schema exactly as in `startTraining`; the loaded
    /// model's feature count is validated against it. After this the model is ready to predict.
    void loadFromFile(const Block & header, const String & target_column, const String & path);

    /// Persist the trained model to `path` in XGBoost's native format. Must be called
    /// after `finalizeTraining`.
    void saveToFile(const String & path) const;

    /// Predict on a batch of feature rows. Returns a Float64 column of predictions with one element per
    /// row of `batch`.
    ColumnPtr predict(const Block & batch, const PredictParameters & params);

    /// Names of the feature columns, in the order the model was trained on. Used by the dictionary to bind
    /// the positional feature arguments of `predictXGBoost` to the columns the backend expects.
    const VectorWithMemoryTracking<String> & getFeatureNames() const { return feature_columns; }

private:
    /// Record the training schema: `target_column` is the label, every other column of `header` a feature.
    /// Shared by `startTraining` and `loadFromFile`.
    void setSchema(const Block & header, const String & target_column);

    void throwIfTypeIsInvalid(const ColumnWithTypeAndName & col);

    UnorderedMapWithMemoryTracking<String, String> sanitizeTrainingParams(const HyperParameters & params);
    String sanitizePredictParams(const PredictParameters & params);

    /// XGBoost C-API handles. `BoosterHandle` and `DMatrixHandle` are `void *`, kept opaque here so this
    /// header stays free of the XGBoost headers (only XGBoostModel.cpp needs them).
    void * booster{nullptr};
    void * dmatrix{nullptr};

    HyperParameters hps;
    int num_iterations{100};

    /// Target column.
    String target_column;
    /// Name of the feature columns.
    VectorWithMemoryTracking<String> feature_columns;
    /// Number of features.
    std::size_t n_features = 0;
    /// Number of rows already ingested.
    std::size_t ingested_rows = 0;

    /// Keeps the features in a flattened vector.
    VectorWithMemoryTracking<float> flattened_features;
    /// Vector of labels.
    VectorWithMemoryTracking<float> labels;

    /// A trained model is shared through the dictionary, so concurrent predictXGBoost / dictGet queries
    /// would otherwise race inside XGBoosterPredict, which writes into a prediction buffer owned by the booster.
    std::mutex predict_mutex;
};

}

#endif
