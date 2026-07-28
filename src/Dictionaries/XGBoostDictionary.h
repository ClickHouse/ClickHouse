#pragma once

#include "config.h"

#if USE_XGBOOST

#include <atomic>
#include <memory>

#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/XGBoostModel.h>


namespace DB
{

/// A computational dictionary that trains an XGBoost model once at load time and then predicts a numeric
/// target from a feature vector. The feature columns form the complex key, and the single attribute is the
/// training target. The source supplies the training rows as `(feature_1, ..., feature_k, target)`.
///
/// A `dictGet` call for the target attribute runs inference for the given feature vector. The dedicated
/// `predictXGBoost` function exposes the same model with the features passed as individual arguments.
class XGBoostDictionary final : public IDictionary
{
public:
    struct Configuration
    {
        /// Name of the single attribute that holds the training target.
        String target_name;
        /// Hyperparameters as a JSON string, consumed by the XGBoost backend (see XGBoostModel).
        HyperParameters hyper_parameters;
        /// Filesystem path used to persist the trained model.
        String model_path;
        DictionaryLifetime dict_lifetime;
    };

    XGBoostDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        Configuration configuration_);

    std::string getTypeName() const override { return "XGBoost"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    /// Records `count` predicted rows from the dedicated `predictXGBoost` function, which bypasses `getColumn`
    /// but should still be reflected in the dictionary query statistics.
    void incrementQueryCount(size_t count) const { query_count.fetch_add(count, std::memory_order_relaxed); }

    /// Every feature vector is predictable, so the found rate is one once any query has run.
    double getFoundRate() const override { return query_count.load(std::memory_order_relaxed) == 0 ? 0.0 : 1.0; }

    double getHitRate() const override { return 1.0; }

    /// This computational dictionary stores no keys; it reports the number of feature columns the model was
    /// trained on.
    size_t getElementCount() const override { return model->getFeatureNames().size(); }

    /// The whole model is resident in memory, so the load factor is always one.
    double getLoadFactor() const override { return 1.0; }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return configuration.dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string &) const override { return false; }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Complex; }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<XGBoostDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), configuration);
    }

    ColumnPtr getColumn(
        const std::string & attribute_name,
        const DataTypePtr & attribute_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultOrFilter default_or_filter) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

    /// Deletes the auto-generated model file this dictionary persisted, so dropping the dictionary does not
    /// leave the file orphaned. Only called on `DROP`, never on a reload or server shutdown.
    void removePersistentFilesOnDrop() const override;

    /// Names of the feature columns in training order (the key columns, in declaration order). Used by
    /// `predictXGBoost` to bind its positional feature arguments to the columns the model expects.
    const VectorWithMemoryTracking<String> & getFeatureNames() const;

    /// Runs inference on a block whose columns are the features (named as in `getFeatureNames`), returning a
    /// Float64 column of predictions with one element per row.
    ColumnPtr predict(const Block & features, const PredictParameters & params) const;

private:
    void loadData();

    DictionaryStructure dict_struct;
    DictionarySourcePtr source_ptr;
    Configuration configuration;

    /// Trained once while the dictionary is being constructed and never modified afterwards.
    std::unique_ptr<XGBoostModel> model;

    size_t bytes_allocated = 0;

    mutable std::atomic<size_t> query_count{0};

    LoggerPtr log;
};

}

#endif
