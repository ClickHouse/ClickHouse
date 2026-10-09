#include <Dictionaries/XGBoostDictionary.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <Dictionaries/XGBoostModel.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
extern const int UNSUPPORTED_METHOD;
}

#if USE_XGBOOST

XGBoostDictionary::XGBoostDictionary(
    const StorageID & dict_id_, const DictionaryStructure & dict_struct_, DictionarySourcePtr source_ptr_, Configuration configuration_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , configuration(std::move(configuration_))
    , log(getLogger("XGBoostDictionary"))
{
    trainModel();
}


void XGBoostDictionary::trainModel()
{
    /// Build the training header: features columns in declaration order, followed by the single
    /// target attribute. The XGBoost backend treats every column other than the target as a feature, so the
    /// feature order is the key-column declaration order.
    ColumnsWithTypeAndName header_columns;
    for (const auto & key_attribute : *dict_struct.key)
        header_columns.emplace_back(key_attribute.type->createColumn(), key_attribute.type, key_attribute.name);
    const auto & target_attribute = dict_struct.getAttribute(configuration.target_name);
    header_columns.emplace_back(target_attribute.type->createColumn(), target_attribute.type, target_attribute.name);
    Block header(header_columns);

    model = std::make_unique<XGBoostModel>(configuration.hyper_parameters);

    model->startTraining(header, configuration.target_name);

    BlockIO io = source_ptr->loadAll();

    io.executeWithCallbacks(
        [&]()
        {
            DictionaryPipelineExecutor executor(io.pipeline, false);
            io.pipeline.setConcurrencyControl(false);

            Block block;
            while (executor.pull(block))
                model->addTrainingData(block);
        });

    model->finalizeTraining();

    LOG_INFO(log, "Loaded XGBoost dictionary trained on {} feature(s)", model->getFeatureNames().size());
}


const VectorWithMemoryTracking<String> & XGBoostDictionary::getFeatureNames() const
{
    return model->getFeatureNames();
}


ColumnPtr XGBoostDictionary::predict(const Block & features, const PredictParameters & params) const
{
    query_count.fetch_add(features.rows(), std::memory_order_relaxed);

    return model->predict(features, params);
}


ColumnPtr XGBoostDictionary::getColumn(
    const std::string &,
    const DataTypePtr &,
    const Columns &,
    const DataTypes &,
    DefaultOrFilter) const
{
    /// Disabled because there is no Context here, which means it is not possible to block access
    /// in case `enable_xgboost` is disabled.
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "An XGBoost dictionary does not support `dictGet`. Use function `predictXGBoost('{}', feature_1, ...)` to predict",
        getFullName());
}


ColumnUInt8::Ptr XGBoostDictionary::hasKeys(const Columns &, const DataTypes &) const
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "An XGBoost dictionary does not support `dictHas`: it stores no keys, it predicts from a feature vector");
}


Pipe XGBoostDictionary::read(const Names &, size_t, size_t) const
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "An XGBoost dictionary trains a model and cannot be read back as a table of rows");
}


#endif


void registerDictionaryXGBoost(DictionaryFactory & factory);
void registerDictionaryXGBoost(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & /* full_name */,
                            [[maybe_unused]] const DictionaryStructure & dict_struct,
                            [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                            [[maybe_unused]] const std::string & config_prefix,
                            [[maybe_unused]] DictionarySourcePtr source_ptr,
                            [[maybe_unused]] ContextPtr global_context,
                            [[maybe_unused]] bool created_from_ddl) -> DictionaryPtr
    {
#if !USE_XGBOOST
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary layout `xgboost` is disabled because ClickHouse was built without XGBoost support");
#else

        /// Only CREATE DICTIONARY is supported.
        if (!created_from_ddl)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "An XGBoost dictionary defined in a configuration file is not supported. Use `CREATE DICTIONARY`");

        /// The structure must be a complex key of one or more numeric feature columns, followed by exactly one
        /// floating-point attribute: the training target.
        if (!dict_struct.key || dict_struct.key->empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "XGBoost dictionary must have at least one key column (the numeric features)");

        for (const auto & key_attribute : *dict_struct.key)
        {
            const WhichDataType which(key_attribute.type);
            if (!which.isNativeNumber())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "XGBoost dictionary feature key '{}' must be a native numeric type, got {}",
                    key_attribute.name,
                    key_attribute.type->getName());
        }

        if (dict_struct.attributes.size() != 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "XGBoost dictionary must have exactly one attribute (the training target), got {}",
                dict_struct.attributes.size());

        /// Check the target data type
        const auto & target_attribute = dict_struct.attributes[0];
        if (!WhichDataType(target_attribute.type).isNativeFloat())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "XGBoost dictionary target attribute '{}' must be Float32 or Float64, got {}. The model predicts a floating-point "
                "value, so an integer target would truncate the prediction",
                target_attribute.name,
                target_attribute.type->getName());

        const String layout_prefix = config_prefix + ".layout.xgboost";

        /// Collect training parameters
        Poco::Util::AbstractConfiguration::Keys layout_keys;
        config.keys(layout_prefix, layout_keys);

        HyperParameters hyper_parameters;
        for (const auto & key : layout_keys)
        {
            /// Poco returns a repeated XML element as `name`, `name[1]`, `name[2]`, ... A parameter written
            /// twice in `LAYOUT(XGBOOST(...))` would otherwise reach the allowlist as `max_depth[1]` and be
            /// reported as an unknown parameter, which hides the real mistake behind Poco's array syntax.
            const auto bracket = key.find('[');
            if (bracket != String::npos)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Training parameter '{}' is specified more than once",
                    key.substr(0, bracket));

            hyper_parameters.emplace(key, config.getString(layout_prefix + "." + key));
        }

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        XGBoostDictionary::Configuration cfg{
            .target_name = target_attribute.name,
            .hyper_parameters = std::move(hyper_parameters),
            .dict_lifetime = dict_lifetime,
        };

        return std::make_unique<XGBoostDictionary>(dict_id, dict_struct, std::move(source_ptr), std::move(cfg));
#endif
    };

    factory.registerLayout(
        "xgboost",
        create_layout,
        /* is_layout_complex= */ true,
        /* has_layout_complex= */ false,
        Documentation{
            .description = R"DOCS_MD(
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

# XGBoost dictionaries

<CloudNotSupportedBadge/>

The `xgboost` (`XGBOOST`) dictionary trains an [XGBoost](https://xgboost.readthedocs.io/) gradient-boosted model, at load time, from a source table of training rows, then predicts a numeric target for any feature vector you pass in. The feature columns are the dictionary key and the single attribute is the target the model learns.

It is suited to tabular regression and binary classification where the features are numeric — for example forecasting a value from several measurements, or scoring rows against a learned target. Multiclass objectives are not supported (see [Layout parameters](#layout-parameters)).

:::note
The XGBoost integration is experimental. Enable it with the `enable_xgboost` setting before creating an `XGBOOST` dictionary or calling `predictXGBoost`:

```sql
SET enable_xgboost = 1;
```

Only `CREATE DICTIONARY` is supported: an `XGBOOST` dictionary defined in a server configuration file fails to load.
:::

[`predictXGBoost`](/reference/functions/regular-functions/machine-learning-functions) is the only way to query the dictionary: it takes the features as individual arguments, returns the prediction, and accepts additional [prediction parameters](#prediction-parameters). The dictionary holds a trained model rather than rows, so the generic dictionary interface — [`dictGet`](/reference/functions/regular-functions/ext-dict-functions#dictGet), `dictHas` and `SELECT * FROM dict` — is not supported and reports an error.

## Quickstart {#quickstart}

Here we train a regressor on the linear target `y = 2*x1 + 3*x2`.

**1. Create a source table** of training rows — the feature columns followed by the target:

```sql
CREATE TABLE training_data (x1 Float64, x2 Float64, y Float64)
ENGINE = MergeTree ORDER BY tuple();
```

**2. Insert training data:**

```sql
INSERT INTO training_data
SELECT number AS x1, number * 2 AS x2, 2 * x1 + 3 * x2 AS y
FROM numbers(100);
```

**3. Create the dictionary** with the `XGBOOST` layout — the feature columns are the key and `y` is the target attribute:

```sql
CREATE DICTIONARY model (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_data'))
LAYOUT(XGBOOST(
    objective 'reg:squarederror'
    num_iterations 100
    max_depth 6
))
LIFETIME(0);
```

`PRIMARY KEY (x1, x2)` makes `x1` and `x2` the features. The target the model learns is `y`, inferred as the single column that is not part of the key; the parameters in `LAYOUT` are XGBoost hyperparameters (see [Layout parameters](#layout-parameters)).

**4. Predict** — `predictXGBoost` takes the features positionally and returns the prediction:

```sql
SELECT predictXGBoost('model', 1.0, 2.0) AS prediction;
```

The ground truth is `2*1 + 3*2 = 8`, so the model's prediction is close to `8`.

## How it works {#how-it-works}

**Training (at load time).** Each source row is a `(features..., target)` observation. When the dictionary loads, the source is read block by block and the model is then trained once over the whole set. Feature and target values are read as floats, so the key columns must be numeric and the target attribute floating-point (see [Dictionary structure](#dictionary-structure)).

:::warning
**Training holds the entire training set in memory.** Reading the source in blocks is not out-of-core training: each block is accumulated rather than consumed, so the whole training set is memory resident before the first boosting round and stays memory resident until the last one.
:::

**Predicting (at query time).** To predict, the model takes the feature vector — in the same order as the key columns were declared — and runs it through the trained booster, returning a `Float64`. A `NULL` feature is passed to the model as a missing value, which XGBoost handles the way it learned to during training, so the prediction is never `NULL`. When every feature is a constant, the model is evaluated once per block instead of once per row.

**The model is not persisted.** It lives only in memory, for as long as the dictionary is loaded, and is trained again from the source on every load — including after a server restart.

**Retraining the model.** Because every load trains from scratch, `SYSTEM RELOAD DICTIONARY` retrains the model against the current contents of the source table:

```sql
INSERT INTO training_data VALUES (5, 10, 40);
SYSTEM RELOAD DICTIONARY model;
```

A non-zero `LIFETIME` also retrains, since a lifetime-triggered reload is an ordinary load. Use it to refresh the model periodically as the training data grows.

## Dictionary structure {#dictionary-structure}

An `XGBOOST` dictionary has a fixed shape:

- The `PRIMARY KEY` is one or more columns of a native numeric type (integers and floats) — the features. At query time this "key" is the feature vector you pass in to predict, not a stored lookup key. The feature order is the key-column declaration order, and `predictXGBoost` binds its positional arguments to that order.
- Alongside them, declare **exactly one attribute of type `Float32` or `Float64`**: the target the model learns. It is always inferred as the single column that is not part of the feature key — there is no parameter to name it, and it is an error to declare more than one attribute.

A column that does not match these requirements is rejected when the dictionary loads, not when you create it.

## Layout parameters {#layout-parameters}

Only the parameters listed below are accepted; any other name fails the load, so typos are caught when the model trains rather than being silently ignored. `num_iterations` is handled by ClickHouse (see its description); every other parameter is forwarded to the XGBoost booster unchanged, as a string, and takes XGBoost's own default and value range — see the [XGBoost parameter reference](https://xgboost.readthedocs.io/en/stable/parameter.html).

| Parameter | Description |
| --- | --- |
| `num_iterations` | Number of boosting rounds (how many trees to train). A positive integer, used as the training loop count rather than forwarded to the booster. Default `100`. |
| `booster` | Booster type: `gbtree`, `gblinear`, or `dart`. |
| `objective` | Learning objective, e.g. `reg:squarederror` or `binary:logistic`. Must be an objective that predicts a single value per row; multiclass objectives (`multi:softmax`, `multi:softprob`) are rejected. |
| `seed` | Random number seed. Training is otherwise deterministic, so this only changes the model when a stochastic parameter (`subsample`, `sampling_method`, any `colsample_*`) is also set. |
| `verbosity` | Logging verbosity of XGBoost: `0` (silent) to `3` (debug), default `1` (warnings). Affects only which messages XGBoost logs, never the trained model. See [Notes](#notes) for where the messages go. |
| `nthread` | Number of parallel threads used for training. Accepted and forwarded, but currently has no effect: the bundled XGBoost is built without OpenMP, so training and prediction always run on a single thread. |
| `eta` | Step-size shrinkage applied after each boosting round. Only this spelling is accepted; XGBoost's `learning_rate` alias is not. |
| `gamma` | Minimum loss reduction required to make a further split on a leaf. |
| `max_depth` | Maximum depth of a tree. |
| `min_child_weight` | Minimum sum of instance weight (hessian) needed in a child. |
| `max_delta_step` | Maximum delta step allowed for each leaf's output. |
| `subsample` | Fraction of the training rows sampled for each boosting round. |
| `sampling_method` | Row sampling method: `uniform` or `gradient_based`. Has no effect unless `subsample` is set to less than `1`. |
| `colsample_bytree` / `colsample_bylevel` / `colsample_bynode` | Fraction of columns (features) sampled per tree / per level / per split. |
| `lambda` | L2 regularization term on weights. Only this spelling is accepted; XGBoost's `reg_lambda` alias is not. |
| `alpha` | L1 regularization term on weights. Only this spelling is accepted; XGBoost's `reg_alpha` alias is not. |
| `tree_method` | Tree construction algorithm: `auto`, `exact`, `approx`, or `hist`. |
| `scale_pos_weight` | Balances positive and negative weights, useful for imbalanced classes. |
| `grow_policy` | How new nodes are added to the tree: `depthwise` or `lossguide`. |
| `max_leaves` | Maximum number of leaf nodes (used with `grow_policy` `lossguide`). |
| `max_bin` | Maximum number of discrete bins used to bucket continuous features (used with `tree_method` `hist`). |
| `num_parallel_tree` | Number of trees grown per boosting round (a value `> 1` trains a boosted random forest). |

Parameter names are case-insensitive, and each may be given only once. Values must be a positive integer, a float, or a quoted string: a negative literal is rejected by the dictionary DDL itself, before the layout sees it, so a negative `seed` cannot be expressed.

For example, a dictionary that also sets the step size `eta`:

```sql
CREATE DICTIONARY model_tuned (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_data'))
LAYOUT(XGBOOST(
    objective 'reg:squarederror'
    num_iterations 100
    max_depth 6
    eta 0.3
))
LIFETIME(0);
```

## Prediction parameters {#prediction-parameters}

`predictXGBoost` accepts an optional trailing constant `Map` of XGBoost prediction parameters, after the features, built with `map`:

```sql
SELECT predictXGBoost('model', 1.0, 2.0, map('type', 0, 'iteration_end', 0));
```

The parameter names map to the prediction parameters of XGBoost's `XGBoosterPredictFromDMatrix`. Only the keys below are accepted; any other key fails the query. Every parameter is an integer or a boolean, so the `Map` values must be an integer type.

| Parameter | Description | Default |
| --- | --- | --- |
| `type` | Prediction type. Only `0` (value) and `1` (margin) are accepted, because `predictXGBoost` returns a single `Float64` per row. Other XGBoost types (`2`/`3` SHAP contributions, `4`/`5` feature interactions, `6` leaf index) emit several values per row and are rejected. | `0` |
| `iteration_begin` | First boosting round to include in the prediction, counted from `0` (inclusive). Must not exceed the number of boosting iterations the model was trained with (`num_iterations`). | `0` |
| `iteration_end` | One past the last boosting round to include (exclusive), so `iteration_end 1` uses only the first round; `0` uses all rounds. Bounded like `iteration_begin`. | `0` |

## Notes {#notes}

- **Computational dictionary semantics.** This is a *computational* dictionary: it holds a trained model, not rows, and `predictXGBoost` is the only way to query it. The generic dictionary interface is not supported and reports an error: `dictGet` (there is no stored attribute to look up — the "key" is a feature vector to predict from), `dictHas` (no keys are stored), `SELECT * FROM dict` and joining the dictionary as a table. Because `predictXGBoost` is the only entry point, the `enable_xgboost` setting must be enabled for every prediction.
- **Numeric columns only.** Every feature (key) column must be a native numeric type and the target attribute must be `Float32` or `Float64`. Values are read as floats during training and prediction. The feature arguments of `predictXGBoost` may also be `Nullable`, see [How it works](#how-it-works).
- **`system.dictionaries` reports no stored items.** The dictionary trains a model instead of storing rows, so `element_count` is `0`, as it is for a `direct` dictionary, and `bytes_allocated` is `0` too: the trained model belongs to XGBoost, which does not report how much memory it holds. `query_count` and `found_rate` count the rows passed to the model by `predictXGBoost`; a call whose features are all constant is evaluated once per block, so it counts one row per block rather than one per row.
- **A failed reload keeps the previous model.** If retraining fails — the source table is gone, its schema changed, a hyperparameter is no longer accepted — the dictionary does not start failing predictions. It keeps serving the last model that trained successfully, and records the error instead. Compare `last_successful_update_time` with `last_exception` in `system.dictionaries` to tell whether the model still reflects the current source data:

  ```sql
  SELECT name, status, last_successful_update_time, last_exception
  FROM system.dictionaries
  WHERE name = 'model';
  ```

- **XGBoost's messages go to the server log.** They are written under the `XGBoost` logger, at the level XGBoost gives them, and are attached to the query that trains or predicts, so they also appear in `system.text_log`. Raise `verbosity` to see more of them.
- **Feature order matters.** `predictXGBoost` binds its positional feature arguments to the key columns in declaration order, and the number of feature arguments must match the number of key columns.
)DOCS_MD",
            .syntax = "LAYOUT(XGBOOST([objective '...'] [num_iterations N] [max_depth N] [eta 0.3] [...]))",
            .introduced_in = {26, 10}});
}

}
