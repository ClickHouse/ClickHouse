---
slug: /sql-reference/statements/create/dictionary/layouts/xgboost
title: 'XGBoost dictionaries'
sidebar_label: 'XGBoost'
sidebar_position: 14
description: 'Configure XGBOOST dictionaries to train a gradient-boosted model and predict a numeric target.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

The `xgboost` (`XGBOOST`) dictionary trains an [XGBoost](https://xgboost.readthedocs.io/) gradient-boosted model once, at load time, from a source table of training rows, then predicts a numeric target for any feature vector you pass in. The feature columns are the dictionary key and the single attribute is the target the model learns.

It is suited to tabular regression and binary classification where the features are numeric — for example forecasting a value from several measurements, or scoring rows against a learned target. Multiclass objectives are not supported (see [Layout parameters](#layout-parameters)).

:::note
The XGBoost integration is experimental. Enable it with the `allow_experimental_xgboost` setting before creating an `XGBOOST` dictionary or calling `predictXGBoost`:

```sql
SET allow_experimental_xgboost = 1;
```

`predictXGBoost` is the only way to predict with such a dictionary, so with the setting off an already created dictionary cannot be used either.
:::

[`predictXGBoost`](/sql-reference/functions/machine-learning-functions#predictxgboost) is the only way to query the dictionary: it takes the features as individual arguments, returns the prediction, and accepts additional [prediction parameters](#prediction-parameters). The dictionary holds a trained model rather than rows, so the generic dictionary interface — [`dictGet`](/sql-reference/functions/ext-dict-functions#dictget), `dictHas` and `SELECT * FROM dict` — is not supported and reports an error (see [Notes](#notes)).

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

`PRIMARY KEY (x1, x2)` makes `x1` and `x2` the features — but for an `XGBOOST` dictionary this "key" is the feature vector you pass in to predict, not a stored value you look up (see [Dictionary structure](#dictionary-structure)). The target the model learns is `y`, inferred as the single column that is not part of the key; the parameters in `LAYOUT` are XGBoost hyperparameters (see [Layout parameters](#layout-parameters)).

**4. Predict** — `predictXGBoost` takes the features positionally and returns the prediction:

```sql
SELECT predictXGBoost('model', 1.0, 2.0) AS prediction;
```

```response
   ┌─prediction─┐
1. │  7.9968586 │
   └────────────┘
```

The ground truth is `2*1 + 3*2 = 8`, so the model's prediction is close.

## How it works {#how-it-works}

**Training (at load time).** Each source row is a `(features..., target)` observation. When the dictionary loads, all rows are streamed into XGBoost and the model is trained once. Feature and target values are read as floats, so the key columns must be numeric and the target attribute floating-point (see [Dictionary structure](#dictionary-structure)).

**Predicting (at query time).** To predict, the model takes the feature vector — in the same order as the key columns were declared — and runs it through the trained booster, returning a `Float64`.

**The model is not persisted.** It lives only in memory, for as long as the dictionary is loaded, and is trained again from the source on every load — including after a server restart. Training cost is therefore paid on each load, which matters for a large source table.

**Retraining the model.** Because every load trains from scratch, `SYSTEM RELOAD DICTIONARY` retrains the model against the current contents of the source table:

```sql
INSERT INTO training_data VALUES (5, 10, 40);
SYSTEM RELOAD DICTIONARY model;
```

A non-zero `LIFETIME` also retrains, since a lifetime-triggered reload is an ordinary load. Use it to refresh the model periodically as the training data grows.

:::warning
Every load trains on the whole source table, and nothing about a trained model survives it. A restart, a `SYSTEM RELOAD DICTIONARY`, or each expiry of a non-zero `LIFETIME` pays the full training cost again, and on a large source table that cost is not small: `LIFETIME(3600)` on a table that takes ten minutes to train means retraining for ten minutes of every hour, indefinitely. Choose `LIFETIME` from how long training actually takes, not from how often the data changes, and prefer `LIFETIME(0)` with an explicit `SYSTEM RELOAD DICTIONARY` when you want to control when that cost is paid.
:::

## Dictionary structure {#dictionary-structure}

An `XGBOOST` dictionary has a fixed shape:

- The `PRIMARY KEY` is one or more columns of a native numeric type (integers and floats) — the features. At query time this "key" is the feature vector you pass in to predict, not a stored lookup key. The feature order is the key-column declaration order, and `predictXGBoost` binds its positional arguments to that order.
- Alongside them, declare **exactly one attribute of type `Float32` or `Float64`**: the target the model learns. It is always inferred as the single column that is not part of the feature key — there is no parameter to name it, and it is an error to declare more than one attribute.

A column that does not match these requirements is rejected when the dictionary loads, not when you create it.

The target must be floating-point because a prediction is a floating-point value: `predictXGBoost` returns a `Float64`, and an integer target column would describe the model as predicting whole numbers — a probability of `0.73` declared as a `UInt8`. This does not prevent binary classification: the labels in the source table may be integers, you simply declare the target column as `Float32` or `Float64` in the dictionary and the source values are converted on load.

## Layout parameters {#layout-parameters}

Only the parameters listed below are accepted; any other name fails the load, so typos are caught when the model trains rather than being silently ignored. `num_iterations` is handled by ClickHouse (see its description); every other parameter is forwarded to the XGBoost booster unchanged, as a string, and takes XGBoost's own default and value range — see the [XGBoost parameter reference](https://xgboost.readthedocs.io/en/stable/parameter.html).

| Parameter | Description |
| --- | --- |
| `num_iterations` | Number of boosting rounds (how many trees to train). A positive integer, used as the training loop count rather than forwarded to the booster. Default `100`. |
| `booster` | Booster type: `gbtree`, `gblinear`, or `dart`. |
| `objective` | Learning objective, e.g. `reg:squarederror` or `binary:logistic`. Must be an objective that predicts a single value per row; multiclass objectives (`multi:softmax`, `multi:softprob`) are rejected — see the note below. |
| `eval_metric` | Evaluation metric(s) used during training. |
| `seed` | Random number seed. |
| `verbosity` | Logging verbosity: `0` (silent) to `3` (debug). |
| `nthread` | Number of parallel threads used for training. |
| `eta` | Step-size shrinkage applied after each boosting round. |
| `gamma` | Minimum loss reduction required to make a further split on a leaf. |
| `max_depth` | Maximum depth of a tree. |
| `min_child_weight` | Minimum sum of instance weight (hessian) needed in a child. |
| `max_delta_step` | Maximum delta step allowed for each leaf's output. |
| `subsample` | Fraction of the training rows sampled for each boosting round. |
| `sampling_method` | Row sampling method: `uniform` or `gradient_based`. |
| `colsample_bytree` / `colsample_bylevel` / `colsample_bynode` | Fraction of columns (features) sampled per tree / per level / per split. |
| `lambda` | L2 regularization term on weights (aliases). |
| `alpha` | L1 regularization term on weights (aliases). |
| `tree_method` | Tree construction algorithm: `auto`, `exact`, `approx`, or `hist`. |
| `scale_pos_weight` | Balances positive and negative weights, useful for imbalanced classes. |
| `grow_policy` | How new nodes are added to the tree: `depthwise` or `lossguide`. |
| `max_leaves` | Maximum number of leaf nodes (used with `grow_policy` `lossguide`). |
| `max_bin` | Maximum number of discrete bins used to bucket continuous features (used with `tree_method` `hist`). |
| `num_parallel_tree` | Number of trees grown per boosting round (a value `> 1` trains a boosted random forest). |

:::note
Multiclass objectives are not supported. `multi:softmax` and `multi:softprob` require XGBoost's `num_class` parameter, which is not in the list above, and `multi:softprob` predicts one probability per class per row while a dictionary returns exactly one `Float64` per row. A dictionary whose `objective` starts with `multi:` is rejected when the model trains. Use a regression objective, or `binary:logistic` for two-class classification.
:::

You can define the dictionary with `CREATE DICTIONARY` DDL (as in the quickstart above).

```sql
CREATE DICTIONARY model (x1 Float64, x2 Float64, y Float64)
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

The parameter names map to the prediction parameters of XGBoost's `XGBoosterPredictFromDMatrix`. Only the keys below are accepted; any other key fails the query. Values are Int64 — `strict_shape` is given as `0` or `1`.

| Parameter | Description | Default |
| --- | --- | --- |
| `type` | Prediction type. Only `0` (value) and `1` (margin) are accepted, because `predictXGBoost` returns a single `Float64` per row. Other XGBoost types (`2`/`3` SHAP contributions, `4`/`5` feature interactions, `6` leaf index) emit several values per row and are rejected. | `0` |
| `iteration_begin` | First boosting iteration (tree) to include in the prediction. | `0` |
| `iteration_end` | Last boosting iteration to include; `0` uses all trees. | `0` |
| `strict_shape` | Apply stricter output-shape rules. | `false` |
| `ntree_limit` | Deprecated; limits the number of trees used. Prefer `iteration_begin` / `iteration_end`. | — |

:::note
`predictXGBoost` returns exactly one `Float64` per input row, so only prediction types that produce a single value per row are accepted: `0` (value) and `1` (margin). Types that emit several values per row - such as SHAP contributions (`2`, `3`) or feature interactions (`4`, `5`) - are rejected with an error.
:::

## Notes {#notes}

- **Computational dictionary semantics.** This is a *computational* dictionary: it holds a trained model, not rows, and `predictXGBoost` is the only way to query it. The generic dictionary interface is not supported and reports an error: `dictGet` (there is no stored attribute to look up — the "key" is a feature vector to predict from), `dictHas` (no keys are stored), `SELECT * FROM dict` and joining the dictionary as a table. Because `predictXGBoost` is the only entry point, the `allow_experimental_xgboost` setting is in charge of every prediction.
- **Numeric columns only.** Every feature (key) column must be a native numeric type and the target attribute must be `Float32` or `Float64`. Values are read as floats during training and prediction.
- **`system.dictionaries` reports no stored items.** The dictionary trains a model instead of storing rows, so `element_count` is `0`, as it is for a `direct` dictionary, and `bytes_allocated` is `0` too: the trained model belongs to XGBoost, which does not report how much memory it holds. `query_count` and `found_rate` count the rows predicted through `predictXGBoost`.
- **Feature order matters.** `predictXGBoost` binds its positional feature arguments to the key columns in declaration order, and the number of feature arguments must match the number of key columns.
