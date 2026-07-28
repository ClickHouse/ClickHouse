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

It is suited to tabular regression and classification where the features are numeric — for example forecasting a value from several measurements, or scoring rows against a learned target.

:::note
The XGBoost integration is experimental. Enable it with the `allow_experimental_xgboost` setting before creating an `XGBOOST` dictionary or calling `predictXGBoost`:

```sql
SET allow_experimental_xgboost = 1;
```
:::

You query the dictionary in one of two ways:

- [`predictXGBoost`](/sql-reference/functions/machine-learning-functions#predictxgboost) takes the features as individual arguments and returns the prediction, accepts additional [prediction parameters] (#prediction-parameters).
- A plain [`dictGet`](/sql-reference/functions/ext-dict-functions#dictget) for the target attribute predicts too, taking the feature vector as the key (see [Notes](#notes)). This method doesn't accept additional prediction parameters, if those are necessary please use `predictXGBoost`.

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

The same result via `dictGet`, passing the feature vector as the key:

```sql
SELECT dictGet('model', 'y', (1.0, 2.0)) AS prediction;
```

## How it works {#how-it-works}

**Training (at load time).** Each source row is a `(features..., target)` observation. When the dictionary loads, all rows are streamed into XGBoost and the model is trained once. Feature and target values are read as floats, so all key and attribute columns must be numeric.

**Predicting (at query time).** To predict, the model takes the feature vector — in the same order as the key columns were declared — and runs it through the trained booster, returning a `Float64`.

**Persisting the trained model.** The trained model is persisted automatically. On the first load the model is trained and written to a file that ClickHouse names after the dictionary's own identity, in a server-owned directory; every later load — including after a server restart — reuses that file and skips training. The path is not configurable: because the file name is unique to the dictionary, a dictionary can only ever reuse a model it trained itself and can never load one trained by a different dictionary.

**Retraining the model.** Because reuse is keyed to the dictionary's identity, `SYSTEM RELOAD DICTIONARY` reuses the persisted model rather than retraining. To retrain after the training data changes, drop and recreate the dictionary — a recreated dictionary has a fresh identity, so it trains from the current source:

```sql
INSERT INTO training_data VALUES (5, 10, 40);
DROP DICTIONARY model;
CREATE DICTIONARY model (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_data'))
LAYOUT(XGBOOST(objective 'reg:squarederror' num_iterations 100 max_depth 6))
LIFETIME(0);
```

:::note
`LIFETIME` does not cause retraining. A lifetime-triggered reload reuses the persisted model just like `SYSTEM RELOAD DICTIONARY`, even if the source table has changed, so a non-zero `LIFETIME` will not refresh the model as the training data grows. Use `LIFETIME(0)` and retrain by dropping and recreating the dictionary.
:::

## Dictionary structure {#dictionary-structure}

An `XGBOOST` dictionary has a fixed shape:

- The `PRIMARY KEY` is one or more numeric columns — the features. At query time this "key" is the feature vector you pass in to predict, not a stored lookup key. The feature order is the key-column declaration order, and `predictXGBoost` binds its positional arguments to that order.
- Alongside them, declare **exactly one numeric attribute**: the target the model learns. It is always inferred as the single column that is not part of the feature key — there is no parameter to name it, and it is an error to declare more than one attribute.

All key and attribute columns must be a native numeric type (integers and floats); a non-numeric column is rejected when the dictionary loads, not when you create it.

## Layout parameters {#layout-parameters}

Only the parameters listed below are accepted; any other name fails the load, so typos are caught when the model trains rather than being silently ignored. `num_iterations` is handled by ClickHouse (see its description); every other parameter is forwarded to the XGBoost booster unchanged, as a string, and takes XGBoost's own default and value range — see the [XGBoost parameter reference](https://xgboost.readthedocs.io/en/stable/parameter.html).

| Parameter | Description |
| --- | --- |
| `num_iterations` | Number of boosting rounds (how many trees to train). A positive integer, used as the training loop count rather than forwarded to the booster. Default `100`. |
| `booster` | Booster type: `gbtree`, `gblinear`, or `dart`. |
| `objective` | Learning objective, e.g. `reg:squarederror`, `binary:logistic`, `multi:softmax`. |
| `eval_metric` | Evaluation metric(s) used during training. |
| `seed` | Random number seed. |
| `verbosity` | Logging verbosity: `0` (silent) to `3` (debug). |
| `nthread` | Number of parallel threads used for training. |
| `eta` / `learning_rate` | Step-size shrinkage applied after each boosting round (aliases). |
| `gamma` | Minimum loss reduction required to make a further split on a leaf. |
| `max_depth` | Maximum depth of a tree. |
| `min_child_weight` | Minimum sum of instance weight (hessian) needed in a child. |
| `max_delta_step` | Maximum delta step allowed for each leaf's output. |
| `subsample` | Fraction of the training rows sampled for each boosting round. |
| `sampling_method` | Row sampling method: `uniform` or `gradient_based`. |
| `colsample_bytree` / `colsample_bylevel` / `colsample_bynode` | Fraction of columns (features) sampled per tree / per level / per split. |
| `lambda` / `reg_lambda` | L2 regularization term on weights (aliases). |
| `alpha` / `reg_alpha` | L1 regularization term on weights (aliases). |
| `tree_method` | Tree construction algorithm: `auto`, `exact`, `approx`, or `hist`. |
| `scale_pos_weight` | Balances positive and negative weights, useful for imbalanced classes. |
| `grow_policy` | How new nodes are added to the tree: `depthwise` or `lossguide`. |
| `max_leaves` | Maximum number of leaf nodes (used with `grow_policy` `lossguide`). |
| `max_bin` | Maximum number of discrete bins used to bucket continuous features (used with `tree_method` `hist`). |
| `num_parallel_tree` | Number of trees grown per boosting round (a value `> 1` trains a boosted random forest). |

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
LIFETIME(3600);
```

## Prediction parameters {#prediction-parameters}

`predictXGBoost` accepts an optional trailing constant `Map` of XGBoost prediction parameters, after the features, built with `map`:

```sql
SELECT predictXGBoost('model', 1.0, 2.0, map('type', 0, 'iteration_end', 0));
```

The parameter names map to the prediction parameters of XGBoost's `XGBoosterPredictFromDMatrix`. Only the keys below are accepted; any other key fails the query. Values are numeric — `strict_shape` is given as `0` or `1`.

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

- **Computational dictionary semantics.** This is a *computational* dictionary: `dictGet(dict, '<target>', (feature_1, ...))` predicts for the given feature vector (the key is the input to predict, not a stored key), and `dictHas` always returns `1`. The dictionary cannot be read back as a table with `SELECT * FROM dict`.
- **Numeric columns only.** Every feature (key) column and the target attribute must be a native numeric type. Values are read as floats during training and prediction.
- **Feature order matters.** `predictXGBoost` binds its positional feature arguments to the key columns in declaration order, and the number of feature arguments must match the number of key columns.
