---
description: 'Documentation for Machine Learning Functions'
sidebar_label: 'Machine Learning'
slug: /sql-reference/functions/machine-learning-functions
title: 'Machine Learning Functions'
doc_type: 'reference'
---

## evalMLMethod {#evalmlmethod}

Prediction using fitted regression models uses `evalMLMethod` function. See link in `linearRegression`.

## stochasticLinearRegression {#stochasticlinearregression}

The [stochasticLinearRegression](/sql-reference/aggregate-functions/reference/stochasticlinearregression) aggregate function implements stochastic gradient descent method using linear model and MSE loss function. Uses `evalMLMethod` to predict on new data.

## stochasticLogisticRegression {#stochasticlogisticregression}

The [stochasticLogisticRegression](/sql-reference/aggregate-functions/reference/stochasticlogisticregression) aggregate function implements stochastic gradient descent method for binary classification problem. Uses `evalMLMethod` to predict on new data.

## naiveBayesClassifier {#naivebayesclassifier}

Classifies input text using a Naive Bayes classifier with n-grams and Laplace smoothing.

The model is stored as a dictionary with the `NAIVE_BAYES` layout, backed by a ClickHouse table containing n-gram counts.

**Syntax**

```sql
naiveBayesClassifier(dictionary_name, input_text)
```

This is equivalent to:
```sql
dictGet(dictionary_name, 'class_id', input_text)
```

**Arguments**

- `dictionary_name` — Name of a dictionary with `NAIVE_BAYES` layout. Must be a constant. [String](../data-types/string.md)
- `input_text` — Text to classify. [String](../data-types/string.md)
  Input is processed exactly as provided (case/punctuation preserved).

**Returned Value**
- Predicted class ID as an unsigned integer. [UInt32](../data-types/int-uint.md)
  Class IDs correspond to categories defined in the training data.

**Related functions**

Two companion functions share the same arguments and dictionary but return probabilities:

- `naiveBayesClassifierWithProb(dictionary_name, input_text)` — returns a `Tuple(class_id UInt32, probability Float64)` for the predicted (most probable) class.
- `naiveBayesClassifierAllProbs(dictionary_name, input_text)` — returns an `Array(Tuple(class_id UInt32, probability Float64))` of every class with its probability, sorted by probability descending.

Probabilities are normalized with a numerically stable softmax and sum to `1.0` across classes.

---

### Setup {#setup}

**Step 1: Create a source table** with n-gram counts:

```sql
CREATE TABLE sentiment_ngrams
(
    class_id UInt32,
    ngram String,
    count UInt64
) ENGINE = MergeTree ORDER BY (class_id, ngram);
```

**Step 2: Populate** with training data (n-gram counts per class):

```sql
INSERT INTO sentiment_ngrams VALUES
    (0, 'good', 10), (0, 'great', 8), (0, 'excellent', 6),
    (1, 'bad', 10), (1, 'terrible', 8), (1, 'awful', 6);
```

**Step 3: Create a dictionary** with the `NAIVE_BAYES` layout:

```sql
CREATE DICTIONARY sentiment_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'sentiment_ngrams'))
LAYOUT(NAIVE_BAYES(n 1 mode 'token' alpha 1.0))
LIFETIME(0);
```

**Step 4: Classify:**

```sql
SELECT naiveBayesClassifier('sentiment_model', 'this is great');
```
```response
┌─naiveBayesClassifier('sentiment_model', 'this is great')─┐
│ 0                                                        │
└──────────────────────────────────────────────────────────┘
```

Or equivalently using `dictGet`:
```sql
SELECT dictGet('sentiment_model', 'class_id', 'this is great');
```

Text dominated by the negative class is classified as `1`:

```sql
SELECT naiveBayesClassifier('sentiment_model', 'this is terrible');
```
```response
┌─naiveBayesClassifier('sentiment_model', 'this is terrible')─┐
│ 1                                                           │
└─────────────────────────────────────────────────────────────┘
```

---

### Layout Parameters {#layout-parameters}

| Parameter        | Description | Example | Default |
| ---------------- | ----------- | ------- | ------- |
| **n**            | N-gram size. `1` = unigrams, `2` = bigrams, `3` = trigrams. | `2` | *Required* |
| **mode**         | Tokenization method: `byte` (raw bytes), `codepoint` (Unicode characters), or `token` (whitespace-delimited words). | `token` | *Required* |
| **alpha**        | Laplace smoothing factor for unseen n-grams. | `0.5` | `1.0` |
| **priors_mode**  | How class prior probabilities are determined: `uniform`, `proportional`, or `explicit`. See below. | `uniform` | `proportional` |
| **priors**       | Explicit class priors, required when `priors_mode` is `explicit`. Must sum to `1.0`. | `'0=0.6,1=0.4'` | — |
| **store_source** | Retain the source n-gram rows so `SELECT * FROM dictionary` works. Doubles memory. | `1` | `0` |

**Prior modes:**
- `proportional` (default) — each class's prior is proportional to its total n-gram count in the training data, i.e. classes seen more often are more likely a priori. Use this when the training class frequencies reflect the real-world frequencies you expect at query time.
- `uniform` — equal probability across all classes. Use this when classes are balanced, or when the training frequencies are not representative of query-time frequencies (so that the prediction depends only on the text).
- `explicit` — probabilities given via the `priors` parameter, e.g. `priors '0=0.6,1=0.4'` (one entry per class, summing to `1.0`).

---

### Implementation Details {#implementation-details}

**Algorithm**
Uses Naive Bayes classification with [Laplace smoothing](https://en.wikipedia.org/wiki/Additive_smoothing) based on n-gram probabilities per [Jurafsky & Martin, Chapter 4](https://web.stanford.edu/~jurafsky/slp3/4.pdf).

**Tokenization modes:**
- `byte`: Each byte is one token. Boundary markers: `0x01` (start), `0xFF` (end).
- `codepoint`: Each Unicode scalar value is one token. Boundary markers: `U+10FFFE` (start), `U+10FFFF` (end).
- `token`: Whitespace-delimited words. Boundary markers: `<s>` (start), `</s>` (end).

For n > 1, the classifier pads the input with `(n - 1)` boundary tokens at each end before extracting n-grams.

**Source table format:**
The source table must have three columns matching the dictionary structure:
- `class_id` (`UInt32`) — the class label
- `ngram` (`String`) — the n-gram text
- `count` (`UInt64`) — occurrence count

**Updating models:**
Since the model is a dictionary backed by a table, you can update the training data and reload:
```sql
INSERT INTO sentiment_ngrams VALUES (0, 'awesome', 5);
SYSTEM RELOAD DICTIONARY sentiment_model;
```

**Dictionary semantics:**
This is a *computational* dictionary, so its lookup interface behaves accordingly:
- `dictGet(dict, 'class_id', text)` classifies `text` (the key is an input to classify, not a stored key). Other attributes are not queryable.
- `dictHas` always returns `1` — any text is classifiable.

---

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->