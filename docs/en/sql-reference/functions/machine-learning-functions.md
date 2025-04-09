---
description: 'Documentation for Machine Learning Functions'
sidebar_label: 'Machine Learning'
sidebar_position: 115
slug: /sql-reference/functions/machine-learning-functions
title: 'Machine Learning Functions'
---

# Machine Learning Functions

## evalMLMethod {#evalmlmethod}

Prediction using fitted regression models uses `evalMLMethod` function. See link in `linearRegression`.

## stochasticLinearRegression {#stochasticlinearregression}

The [stochasticLinearRegression](/sql-reference/aggregate-functions/reference/stochasticlinearregression) aggregate function implements stochastic gradient descent method using linear model and MSE loss function. Uses `evalMLMethod` to predict on new data.

## stochasticLogisticRegression {#stochasticlogisticregression}

The [stochasticLogisticRegression](/sql-reference/aggregate-functions/reference/stochasticlogisticregression) aggregate function implements stochastic gradient descent method for binary classification problem. Uses `evalMLMethod` to predict on new data.

## naiveBayesClassifier {#naivebayesclassifier}

The function loads language models based on n-grams. Given a model name and input text, it returns the most likely class using a Naive Bayes classifier with Laplace smoothing.

**Syntax**

```sql
naiveBayesClassifier(model_name, input)
```

**Arguments**

- `model_name` — The name of the model to use. [String](../data-types/string.md)
- `input` — The input text that will be classified. [String](../data-types/string.md)

**Implementation Details**

The algorithm for building and classifying text using Naive Bayes is based on [this](https://web.stanford.edu/~jurafsky/slp3/4.pdf). and has been extended to support n-grams for n > 1.


**Returned value**

- Returns the predicted class identifier as an unsigned integer. [UInt32](../data-types/int-uint.md)

**Example**

Query:

```sql
SELECT naiveBayesClassifier('language', 'How are you?');
```

Result:

```response
┌─naiveBayesClassifier('language', 'How are you?')─┐
│                              0                   │
└──────────────────────────────────────────────────┘
```

**Configuration**

Currently, ClickHouse does not include preloaded models for this function. Users must create the model manually and add its details via configuration files.


| Property        | Description                                                                                                                                                                                         | Example                                                   | Default Value                                                   |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- | --------------------------------------------------------------- |
| **name**        | A unique identifier for the model; used in place of the `model_name` parameter.                                                                                                                     | sentiment                                                 | (Required)                                                      |
| **path**        | The file path to the serialized binary model.                                                                                                                                                       | `/etc/clickhouse-server/config.d/sentiment.bin`           | (Required)                                                      |
| **n**           | The n-gram size used for tokenization. A value of `1` means each word is treated as an individual n-gram; a value of `2` forms n-grams from consecutive word pairs.                                 | 1                                                         | (Required)                                                      |
| **alpha**       | The Laplace smoothing parameter applied during classification to address n-grams that do not appear in the model.                                                                                   | 1.0                                                       | 1.0                                                             |
| **start_token** | The token inserted at the beginning of the input text to capture sentence boundaries. `(n - 1)` start tokens are prepended. The corpus documents should also include these tokens at the beginning. | `<s>` (XML-escaped as `&lt;s&gt;`)                        | `<s>` (XML-escaped as `&lt;s&gt;`)                              |
| **end_token**   | The token inserted at the end of the input text to capture sentence boundaries. `(n - 1)` end tokens are appended. The corpus documents should also include these tokens at the end.                | `</s>` (XML-escaped as `&lt;/s&gt;`)                      | `</s>` (XML-escaped as `&lt;/s&gt;`)                            |
| **priors**      | A collection of prior probabilities for each class. Each entry specifies a **class** (identifier) and a **value** (probability).                                                                    | Class `0` with value `0.4` and Class `1` with value `0.6` | (If not provided, equal probability is assumed for each class.) |


**Model Format**

```text
<class_id> <ngram> <count>
```

- **class_id**: The unsigned integer identifier for a class. For example, if there are two classes (e.g., positive and negative), you might designate `0` for positive and `1` for negative.
- **ngram**: The n-gram string derived from the training data. For instance, if `n = 2`, possible n-grams include "I am" and "am happy".
- **count**: The number of times the n-gram appears in the training data for that class. For example, if "I am happy" appears 5 times for class `0`, the line would be:

```text
0 I am happy 5
```

The model file contains these tuples serialized in binary form, stored consecutively.

This [repository](https://github.com/nihalzp/ch-nb-accuracy-test) provides reference scripts that may be used to create the model file.

```xml
<clickhouse>
    <nb_models>
        <model>
            <name>sentiment</name>
            <path>/etc/clickhouse-server/config.d/sentiment_model.bin</path>
            <n>1</n>
            <alpha>1.0</alpha>
            <start_token>&lt;s&gt;</start_token>
            <end_token>&lt;/s&gt;</end_token>
            <priors>
                <prior>
                    <class>0</class>
                    <value>0.55</value>
                </prior>
                <prior>
                    <class>1</class>
                    <value>0.45</value>
                </prior>
            </priors>
        </model>
    </nb_models>
</clickhouse>
```
