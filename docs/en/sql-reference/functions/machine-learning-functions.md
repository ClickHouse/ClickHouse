---
description: 'Documentation for Machine Learning Functions'
sidebar_label: 'Machine Learning'
slug: /sql-reference/functions/machine-learning-functions
title: 'Machine Learning Functions'
doc_type: 'reference'
---

# Machine learning functions

## evalMLMethod {#evalmlmethod}

Prediction using fitted regression models uses `evalMLMethod` function. See link in `linearRegression`.

## stochasticLinearRegression {#stochasticlinearregression}

The [stochasticLinearRegression](/sql-reference/aggregate-functions/reference/stochasticlinearregression) aggregate function implements stochastic gradient descent method using linear model and MSE loss function. Uses `evalMLMethod` to predict on new data.

## stochasticLogisticRegression {#stochasticlogisticregression}

The [stochasticLogisticRegression](/sql-reference/aggregate-functions/reference/stochasticlogisticregression) aggregate function implements stochastic gradient descent method for binary classification problem. Uses `evalMLMethod` to predict on new data.

## naiveBayesClassifier {#naivebayesclassifier}

Classifies input text using a Naive Bayes model with n-grams and Laplace smoothing. The model must be configured in ClickHouse before use.

**Syntax**

```sql
naiveBayesClassifier(model_name, input_text);
```

**Arguments**

- `model_name` — Name of the pre-configured model. [String](../data-types/string.md)
  The model must be defined in ClickHouse's configuration files (see below).
- `input_text` — Text to classify. [String](../data-types/string.md)
  Input is processed exactly as provided (case/punctuation preserved).

**Returned Value**
- Predicted class ID as an unsigned integer. [UInt32](../data-types/int-uint.md)
  Class IDs correspond to categories defined during model construction.

**Example**

Classify text with a language detection model:
```sql
SELECT naiveBayesClassifier('language', 'How are you?');
```
```response
┌─naiveBayesClassifier('language', 'How are you?')─┐
│ 0                                                │
└──────────────────────────────────────────────────┘
```
*Result `0` might represent English, while `1` could indicate French - class meanings depend on your training data.*

---

### Implementation Details {#implementation-details}

**Algorithm**
Uses Naive Bayes classification algorithm with [Laplace smoothing](https://en.wikipedia.org/wiki/Additive_smoothing) to handle unseen n-grams based on n-gram probabilities based on [this](https://web.stanford.edu/~jurafsky/slp3/4.pdf).

**Key Features**
- Supports n-grams of any size
- Three tokenization modes:
  - `byte`: Operates on raw bytes. Each byte is one token.
  - `codepoint`: Operates on Unicode scalar values decoded from UTF‑8. Each codepoint is one token.
  - `token`: Splits on runs of Unicode whitespace (regex \s+). Tokens are substrings of non‑whitespace; punctuation is part of the token if adjacent (e.g., "you?" is one token).

---

### Model Configuration {#model-configuration}

You can find sample source code for creating a Naive Bayes model for language detection [here](https://github.com/nihalzp/ClickHouse-NaiveBayesClassifier-Models).

Additionally, sample models and their associated config files are available [here](https://github.com/nihalzp/ClickHouse-NaiveBayesClassifier-Models/tree/main/models).

Here is an example configuration for a naive Bayes model in ClickHouse:

```xml
<clickhouse>
    <nb_models>
        <model>
            <name>sentiment</name>
            <path>/etc/clickhouse-server/config.d/sentiment.bin</path>
            <n>2</n>
            <mode>token</mode>
            <alpha>1.0</alpha>
            <priors>
                <prior>
                    <class>0</class>
                    <value>0.6</value>
                </prior>
                <prior>
                    <class>1</class>
                    <value>0.4</value>
                </prior>
            </priors>
        </model>
    </nb_models>
</clickhouse>
```

**Configuration Parameters**

| Parameter  | Description                                                                                                     | Example                                                  | Default            |
| ---------- | --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- | ------------------ |
| **name**   | Unique model identifier                                                                                         | `language_detection`                                     | *Required*         |
| **path**   | Full path to model binary                                                                                       | `/etc/clickhouse-server/config.d/language_detection.bin` | *Required*         |
| **mode**   | Tokenization method:<br/>- `byte`: Byte sequences<br/>- `codepoint`: Unicode characters<br/>- `token`: Word tokens | `token`                                                  | *Required*         |
| **n**      | N-gram size (`token` mode):<br/>- `1`=single word<br/>- `2`=word pairs<br/>- `3`=word triplets                     | `2`                                                      | *Required*         |
| **alpha**  | Laplace smoothing factor used during classification to address n-grams that do not appear in the model          | `0.5`                                                    | `1.0`              |
| **priors** | Class probabilities (% of the documents belonging to a class)                                                                             | 60% class 0, 40% class 1                                 | Equal distribution |

**Model Training Guide**

**File Format**
In human-readable format, for `n=1` and `token` mode, the model might look like this:
```text
<class_id> <n-gram> <count>
0 excellent 15
1 refund 28
```

For `n=3` and `codepoint` mode, it might look like:
```text
<class_id> <n-gram> <count>
0 exc 15
1 ref 28
```

Human-readable format is not used by ClickHouse directly; it must be converted to the binary format described below.

**Binary Format Details**
Each n-gram stored as:
1. 4-byte `class_id` (UInt, little-endian)
2. 4-byte `n-gram` bytes length (UInt, little-endian)
3. Raw `n-gram` bytes
4. 4-byte `count` (UInt, little-endian)

**Preprocessing Requirements**
Before the model is being created from the document corpus, the documents must be preprocessed to extract n-grams according to the specified `mode` and `n`. The following steps outline the preprocessing:
1. **Add boundary markers at the start and end of each document based on tokenization mode:**
   - **Byte**: `0x01` (start), `0xFF` (end)
   - **Codepoint**: `U+10FFFE` (start), `U+10FFFF` (end)
   - **Token**: `<s>` (start), `</s>` (end)

   *Note:* `(n - 1)` tokens are added at both the beginning and the end of the document.

2. **Example for `n=3` in `token` mode:**

   - **Document:** `"ClickHouse is fast"`
   - **Processed as:** `<s> <s> ClickHouse is fast </s> </s>`
   - **Generated trigrams:**
     - `<s> <s> ClickHouse`
     - `<s> ClickHouse is`
     - `ClickHouse is fast`
     - `is fast </s>`
     - `fast </s> </s>`


To simplify model creation for `byte` and `codepoint` modes, it may be convenient to first tokenize the document into tokens (a list of `byte`s for `byte` mode and a list of `codepoint`s for `codepoint` mode). Then, append `n - 1` start tokens at the beginning and `n - 1` end tokens at the end of the document. Finally, generate the n-grams and write them to the serialized file.

---

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->