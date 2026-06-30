---
slug: /sql-reference/statements/create/dictionary/layouts/naive-bayes
title: 'Naive Bayes dictionaries'
sidebar_label: 'Naive Bayes'
sidebar_position: 13
description: 'Configure NAIVE_BAYES dictionaries for text classification.'
doc_type: 'reference'
---

The `naive_bayes` (`NAIVE_BAYES`) dictionary classifies text with a multinomial [Naive Bayes](https://en.wikipedia.org/wiki/Naive_Bayes_classifier) model (the standard event model for text, scoring each class by how often the input's n-grams occur in it). It is backed by a table of pre-aggregated, per-class **n-gram counts**; it compiles that table into a model once, at load time, and then classifies any input text into one of the classes.

It is suited to fast, lightweight text classification such as sentiment analysis, topic or spam labelling, and language or script detection.

You query the dictionary with the [`naiveBayesClassifier`](/sql-reference/functions/machine-learning-functions#naivebayesclassifier) functions, which return the predicted class id and, optionally, class probabilities. A plain [`dictGet`](/sql-reference/functions/ext-dict-functions#dictget) classifies as well (see [Notes](#notes)).

## How it works {#how-it-works}

**Training (at load time).** Each source row is a `(n-gram, class, count)` observation. When the dictionary loads, the rows are compiled once into the model. Duplicate `(n-gram, class)` rows are summed, and rows with `count = 0` are ignored.

**Classifying (at query time).** To classify a string, the model splits it into n-grams according to `mode` and `n` (see [Tokenization modes](#tokenization-modes)), then scores each class by combining the class prior with how often the input's n-grams were seen in that class. The `alpha` smoothing factor keeps an n-gram that was seen in training but not in a given class from ruling that class out. An n-gram that was not seen in training at all is ignored — it is not part of the model's vocabulary, so it contributes to no class (as in standard multinomial Naive Bayes). The class with the highest score is the prediction; the probability functions turn the scores into probabilities that sum to `1.0`. These are normalized model posteriors and, like most Naive Bayes probabilities, may be poorly calibrated.

For example, in the [quickstart](#quickstart) below `'good great love'` is classified as `0` because each of those words was seen far more often in class `0`'s counts than in class `1`'s, so class `0` scores much higher.

The algorithm follows the multinomial Naive Bayes model for text classification; see [Manning, Raghavan & Schütze, *Introduction to Information Retrieval*, ch. 13 (*Text Classification and Naive Bayes*)](https://nlp.stanford.edu/IR-book/html/htmledition/text-classification-and-naive-bayes-1.html).

## Quickstart {#quickstart}

A complete, runnable token-mode example. The `sentiment` dictionary built here is reused throughout this page.

**1. Create a source table** of per-class n-gram counts:

```sql
CREATE TABLE sentiment_ngrams (class_id UInt32, ngram String, count UInt64)
ENGINE = MergeTree ORDER BY (class_id, ngram);
```

**2. Insert training data** — here single words (unigrams) with how often each occurs in the positive (`0`) and negative (`1`) class:

```sql
INSERT INTO sentiment_ngrams VALUES
    (0,'good',10),(0,'great',8),(0,'excellent',6),(0,'love',7),(0,'happy',5),
    (0,'amazing',4),(0,'wonderful',3),(0,'best',3),(0,'fantastic',2),(0,'nice',4),
    (1,'bad',10),(1,'terrible',8),(1,'awful',6),(1,'hate',7),(1,'worst',5),
    (1,'horrible',4),(1,'poor',3),(1,'disappointing',3),(1,'ugly',2),(1,'sad',4);
```

**3. Create the dictionary** with the `NAIVE_BAYES` layout:

```sql
CREATE DICTIONARY sentiment (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'sentiment_ngrams'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 1.0))
LIFETIME(0);
```

**4. Classify** — `naiveBayesClassifier` returns the class id:

```sql
SELECT naiveBayesClassifier('sentiment', 'this is great');
```

```response
0
```

```sql
SELECT naiveBayesClassifier('sentiment', 'this is terrible');
```

```response
1
```

The same result via `dictGet`:

```sql
SELECT dictGet('sentiment', 'class_id', 'this is great');
```

```response
0
```

Get the probability of the prediction, or of every class (rounded here for readability):

```sql
SELECT (r.1, round(r.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('sentiment', 'good great love') AS r);
```

```response
(0,0.9987)
```

```sql
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('sentiment', 'good great love'));
```

```response
[(0,0.9987),(1,0.0013)]
```

## Dictionary structure {#dictionary-structure}

A `NAIVE_BAYES` dictionary has a fixed shape:

- The `PRIMARY KEY` is a single `String` column — the n-gram. At query time this "key" is the text you pass in to classify, not a stored lookup key.
- Alongside it, declare **exactly two unsigned-integer attributes**: the class label and the occurrence count. Class ids are `UInt32` throughout — the model stores them as `UInt32` and the [`naiveBayesClassifier`](/sql-reference/functions/machine-learning-functions#naivebayesclassifier) functions return `UInt32` — so a class label must fit in `UInt32` (at most `4294967295`) even when its attribute is declared `UInt64`. A source row whose class id is larger is rejected when the dictionary loads, not when it is created.
- The `class_attribute` layout parameter names which attribute is the class label; the other is automatically the count. The two attributes can be declared in either order.

The source table holds **pre-aggregated** counts: one row per `(n-gram, class)` with how many times that n-gram was observed in that class. Producing those counts (tokenizing your corpus and grouping) is done by your training pipeline, or in ClickHouse itself from raw labelled text — see [Build training data from raw text](#build-training-data-from-raw-text); the dictionary only consumes them.

**Updating the model.** Because the model is a dictionary backed by a table, retrain by updating the table and reloading:

```sql
INSERT INTO sentiment_ngrams VALUES (0, 'awesome', 5);
SYSTEM RELOAD DICTIONARY sentiment;
```

## Build training data from raw text {#build-training-data-from-raw-text}

If you start from raw labelled text rather than pre-aggregated counts, the [`naiveBayesNgrams`](/sql-reference/functions/splitting-merging-functions#naivebayesngrams) function tokenizes text into exactly the n-grams this layout expects — the same `mode`, `n`, and boundary tokens — so the training data matches what the dictionary produces at query time. Pass it the same `n` / `mode` / `start_token` / `end_token` you use in the layout.

Given a table of `(class_id, text)` rows, build the `(ngram, class_id, count)` source with one `GROUP BY`:

```sql
CREATE TABLE docs (class_id UInt32, text String) ENGINE = MergeTree ORDER BY class_id;
INSERT INTO docs VALUES
    (0, 'good great wonderful'), (0, 'great good nice'),
    (1, 'bad terrible awful'), (1, 'terrible bad horrible');

CREATE TABLE training_data (ngram String, class_id UInt32, count UInt64)
ENGINE = MergeTree ORDER BY (class_id, ngram);

INSERT INTO training_data
SELECT ngram, class_id, count()
FROM docs
ARRAY JOIN naiveBayesNgrams(text, 1, 'token') AS ngram
GROUP BY ngram, class_id;
```

`training_data` is now a valid source for a `NAIVE_BAYES` dictionary (here token unigrams; change the `n` and `mode` arguments to match the layout). Do not normalize the text (for example with `lower`) unless you are prepared for query-time input — which the dictionary tokenizes as-is — to no longer match.

:::note Priors and document counts
The `proportional` prior (the default) is weighted by each class's **total n-gram count**, not by its number of documents. If you want the classic document-frequency prior (`documents_in_class / total_documents`), compute it from the raw `docs` table and pass it with `priors_mode 'explicit'`:

```sql
SELECT arrayStringConcat(
         groupArray(concat(toString(class_id), '=', toString(round(frac, 6)))), ',')
FROM (SELECT class_id, count() / sum(count()) OVER () AS frac FROM docs GROUP BY class_id);
```
:::

## Layout parameters {#layout-parameters}

| Parameter | Description | Example | Default |
| --- | --- | --- | --- |
| `class_attribute` | Name of the attribute that holds the class label; the other attribute is the count. | `'class_id'` | *Required* |
| `n` | N-gram size: `1` = unigrams, `2` = bigrams, `3` = trigrams, … (1–1024). | `2` | *Required* |
| `mode` | Tokenization method: `byte`, `codepoint`, or `token`. See [Tokenization modes](#tokenization-modes). | `'token'` | *Required* |
| `alpha` | Additive (Lidstone) smoothing for n-gram likelihoods; `alpha = 1` is Laplace smoothing (must be finite and `> 0`). | `0.5` | `1.0` |
| `priors_mode` | How class priors are determined: `uniform`, `proportional`, or `explicit`. See [Prior modes](#prior-modes). | `'uniform'` | `'proportional'` |
| `priors` | Explicit per-class priors; required only when `priors_mode` is `explicit`. Must sum to `1.0`. | `'0=0.6,1=0.4'` | — |
| `store_source` | Retain the source rows so `SELECT * FROM dictionary` works. Roughly doubles memory. | `1` | `0` |
| `start_token` | Boundary token prepended `(n-1)` times to the input. See [Boundary tokens](#boundary-tokens-padding). | `'0x01'` / `'<s>'` | — (no padding) |
| `end_token` | Boundary token appended `(n-1)` times to the input. | `'0xFF'` / `'</s>'` | — (no padding) |

The dictionary can be defined with `CREATE DICTIONARY ... LAYOUT(NAIVE_BAYES(...))` as shown here, or equivalently in an XML configuration file under `<layout><naive_bayes>...</naive_bayes></layout>`; see [Dictionary layouts](/sql-reference/statements/create/dictionary/layouts) for the configuration-file form. In a configuration file, `start_token` and `end_token` for `byte` and `codepoint` mode must be given as numbers (raw bytes cannot travel through the config), exactly as in the DDL form.

## Tokenization modes {#tokenization-modes}

`mode` decides what a "token" is, and therefore what the n-grams look like. The source n-grams must have been produced with the **same** `mode` and `n`.

- `byte` — each token is a single byte; no UTF-8 assumption. With `n = 2`, `'abc'` yields the byte bigrams `'ab'`, `'bc'`. *Good for* language or encoding detection on arbitrary byte sequences, and any data where sub-character signal matters. Usually paired with `n >= 2`.
- `codepoint` — each token is one Unicode code point; the input is interpreted as UTF-8. With `n = 1`, `'café'` yields the code points `'c'`, `'a'`, `'f'`, `'é'`. *Good for* script and language detection, and short or CJK text where whitespace word boundaries are unreliable. (Source n-grams must be valid UTF-8; query input is decoded leniently — see [Notes](#notes).)
- `token` — each token is a word delimited by **ASCII whitespace** (space, tab, newline, carriage return, form feed, vertical tab; runs collapse to one separator). Non-ASCII Unicode whitespace such as `U+00A0` (no-break space) or `U+2003` (em space) is **not** a separator and stays inside a token. With `n = 2`, `'a b c'` yields the word bigrams `'a b'`, `'b c'`. *Good for* word-level classification on space-separated languages — sentiment, topic, spam, language of a sentence. This is the most common choice.

## Prior modes {#prior-modes}

The prior is the model's belief about each class *before* it looks at the text. `priors_mode` chooses how it is set.

- `proportional` (default) — each class's prior is proportional to its total n-gram count in the training data, so classes seen more often are more likely a priori. **Choose it** when the training class proportions (by total n-gram count) match the frequencies you expect at query time. **Nothing to supply** — it is derived from the source counts.

  ```sql
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'proportional'))
  ```

- `uniform` — every class is equally likely a priori. **Choose it** when the classes are balanced, or when training frequencies are not representative of query time, so the prediction depends only on the text. **Nothing to supply.**

  ```sql
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'uniform'))
  ```

  (With the balanced `sentiment` data both classes have the same total count, so `proportional` and `uniform` coincide there; they differ only when classes have different totals.)

- `explicit` — you provide the priors with `priors '0=0.6,1=0.4'`: one `class=probability` entry per class, each in `(0, 1]`, together summing to `1.0`. **Choose it** when you know the real base rates and they differ from training — e.g. only 1% of production traffic is spam even though the training set was balanced. **Compute them** from the expected real-world share of each class.

  ```sql
  CREATE DICTIONARY sentiment_explicit (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
  PRIMARY KEY ngram
  SOURCE(CLICKHOUSE(TABLE 'sentiment_ngrams'))
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors '0=0.9,1=0.1'))
  LIFETIME(0);
  ```

  A strong prior visibly pulls a weak prediction. With the default priors, the single word `'bad'` is class `1` with high confidence; a `0.9` prior on class `0` drags it down to a near tie:

  ```sql
  SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('sentiment', 'bad'));          -- default
  SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('sentiment_explicit', 'bad')); -- explicit
  ```

  ```response
  [(1,0.9167),(0,0.0833)]
  [(1,0.55),(0,0.45)]
  ```

## Boundary tokens (padding) {#boundary-tokens-padding}

Padding is off by default; most models do not need it. It only matters for `n > 1`.

**Why it helps.** With `n > 1`, n-grams in the middle of the text get full left and right context, but the first and last tokens do not. Adding boundary tokens creates n-grams that mark "start of text" and "end of text", turning position-sensitive signals into features the model can learn — for example a word that is distinctive when it *begins* a message, or a character typical at a word's *end*.

**What you must do:**

1. **Decide per side.** `start_token` and `end_token` are independent — set one, both, or neither. An empty value means that side is not padded.
2. **Choose rare values** that will not collide with real data, e.g. `0x01` / `0xFF` for `byte`, `U+10FFFE` / `U+10FFFF` for `codepoint`, or `<s>` / `</s>` for `token`.
3. **Produce the training n-grams with the same padding.** The dictionary consumes pre-aggregated n-grams and cannot add padding itself, so the convention is shared between your training pipeline and the layout.

The token format depends on the mode: for `byte` and `codepoint` give a **number** (decimal or `0x` hex, resolved to the byte / UTF-8 code point); for `token` give the **literal** token string.

Padding can change the prediction. The training rows below place `x` at the start of class `0` and at the end of class `0`, but `xy` only in class `1`. Without padding, the input `'xy'` matches class `1`; with `0x01`/`0xFF` padding it becomes `<start>xy<end>`, whose boundary bigrams now point to class `0`:

```sql
CREATE TABLE pad_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO pad_src VALUES (0, '\x01x', 9), (1, 'xy', 9), (0, 'y\xFF', 9);

CREATE DICTIONARY pad_nopad (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'pad_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte')) LIFETIME(0);

CREATE DICTIONARY pad_padded (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'pad_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '0x01' end_token '0xFF')) LIFETIME(0);

SELECT naiveBayesClassifier('pad_nopad', 'xy'), naiveBayesClassifier('pad_padded', 'xy');
```

```response
1 0
```

(The decimal forms `start_token '1' end_token '255'` are equivalent to `'0x01'`/`'0xFF'`.)

## More examples {#more-examples}

**Byte mode** — byte bigrams (`n = 2`, `mode 'byte'`):

```sql
CREATE TABLE charset_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO charset_src VALUES (0,'ab',5),(0,'bc',5),(0,'cd',5),(1,'xy',5),(1,'yz',5),(1,'zw',5);

CREATE DICTIONARY charset (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'charset_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte')) LIFETIME(0);

SELECT naiveBayesClassifier('charset', 'abcd'), naiveBayesClassifier('charset', 'xyzw');
```

```response
0 1
```

**Code-point mode** — per-character script detection (`n = 1`, `mode 'codepoint'`; class `0` = Latin, `1` = Cyrillic):

```sql
CREATE TABLE script_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO script_src VALUES (0,'a',5),(0,'b',5),(0,'c',5),(0,'d',5),(1,'а',5),(1,'б',5),(1,'в',5),(1,'г',5);

CREATE DICTIONARY script (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'script_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'codepoint')) LIFETIME(0);

SELECT naiveBayesClassifier('script', 'abcd'), naiveBayesClassifier('script', 'абвг');
```

```response
0 1
```

**Read the training data back** with `store_source`:

```sql
CREATE TABLE stored_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO stored_src VALUES (0,'alpha',3),(0,'beta',2),(1,'gamma',4);

CREATE DICTIONARY stored (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'stored_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' store_source 1)) LIFETIME(0);

SELECT ngram, class_id, count FROM stored ORDER BY ngram;
```

```response
alpha 0 3
beta 0 2
gamma 1 4
```

**Classify a whole column** — the functions work directly over a table:

```sql
CREATE TABLE reviews (text String) ENGINE = Memory;
INSERT INTO reviews VALUES ('good great love'),('bad terrible hate'),('excellent wonderful'),('awful worst');

SELECT text, naiveBayesClassifier('sentiment', text) AS class FROM reviews ORDER BY text;
```

```response
awful worst 1
bad terrible hate 1
excellent wonderful 0
good great love 0
```

**Empty input** is classified from the priors alone (with the balanced `sentiment` data, that is an even split):

```sql
SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('sentiment', ''));
```

```response
[(0,0.5),(1,0.5)]
```

## Notes {#notes}

- **Computational dictionary semantics.** This is a *computational* dictionary: `dictGet(dict, '<class_attribute>', text)` classifies `text` (the key is an input to classify, not a stored key), the count attribute is not queryable, and `dictHas` always returns `1`.
- **Source validation at load.** Every source n-gram must match the configured `n` and `mode` (in `codepoint` mode it must also be valid UTF-8); a mismatch fails the load. A source whose every count is zero, or that is empty, also fails to load.
- **Query-time tokenization is lenient.** Unlike source validation, query input is never rejected. In `codepoint` mode, bytes that are not valid UTF-8 are decoded on a best-effort basis instead of failing the query; in `token` mode, only ASCII whitespace separates words (Unicode whitespace such as `U+00A0` stays inside a token). Malformed input still classifies — typically from the priors, since its n-grams will not match the trained ones.
