---
toc_priority: 67
toc_title: NLP
---

# Natural Language Processing functions {#nlp-functions}

<!-- ## normalize {#normalize}

Performs normalization on a given text.

**Syntax**

``` sql
normalize('language', text)
```

**Arguments**

-   `language` — Language which rules will be applied. [String](../../sql-reference/data-types/string.md#string).
-   `text` — Text that needs to be normalized. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Query:

``` sql
SELECT normalize("I think it's a blessing in disguise.");
```

Result:

``` text
┌─normalize("I think it's a blessing in disguise.")─┐
│           "I think it is a blessing in disguise." │
└───────────────────────────────────────────────────┘
``` -->

## tokenize {#tokenize}

Performs tokenization on a given text.

**Syntax**

``` sql
tokenize(text)
```

**Arguments**

-   `text` — Text that needs to be tokenized. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Query:

``` sql
SELECT tokenize('I think it is a blessing in disguise.') as res;
```

Result:

``` text
┌─res────────────────────────────────────────────────────┐
│ ['I','think','it','is','a','blessing','in','disguise'] │
└────────────────────────────────────────────────────────┘
```

## stem {#stem}

Performs stemming on a previously tokenized text.

**Syntax**

``` sql
stem('language', word)
```

**Arguments**

-   `language` — Language which rules will be applied. [String](../../sql-reference/data-types/string.md#string).
-   `word` — word that needs to be stemmed (in lowercase). [String](../../sql-reference/data-types/string.md#string).

**Examples**

Query:

``` sql
SELECT SELECT arrayMap(x -> stem('en', x), ['I', 'think', 'it', 'is', 'a', 'blessing', 'in', 'disguise']) as res;
```

Result:

``` text
┌─res────────────────────────────────────────────────┐
│ ['I','think','it','is','a','bless','in','disguis'] │
└────────────────────────────────────────────────────┘
```

<!-- ## lemmatize {#lemmatize}

Performs lemmatization on a given word.

**Syntax**

``` sql
lemmatize('language', word)
```

**Arguments**

-   `language` — Language which rules will be applied. [String](../../sql-reference/data-types/string.md#string).
-   `word` — Text thats need to be normalized. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Query:

``` sql
SELECT normalize("It can't be true!");
```

Result:

``` text
┌─normalize("It can't be true!")─┐
│           "It cannot be true!" │
└────────────────────────────────┘
```

## isStopWord {#isstopword}
 -->
