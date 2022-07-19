---
sidebar_position: 67
sidebar_label: NLP
---

# [experimental] Natural Language Processing functions

:::warning    
This is an experimental feature that is currently in development and is not ready for general use. It will change in unpredictable backwards-incompatible ways in future releases. Set `allow_experimental_nlp_functions = 1` to enable it.
:::

## stem

Performs stemming on a given word.

**Syntax**

``` sql
stem('language', word)
```

**Arguments**

-   `language` — Language which rules will be applied. Must be in lowercase. [String](../../sql-reference/data-types/string.md#string).
-   `word` — word that needs to be stemmed. Must be in lowercase. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Query:

``` sql
SELECT arrayMap(x -> stem('en', x), ['I', 'think', 'it', 'is', 'a', 'blessing', 'in', 'disguise']) as res;
```

Result:

``` text
┌─res────────────────────────────────────────────────┐
│ ['I','think','it','is','a','bless','in','disguis'] │
└────────────────────────────────────────────────────┘
```

## lemmatize

Performs lemmatization on a given word. Needs dictionaries to operate, which can be obtained [here](https://github.com/vpodpecan/lemmagen3/tree/master/src/lemmagen3/models).

**Syntax**

``` sql
lemmatize('language', word)
```

**Arguments**

-   `language` — Language which rules will be applied. [String](../../sql-reference/data-types/string.md#string).
-   `word` — Word that needs to be lemmatized. Must be lowercase. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Query:

``` sql
SELECT lemmatize('en', 'wolves');
```

Result:

``` text
┌─lemmatize("wolves")─┐
│              "wolf" │
└─────────────────────┘
```

Configuration:
``` xml
<lemmatizers>
    <lemmatizer>
        <lang>en</lang>
        <path>en.bin</path>
    </lemmatizer>
</lemmatizers>
```

## synonyms

Finds synonyms to a given word. There are two types of synonym extensions: `plain` and `wordnet`.

With the `plain` extension type we need to provide a path to a simple text file, where each line corresponds to a certain synonym set. Words in this line must be separated with space or tab characters.

With the `wordnet` extension type we need to provide a path to a directory with WordNet thesaurus in it. Thesaurus must contain a WordNet sense index.

**Syntax**

``` sql
synonyms('extension_name', word)
```

**Arguments**

-   `extension_name` — Name of the extension in which search will be performed. [String](../../sql-reference/data-types/string.md#string).
-   `word` — Word that will be searched in extension. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Query:

``` sql
SELECT synonyms('list', 'important');
```

Result:

``` text
┌─synonyms('list', 'important')────────────┐
│ ['important','big','critical','crucial'] │
└──────────────────────────────────────────┘
```

Configuration:
``` xml
<synonyms_extensions>
    <extension>
        <name>en</name>
        <type>plain</type>
        <path>en.txt</path>
    </extension>
    <extension>
        <name>en</name>
        <type>wordnet</type>
        <path>en/</path>
    </extension>
</synonyms_extensions>
```
