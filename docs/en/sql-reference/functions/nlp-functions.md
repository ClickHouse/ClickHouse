---
toc_priority: 67
toc_title: NLP
---

# Natural Language Processing functions {#nlp-functions}

## stem {#stem}

Performs stemming on a previously tokenized text.

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
SELECT SELECT arrayMap(x -> stem('en', x), ['I', 'think', 'it', 'is', 'a', 'blessing', 'in', 'disguise']) as res;
```

Result:

``` text
┌─res────────────────────────────────────────────────┐
│ ['I','think','it','is','a','bless','in','disguis'] │
└────────────────────────────────────────────────────┘
```

## lemmatize {#lemmatize}

Performs lemmatization on a given word.

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

## synonyms {#synonyms}

Finds synonyms to a given word. 

**Syntax**

``` sql
synonyms('extension_name', word)
```

**Arguments**

-   `extension_name` — Name of the extention in which search will be performed. [String](../../sql-reference/data-types/string.md#string).
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