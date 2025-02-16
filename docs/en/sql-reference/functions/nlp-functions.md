---
slug: /en/sql-reference/functions/nlp-functions
sidebar_position: 130
sidebar_label: NLP (experimental)
---

# Natural Language Processing (NLP) Functions

:::note
This is an experimental feature that is currently in development and is not ready for general use. It will change in unpredictable backwards-incompatible ways in future releases. Set `allow_experimental_nlp_functions = 1` to enable it.
:::

## stem

Performs stemming on a given word.

### Syntax

``` sql
stem('language', word)
```

### Arguments

- `language` — Language which rules will be applied. Use the two letter [ISO 639-1 code](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes).
- `word` — word that needs to be stemmed. Must be in lowercase. [String](../../sql-reference/data-types/string.md#string).

### Examples

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
### Supported languages for stem()

:::note
The stem() function uses the [Snowball stemming](https://snowballstem.org/) library, see the Snowball website for updated languages etc.
:::

- Arabic
- Armenian
- Basque
- Catalan
- Danish
- Dutch
- English
- Finnish
- French
- German
- Greek
- Hindi
- Hungarian
- Indonesian
- Irish
- Italian
- Lithuanian
- Nepali
- Norwegian
- Porter
- Portuguese
- Romanian
- Russian
- Serbian
- Spanish
- Swedish
- Tamil
- Turkish
- Yiddish

## lemmatize

Performs lemmatization on a given word. Needs dictionaries to operate, which can be obtained [here](https://github.com/vpodpecan/lemmagen3/tree/master/src/lemmagen3/models).

### Syntax

``` sql
lemmatize('language', word)
```

### Arguments

- `language` — Language which rules will be applied. [String](../../sql-reference/data-types/string.md#string).
- `word` — Word that needs to be lemmatized. Must be lowercase. [String](../../sql-reference/data-types/string.md#string).

### Examples

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

### Configuration

This configuration specifies that the dictionary `en.bin` should be used for lemmatization of English (`en`) words.  The `.bin` files can be downloaded from 
[here](https://github.com/vpodpecan/lemmagen3/tree/master/src/lemmagen3/models).

``` xml
<lemmatizers>
    <lemmatizer>
        <!-- highlight-start -->
        <lang>en</lang>
        <path>en.bin</path>
        <!-- highlight-end -->
    </lemmatizer>
</lemmatizers>
```

## synonyms

Finds synonyms to a given word. There are two types of synonym extensions: `plain` and `wordnet`.

With the `plain` extension type we need to provide a path to a simple text file, where each line corresponds to a certain synonym set. Words in this line must be separated with space or tab characters.

With the `wordnet` extension type we need to provide a path to a directory with WordNet thesaurus in it. Thesaurus must contain a WordNet sense index.

### Syntax

``` sql
synonyms('extension_name', word)
```

### Arguments

- `extension_name` — Name of the extension in which search will be performed. [String](../../sql-reference/data-types/string.md#string).
- `word` — Word that will be searched in extension. [String](../../sql-reference/data-types/string.md#string).

### Examples

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

### Configuration
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

## detectLanguage

Detects the language of the UTF8-encoded input string. The function uses the [CLD2 library](https://github.com/CLD2Owners/cld2) for detection, and it returns the 2-letter ISO language code.

The `detectLanguage` function works best when providing over 200 characters in the input string.

### Syntax

``` sql
detectLanguage('text_to_be_analyzed')
```

### Arguments

- `text_to_be_analyzed` — A collection (or sentences) of strings to analyze. [String](../../sql-reference/data-types/string.md#string).

### Returned value

- The 2-letter ISO code of the detected language

Other possible results:

- `un` = unknown, can not detect any language.
- `other` = the detected language does not have 2 letter code.

### Examples

Query:

```sql
SELECT detectLanguage('Je pense que je ne parviendrai jamais à parler français comme un natif. Where there’s a will, there’s a way.');
```

Result:

```response
fr
```

## detectLanguageMixed

Similar to the `detectLanguage` function, but `detectLanguageMixed` returns a `Map` of 2-letter language codes that are mapped to the percentage of the certain language in the text.


### Syntax

``` sql
detectLanguageMixed('text_to_be_analyzed')
```

### Arguments

- `text_to_be_analyzed` — A collection (or sentences) of strings to analyze. [String](../../sql-reference/data-types/string.md#string).

### Returned value

- `Map(String, Float32)`: The keys are 2-letter ISO codes and the values are a percentage of text found for that language


### Examples

Query:

```sql
SELECT detectLanguageMixed('二兎を追う者は一兎をも得ず二兎を追う者は一兎をも得ず A vaincre sans peril, on triomphe sans gloire.');
```

Result:

```response
┌─detectLanguageMixed()─┐
│ {'ja':0.62,'fr':0.36  │
└───────────────────────┘
```

## detectLanguageUnknown

Similar to the `detectLanguage` function, except the `detectLanguageUnknown` function works with non-UTF8-encoded strings. Prefer this version when your character set is UTF-16 or UTF-32.


### Syntax

``` sql
detectLanguageUnknown('text_to_be_analyzed')
```

### Arguments

- `text_to_be_analyzed` — A collection (or sentences) of strings to analyze. [String](../../sql-reference/data-types/string.md#string).

### Returned value

- The 2-letter ISO code of the detected language

Other possible results:

- `un` = unknown, can not detect any language.
- `other` = the detected language does not have 2 letter code.

### Examples

Query:

```sql
SELECT detectLanguageUnknown('Ich bleibe für ein paar Tage.');
```

Result:

```response
┌─detectLanguageUnknown('Ich bleibe für ein paar Tage.')─┐
│ de                                                     │
└────────────────────────────────────────────────────────┘
```

## detectCharset

The `detectCharset` function detects the character set of the non-UTF8-encoded input string.


### Syntax

``` sql
detectCharset('text_to_be_analyzed')
```

### Arguments

- `text_to_be_analyzed` — A collection (or sentences) of strings to analyze. [String](../../sql-reference/data-types/string.md#string).

### Returned value

- A `String` containing the code of the detected character set

### Examples

Query:

```sql
SELECT detectCharset('Ich bleibe für ein paar Tage.');
```

Result:

```response
┌─detectCharset('Ich bleibe für ein paar Tage.')─┐
│ WINDOWS-1252                                   │
└────────────────────────────────────────────────┘
```
