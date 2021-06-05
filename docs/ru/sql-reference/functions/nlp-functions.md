---
toc_priority: 67
toc_title: NLP
---

# Функции для работы с ествественным языком {#nlp-functions}

## tokenize, tokenizeWhitespace {#tokenize}

Данные функции проводят токенизацию - разделение заданного текста на слова.

**Синтаксис**

``` sql
tokenize(text)
```

**Аргументы**

-   `text` — Текст подлежащий токенизации. [String](../../sql-reference/data-types/string.md#string).

**Примеры**

Запрос:

``` sql
SELECT tokenize('I think it is a blessing in disguise.') as res;
```

Результат:

``` text
┌─res────────────────────────────────────────────────────┐
│ ['I','think','it','is','a','blessing','in','disguise'] │
└────────────────────────────────────────────────────────┘
```

## stem {#stem}

Данная функция проводит стемминг заданного слова.

**Синтаксис**

``` sql
stem('language', word)
```

**Аргументы**

-   `language` — Язык, правила которого будут применены для стемминга. Допускается только нижний регистр. [String](../../sql-reference/data-types/string.md#string).
-   `word` — Слово подлежащее стеммингу. Допускается только нижний регистр. [String](../../sql-reference/data-types/string.md#string).

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

Данная функция проводит лемматизацию для заданного слова.

**Синтаксис**

``` sql
lemmatize('language', word)
```

**Аргументы**

-   `language` — Язык, правила которого будут применены для лемматизации. [String](../../sql-reference/data-types/string.md#string).
-   `word` — Слово, подлежащее лемматизации. Допускается только нижний регистр. [String](../../sql-reference/data-types/string.md#string).

**Примеры**

Запрос:

``` sql
SELECT lemmatize('en', 'wolves');
```

Результат:

``` text
┌─lemmatize("wolves")─┐
│              "wolf" │
└─────────────────────┘
```

Конфигурация:
``` xml
<lemmatizers>
    <lemmatizer>
        <lang>en</lang>
        <path>en.bin</path>
    </lemmatizer>
</lemmatizers>
```

## synonyms {#synonyms}

Находит синонимы к заданному слову.

**Синтаксис**

``` sql
synonyms('extension_name', word)
```

**Аргументы**

-   `extension_name` — Название расширения, в котором будет проводиться поиск. [String](../../sql-reference/data-types/string.md#string).
-   `word` — Слово, которое будет искаться в расширении. [String](../../sql-reference/data-types/string.md#string).

**Примеры**

Запрос:

``` sql
SELECT synonyms('list', 'important');
```

Результат:

``` text
┌─synonyms('list', 'important')────────────┐
│ ['important','big','critical','crucial'] │
└──────────────────────────────────────────┘
```

Конфигурация:
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