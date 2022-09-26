---
sidebar_position: 67
sidebar_label: NLP
---

# [экспериментально] Функции для работы с естественным языком {#nlp-functions}

:::danger "Предупреждение"
    Сейчас использование функций для работы с естественным языком является экспериментальной возможностью. Чтобы использовать данные функции, включите настройку `allow_experimental_nlp_functions = 1`.

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
SELECT arrayMap(x -> stem('en', x), ['I', 'think', 'it', 'is', 'a', 'blessing', 'in', 'disguise']) as res;
```

Result:

``` text
┌─res────────────────────────────────────────────────┐
│ ['I','think','it','is','a','bless','in','disguis'] │
└────────────────────────────────────────────────────┘
```

## lemmatize {#lemmatize}

Данная функция проводит лемматизацию для заданного слова. Для работы лемматизатора необходимы словари, которые можно найти [здесь](https://github.com/vpodpecan/lemmagen3/tree/master/src/lemmagen3/models).

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

Находит синонимы к заданному слову. Представлены два типа расширений словарей: `plain` и `wordnet`.

Для работы расширения типа `plain` необходимо указать путь до простого текстового файла, где каждая строка соответствует одному набору синонимов. Слова в данной строке должны быть разделены с помощью пробела или знака табуляции.

Для работы расширения типа `plain` необходимо указать путь до WordNet тезауруса. Тезаурус должен содержать WordNet sense index.

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