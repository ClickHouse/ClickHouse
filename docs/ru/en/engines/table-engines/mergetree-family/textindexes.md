---
description: 'Быстро находите искомые слова и фразы в тексте.'
keywords: ['полнотекстовый поиск', 'текстовый индекс', 'индекс', 'индексы']
sidebar_label: 'Полнотекстовый поиск с помощью текстовых индексов'
slug: /engines/table-engines/mergetree-family/textindexes
title: 'Полнотекстовый поиск с помощью текстовых индексов'
doc_type: 'reference'
---

Текстовые индексы (также известные как [обратные индексы](https://en.wikipedia.org/wiki/Inverted_index)) обеспечивают быстрый полнотекстовый поиск по текстовым данным.
Текстовый индекс хранит соответствие между токенами и номерами строк, в которых встречается каждый токен.
Токены создаются в процессе, называемом токенизацией.
Например, токенизатор ClickHouse по умолчанию преобразует английское предложение &quot;The cat likes mice.&quot; в токены [&quot;The&quot;, &quot;cat&quot;, &quot;likes&quot;, &quot;mice&quot;].

В качестве примера предположим, что есть таблица с одним столбцом и тремя строками

```result
1: The cat likes mice.
2: Mice are afraid of dogs.
3: I have two dogs and a cat.
```

Соответствующие токены:

```result
1: The, cat, likes, mice
2: Mice, are, afraid, of, dogs
3: I, have, two, dogs, and, a, cat
```

Обычно мы выполняем поиск без учета регистра, поэтому приводим токены к нижнему регистру:

```result
1: the, cat, likes, mice
2: mice, are, afraid, of, dogs
3: i, have, two, dogs, and, a, cat
```

Мы также удалим стоп-слова, такие как &quot;I&quot;, &quot;the&quot; и &quot;and&quot;, поскольку они встречаются почти в каждой строке:

```result
1: cat, likes, mice
2: mice, afraid, dogs
3: have, two, dogs, cat
```

Текстовый индекс в этом случае (концептуально) содержит следующую информацию:

```result
afraid : [2]
cat    : [1, 3]
dogs   : [2, 3]
have   : [3]
likes  : [1]
mice   : [1]
two    : [3]
```

Для заданного поискового токена эта структура индекса позволяет быстро находить все соответствующие строки.

<div id="creating-a-text-index">
  ## Создание текстового индекса
</div>

Текстовые индексы доступны для общего использования (GA) в ClickHouse версии 26.2 и новее.
В этих версиях для использования текстового индекса не требуется настраивать какие-либо специальные параметры.
Мы настоятельно рекомендуем использовать ClickHouse версии &gt;= 26.2 в продакшне.

:::note
Текстовые индексы можно использовать с любой версией ClickHouse &gt;= 26.2 независимо от настройки [compatibility](../../../operations/settings/settings#compatibility).
:::

Чтобы создать текстовый индекс, используйте следующий синтаксис:

```sql title="Query"
CREATE TABLE table
(
    key UInt64,
    str String,
    INDEX text_idx str TYPE text(
                                -- Mandatory parameters:
                                tokenizer = splitByNonAlpha
                                            | splitByString[(S)]
                                            | asciiCJK
                                            | ngrams[(N)]
                                            | sparseGrams[(min_length[, max_length[, min_cutoff_length]])]
                                            | array
                                -- Optional parameters:
                                [, preprocessor = expression(str)]
                                [, postprocessor = expression(str)]
                                [, positions = 0 | 1 ] -- experimental
                                -- Optional advanced parameters:
                                [, dictionary_block_size = D]
                                [, dictionary_block_frontcoding_compression = B]
                                [, posting_list_block_size = C]
                                [, posting_list_codec = 'none' | 'bitpacking' ]
                            )
)
ENGINE = MergeTree
ORDER BY key
```

Текстовые индексы можно создавать для столбцов следующих типов:

* [String](/ru/sql-reference/data-types/string.md) и [FixedString](/ru/sql-reference/data-types/fixedstring.md),
* [Array(String)](/ru/sql-reference/data-types/array.md) и [Array(FixedString)](/ru/sql-reference/data-types/array.md),
* [Map](/ru/sql-reference/data-types/map.md) (с помощью функций [mapKeys](/ru/sql-reference/functions/tuple-map-functions.md/#mapKeys) и [mapValues](/ru/sql-reference/functions/tuple-map-functions.md/#mapValues)),
* [JSON](/ru/sql-reference/data-types/newjson.md) (с помощью функций [JSONAllPaths](/ru/sql-reference/functions/json-functions.md/#JSONAllPaths) и [`JSONAllValues`](/ru/sql-reference/functions/json-functions.md#JSONAllValues)).

Также поддерживаются столбцы типа [Nullable(T)](/ru/sql-reference/data-types/nullable.md) и [LowCardinality()](/ru/sql-reference/data-types/lowcardinality.md), включая `Array(Nullable(String or FixedString))`.

Кроме того, текстовый индекс можно добавить в существующую таблицу:

```sql title="Query"
ALTER TABLE table
    ADD INDEX text_idx str TYPE text(
                                -- Mandatory parameters:
                                tokenizer = splitByNonAlpha
                                            | splitByString[(S)]
                                            | asciiCJK
                                            | ngrams[(N)]
                                            | sparseGrams[(min_length[, max_length[, min_cutoff_length]])]
                                            | array
                                -- Optional parameters:
                                [, preprocessor = expression(str)]
                                [, postprocessor = expression(str)]
                                [, positions = 0 | 1 ] -- experimental
                                -- Optional advanced parameters:
                                [, dictionary_block_size = D]
                                [, dictionary_block_frontcoding_compression = B]
                                [, posting_list_block_size = C]
                                [, posting_list_codec = 'none' | 'bitpacking' ]
                            )

```

Если вы добавляете индекс в существующую таблицу, мы рекомендуем материализовать индекс для существующих частей таблицы (иначе для частей без индекса поиск будет выполняться медленным полным перебором).

```sql title="Query"
ALTER TABLE table MATERIALIZE INDEX text_idx SETTINGS mutations_sync = 2;
```

Чтобы удалить текстовый индекс, выполните

```sql title="Query"
ALTER TABLE table DROP INDEX text_idx;
```

**Аргумент `tokenizer` (обязательный)**. Аргумент `tokenizer` определяет используемый токенизатор:

* `splitByNonAlpha` разбивает строки по неалфавитно-цифровым ASCII-символам (см. функцию [splitByNonAlpha](/ru/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha)).
* `splitByString(S)` разбивает строки по заданным пользователем строкам-разделителям `S` (см. функцию [splitByString](/ru/sql-reference/functions/splitting-merging-functions.md/#splitByString)).
  Разделители можно задать с помощью необязательного параметра, например: `tokenizer = splitByString([', ', '; ', '\n', '\\'])`.
  Обратите внимание, что каждая строка может состоять из нескольких символов (`', '` в примере).
  Если список разделителей не задан явно (например, `tokenizer = splitByString`), по умолчанию используется один пробел `[' ']`.
* `asciiCJK` разбивает строки на токены по правилам границ слов Unicode (аналогично [Unicode Text Segmentation (UAX #29)](https://unicode.org/reports/tr29/)). ASCII-буквенно-цифровые символы и символы подчёркивания образуют токены с соединителями (ASCII `:` для букв, `.` и `'` для символов одного типа). Не-ASCII-символы Unicode, включая символы [CJK](https://en.wikipedia.org/wiki/CJK_characters), становятся односимвольными токенами.
* `ngrams(N)` разбивает строки на `N`-граммы одинаковой длины (см. функцию [ngrams](/ru/sql-reference/functions/splitting-merging-functions.md/#ngrams)).
  Длину n-граммы можно задать с помощью необязательного целочисленного параметра от 1 до 8, например: `tokenizer = ngrams(3)`.
  Если размер n-граммы не задан явно (например, `tokenizer = ngrams`), по умолчанию используется значение 3.
* `sparseGrams(min_length, max_length, min_cutoff_length)` разбивает строки на n-граммы переменной длины — не короче `min_length` и не длиннее `max_length` символов включительно (см. функцию [sparseGrams](/ru/sql-reference/functions/string-functions#sparseGrams)).
  Если не указано явно, значения `min_length` и `max_length` по умолчанию равны 3 и 100.
  Если передан параметр `min_cutoff_length`, возвращаются только n-граммы длиной не меньше `min_cutoff_length`.
  В отличие от `ngrams(N)`, токенизатор `sparseGrams` создаёт N-граммы переменной длины, что позволяет более гибко представлять исходный текст.
  Например, `tokenizer = sparseGrams(3, 5, 4)` внутри генерирует из входной строки 3-, 4- и 5-граммы, но возвращаются только 4- и 5-граммы.
* `array` не выполняет токенизацию, то есть каждое значение строки является токеном (см. функцию [array](/ru/sql-reference/functions/array-functions.md/#array)).

Все доступные токенизаторы перечислены в [system.tokenizers](../../../operations/system-tables/tokenizers.md).

:::note
Токенизатор `splitByString` применяет разделители слева направо.
Это может приводить к неоднозначностям.
Например, строки-разделители `['%21', '%']` приведут к тому, что `%21abc` будет токенизировано как `['abc']`, тогда как при перестановке этих строк-разделителей на `['%', '%21']` на выходе получится `['21abc']`.
В большинстве случаев нужно, чтобы при сопоставлении более длинные разделители имели приоритет.
Обычно этого можно добиться, передавая строки-разделители в порядке убывания длины.
Если строки-разделители образуют [префиксный код](https://en.wikipedia.org/wiki/Prefix_code), их можно передавать в произвольном порядке.
:::

Чтобы понять, как токенизатор разбивает входную строку, можно использовать функции [tokens](/ru/sql-reference/functions/splitting-merging-functions.md/#tokens) и [tokensForLikePattern](/ru/sql-reference/functions/splitting-merging-functions.md/#tokensForLikePattern):

Пример:

```sql title="Query"
SELECT tokens('abc def', 'ngrams', 3);
```

```result title="Response"
['abc','bc ','c d',' de','def']
```

*Работа с входными данными, содержащими символы не из ASCII.*
Текстовые индексы можно создавать для текстовых данных на любом языке и в любой кодировке.
Для текста, содержащего символы не из ASCII, рекомендуется токенизатор `asciiCJK`, поскольку он корректно обрабатывает границы слов в Unicode, включая символы CJK.
:::

**Аргумент препроцессора (необязательно)**. Препроцессор — это выражение, которое применяется к входной строке перед токенизацией.

Типичные сценарии использования аргумента препроцессора:

1. Приведение к нижнему/верхнему регистру или сворачивание регистра для регистронезависимого сопоставления, например [lower](/ru/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/ru/sql-reference/functions/string-functions.md/#lowerUTF8), [caseFoldUTF8](/ru/sql-reference/functions/string-functions.md/#caseFoldUTF8).
2. Нормализация UTF-8, например [normalizeUTF8NFC](/ru/sql-reference/functions/string-functions.md/#normalizeUTF8NFC), [normalizeUTF8NFD](/ru/sql-reference/functions/string-functions.md/#normalizeUTF8NFD), [normalizeUTF8NFKC](/ru/sql-reference/functions/string-functions.md/#normalizeUTF8NFKC), [normalizeUTF8NFKD](/ru/sql-reference/functions/string-functions.md/#normalizeUTF8NFKD), [normalizeUTF8NFKCCasefold](/ru/sql-reference/functions/string-functions.md/#normalizeUTF8NFKCCasefold), [toValidUTF8](/ru/sql-reference/functions/string-functions.md/#toValidUTF8).
3. Удаление или преобразование нежелательных символов или подстрок, например диакритических знаков: [extractTextFromHTML](/ru/sql-reference/functions/string-functions.md/#extractTextFromHTML), [substring](/ru/sql-reference/functions/string-functions.md/#substring), [idnaEncode](/ru/sql-reference/functions/string-functions.md/#idnaEncode), [translate](/ru/sql-reference/functions/string-replace-functions.md/#translate), [removeDiacriticsUTF8](/ru/sql-reference/functions/string-functions.md/#removeDiacriticsUTF8).

Выражение препроцессора должно преобразовывать входное значение типа [String](/ru/sql-reference/data-types/string.md) или [FixedString](/ru/sql-reference/data-types/fixedstring.md) в значение того же типа.
Если текстовый индекс построен по столбцу типа `Nullable(T)` или `LowCardinality(T)`, то выражение препроцессора должно принимать nullable- или low-cardinality-значения (то есть не должно генерировать исключение).

Примеры:

* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(col, '\n', 1))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(extractTextFromHTML(col)))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(col)))`

Кроме того, выражение препроцессора должно ссылаться только на столбец или выражение, поверх которого определён текстовый индекс.

Примеры:

* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(lower(col)))`
* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(col), lower(col)))`
* Недопустимо: `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(col, col))`

Использование недетерминированных функций запрещено.

:::note
Препроцессор по сути эквивалентен оборачиванию столбца или выражения индекса в выражение препроцессора.
Например, препроцессор `lower` в `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))` можно эмулировать с помощью `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha')`.
Недостаток второй формы в том, что эмулированный препроцессор применяется только в том случае, если он соответствует условию фильтрации в предложении WHERE.
Например, `WHERE hasAllTokens(lower(col), [...])` соответствует, а `WHERE hasAllTokens(col, [...])` — нет.
Поэтому для оптимального пользовательского опыта мы рекомендуем использовать выражения препроцессора.
:::

Функции [hasToken](/ru/sql-reference/functions/string-search-functions.md/#hasToken), [hasAllTokens](/ru/sql-reference/functions/string-search-functions.md/#hasAllTokens), [hasAnyTokens](/ru/sql-reference/functions/string-search-functions.md/#hasAnyTokens) и [hasPhrase](/ru/sql-reference/functions/string-search-functions.md/#hasPhrase) используют препроцессор, чтобы сначала преобразовать поисковый термин перед его токенизацией.
Обратите внимание: поскольку препроцессор применяется только на пути текстового индекса, результаты этих функций могут различаться между запросами, использующими текстовый индекс, и запросами, которые его не используют (например, `SETTINGS use_skip_indexes = 0`).

Например,

```sql title="Query"
CREATE TABLE table
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM table WHERE hasToken(str, 'Foo');
```

эквивалентно:

```sql title="Query"
CREATE TABLE table
(
    str String,
    INDEX idx lower(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM table WHERE hasToken(str, lower('Foo'));
```

В этом случае выражение препроцессора преобразует каждый элемент массива отдельно.

Пример:

```sql title="Query"
CREATE TABLE table
(
    arr Array(String),
    INDEX idx arr TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arr))

    -- This is not legal:
    INDEX idx_illegal arr TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = arraySort(arr))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM tab WHERE hasAllTokens(arr, 'foo');
```

Чтобы задать препроцессор для текстового индекса, создаваемого на столбцах типа [Map](/ru/sql-reference/data-types/map.md), нужно определить, будет ли индекс строиться
по ключам или по значениям.

Пример:

```sql title="Query"
CREATE TABLE table
(
    map Map(String, String),
    INDEX idx mapKeys(map)  TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(mapKeys(map)))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'foo');
```

**Аргумент постпроцессора (необязательно)**. Постпроцессор — это выражение, которое применяется к каждому токену после токенизации.

В отличие от препроцессора, который преобразует всю входную строку до того, как токенизатор разобьет ее на токены, постпроцессор работает с самими токенами — по одному за раз.
Это естественное место для преобразований, которые по своей природе выполняются на уровне токенов.

Типичные варианты использования аргумента постпроцессора:

1. **Фильтрация стоп-слов (чрезвычайно частых токенов)**. Очень распространенные токены, такие как &quot;the&quot;, &quot;a&quot; и &quot;is&quot;, почти не имеют поисковой ценности и раздувают индекс.
   Постпроцессор можно использовать, чтобы отбрасывать их, преобразуя в пустые токены — пустые токены игнорируются, то есть не добавляются в индекс.
   Пример: `if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)`
2. **Удаление временных меток**. Строки журналов часто начинаются со структурированной временной метки, такой как `2024-01-15T10:23:45`, или содержат ее.
   Индексирование токенов с временными метками раздувает индекс строками, не имеющими поисковой ценности.
   Есть два взаимодополняющих способа игнорировать временные метки:
   * **Подход с постпроцессором**: используйте токенизатор `splitByString` (разбиение по пробельным символам), чтобы вся временная метка стала одним токеном, а затем `parseDateTimeOrNull`, чтобы распознать и отбросить ее.
     Пример: `if(isNull(parseDateTimeOrNull(str, '%Y-%m-%dT%H:%i:%S')), str, '')`
     Для временных меток со смещением часового пояса или дробными секундами используйте `parseDateTimeBestEffortOrNull(str)` без явной строки формата.
   * **Подход с препроцессором**: удалите временную метку из полной строки журнала *до* токенизации с помощью регулярного выражения.
     Пример: `replaceRegexpAll(str, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')`
     Это работает с любым токенизатором и более эффективно, поскольку символы временной метки вообще не токенизируются.
     Оба подхода можно сочетать: препроцессор удаляет временную метку, а постпроцессор нормализует или фильтрует оставшиеся токены (например, приводит к нижнему регистру и удаляет слова уровня серьёзности, такие как `ERROR` или `INFO`).
3. **Стемминг**. Приведение каждого токена к его основе улучшает полноту поиска за счет сопоставления морфологических вариантов с общим корнем.
   Например, при английском стемминге &quot;running&quot;, &quot;runs&quot; и &quot;run&quot; все приводятся к &quot;run&quot;, поэтому запрос по любому из этих вариантов найдет их все.
   ClickHouse предоставляет встроенную функцию [stem](/ru/sql-reference/functions/string-functions.md/#stem) для нескольких языков.
   Пример: `stem(str, 'en')`
4. **Нормализация регистра**. Приведение токенов к нижнему или верхнему регистру для регистронезависимого сопоставления, например [lower](/ru/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/ru/sql-reference/functions/string-functions.md/#lowerUTF8).
   Для приведения к нижнему и верхнему регистру мы рекомендуем использовать препроцессор вместо постпроцессора.

Выражение постпроцессора преобразует токены типа [String](/ru/sql-reference/data-types/string.md) в токены того же типа.
Кроме того, выражение постпроцессора должно ссылаться только на столбец или выражение, поверх которого определен текстовый индекс.
Когда столбец имеет тип `Array(String)`, постпроцессор по-прежнему работает с отдельными токенами как с обычными значениями `String`.

Использование недетерминированных функций запрещено.

Постпроцессор применяется к каждому сгенерированному токену при построении индекса (для токенизатора `array` каждый элемент массива является токеном). Во время выполнения запроса поведение зависит от функции:

* Для `hasToken`, `hasAllTokens`, `hasAnyTokens` и `hasPhrase` (с любым поддерживаемым токенизатором): постпроцессор применяется и к токенам в исходном тексте, и к искомой подстроке, что обеспечивает полностью нормализованное сопоставление (например, регистронезависимый поиск). Для `hasPhrase` токены после постобработки располагаются подряд, поэтому токен, который постпроцессор отбрасывает, не оставляет позиционного разрыва, и фраза всё равно будет найдена через него — например, при постпроцессоре стоп-слов, который отбрасывает `the`, `hasPhrase(col, 'see cat')` совпадает с документом `see the cat`.
* Для всех остальных функций (`=`, `IN`, `has`, `hasAny`, `hasAll`, `mapContains*`): для поиска с подсказкой индекса постобрабатывается только искомая подстрока; предикат на уровне строки по-прежнему сравнивается с исходными значениями столбца.

Примеры:

* Удаление стоп-слов с помощью выражения постпроцессора:

```sql
CREATE TABLE table
(
    str String,
    INDEX idx(str) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)
    )
)
ENGINE = MergeTree
ORDER BY tuple();
```

* Удалите временные метки с помощью выражения постпроцессора:

```sql
-- Log lines: '2024-01-15T10:23:45 ERROR connection failed'
-- The splitByString tokenizer (default: whitespace) keeps the full timestamp as one token.
-- parseDateTimeOrNull detects and drops it; non-timestamp words are kept.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer    = 'splitByString',
        postprocessor = if(isNull(parseDateTimeOrNull(line, '%Y-%m-%dT%H:%i:%S')), line, '')
    )
)
ENGINE = MergeTree ORDER BY id;

-- Only message-level words are indexed; timestamp tokens are not stored.
SELECT count() FROM logs WHERE hasAllTokens(line, ['ERROR']);       -- fast index lookup
SELECT count() FROM logs WHERE hasAllTokens(line, ['2024-01-15T10:23:45']);  -- returns 0: token was never indexed
```

* Удалите временные метки с помощью выражения предобработки:

```sql
-- The preprocessor strips the ISO timestamp prefix before tokenization.
-- Any tokenizer can be used; timestamp characters are never seen by the tokenizer.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer   = 'splitByNonAlpha',
        preprocessor = replaceRegexpAll(line, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')
    )
)
ENGINE = MergeTree ORDER BY id;
```

* Удалите временные метки с помощью комбинированного выражения препроцессора и постпроцессора:

```sql
-- Preprocessor strips the timestamp, then lowercases the remainder.
-- Postprocessor drops the severity word (error, info, warn, debug) after tokenization.
-- Result: only substantive message words are stored in the index.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer    = 'splitByNonAlpha',
        preprocessor = lower(replaceRegexpAll(line, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')),
        postprocessor = if(line IN ('error', 'info', 'warn', 'warning', 'debug', 'critical'), '', line)
    )
)
ENGINE = MergeTree ORDER BY id;

-- Example log line: '2024-01-15T10:23:45 ERROR connection failed'
-- After preprocessor:  'error connection failed'
-- After tokenization:  ['error', 'connection', 'failed']
-- After postprocessor: ['connection', 'failed']   ← 'error' dropped as severity word
SELECT count() FROM logs WHERE hasAllTokens(line, ['connection']);
```

* Используйте выражение постпроцессора для стемминга токенов:

```sql
CREATE TABLE table
(
    str String,
    INDEX idx(str) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = stem(str, 'en')
    )
)
ENGINE = MergeTree
ORDER BY tuple();

-- The query token 'running' is stemmed to 'run' before the lookup,
-- matching rows that contain 'run', 'runs', 'ran', 'running', etc.
SELECT count() FROM table WHERE hasAllTokens(str, ['running']);
```

**Поддержка функций**.

Для предикатов, обращающихся к текстовому индексу, препроцессор и постпроцессор применяются к искомому значению перед проверкой на уровне гранулы, чтобы при поиске по индексу использовались те же токены, которые были сохранены при построении индекса.
Для большинства функций (`=`, `IN`, `startsWith`, `endsWith`, `LIKE`, `mapContains*`) текстовый индекс используется только для пропуска нерелевантных блоков данных; ClickHouse по-прежнему проверяет каждую оставшуюся строку, применяя исходный предикат к исходным данным столбца.
Для функций поиска токенов (`hasToken`, `hasAllTokens`, `hasAnyTokens`) текстовый индекс является основным способом вычисления: ClickHouse нормализует needle с помощью тех же препроцессора, токенизатора и постпроцессора, которые применялись при построении индекса, и использует эту нормализованную форму как для индексированных, так и для неиндексированных частей таблицы. При наличии постпроцессора токены haystack также нормализуются во время выполнения запроса (для любого токенизатора, а не только `array`), поэтому обе стороны сравнения преобразуются единообразно, и результат не зависит от того, читается ли индекс напрямую (настройка `query_plan_direct_read_from_text_index`) или есть ли у конкретной части материализованный индекс — например, это позволяет включить регистронезависимое сопоставление для `hasAllTokens(col, ['FOO'])` с постпроцессором `lower`.
Без `positions` `hasPhrase` использует индекс только как подсказку и проверяет каждую оставшуюся строку с помощью исходного предиката; постпроцессор дополнительно одинаково нормализует и фразу, и токены haystack, поэтому результат не зависит от пути чтения, а токены, которые постпроцессор отбрасывает, не нарушают смежность фразы. При `positions = 1` `hasPhrase` использует точное прямое чтение (при этом постпроцессор, если он есть, всё равно применяется).
Искомые токены, которые постпроцессор преобразует в пустую строку, игнорируются, то есть считаются отсутствующими в поисковой фразе.

| Функция                                                                                     | Поддерживает препроцессор                                    | Совместимые токенизаторы                                 | Поддерживает постпроцессор |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------------- | -------------------------- |
| `=`                                                                                         | да                                                           | все                                                      | да                         |
| `IN`                                                                                        | да                                                           | все                                                      | да                         |
| [hasToken](/ru/sql-reference/functions/string-search-functions.md/#hasToken)                   | да                                                           | все (в первую очередь для `splitByNonAlpha`)             | да                         |
| [hasAnyTokens(col, str)](/ru/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | да                                                           | все                                                      | да                         |
| [hasAllTokens(col, str)](/ru/sql-reference/functions/string-search-functions.md/#hasAllTokens) | да                                                           | все                                                      | да                         |
| [hasAnyTokens(col, arr)](/ru/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | нет (элементы массива используются как токены без изменений) | все                                                      | да                         |
| [hasAllTokens(col, arr)](/ru/sql-reference/functions/string-search-functions.md/#hasAllTokens) | нет (элементы массива используются как токены без изменений) | все                                                      | да                         |
| [hasPhrase](/ru/sql-reference/functions/string-search-functions.md/#hasPhrase)                 | да                                                           | `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` | да                         |
| [startsWith](/ru/sql-reference/functions/string-functions.md/#startsWith)                      | да                                                           | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | да                         |
| [endsWith](/ru/sql-reference/functions/string-functions.md/#endsWith)                          | да                                                           | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | да                         |
| [like](/ru/sql-reference/functions/string-search-functions.md/#like)                           | да¹                                                          | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | да¹                        |
| [match](/ru/sql-reference/functions/string-search-functions.md/#match)                         | да¹                                                          | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | да¹                        |
| [ilike](/ru/sql-reference/functions/string-search-functions.md/#like)                          | да² (только `lower`/`upper`)                                 | `splitByNonAlpha`, `array`²                              | нет²                       |
| [mapContainsKey](/ru/sql-reference/functions/tuple-map-functions#mapContainsKey)               | да                                                           | все                                                      | да                         |
| [mapContainsValue](/ru/sql-reference/functions/tuple-map-functions#mapContainsValue)           | да                                                           | все                                                      | да                         |
| [mapContainsKeyLike](/ru/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)       | да                                                           | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | да                         |
| [mapContainsValueLike](/ru/sql-reference/functions/tuple-map-functions#mapContainsValueLike)   | да                                                           | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | да                         |
| [has](/ru/sql-reference/functions/array-functions.md/#has)                                     | да                                                           | `array`                                                  | да                         |
| [hasAny](/ru/sql-reference/functions/array-functions.md/#hasAny)                               | да                                                           | `array`                                                  | да                         |
| [hasAll](/ru/sql-reference/functions/array-functions.md/#hasAll)                               | да                                                           | `array`                                                  | да                         |

¹ `LIKE` и `match` используют прямое чтение в качестве подсказки для перечисленных токенизаторов; в противном случае выполняется brute-force scan.
`LIKE` также поддерживает *прямое чтение (без подсказки)* (включается через `use_text_index_like_evaluation_by_dictionary_scan`) для токенизаторов `splitByNonAlpha` и `array` без препроцессора и постпроцессора.

² `ILIKE` поддерживается только через прямое чтение (без подсказки) (`use_text_index_like_evaluation_by_dictionary_scan = 1`, токенизатор `splitByNonAlpha` или `array`).
Использование индекса в качестве подсказки не предусмотрено: если настройка отключена или токенизатор не входит в поддерживаемый набор, индекс для `ILIKE` не используется.
Препроцессор, если он задан, должен быть `lower` или `upper`; постпроцессоры не поддерживаются.

**Экспериментально: аргумент Positions (необязательный)**.

Экспериментальный параметр `positions` (по умолчанию: `0`) определяет, хранит ли индекс позиции токенов.
Если установить значение `1`, индекс дополнительно сохраняет позиционные данные (в файле `.pos`), что позволяет выполнять точный поиск по фразе через прямое чтение для функции [`hasPhrase`](#functions-example-hasphrase).
Хранение позиций увеличивает размер индекса на диске и затраты на запись, поэтому эта возможность включается только явно.
Формат хранения на диске пока не является стабильным, поэтому этот параметр экспериментальный и в будущих релизах может измениться.
Поэтому для создания индекса с `positions = 1` требуется включить настройку MergeTree [`allow_experimental_text_index_positions`](/ru/operations/settings/merge-tree-settings#allow_experimental_text_index_positions).
Установите `positions = 0` (значение по умолчанию), чтобы сохранить хранение только списков вхождений; текстовые индексы, созданные без этого аргумента, останутся без позиций.

:::warning
Этот аргумент экспериментальный и должен использоваться только для тестирования.
Включите настройку MergeTree [`allow_experimental_text_index_positions`](/ru/operations/settings/merge-tree-settings#allow_experimental_text_index_positions), чтобы разрешить хранение позиций.
:::

<details markdown="1">
  <summary>Необязательные дополнительные параметры</summary>

  Значения по умолчанию следующих дополнительных параметров хорошо подходят практически для всех случаев.
  Мы не рекомендуем их изменять.

  Необязательный параметр `dictionary_block_size` (по умолчанию: 512) задает размер блоков словаря в строках.

  Необязательный параметр `dictionary_block_frontcoding_compression` (по умолчанию: 1) определяет, используют ли блоки словаря front coding для сжатия.

  Необязательный параметр `posting_list_block_size` (по умолчанию: 1048576) задает размер блоков списка вхождений в строках.

  Необязательный параметр `posting_list_codec` (по умолчанию: `none`) задает кодек для списка вхождений:

  * `none` - списки вхождений хранятся без дополнительного сжатия.
  * `bitpacking` - применяется [дифференциальное (delta) кодирование](https://en.wikipedia.org/wiki/Delta_encoding), а затем [битовая упаковка](https://dev.to/madhav_baby_giraffe/bit-packing-the-secret-to-optimizing-data-storage-and-transmission-m70) (в пределах блоков фиксированного размера). Замедляет запросы SELECT, поэтому на данный момент не рекомендуется.

  Перечисленные выше дополнительные параметры также можно задавать на уровне таблицы через соответствующие настройки MergeTree: [`text_index_dictionary_block_size`](/ru/operations/settings/merge-tree-settings#text_index_dictionary_block_size), [`text_index_dictionary_block_frontcoding_compression`](/ru/operations/settings/merge-tree-settings#text_index_dictionary_block_frontcoding_compression), [`text_index_posting_list_block_size`](/ru/operations/settings/merge-tree-settings#text_index_posting_list_block_size) и [`text_index_posting_list_codec`](/ru/operations/settings/merge-tree-settings#text_index_posting_list_codec).
  Они применяются ко всем текстовым индексам таблицы, для которых параметр не задан явно.

  Основной сценарий использования настроек уровня таблицы — изменить параметры индекса существующей таблицы без удаления и повторного создания текстового индекса во всех частях таблицы.
  Изменение настройки уровня таблицы применяет новые параметры только к текстовым индексам, построенным для новых частей; существующие части сохраняют свою текущую структуру.

  Аргумент, указанный в определении индекса, имеет приоритет над настройкой таблицы, например:

  ```sql
  CREATE TABLE table(
      s String,
      -- Этот индекс использует 'bitpacking', переопределяя значение по умолчанию на уровне таблицы ниже:
      INDEX idx_a s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'),
      -- Этот индекс наследует 'none' из настройки таблицы:
      INDEX idx_b lower(s) TYPE text(tokenizer = 'splitByNonAlpha'))
  ENGINE = MergeTree()
  ORDER BY tuple()
  SETTINGS text_index_posting_list_codec = 'none';
  ```
</details>

*Гранулярность индекса.*
Текстовые индексы реализованы в ClickHouse как тип [индексов пропуска данных](/ru/engines/table-engines/mergetree-family/mergetree.md/#skip-index-types).
Однако, в отличие от других индексов пропуска данных, текстовые индексы используют бесконечную гранулярность (100 миллионов).
Это можно увидеть в определении таблицы для текстового индекса.

Пример:

```sql title="Query"
CREATE TABLE table(
    k UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = ngrams(2)))
ENGINE = MergeTree()
ORDER BY k;

SHOW CREATE TABLE table;
```

```result title="Response"
┌─statement──────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.table                                            ↴│
│↳(                                                                     ↴│
│↳    `k` UInt64,                                                       ↴│
│↳    `s` String,                                                       ↴│
│↳    INDEX idx s TYPE text(tokenizer = ngrams(2)) GRANULARITY 100000000↴│ <-- here
│↳)                                                                     ↴│
│↳ENGINE = MergeTree                                                    ↴│
│↳ORDER BY k                                                            ↴│
│↳SETTINGS index_granularity = 8192                                      │
└────────────────────────────────────────────────────────────────────────┘
```

Очень большая гранулярность индекса гарантирует, что текстовый индекс создаётся для всей части данных.
Явно указанная гранулярность индекса не учитывается.

<div id="using-a-text-index">
  ## Использование текстового индекса
</div>

Использовать текстовый индекс в запросах SELECT просто: распространённые функции поиска по строкам автоматически задействуют индекс.
Если для столбца или части таблицы индекс отсутствует, функции поиска по строкам будут переключаться на медленный полный перебор.

:::note
Мы рекомендуем использовать функции `hasAnyTokens` и `hasAllTokens` для поиска по текстовому индексу; см. [ниже](#functions-example-hasanytokens-hasalltokens).
Эти функции работают со всеми доступными токенизаторами и любыми возможными выражениями препроцессора и постпроцессора.
Поскольку другие поддерживаемые функции исторически появились раньше текстового индекса, во многих случаях им пришлось сохранить прежнее поведение (например, без поддержки препроцессора или постпроцессора).
:::

<div id="functions-support">
  ### Поддерживаемые функции
</div>

Текстовый индекс можно использовать, если в секции `WHERE` или секциях `PREWHERE` используются текстовые функции:

```sql
SELECT [...]
FROM [...]
WHERE string_search_function(column_with_text_index)
```

<div id="functions-example-equals">
  #### `=`
</div>

`=` ([equals](/ru/sql-reference/functions/comparison-functions.md/#equals)) выполняет поиск по точному совпадению со всем указанным поисковым запросом.

Пример:

```sql
SELECT * from table WHERE str = 'Hello';
```

<div id="functions-example-in">
  #### `IN`
</div>

`IN` ([in](/ru/sql-reference/functions/in-functions)) похожа на `equals`, но сопоставляет все поисковые термины.

Пример:

```sql
SELECT * from table WHERE str IN ('Hello', 'World');
```

:::note
Текстовый индекс не поддерживает `NOT IN` (`notIn`).
:::

<div id="functions-example-like-match">
  #### `LIKE` and `match`
</div>

:::note
В настоящее время эти функции используют текстовый индекс для фильтрации только в том случае, если токенизатор индекса — `splitByNonAlpha`, `ngrams` или `sparseGrams`.
:::

:::note
`NOT LIKE` (`notLike`) не поддерживается текстовым индексом.
:::

Чтобы использовать `LIKE` ([like](/ru/sql-reference/functions/string-search-functions.md/#like)) и функцию [match](/ru/sql-reference/functions/string-search-functions.md/#match) с текстовыми индексами, ClickHouse должен уметь извлекать полные токены из поискового выражения.
Для индекса с токенизатором `ngrams` это возможно, если длина искомых строк между подстановочными шаблонами равна или превышает длину n-граммы.

Пример для текстового индекса с токенизатором `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE comment LIKE 'support%';
```

`support` в примере может соответствовать `support`, `supports`, `supporting` и т. д.
Такой запрос является поиском по подстроке, и его нельзя ускорить с помощью текстового индекса.

Чтобы задействовать текстовый индекс для LIKE-запросов, шаблон LIKE нужно переписать следующим образом:

```sql
SELECT count() FROM table WHERE comment LIKE ' support %'; -- or `% support %`
```

Пробелы слева и справа от `support` гарантируют, что этот термин можно извлечь как токен.

К счастью, есть особый случай, когда ClickHouse может использовать инвертированный индекс, чтобы значительно ускорить запросы LIKE.

Подробности см. в [разделе о настройке производительности LIKE/ILIKE](#like-ilike-queries-perf).

<div id="functions-example-multisearchany-multimatchany">
  #### `multiSearchAny` and `multiMatchAny`
</div>

[multiSearchAny](/ru/sql-reference/functions/string-search-functions.md/#multiSearchAny) и его вариант для UTF-8 [multiSearchAnyUTF8](/ru/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8) проверяют, встречается ли в строке хотя бы одна из нескольких буквальных подстрок, а [multiMatchAny](/ru/sql-reference/functions/string-search-functions.md/#multiMatchAny) проверяет, соответствует ли строка хотя бы одному из нескольких регулярных выражений.
Эти функции используют текстовый индекс при тех же условиях, что и `LIKE` и `match` (см. выше): ClickHouse должен иметь возможность извлечь полные токены из каждой искомой подстроки, а список искомых подстрок должен быть константным.
Гранула считывается, если в ней может присутствовать хотя бы одна искомая подстрока.

Для `multiMatchAny`, если отдельный шаблон регулярного выражения нельзя свести к требованию по токену (например, `.*`, который соответствует любому document), текстовый индекс использовать нельзя, и запрос переходит к полному сканированию.

Как и в случае с `LIKE` и `match`, поиск по подстрокам и регулярным выражениям лучше всего работает с токенизаторами `ngrams` и `sparseGrams`.
Эти токенизаторы индексируют перекрывающиеся символьные n-граммы, поэтому искомая подстрока раскладывается на n-граммы, которые присутствуют в индексе везде, где эта подстрока встречается, независимо от того, начинается она или заканчивается в середине слова.
Поэтому искомую подстроку можно использовать как есть, если она не короче размера n-граммы.

Пример текстового индекса с токенизатором `ngrams`:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, ['clickhouse', 'support']);
```

В отличие от него, токенизатор `splitByNonAlpha` индексирует только полные токены (целые слова).
Поскольку искомая подстрока может начинаться или заканчиваться в середине слова, ClickHouse отбрасывает начальный и конечный токены каждой такой подстроки, поэтому индекс может отсекать гранулы только по полным токенам.
Чтобы при поиске подстрок и по регулярным выражениям использовался индекс с `splitByNonAlpha`, окружайте каждую искомую подстроку символами-разделителями (например, пробелами), чтобы она образовывала один или несколько полных токенов.

Пример для текстового индекса с токенизатором `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, [' clickhouse ', ' support ']);
```

<div id="functions-example-startswith-endswith">
  #### `startsWith` и `endsWith`
</div>

Подобно `LIKE`, функции [startsWith](/ru/sql-reference/functions/string-functions.md/#startsWith) и [endsWith](/ru/sql-reference/functions/string-functions.md/#endsWith) могут использовать текстовый индекс только в том случае, если из поискового запроса можно извлечь полные токены.
Для индекса с токенизатором `ngrams` это возможно, если длина искомых строк между подстановочными шаблонами равна длине n-граммы или больше неё.
Если текстовый индекс использует постпроцессор, эти функции по-прежнему могут использовать индекс в режиме Hint, если извлечённые токены-подсказки после нормализации остаются непустыми. Если нормализация удаляет все токены-подсказки, индекс для этого предиката не используется.

Пример для текстового индекса с токенизатором `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE startsWith(comment, 'clickhouse support');
```

В этом примере токеном считается только `clickhouse`.
`support` не считается токеном, потому что может совпадать с `support`, `supports`, `supporting` и т. д.

Чтобы найти все строки, начинающиеся с `clickhouse supports`, добавьте в конец шаблона поиска пробел:

```sql
startsWith(comment, 'clickhouse supports ')`
```

Аналогично, `endsWith` следует использовать с ведущим пробелом:

```sql
SELECT count() FROM table WHERE endsWith(comment, ' olap engine');
```

<div id="functions-example-hastoken">
  #### `hasToken`
</div>

:::note
У `hasToken` есть определённые ограничения при использовании для lookup в текстовых индексах с токенизаторами, отличными от `splitByNonAlpha`, и/или с выражениями предобработчика/постпроцессора.
Вместо неё рекомендуется использовать `hasAnyTokens` и `hasAllTokens`.

Регистронезависимые варианты `hasTokenCaseInsensitive` и `hasTokenCaseInsensitiveOrNull` не учитывают текстовый индекс — они всегда выполняются как полное сканирование всех строк, даже для столбцов с текстовым индексом. Для регистронезависимого сопоставления используйте предобработчик или постпроцессор `lower(...)` в сочетании с `hasToken` / `hasAllTokens` / `hasAnyTokens`.
:::

Функция [hasToken](/ru/sql-reference/functions/string-search-functions.md/#hasToken) выполняет сопоставление с одним заданным токеном.

В отличие от ранее упомянутых функций, она не токенизирует поисковый термин (предполагается, что входное значение — это один токен).

Пример:

```sql
SELECT count() FROM table WHERE hasToken(comment, 'clickhouse');
```

<div id="functions-example-hasanytokens-hasalltokens">
  #### `hasAnyTokens` and `hasAllTokens`
</div>

Функции [hasAnyTokens](/ru/sql-reference/functions/string-search-functions.md/#hasAnyTokens) и [hasAllTokens](/ru/sql-reference/functions/string-search-functions.md/#hasAllTokens) выполняют поиск по любому из указанных токенов или по всем токенам.

Эти две функции принимают поисковые токены либо в виде строки, которая будет разбита на токены с помощью того же токенизатора, что и для индексного столбца, либо в виде массива уже обработанных токенов, к которым перед поиском токенизация применяться не будет.
Дополнительные сведения см. в документации по функциям.

Пример:

```sql
-- Search tokens passed as string argument
SELECT count() FROM table WHERE hasAnyTokens(comment, 'clickhouse olap');
SELECT count() FROM table WHERE hasAllTokens(comment, 'clickhouse olap');

-- Search tokens passed as Array(String)
SELECT count() FROM table WHERE hasAnyTokens(comment, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAllTokens(comment, ['clickhouse', 'olap']);
```

<div id="functions-example-hasphrase">
  #### `hasPhrase`
</div>

Функция [hasPhrase](/ru/sql-reference/functions/string-search-functions.md/#hasPhrase) выполняет поиск по фразе: все токены должны идти подряд и в том же порядке, что и в поисковой строке.

В отличие от `hasAllTokens`, для которой достаточно, чтобы все токены где-то присутствовали, `hasPhrase` требует, чтобы они образовывали непрерывную последовательность.
Поисковая фраза токенизируется тем же токенизатором, который настроен для столбца с индексом.
Если текстовый индекс использует постпроцессор, поисковая фраза также нормализуется перед обращением к индексу.
Обратите внимание, что функция требует один из следующих токенизаторов: `splitByNonAlpha`, `splitByString`, `ngrams` или `asciiCJK`.

Пример:

```sql
-- Matches: 'clickhouse' and 'olap' must appear consecutively in that order
SELECT count() FROM table WHERE hasPhrase(comment, 'clickhouse olap');

-- Does NOT match a row containing 'olap clickhouse' (wrong order)
-- Does NOT match a row containing 'clickhouse fast olap' (non-consecutive)
```

<div id="functions-example-has">
  #### `has`
</div>

Функция для массивов [has](/ru/sql-reference/functions/array-functions#has) выполняет поиск одного токена в массиве строк.

Пример:

```sql
SELECT count() FROM table WHERE has(array, 'clickhouse');
```

<div id="functions-example-hasany-hasall">
  #### `hasAny` и `hasAll`
</div>

Функции для работы с массивами [hasAny](/ru/sql-reference/functions/array-functions#hasAny) и [hasAll](/ru/sql-reference/functions/array-functions#hasAll) проверяют, содержит ли индексируемый столбец типа Array какие-либо или все строки needle из константного набора.

Пример:

```sql
SELECT count() FROM table WHERE hasAny(tags, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAll(tags, ['clickhouse', 'olap']);
```

<div id="functions-example-mapcontains">
  #### `mapContains`
</div>

Функция [mapContains](/ru/sql-reference/functions/tuple-map-functions#mapContainsKey) (псевдоним `mapContainsKey`) ищет совпадения среди токенов, извлечённых из искомой строки, по ключам map.
Поведение аналогично функции `equals` для столбца `String`.
Текстовый индекс используется только в том случае, если он был создан для выражения `mapKeys(map)`.

Пример:

```sql
SELECT count() FROM table WHERE mapContainsKey(map, 'clickhouse');
-- OR
SELECT count() FROM table WHERE mapContains(map, 'clickhouse');
```

<div id="functions-example-mapcontainsvalue">
  #### `mapContainsValue`
</div>

Функция [mapContainsValue](/ru/sql-reference/functions/tuple-map-functions#mapContainsValue) ищет совпадения с токенами, извлечёнными из искомой строки, в значениях map.
Поведение аналогично функции `equals` для столбца `String`.
Текстовый индекс используется только в том случае, если он был создан для выражения `mapValues(map)`.

Пример:

```sql
SELECT count() FROM table WHERE mapContainsValue(map, 'clickhouse');
```

<div id="functions-example-mapcontainslike">
  #### `mapContainsKeyLike` и `mapContainsValueLike`
</div>

Функции [mapContainsKeyLike](/ru/sql-reference/functions/tuple-map-functions#mapContainsKeyLike) и [mapContainsValueLike](/ru/sql-reference/functions/tuple-map-functions#mapContainsValueLike) проверяют соответствие шаблону всех ключей или значений (соответственно) в Map.

Пример:

```sql
SELECT count() FROM table WHERE mapContainsKeyLike(map, '% clickhouse %');
SELECT count() FROM table WHERE mapContainsValueLike(map, '% clickhouse %');
```

<div id="functions-example-access-operator">
  #### `operator[]`
</div>

Оператор доступа [operator[]](/ru/sql-reference/operators#access-operators) можно использовать вместе с текстовым индексом для фильтрации ключей и значений. Текстовый индекс используется только если он создан для выражений `mapKeys(map)` или `mapValues(map)`, либо для обоих сразу.

Пример:

```sql
SELECT count() FROM table WHERE map['engine'] = 'clickhouse';
```

См. следующие примеры использования столбцов типа `Array(T)` и `Map(K, V)` с текстовым индексом.

<div id="text-index-example-array">
  ### Индексирование столбцов Array(String)
</div>

Представьте платформу для ведения блогов, где авторы помечают свои записи ключевыми словами.
Мы хотим, чтобы пользователи находили связанный контент через поиск или по клику на топики.

Рассмотрим следующее определение таблицы:

```sql
CREATE TABLE posts
(
    post_id UInt64,
    title String,
    content String,
    keywords Array(String)
)
ENGINE = MergeTree
ORDER BY (post_id);
```

Без текстового индекса для поиска постов по определённому ключевому слову (например, `clickhouse`) требуется просканировать все записи:

```sql
SELECT count() FROM posts WHERE has(keywords, 'clickhouse'); -- slow full-table scan - checks every keyword in every post
```

По мере роста платформы это работает всё медленнее, поскольку запросу приходится проверять массив `keywords` в каждой строке.
Чтобы устранить эту проблему с производительностью, определим текстовый индекс для столбца `keywords`:

```sql
ALTER TABLE posts ADD INDEX keywords_idx(keywords) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE posts MATERIALIZE INDEX keywords_idx; -- Don't forget to rebuild the index for existing data
```

<div id="text-index-example-map">
  ### Индексация столбцов Map
</div>

Во многих сценариях обсервабилити сообщения лога разбиваются на &quot;компоненты&quot; и сохраняются в подходящих типах данных, например дата-время для временной метки, enum для уровня лога и т. д.
Поля метрик лучше всего хранить как пары ключ-значение.
Командам эксплуатации необходимо эффективно искать по журналам данные для отладки, расследования инцидентов безопасности и мониторинга.

Рассмотрим эту таблицу журналов:

```sql
CREATE TABLE logs
(
    id UInt64,
    timestamp DateTime,
    message String,
    attributes Map(String, String)
)
ENGINE = MergeTree
ORDER BY (timestamp);
```

Без текстового индекса для поиска по данным [Map](/ru/sql-reference/data-types/map.md) требуется полное сканирование таблицы:

```sql
-- Finds all logs with rate limiting data:
SELECT * FROM logs WHERE has(mapKeys(attributes), 'rate_limit'); -- slow full-table scan

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- slow full-table scan
```

По мере роста объёма журналов эти запросы начинают выполняться медленно.

Решение — создать текстовый индекс для ключей и значений [Map](/ru/sql-reference/data-types/map.md).
Используйте [mapKeys](/ru/sql-reference/functions/tuple-map-functions.md/#mapKeys), чтобы создать текстовый индекс, если нужно находить записи журнала по именам полей или типам атрибутов:

```sql
ALTER TABLE logs ADD INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_keys_idx;
```

Используйте [mapValues](/ru/sql-reference/functions/tuple-map-functions.md/#mapValues), чтобы создать текстовый индекс, если вам нужно выполнять поиск по содержимому атрибутов:

```sql
ALTER TABLE logs ADD INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_vals_idx;
```

Примеры запросов:

```sql
-- Find all rate-limited requests:
SELECT * FROM logs WHERE mapContainsKey(attributes, 'rate_limit'); -- fast

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- fast

-- Finds all logs where any attribute includes an error:
SELECT * FROM logs WHERE mapContainsValueLike(attributes, '% error %'); -- fast
```

<div id="text-index-example-json">
  ### Индексация JSON-столбцов
</div>

Текстовые индексы можно использовать с `JSON`-столбцами тремя способами:

1. **Индексы для конкретных подстолбцов** — создайте текстовый индекс для известного JSON-пути, как для обычного столбца. При этом индексируются *значения* по этому пути.
2. **Индексы по путям с [JSONAllPaths](/ru/sql-reference/functions/json-functions.md/#JSONAllPaths)** — индексируют *все пути*, присутствующие в каждой грануле, чтобы пропускать гранулы, которые заведомо не могут содержать запрашиваемый путь. Аналогично столбцам `Map`.
3. **Индексы по значениям с [JSONAllValues](/ru/sql-reference/functions/json-functions.md#JSONAllValues)** — индексируют *все значения* по всем JSON-путям, чтобы ускорить полнотекстовый поиск по любому подстолбцу JSON с помощью одного индекса.

<div id="json-indexes-on-subcolumns">
  #### Индексы для определённых подстолбцов
</div>

Вы можете создать индекс пропуска данных для любого подстолбца JSON, используя тот же синтаксис, что и для обычных столбцов.

Есть два способа сослаться на подстолбец JSON в выражении индекса:

* **Типизированный путь**, объявленный в подсказке типа JSON, — прямой доступ по имени: `json.a`.
* **Динамический путь** с явным приведением типа — используйте синтаксис приведения `::`: `json.b::String`.

Пример определения индекса:

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id String),
    INDEX idx_sensor data.sensor_id TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_location data.location::String TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', 'id_' || number , 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', 'id_' || number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

Пример запроса:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id = 'id_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: text
        Condition: (mode: All; tokens: ["5", "id"])
        Parts: 1/2
        Granules: 1/8
```

Пример запроса:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: text
        Condition: (mode: All; tokens: ["5", "room"])
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  #### Индексы по путям с JSONAllPaths
</div>

Как и для столбцов `Map`, для [JSON](/ru/sql-reference/data-types/newjson.md)-столбцов можно создавать текстовые индексы с помощью [`JSONAllPaths`](/ru/sql-reference/functions/json-functions.md/#JSONAllPaths).
Индекс хранит набор JSON-путей, присутствующих в каждой грануле, и использует их, чтобы пропускать гранулы, в которых искомый путь отсутствует.

Пример определения индекса:

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

Вы можете использовать `EXPLAIN indexes = 1`, чтобы убедиться, что используется индекс пропуска данных.
Когда путь существует только в одной части, индекс пропускает другую часть.

Пример:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["user.name"])
        Parts: 1/2
        Granules: 1/2
```

Если путь отсутствует во всех частях, все части и гранулы пропускаются.

Пример:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["nonexistent"])
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL` также использует индекс — он пропускает гранулы, в которых путь отсутствует (поскольку значение было бы `NULL`):

Пример:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["user.name"])
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallvalues">
  #### Индексы по значениям с JSONAllValues
</div>

Текстовые индексы можно использовать для ускорения поиска по [JSON](/ru/sql-reference/data-types/newjson.md)-столбцам с помощью функции [`JSONAllValues`](/ru/sql-reference/functions/json-functions.md#JSONAllValues).

`JSONAllValues` возвращает все значения из JSON-столбца в виде `Array(String)`.
Значения нестроковых типов данных (например, целые числа и массивы) преобразуются в текстовое представление.
Текстовый индекс, построенный с использованием `JSONAllValues`, индексирует эти текстовые представления по всем JSON-путям в каждой строке.
Такой индекс затем может ускорять запросы, которые фильтруют по отдельным подстолбцам JSON.
Когда запрос фильтрует по конкретному подстолбцу (например, `data.user_name = 'alice'`), текстовый индекс может быстро отсеивать строки (и гранулы), в которых искомые токены отсутствуют в каких-либо JSON-значениях.

:::note
Индекс может давать ложноположительные срабатывания, если одинаковые токены встречаются в разных JSON-путях.
Например, если строка 1 содержит `{"a": "hello", "b": "world"}`, а запрос ищет `data.a = 'world'`, текстовый индекс не может определить, что `world` относится к пути `b`, а не `a`.
В таких случаях индекс не отсеет строку, а окончательную проверку выполнит фильтр по фактическим данным столбца.
Это то же поведение, что и в других сценариях использования текстового индекса, где индекс выступает как быстрый предварительный фильтр.
:::

<div id="json-all-values-creating-the-index">
  ##### Создание индекса
</div>

Пример определения индекса:

```sql
CREATE TABLE events
(
    id UInt64,
    data JSON,
    INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="json-all-values-supported-query-patterns">
  ##### Поддерживаемые шаблоны запросов
</div>

После создания индекс может ускорять запросы к подстолбцам JSON с использованием тех же функций, что и для столбцов `String`, а также функции `equals` для всех столбцов.

Доступ к подстолбцам:

```sql
SELECT * FROM events WHERE data.user_name = 'alice';
SELECT * FROM events WHERE data.message LIKE '% error %';
SELECT * FROM events WHERE startsWith(data.status, 'fail');
SELECT * FROM events WHERE hasToken(data.title, 'clickhouse');
```

Доступ к подстолбцу с явным `CAST`:

```sql
SELECT * FROM events WHERE hasAllTokens(data.message::String, 'connection timeout');
SELECT * FROM events WHERE data.status_code::UInt64 = 404;
SELECT * FROM events WHERE has(data.tags::Array(String), 'bug')
```

Оператор `IN`:

```sql
SELECT * FROM events WHERE data.level IN ('error', 'critical');
```

<div id="text-index-phrase-search">
  ### Поиск по фразе
</div>

Например, обычный поиск по текстовому индексу

```sql
SELECT *
FROM tab
WHERE hasAllTokens(col, 'weather in Tokyo')
```

соответствует всем строкам, содержащим заданные токены в произвольном порядке.
В примере строка `While she stayed in Tokyo, the weather was great.` удовлетворяет условию фильтра.

Напротив, поиск по фразе предполагает совпадение токенов в заданном порядке.
Например,

```sql
SELECT *
FROM tab
WHERE hasPhrase(col, 'weather in Tokyo')
```

совпадает с любой строкой, содержащей последовательность токенов `weather in Tokyo`, например `How is the weather in Tokyo?`?

Текстовый индекс ускоряет поиск по фразам, находя пересечение списков вхождений для всех токенов фразы, чтобы определить гранулы-кандидаты.
Внутри этих гранул ClickHouse затем проверяет, что токены действительно идут подряд.
Этот процесс относительно затратен и работает медленнее, чем обычные запросы текстового поиска.
Чтобы ускорить запросы поиска по фразам, включите хранение позиций в текстовом индексе (см. `Optional parameters` выше).

`hasPhrase` можно использовать вместе с токенизаторами `splitByNonAlpha`, `splitByString`, `ngrams` и `asciiCJK`.
Указанная фраза токенизируется с помощью токенизатора индекса.
Символы-разделители во фразе игнорируются: `hasPhrase(text, 'quick+brown')` эквивалентно `hasPhrase(text, 'quick brown')`, если в качестве токенизатора используется `splitByNonAlpha`.

<div id="text-index-phrase-search-example">
  #### Пример
</div>

```sql
CREATE TABLE tab (
    id UInt32,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'weather in New York'),
    (2, 'New weather in York'),
    (3, 'weather in New Orleans');
```

```sql title="Query"
SELECT id, text FROM tab WHERE hasPhrase(text, 'weather in New York');
```

```result title="Response"
   ┌─id─┬─text────────────────┐
1. │  1 │ weather in New York │
   └────┴─────────────────────┘
```

Строка 2 (`'New weather in York'`) не совпадает, потому что токены идут в неправильном порядке.
Строка 3 (`'weather in New Orleans'`) не совпадает, потому что не содержит токена `'York'`.

<div id="performance-tuning">
  ## Настройка производительности
</div>

<div id="direct-read">
  ### Прямое чтение
</div>

Некоторые типы текстовых запросов можно существенно ускорить благодаря оптимизации под названием &quot;прямое чтение&quot;.

Пример:

```sql
SELECT column_a, column_b, ...
FROM [...]
WHERE string_search_function(column_with_text_index)
```

Оптимизация прямого чтения обрабатывает запрос, используя исключительно текстовый индекс (то есть поиск по текстовому индексу), без обращения к исходному текстовому столбцу.
Поиск по текстовому индексу считывает сравнительно мало данных и поэтому работает значительно быстрее, чем обычные индексы пропуска данных в ClickHouse (которые сначала выполняют поиск по индексу пропуска данных, а затем загружают и фильтруют оставшиеся гранулы).

Прямое чтение управляется двумя настройками:

* Настройка [query&#95;plan&#95;direct&#95;read&#95;from&#95;text&#95;index](../../../operations/settings/settings#query_plan_direct_read_from_text_index) (`true` по умолчанию), которая определяет, включено ли прямое чтение в целом.
* Настройка [use&#95;skip&#95;indexes&#95;on&#95;data&#95;read](../../../operations/settings/settings#use_skip_indexes_on_data_read) была необходимым условием для прямого чтения в версиях ClickHouse &lt; 26.4.

**Поддерживаемые функции**

Оптимизация прямого чтения поддерживает функции `hasToken`, `hasAllTokens` и `hasAnyTokens`.
Если текстовый индекс определён с токенизатором `array`, прямое чтение также поддерживается для функций `equals`, `has`, `hasAny`, `hasAll`, `mapContainsKey` и `mapContainsValue`.
Эти функции также можно комбинировать с помощью операторов `AND`, `OR` и `NOT`.
Секции `WHERE` или `PREWHERE` также могут содержать дополнительные фильтры, не связанные с функциями текстового поиска (для текстовых или других столбцов) — в этом случае оптимизация прямого чтения всё равно будет использоваться, но менее эффективно (она применяется только к поддерживаемым функциям текстового поиска).

Чтобы проверить, использует ли запрос прямое чтение, выполните его с `EXPLAIN PLAN actions = 1`.
В качестве примера рассмотрим запрос с отключённым прямым чтением

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 0, -- disable direct read
```

возвращает

```text
[...]
Filter ((WHERE + Change column names to column identifiers))
Filter column: hasToken(__table1.col, 'some_token'_String) (removed)
Actions: INPUT : 0 -> col String : 0
         COLUMN Const(String) -> 'some_token'_String String : 1
         FUNCTION hasToken(col :: 0, 'some_token'_String :: 1) -> hasToken(__table1.col, 'some_token'_String) UInt8 : 2
[...]
```

в то время как тот же запрос, выполненный с `query_plan_direct_read_from_text_index = 1`

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 1, -- enable direct read
```

возвращает

```text
[...]
Expression (Before GROUP BY)
Positions:
  Filter
  Filter column: __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 (removed)
  Actions: INPUT :: 0 -> __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 UInt8 : 0
[...]
```

Второй вывод EXPLAIN PLAN содержит виртуальный столбец `__text_index_<index_name>_<function_name>_<id>`.
Если этот столбец присутствует, значит используется прямое чтение.

Если предложение WHERE содержит только функции текстового поиска, запрос может полностью избежать чтения данных столбца и получить максимальный прирост производительности за счет прямого чтения.
Однако даже если к текстовому столбцу обращаются и в других частях запроса, прямое чтение все равно даст прирост производительности.

**Прямое чтение как подсказка**

Прямое чтение как подсказка основано на тех же принципах, что и обычное прямое чтение, но дополнительно добавляет фильтр, построенный на основе данных текстового индекса, не исключая при этом исходный текстовый столбец.
Оно используется для функций, для которых чтение только из текстового индекса приводило бы к ложноположительным срабатываниям.

Поддерживаются следующие функции: `like`, `startsWith`, `endsWith`, `equals`, `has`, `hasPhrase`, `mapContainsKey` и `mapContainsValue`.

Дополнительный фильтр может повысить избирательность и в сочетании с другими фильтрами сильнее ограничить результирующий набор, помогая уменьшить объем данных, считываемых из других столбцов.

Прямое чтение как подсказка управляется настройкой [query&#95;plan&#95;text&#95;index&#95;add&#95;hint](../../../operations/settings/settings#query_plan_text_index_add_hint) (включена по умолчанию).

Пример запроса без подсказки:

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE (col LIKE '%some-token%') AND (d >= today())
SETTINGS query_plan_text_index_add_hint = 0
FORMAT TSV
```

возвращает

```text
[...]
Prewhere filter column: and(like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

в то время как тот же запрос, выполненный с `query_plan_text_index_add_hint = 1`

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE col LIKE '%some-token%'
SETTINGS query_plan_text_index_add_hint = 1
```

возвращает

```text
[...]
Prewhere filter column: and(__text_index_idx_col_like_d306f7c9c95238594618ac23eb7a3f74, like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

Во втором выводе EXPLAIN PLAN видно, что в условие фильтрации добавлен дополнительный конъюнкт (`__text_index_...`).
Благодаря оптимизации [PREWHERE](/ru/sql-reference/statements/select/prewhere) условие фильтрации разбивается на три отдельных конъюнкта, которые применяются в порядке возрастания вычислительной сложности.
Для этого запроса порядок применения такой: сначала `__text_index_...`, затем `greaterOrEquals(...)`, и наконец `like(...)`.
Такой порядок позволяет пропускать ещё больше гранул данных, чем текстовый индекс и исходный фильтр по отдельности, до чтения тяжёлых столбцов, используемых в запросе после условия `WHERE`, что дополнительно уменьшает объём читаемых данных.

<div id="like-ilike-queries-perf">
  ### Запросы LIKE/ILIKE
</div>

Если шаблон запроса LIKE/ILIKE имеет вид `%<буквенно-цифровые-символы-без-пробелов>%`, а токенизатор текстового индекса — `splitByNonAlpha` или `array`, ClickHouse использует инвертированный индекс, чтобы значительно ускорить запросы LIKE/ILIKE. Для этого ClickHouse сканирует словарь инвертированного индекса вместо полного сканирования таблицы, чтобы найти совпадающий шаблон.

Когда эта оптимизация включена, запросы LIKE/ILIKE должны выполняться значительно быстрее, чем при полном сканировании таблицы. Однако если шаблон совпадает с большинством токенов в словаре, производительность может быть ниже, чем при полном сканировании таблицы. К счастью, чтобы этого избежать, предусмотрен механизм fallback.

Оптимизация управляется настройкой:

* [use&#95;text&#95;index&#95;like&#95;evaluation&#95;by&#95;dictionary&#95;scan](../../../operations/settings/settings#use_text_index_like_evaluation_by_dictionary_scan)

Механизм fallback управляется двумя настройками:

* [text&#95;index&#95;like&#95;min&#95;pattern&#95;length](../../../operations/settings/settings#text_index_like_min_pattern_length)
* [text&#95;index&#95;like&#95;max&#95;postings&#95;to&#95;read](../../../operations/settings/settings#text_index_like_max_postings_to_read)

Эта оптимизация поддерживает только функции `like` и `ilike`.

<div id="caching">
  ### Кэширование
</div>

Существуют различные кэши на уровне сервера, которые кэшируют в памяти части текстового индекса (см. раздел [Подробности реализации](#implementation)):
В настоящее время доступны кэши для десериализованных заголовков, токенов и списков вхождений текстового индекса, чтобы сократить I/O.
Используйте настройки [use&#95;text&#95;index&#95;header&#95;cache](/ru/operations/settings/settings#use_text_index_header_cache), [use&#95;text&#95;index&#95;tokens&#95;cache](/ru/operations/settings/settings#use_text_index_tokens_cache) и [use&#95;text&#95;index&#95;postings&#95;cache](/ru/operations/settings/settings#use_text_index_postings_cache), чтобы отключить для запросов чтение из отдельных кэшей и запись в них.

Чтобы очистить кэши, используйте оператор [SYSTEM CLEAR TEXT INDEX CACHES](../../../sql-reference/statements/system#drop-text-index-caches)

Чтобы настроить кэши, см. следующие настройки сервера.

<div id="caching-tokens">
  #### Настройки кэша токенов
</div>

| Setting                                                                                                                                             | Description                                                                                   |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;tokens&#95;cache&#95;policy](/ru/operations/server-configuration-parameters/settings#text_index_tokens_cache_policy)               | Имя политики кэша токенов текстового индекса.                                                 |
| [text&#95;index&#95;tokens&#95;cache&#95;size](/ru/operations/server-configuration-parameters/settings#text_index_tokens_cache_size)                   | Максимальный размер кэша в байтах.                                                            |
| [text&#95;index&#95;tokens&#95;cache&#95;max&#95;entries](/ru/operations/server-configuration-parameters/settings#text_index_tokens_cache_max_entries) | Максимальное количество десериализованных токенов в кэше.                                     |
| [text&#95;index&#95;tokens&#95;cache&#95;size&#95;ratio](/ru/operations/server-configuration-parameters/settings#text_index_tokens_cache_size_ratio)   | Размер защищённой очереди в кэше токенов текстового индекса относительно общего размера кэша. |

<div id="caching-header">
  #### Настройки кэша заголовков текстового индекса
</div>

| Параметр                                                                                                                                            | Описание                                                                                         |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| [text&#95;index&#95;header&#95;cache&#95;policy](/ru/operations/server-configuration-parameters/settings#text_index_header_cache_policy)               | Имя политики кэша заголовков текстового индекса.                                                 |
| [text&#95;index&#95;header&#95;cache&#95;size](/ru/operations/server-configuration-parameters/settings#text_index_header_cache_size)                   | Максимальный размер кэша в байтах.                                                               |
| [text&#95;index&#95;header&#95;cache&#95;max&#95;entries](/ru/operations/server-configuration-parameters/settings#text_index_header_cache_max_entries) | Максимальное количество десериализованных заголовков в кэше.                                     |
| [text&#95;index&#95;header&#95;cache&#95;size&#95;ratio](/ru/operations/server-configuration-parameters/settings#text_index_header_cache_size_ratio)   | Размер защищённой очереди в кэше заголовков текстового индекса относительно общего размера кэша. |

<div id="caching-posting-lists">
  #### Настройки кэша списков вхождений
</div>

| Настройка                                                                                                                                               | Описание                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;postings&#95;cache&#95;policy](/ru/operations/server-configuration-parameters/settings#text_index_postings_cache_policy)               | Имя политики кэша списков вхождений текстового индекса.                                                 |
| [text&#95;index&#95;postings&#95;cache&#95;size](/ru/operations/server-configuration-parameters/settings#text_index_postings_cache_size)                   | Максимальный размер кэша в байтах.                                                                      |
| [text&#95;index&#95;postings&#95;cache&#95;max&#95;entries](/ru/operations/server-configuration-parameters/settings#text_index_postings_cache_max_entries) | Максимальное количество десериализованных списков вхождений в кэше.                                     |
| [text&#95;index&#95;postings&#95;cache&#95;size&#95;ratio](/ru/operations/server-configuration-parameters/settings#text_index_postings_cache_size_ratio)   | Размер защищённой очереди в кэше списков вхождений текстового индекса относительно общего размера кэша. |

<div id="limitations">
  ## Ограничения
</div>

На данный момент у текстового индекса есть следующие ограничения:

* Материализация текстовых индексов с большим количеством токенов (например, 10 миллиардов токенов) может потреблять значительный объём памяти. Материализация текстового
  индекса может происходить как напрямую (`ALTER TABLE <table> MATERIALIZE INDEX <index>`), так и косвенно — при слиянии частей.
* Материализовать текстовые индексы на частях, содержащих более 4.294.967.296 (= 2^32 = примерно 4,2 миллиарда) строк, невозможно. Без материализованного текстового индекса запросы переключаются на медленный brute-force поиск внутри части. Для оценки наихудшего случая предположим, что часть содержит один столбец типа String, а настройка MergeTree `max_bytes_to_merge_at_max_space_in_pool` (по умолчанию: 150 GB) не изменялась. В этом случае такая ситуация возникает, если в столбце в среднем менее 29,5 символа на строку. На практике таблицы также содержат другие столбцы, и этот порог во много раз ниже (в зависимости от количества, типа и размера других столбцов).

<div id="text-index-vs-bloom-filter-indexes">
  ## Текстовые индексы и индексы на основе фильтра Блума
</div>

Предикаты для строковых значений можно ускорить с помощью текстовых индексов и индексов на основе фильтра Блума (тип индекса `bloom_filter`, `ngrambf_v1`, `tokenbf_v1`, `sparse_grams`), однако по своей архитектуре и предполагаемым сценариям использования они принципиально различаются:

**Индексы на основе фильтра Блума**

* Основаны на вероятностных структурах данных, которые могут давать ложноположительные срабатывания.
* Могут отвечать только на вопросы о принадлежности множеству, то есть столбец может содержать токен X или точно не содержит X.
* Хранят информацию на уровне гранул, что позволяет пропускать крупные диапазоны при выполнении запроса.
* Их сложно правильно настроить (пример см. [здесь](mergetree#n-gram-bloom-filter)).
* Они довольно компактны (от нескольких килобайт до нескольких мегабайт на часть).

**Текстовые индексы**

* Строят детерминированный инвертированный индекс по токенам. Сам индекс не может давать ложноположительных срабатываний.
* Специально оптимизированы для рабочих нагрузок текстового поиска.
* Хранят информацию на уровне строки, что обеспечивает эффективный поиск терминов.
* Они довольно велики (от десятков до сотен мегабайт на часть).

Индексы на основе фильтра Блума поддерживают полнотекстовый поиск лишь как «побочный эффект»:

* Они не поддерживают расширенную токенизацию и предварительную обработку.
* Они не поддерживают поиск по нескольким токенам.
* Они не обеспечивают характеристик производительности, ожидаемых от инвертированного индекса.

Текстовые индексы, напротив, изначально предназначены для полнотекстового поиска:

* Они обеспечивают токенизацию и предварительную обработку
* Они эффективно поддерживают `hasAllTokens`, `LIKE`, `match` и аналогичные функции текстового поиска.
* Они обладают значительно лучшей масштабируемостью для больших текстовых корпусов.

<div id="implementation">
  ## Подробности реализации
</div>

Каждый текстовый индекс состоит из двух (абстрактных) структур данных:

* словаря, который сопоставляет каждому токену список вхождений, и
* набора списков вхождений, каждый из которых представляет собой набор номеров строк.

Текстовый индекс строится для всей части.
В отличие от других индексов пропуска данных, текстовый индекс при слиянии частей данных можно объединять, а не перестраивать заново (см. ниже).

При создании индекса для каждой части создаются три файла:

**Файл блоков словаря (.dct)**

Токены в текстовом индексе сортируются и сохраняются в блоках словаря по 512 токенов в каждом (размер блока настраивается параметром `dictionary_block_size`).
Файл блоков словаря (.dct) содержит все блоки словаря для всех гранул индекса в части.

**Файл заголовка индекса (.idx)**

Файл заголовка индекса содержит для каждого блока словаря первый токен блока и его относительное смещение в файле блоков словаря.

Эта структура разреженного индекса похожа на [разреженный индекс первичного ключа](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes)) в ClickHouse.

**Файл списков вхождений (.pst)**

Списки вхождений для всех токенов последовательно записываются в файл списков вхождений.
Чтобы экономить место и при этом сохранять высокую скорость операций пересечения и объединения, списки вхождений хранятся в виде [roaring bitmaps](https://roaringbitmap.org/).
Если список вхождений больше `posting_list_block_size`, он разбивается на несколько блоков, которые последовательно записываются в файл списков вхождений.

**Файл позиций (.pos)**

Необязательный, только если аргумент индекса `positions = 1`.
Хранит позиции токенов в совпадающих строках.

**Слияние текстовых индексов**

При слиянии частей данных текстовый индекс не нужно перестраивать с нуля; вместо этого его можно эффективно объединить на отдельном этапе процесса слияния.
На этом этапе отсортированные словари текстовых индексов каждой входной части считываются и объединяются в новый общий словарь.
Номера строк в списках вхождений также пересчитываются, чтобы отразить их новые позиции в слитой части данных, с использованием сопоставления старых и новых номеров строк, которое создаётся на начальной фазе слияния.
Этот способ слияния текстовых индексов аналогичен тому, как объединяются [проекции](/ru/docs/sql-reference/statements/alter/projection#projection-indexes) со столбцом `_part_offset`.
Если индекс не материализован в исходной части, он строится, записывается во временный файл, а затем объединяется вместе с индексами из других частей и из других временных файлов индекса.

**Отладка**

Табличную функцию [mergeTreeTextIndex](../../../sql-reference/table-functions/mergeTreeTextIndex.md) можно использовать для анализа текстовых индексов.

<div id="hacker-news-dataset">
  ## Пример: набор данных Hacker News
</div>

Давайте посмотрим, как текстовые индексы повышают производительность на большом наборе данных с большим объемом текста.
Мы будем использовать 28,7 млн строк комментариев с популярного сайта Hacker News.
Вот таблица без текстового индекса:

```sql
CREATE TABLE hackernews (
    id UInt64,
    deleted UInt8,
    type String,
    author String,
    timestamp DateTime,
    comment String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    children Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32
)
ENGINE = MergeTree
ORDER BY (type, author);
```

28,7 млн строк хранятся в файле Parquet в S3 — давайте вставим их в таблицу `hackernews`:

```sql
INSERT INTO hackernews
    SELECT * FROM s3Cluster(
        'default',
        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.parquet',
        'Parquet',
        '
    id UInt64,
    deleted UInt8,
    type String,
    by String,
    time DateTime,
    text String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    kids Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32');
```

Мы будем использовать `ALTER TABLE`, добавим текстовый индекс на столбец comment, а затем материализуем его:

```sql
-- Add the index
ALTER TABLE hackernews ADD INDEX comment_idx comment TYPE text(tokenizer = splitByNonAlpha);

-- Materialize the index for existing data
ALTER TABLE hackernews MATERIALIZE INDEX comment_idx SETTINGS mutations_sync = 2;
```

Теперь выполним запросы с использованием функций `hasToken`, `hasAnyTokens` и `hasAllTokens`.
Следующие примеры наглядно покажут заметную разницу в производительности между стандартным сканированием индекса и оптимизацией прямого чтения.

<div id="using-hasToken">
  ### 1. Использование `hasToken`
</div>

`hasToken` проверяет, содержит ли текст конкретный токен.
Мы будем искать токен &#39;ClickHouse&#39; с учётом регистра.

**Прямое чтение отключено (стандартное сканирование)**
По умолчанию ClickHouse использует индекс пропуска данных, чтобы отфильтровать гранулы, а затем считывает данные столбца для этих гранул.
Мы можем смоделировать это поведение, отключив прямое чтение.

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│     516 │
└─────────┘

1 row in set. Elapsed: 0.362 sec. Processed 24.90 million rows, 9.51 GB
```

**Прямое чтение включено (быстрое чтение по индексу)**
Теперь выполним тот же запрос с включенным прямым чтением (это значение по умолчанию).

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│     516 │
└─────────┘

1 row in set. Elapsed: 0.008 sec. Processed 3.15 million rows, 3.15 MB
```

Запрос с прямым чтением более чем в 45 раз быстрее (0.362s против 0.008s) и обрабатывает значительно меньше данных (9.51 GB против 3.15 MB), поскольку читает данные только из индекса.

<div id="using-hasAnyTokens">
  ### 2. Использование `hasAnyTokens`
</div>

`hasAnyTokens` проверяет, содержит ли текст хотя бы один из указанных токенов.
Мы будем искать комментарии, содержащие &#39;love&#39; или &#39;ClickHouse&#39;.

**Прямое чтение отключено (стандартное сканирование)**

```sql
SELECT count()
FROM hackernews
WHERE hasAnyTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│  408426 │
└─────────┘

1 row in set. Elapsed: 1.329 sec. Processed 28.74 million rows, 9.72 GB
```

**Включено прямое чтение (быстрое чтение по индексу)**

```sql
SELECT count()
FROM hackernews
WHERE hasAnyTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│  408426 │
└─────────┘

1 row in set. Elapsed: 0.015 sec. Processed 27.99 million rows, 27.99 MB
```

Ускорение для этого распространённого поиска с &quot;OR&quot; ещё более впечатляющее.
Запрос выполняется почти в 89 раз быстрее (1.329s против 0.015s), поскольку удаётся избежать полного сканирования столбца.

<div id="using-hasAllTokens">
  ### 3. Использование `hasAllTokens`
</div>

`hasAllTokens` проверяет, содержит ли текст все указанные токены.
Мы будем искать комментарии, содержащие и &#39;love&#39;, и &#39;ClickHouse&#39;.

**Прямое чтение отключено (Standard scan)**
Даже при отключённом прямом чтении стандартный индекс пропуска данных всё равно работает эффективно.
Он сокращает 28.7M строк до всего 147.46K, но при этом всё равно должен прочитать 57.03 MB из столбца.

```sql
SELECT count()
FROM hackernews
WHERE hasAllTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│      11 │
└─────────┘

1 row in set. Elapsed: 0.184 sec. Processed 147.46 thousand rows, 57.03 MB
```

**Прямое чтение включено (быстрое чтение по индексу)**
Прямое чтение обрабатывает запрос по данным индекса, считывая всего 147.46 KB.

```sql
SELECT count()
FROM hackernews
WHERE hasAllTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│      11 │
└─────────┘

1 row in set. Elapsed: 0.007 sec. Processed 147.46 thousand rows, 147.46 KB
```

Для такого поиска с &quot;AND&quot; оптимизация прямого чтения работает более чем в 26 раз быстрее (0.184s против 0.007s), чем стандартное сканирование индекса пропуска данных.

<div id="compound-search">
  ### 4. Составной поиск: OR, AND, NOT, ...
</div>

Оптимизация прямого чтения также применяется к составным булевым выражениям.
Здесь мы выполним регистронезависимый поиск: &#39;ClickHouse&#39; OR &#39;clickhouse&#39;.

**Прямое чтение отключено (обычное сканирование)**

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse') OR hasToken(comment, 'clickhouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│     769 │
└─────────┘

1 row in set. Elapsed: 0.450 sec. Processed 25.87 million rows, 9.58 GB
```

**Включено прямое чтение (быстрое чтение по индексу)**

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse') OR hasToken(comment, 'clickhouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│     769 │
└─────────┘

1 row in set. Elapsed: 0.013 sec. Processed 25.87 million rows, 51.73 MB
```

За счёт объединения результатов из индекса запрос с прямым чтением выполняется в 34 раза быстрее (0.450s против 0.013s) и не требует чтения 9.58 GB данных столбца.
Для этого конкретного случая предпочтительнее и эффективнее использовать синтаксис `hasAnyTokens(comment, ['ClickHouse', 'clickhouse'])`.

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Объявляем о переходе полнотекстового поиска ClickHouse в статус General Availability](https://clickhouse.com/blog/full-text-search-ga-release)
* Блог: [Создание высокопроизводительного полнотекстового поиска для Объектного хранилища](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)
* Видео: [Введение в полнотекстовый поиск в ClickHouse](https://www.youtube.com/watch?v=9zPmf1a_heU)
* Видео: [Как устроен полнотекстовый поиск в ClickHouse: масштаб и скорость](https://www.youtube.com/watch?v=8JbqE_ubfkU)
* Презентация: [Полнотекстовый поиск в ClickHouse изнутри: быстрый, нативный и столбцовый](https://github.com/ClickHouse/clickhouse-presentations/blob/master/2025-tumuchdata-munich/ClickHouse_%20full-text%20search%20-%2011.11.2025%20Munich%20Database%20Meetup.pdf)
* Презентация: [Инвертированные индексы баз данных: зачем, что и как, FOSDEM 2026](https://presentations.clickhouse.com/2026-fosdem-inverted-index/Inverted_indexes_the_what_the_why_the_how.pdf)

**Устаревшие материалы**

* Блог: [Представляем инвертированные индексы в ClickHouse](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
* Блог: [Полнотекстовый поиск в ClickHouse изнутри: быстрый, нативный и столбцовый](https://clickhouse.com/blog/clickhouse-full-text-search)
* Видео: [Полнотекстовые индексы: проектирование и эксперименты](https://www.youtube.com/watch?v=O_MnyUkrIq8)