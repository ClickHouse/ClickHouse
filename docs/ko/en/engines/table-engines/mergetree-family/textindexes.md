---
description: '텍스트에서 검색어를 빠르게 찾습니다.'
keywords: ['전문 검색', '텍스트 인덱스', '인덱스', '인덱스']
sidebar_label: '텍스트 인덱스를 사용한 전문 검색'
slug: /engines/table-engines/mergetree-family/textindexes
title: '텍스트 인덱스를 사용한 전문 검색'
doc_type: 'reference'
---

텍스트 인덱스([역 인덱스](https://en.wikipedia.org/wiki/Inverted_index)라고도 함)를 사용하면 텍스트 데이터에 대해 빠른 전문 검색이 가능합니다.
텍스트 인덱스는 토큰과 각 토큰을 포함하는 행 번호 사이의 매핑을 저장합니다.
토큰은 토큰화라고 하는 과정을 통해 생성됩니다.
예를 들어, ClickHouse의 기본 토크나이저는 영어 문장 &quot;The cat likes mice.&quot;를 [&quot;The&quot;, &quot;cat&quot;, &quot;likes&quot;, &quot;mice&quot;] 토큰으로 변환합니다.

예를 들어, 단일 컬럼과 3개의 행이 있는 테이블을 가정합니다

```result
1: The cat likes mice.
2: Mice are afraid of dogs.
3: I have two dogs and a cat.
```

해당 토큰은 다음과 같습니다:

```result
1: The, cat, likes, mice
2: Mice, are, afraid, of, dogs
3: I, have, two, dogs, and, a, cat
```

일반적으로는 대소문자를 구분하지 않고 검색하므로, 토큰을 소문자로 변환합니다:

```result
1: the, cat, likes, mice
2: mice, are, afraid, of, dogs
3: i, have, two, dogs, and, a, cat
```

또한 &quot;I&quot;, &quot;the&quot;, &quot;and&quot;와 같은 불용어는 거의 모든 행에 나타나므로 제거합니다:

```result
1: cat, likes, mice
2: mice, afraid, dogs
3: have, two, dogs, cat
```

따라서 텍스트 인덱스에는 (개념적으로) 다음 정보가 포함됩니다:

```result
afraid : [2]
cat    : [1, 3]
dogs   : [2, 3]
have   : [3]
likes  : [1]
mice   : [1]
two    : [3]
```

검색 토큰이 주어지면 이 인덱스 구조를 통해 일치하는 모든 행을 빠르게 찾을 수 있습니다.

<div id="creating-a-text-index">
  ## 텍스트 인덱스 생성
</div>

텍스트 인덱스는 ClickHouse 버전 26.2 이상에서 일반 제공(GA)됩니다.
이 버전에서는 텍스트 인덱스를 사용하기 위해 별도 설정을 구성할 필요가 없습니다.
운영 환경의 사용 사례에는 ClickHouse 버전 &gt;= 26.2를 사용할 것을 강력히 권장합니다.

:::note
텍스트 인덱스는 [compatibility](../../../operations/settings/settings#compatibility) 설정과 관계없이 ClickHouse 버전 &gt;= 26.2에서 사용할 수 있습니다.
:::

텍스트 인덱스를 생성하려면 다음 구문을 사용하십시오:

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

텍스트 인덱스는 다음 타입의 컬럼에 정의할 수 있습니다.

* [String](/ko/sql-reference/data-types/string.md) 및 [FixedString](/ko/sql-reference/data-types/fixedstring.md)
* [Array(String)](/ko/sql-reference/data-types/array.md) 및 [Array(FixedString)](/ko/sql-reference/data-types/array.md)
* [맵](/ko/sql-reference/data-types/map.md)([mapKeys](/ko/sql-reference/functions/tuple-map-functions.md/#mapKeys) 및 [mapValues](/ko/sql-reference/functions/tuple-map-functions.md/#mapValues) 함수를 통해)
* [JSON](/ko/sql-reference/data-types/newjson.md)([JSONAllPaths](/ko/sql-reference/functions/json-functions.md/#JSONAllPaths) 및 [`JSONAllValues`](/ko/sql-reference/functions/json-functions.md#JSONAllValues) 함수를 통해)

[Nullable(T)](/ko/sql-reference/data-types/nullable.md) 및 [LowCardinality()](/ko/sql-reference/data-types/lowcardinality.md) 타입의 컬럼도 지원하며, `Array(Nullable(String or FixedString))`도 포함됩니다.

또는 기존 테이블에 텍스트 인덱스를 추가하려면 다음과 같이 하십시오.

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

기존 테이블에 인덱스를 추가했다면 기존 테이블 파트에도 인덱스를 구체화하는 것이 좋습니다(그렇지 않으면 인덱스가 없는 파트에 대한 검색은 느린 브루트 포스(전체 스캔) 방식으로 폴백됩니다).

```sql title="Query"
ALTER TABLE table MATERIALIZE INDEX text_idx SETTINGS mutations_sync = 2;
```

텍스트 인덱스를 제거하려면 다음을 실행하세요

```sql title="Query"
ALTER TABLE table DROP INDEX text_idx;
```

**토크나이저 인수(필수)**. `tokenizer` 인수로 사용할 토크나이저를 지정합니다:

* `splitByNonAlpha`는 영숫자가 아닌 ASCII 문자를 기준으로 문자열을 분할합니다(함수 [splitByNonAlpha](/ko/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha) 참고).
* `splitByString(S)`는 사용자 정의 구분 문자열 `S`를 기준으로 문자열을 분할합니다(함수 [splitByString](/ko/sql-reference/functions/splitting-merging-functions.md/#splitByString) 참고).
  구분자는 선택적 매개변수로 지정할 수 있습니다. 예를 들어 `tokenizer = splitByString([', ', '; ', '\n', '\\'])`와 같습니다.
  각 문자열은 여러 문자로 구성될 수 있습니다(예시의 `', '`).
  명시적으로 지정하지 않으면(예: `tokenizer = splitByString`) 기본 구분자 목록은 단일 공백 `[' ']`입니다.
* `asciiCJK`는 유니코드 단어 경계 규칙을 사용해 문자열을 토큰으로 분할합니다([Unicode Text Segmentation (UAX #29)](https://unicode.org/reports/tr29/)와 유사). ASCII 영숫자와 밑줄은 연결 문자를 포함해 토큰을 구성합니다(문자의 경우 ASCII `:`, 같은 유형의 문자 사이에서는 `.` 및 `'`). [CJK](https://en.wikipedia.org/wiki/CJK_characters) 문자를 포함한 비ASCII 유니코드 문자는 모두 단일 문자 토큰이 됩니다.
* `ngrams(N)`는 문자열을 동일한 크기의 `N`-그램으로 분할합니다(함수 [ngrams](/ko/sql-reference/functions/splitting-merging-functions.md/#ngrams) 참고).
  ngram 길이는 1에서 8 사이의 선택적 정수 매개변수로 지정할 수 있습니다. 예를 들어 `tokenizer = ngrams(3)`와 같습니다.
  명시적으로 지정하지 않으면(예: `tokenizer = ngrams`) 기본 ngram 크기는 3입니다.
* `sparseGrams(min_length, max_length, min_cutoff_length)`는 문자열을 최소 `min_length`자, 최대 `max_length`자(포함) 길이의 가변 길이 n-그램으로 분할합니다(함수 [sparseGrams](/ko/sql-reference/functions/string-functions#sparseGrams) 참고).
  명시적으로 지정하지 않으면 `min_length`와 `max_length`의 기본값은 각각 3과 100입니다.
  `min_cutoff_length` 매개변수를 지정하면 길이가 `min_cutoff_length` 이상인 n-그램만 반환됩니다.
  `ngrams(N)`와 비교하면 `sparseGrams` 토크나이저는 가변 길이 N-그램을 생성하므로 원본 텍스트를 더 유연하게 표현할 수 있습니다.
  예를 들어 `tokenizer = sparseGrams(3, 5, 4)`는 내부적으로 입력 문자열에서 3-, 4-, 5-그램을 생성하지만, 실제로는 4-그램과 5-그램만 반환합니다.
* `array`는 토큰화를 수행하지 않습니다. 즉, 각 행 값이 하나의 토큰이 됩니다(함수 [array](/ko/sql-reference/functions/array-functions.md/#array) 참고).

사용 가능한 모든 토크나이저는 [system.tokenizers](../../../operations/system-tables/tokenizers.md)에 나열되어 있습니다.

:::note
`splitByString` 토크나이저는 분할 구분자를 왼쪽에서 오른쪽으로 적용합니다.
이로 인해 모호성이 발생할 수 있습니다.
예를 들어 구분 문자열 `['%21', '%']`를 사용하면 `%21abc`는 `['abc']`로 토큰화되지만, 두 구분 문자열의 순서를 `['%', '%21']`로 바꾸면 `['21abc']`가 출력됩니다.
대부분의 경우 더 긴 구분자가 먼저 일치하도록 하는 것이 좋습니다.
일반적으로는 구분 문자열을 길이 내림차순으로 전달하면 됩니다.
구분 문자열이 [prefix code](https://en.wikipedia.org/wiki/Prefix_code)를 이루는 경우에는 임의의 순서로 전달할 수 있습니다.
:::

토크나이저가 입력 문자열을 어떻게 분할하는지 이해하려면 [tokens](/ko/sql-reference/functions/splitting-merging-functions.md/#tokens) 및 [tokensForLikePattern](/ko/sql-reference/functions/splitting-merging-functions.md/#tokensForLikePattern) 함수를 사용할 수 있습니다:

예시:

```sql title="Query"
SELECT tokens('abc def', 'ngrams', 3);
```

```result title="Response"
['abc','bc ','c d',' de','def']
```

*비ASCII 입력 다루기.*
텍스트 인덱스는 모든 언어와 문자 집합의 텍스트 데이터에 대해 생성할 수 있습니다.
비ASCII 텍스트의 경우 CJK 문자를 포함한 유니코드 단어 경계를 올바르게 처리하므로 `asciiCJK` 토크나이저 사용을 권장합니다.
:::

**전처리기 인수(선택 사항)**. 전처리기는 토큰화 전에 입력 문자열에 적용되는 표현식을 의미합니다.

전처리기 인수의 일반적인 사용 사례는 다음과 같습니다.

1. 대소문자를 구분하지 않는 매칭을 가능하게 하기 위한 소문자/대문자 변환 또는 케이스 폴딩(case folding), 예: [lower](/ko/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/ko/sql-reference/functions/string-functions.md/#lowerUTF8), [caseFoldUTF8](/ko/sql-reference/functions/string-functions.md/#caseFoldUTF8).
2. UTF-8 정규화, 예: [normalizeUTF8NFC](/ko/sql-reference/functions/string-functions.md/#normalizeUTF8NFC), [normalizeUTF8NFD](/ko/sql-reference/functions/string-functions.md/#normalizeUTF8NFD), [normalizeUTF8NFKC](/ko/sql-reference/functions/string-functions.md/#normalizeUTF8NFKC), [normalizeUTF8NFKD](/ko/sql-reference/functions/string-functions.md/#normalizeUTF8NFKD), [normalizeUTF8NFKCCasefold](/ko/sql-reference/functions/string-functions.md/#normalizeUTF8NFKCCasefold), [toValidUTF8](/ko/sql-reference/functions/string-functions.md/#toValidUTF8).
3. 악센트처럼 불필요한 문자 또는 부분 문자열을 제거하거나 변환하는 작업, 예: [extractTextFromHTML](/ko/sql-reference/functions/string-functions.md/#extractTextFromHTML), [substring](/ko/sql-reference/functions/string-functions.md/#substring), [idnaEncode](/ko/sql-reference/functions/string-functions.md/#idnaEncode), [translate](/ko/sql-reference/functions/string-replace-functions.md/#translate), [removeDiacriticsUTF8](/ko/sql-reference/functions/string-functions.md/#removeDiacriticsUTF8).

전처리기 표현식은 [String](/ko/sql-reference/data-types/string.md) 또는 [FixedString](/ko/sql-reference/data-types/fixedstring.md) 타입의 입력 값을 동일한 타입의 값으로 변환해야 합니다.
텍스트 인덱스가 `Nullable(T)` 또는 `LowCardinality(T)` 타입의 컬럼에 생성된 경우, 전처리기 표현식은 널 허용 또는 low-cardinality 값을 처리할 수 있어야 합니다(즉, 예외를 발생시키지 않아야 합니다).

예시:

* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(col, '\n', 1))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(extractTextFromHTML(col)))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(col)))`

또한 전처리기 표현식은 텍스트 인덱스가 정의된 컬럼 또는 표현식만 참조해야 합니다.

예시:

* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(lower(col)))`
* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(col), lower(col)))`
* 허용되지 않음: `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(col, col))`

비결정적 함수는 사용할 수 없습니다.

:::note
전처리기는 원칙적으로 인덱스 컬럼 또는 표현식을 전처리기 표현식으로 감싸는 것과 동일합니다.
예를 들어, `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`의 `lower` 전처리기는 `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha')`로 에뮬레이션할 수 있습니다.
후자의 형식은 에뮬레이션된 전처리기가 WHERE 절의 필터 조건과 일치할 때만 적용된다는 단점이 있습니다.
예를 들어, `WHERE hasAllTokens(lower(col), [...])`는 일치하지만 `WHERE hasAllTokens(col, [...])`는 일치하지 않습니다.
따라서 최적의 사용 경험을 위해 전처리기 표현식을 사용하는 것이 좋습니다.
:::

함수 [hasToken](/ko/sql-reference/functions/string-search-functions.md/#hasToken), [hasAllTokens](/ko/sql-reference/functions/string-search-functions.md/#hasAllTokens), [hasAnyTokens](/ko/sql-reference/functions/string-search-functions.md/#hasAnyTokens), [hasPhrase](/ko/sql-reference/functions/string-search-functions.md/#hasPhrase)는 검색어를 토큰화하기 전에 먼저 전처리기로 변환합니다.
전처리기는 텍스트 인덱스 경로에서만 적용되므로, 이러한 함수의 결과는 텍스트 인덱스를 사용하는 쿼리와 사용하지 않는 쿼리 간에 다를 수 있습니다(예: `SETTINGS use_skip_indexes = 0`).

예를 들어,

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

다음과 동일합니다:

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

이 경우 전처리기 표현식은 배열의 각 요소를 개별적으로 변환합니다.

예시:

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

텍스트 인덱스에서 [맵](/ko/sql-reference/data-types/map.md) 타입 컬럼에 전처리기를 정의하려면, 인덱스를 맵 키에 생성할지 값에 생성할지 결정해야 합니다.

예시:

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

**후처리기 인수(선택 사항)**. 후처리기는 토큰화 후 각 출력 토큰에 적용되는 표현식을 의미합니다.

전처리기는 토크나이저가 입력 문자열 전체를 토큰으로 분할하기 전에 이를 변환하지만, 후처리기는 토큰 자체를 한 번에 하나씩 처리합니다.
즉, 본질적으로 토큰 수준에서 이루어지는 변환을 적용하기에 적합한 위치입니다.

후처리기 인수의 일반적인 사용 사례는 다음과 같습니다.

1. **불용어(매우 빈도가 높은 토큰) 필터링**. &quot;the&quot;, &quot;a&quot;, &quot;is&quot;와 같이 매우 흔한 토큰은 검색 관련성이 낮고 인덱스만 불필요하게 키웁니다.
   후처리기를 사용하면 이를 빈 토큰으로 변환해 제거할 수 있습니다. 빈 토큰은 무시되며, 즉 인덱스에 추가되지 않습니다.
   예시: `if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)`
2. **타임스탬프 제거**. 로그 줄은 `2024-01-15T10:23:45`와 같은 구조화된 타임스탬프로 시작하거나 이를 포함하는 경우가 많습니다.
   타임스탬프 토큰을 인덱싱하면 검색 관련성이 없는 문자열 때문에 인덱스가 불필요하게 커집니다.
   타임스탬프를 무시하는 상호 보완적인 두 가지 접근 방식이 있습니다.
   * **후처리기 접근 방식**: `splitByString` 토크나이저(공백 분할)를 사용해 타임스탬프 전체가 하나의 토큰이 되도록 한 다음, `parseDateTimeOrNull`을 사용해 이를 감지하고 제거합니다.
     예시: `if(isNull(parseDateTimeOrNull(str, '%Y-%m-%dT%H:%i:%S')), str, '')`
     시간대 오프셋이나 소수 초를 포함하는 타임스탬프에는 명시적인 포맷 문자열 없이 `parseDateTimeBestEffortOrNull(str)`을 사용하십시오.
   * **전처리기 접근 방식**: 토큰화 *이전*에 정규식을 사용해 전체 로그 줄에서 타임스탬프를 제거합니다.
     예시: `replaceRegexpAll(str, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')`
     이 방식은 어떤 토크나이저와도 함께 사용할 수 있으며, 타임스탬프 문자가 아예 토큰화되지 않으므로 더 효율적입니다.
     두 접근 방식은 함께 사용할 수 있습니다. 전처리기가 타임스탬프를 제거하고, 후처리기가 남은 토큰을 정규화하거나 필터링합니다(예: 소문자 변환 + `ERROR` 또는 `INFO` 같은 심각도 단어 제거).
3. **어간 추출**. 각 토큰을 어간으로 매핑하면 동일한 어근을 공유하는 형태 변형도 일치시킬 수 있어 검색 재현율이 향상됩니다.
   예를 들어 영어 어간 추출에서는 &quot;running&quot;, &quot;runs&quot;, &quot;run&quot;이 모두 &quot;run&quot;으로 어간 추출되므로, 이 변형들 가운데 어느 것으로 쿼리하더라도 모두 일치합니다.
   ClickHouse는 여러 언어에 대해 기본 제공 [stem](/ko/sql-reference/functions/string-functions.md/#stem) 함수를 제공합니다.
   예시: `stem(str, 'en')`
4. **대소문자 정규화**. [lower](/ko/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/ko/sql-reference/functions/string-functions.md/#lowerUTF8) 등을 사용해 토큰을 소문자 또는 대문자로 변환하면 대소문자를 구분하지 않는 매칭이 가능해집니다.
   소문자화 및 대문자화에는 후처리기보다 전처리기를 권장합니다.

후처리기 표현식은 [String](/ko/sql-reference/data-types/string.md) 타입의 토큰을 동일한 타입의 토큰으로 변환합니다.
또한 후처리기 표현식은 텍스트 인덱스가 정의된 컬럼 또는 표현식만 참조해야 합니다.
컬럼 타입이 `Array(String)`인 경우에도 후처리기는 개별 토큰을 일반 `String` 값으로 처리합니다.

비결정적 함수는 사용할 수 없습니다.

후처리기는 인덱스 빌드 중 생성된 각 토큰에 적용됩니다(`array` 토크나이저에서는 각 배열 요소가 하나의 토큰입니다). 쿼리 시점에는 동작 방식이 함수에 따라 달라집니다.

* `hasToken`, `hasAllTokens`, `hasAnyTokens`, `hasPhrase`의 경우(지원되는 모든 토크나이저 포함): 후처리기가 검색 대상(haystack) 토큰과 검색 needle 모두에 적용되므로 완전히 정규화된 매칭(예: 대소문자를 구분하지 않는 검색)이 가능합니다. `hasPhrase`에서는 후처리된 토큰이 촘촘하게 배치되므로, 후처리기가 제거한 토큰이 있더라도 위치상의 간격이 남지 않아 그 사이를 건너서도 구문이 계속 일치합니다. 예를 들어 `the`를 제거하는 불용어 후처리기를 사용하면 `hasPhrase(col, 'see cat')`는 `see the cat` 문서와 일치합니다.
* 그 밖의 모든 함수(`=`, `IN`, `has`, `hasAny`, `hasAll`, `mapContains*`)의 경우: 인덱스 힌트 lookup에는 검색 needle에만 후처리기가 적용되며, 행 수준 프레디케이트는 여전히 원래 컬럼 값과 비교합니다.

예시:

* 후처리기 표현식을 사용해 불용어를 제거합니다:

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

* 후처리기 표현식을 사용해 타임스탬프를 제거합니다:

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

* 전처리기 표현식을 사용해 타임스탬프를 제거합니다:

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

* 전처리기와 후처리기 표현식을 함께 사용하여 타임스탬프를 제거합니다:

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

* 후처리기 표현식을 사용하여 토큰의 어간을 추출합니다:

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

**함수 지원**.

텍스트 인덱스를 참조하는 프레디케이트의 경우, 인덱스 조회에서 인덱스 빌드 시 저장된 것과 동일한 토큰을 사용하도록 granule 단위 검사 전에 검색 값에 전처리기와 후처리기가 적용됩니다.
대부분의 함수(`=`, `IN`, `startsWith`, `endsWith`, `LIKE`, `mapContains*`)에서는 텍스트 인덱스가 관련 없는 데이터 블록을 건너뛰는 용도로만 사용되며, ClickHouse는 남은 각 행에 대해 원본 컬럼 데이터에 원래 프레디케이트를 적용해 계속 검증합니다.
토큰 검색 함수(`hasToken`, `hasAllTokens`, `hasAnyTokens`)에서는 텍스트 인덱스가 주된 평가 경로입니다. ClickHouse는 인덱스 빌드 시 적용된 것과 동일한 전처리기, 토크나이저, 후처리기를 통해 needle을 정규화하고, 인덱스가 있는 테이블 파트와 없는 테이블 파트 모두에 이 정규화된 형식을 사용합니다. 후처리기가 있으면 haystack 토큰도 쿼리 시점에 정규화되며(`array`뿐 아니라 모든 토크나이저에 적용됨), 비교 양쪽이 일관되게 변환되므로 인덱스를 직접 읽는지(`query_plan_direct_read_from_text_index` 설정) 또는 특정 파트에 구체화된 인덱스가 있는지와 관계없이 결과가 달라지지 않습니다. 예를 들어 `lower` 후처리기를 사용하면 `hasAllTokens(col, ['FOO'])`에서 대소문자를 구분하지 않는 매칭을 활성화할 수 있습니다.
`positions`가 없으면 `hasPhrase`는 인덱스를 힌트로만 사용하고, 남은 각 행을 원래 프레디케이트로 검증합니다. 또한 후처리기는 구문과 haystack 토큰 모두를 동일한 방식으로 정규화하므로 결과는 읽기 경로와 무관하며, 후처리기가 제거한 토큰도 구문 인접성을 깨뜨리지 않습니다. `positions = 1`이면 `hasPhrase`는 정확한 직접 읽기를 사용합니다(후처리기가 있으면 계속 적용됨).
후처리기가 빈 문자열로 매핑하는 검색 토큰은 무시되며, 즉 검색 구문에 없는 것으로 처리됩니다.

| 함수                                                                                          | 전처리기 지원                  | 호환되는 토크나이저                                               | 후처리기 지원 |
| ------------------------------------------------------------------------------------------- | ------------------------ | -------------------------------------------------------- | ------- |
| `=`                                                                                         | 예                        | 모두                                                       | 예       |
| `IN`                                                                                        | 예                        | 모두                                                       | 예       |
| [hasToken](/ko/sql-reference/functions/string-search-functions.md/#hasToken)                   | 예                        | 모두 (`splitByNonAlpha`에 맞게 설계됨)                           | 예       |
| [hasAnyTokens(col, str)](/ko/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | 예                        | 모두                                                       | 예       |
| [hasAllTokens(col, str)](/ko/sql-reference/functions/string-search-functions.md/#hasAllTokens) | 예                        | 모두                                                       | 예       |
| [hasAnyTokens(col, arr)](/ko/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | 아니요 (배열 요소 자체가 토큰으로 사용됨) | 모두                                                       | 예       |
| [hasAllTokens(col, arr)](/ko/sql-reference/functions/string-search-functions.md/#hasAllTokens) | 아니요 (배열 요소 자체가 토큰으로 사용됨) | 모두                                                       | 예       |
| [hasPhrase](/ko/sql-reference/functions/string-search-functions.md/#hasPhrase)                 | 예                        | `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` | 예       |
| [startsWith](/ko/sql-reference/functions/string-functions.md/#startsWith)                      | 예                        | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | 예       |
| [endsWith](/ko/sql-reference/functions/string-functions.md/#endsWith)                          | 예                        | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | 예       |
| [like](/ko/sql-reference/functions/string-search-functions.md/#like)                           | 예¹                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | 예¹      |
| [match](/ko/sql-reference/functions/string-search-functions.md/#match)                         | 예¹                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | 예¹      |
| [ilike](/ko/sql-reference/functions/string-search-functions.md/#like)                          | 예² (`lower`/`upper`만)    | `splitByNonAlpha`, `array`²                              | 아니요²    |
| [mapContainsKey](/ko/sql-reference/functions/tuple-map-functions#mapContainsKey)               | 예                        | 모두                                                       | 예       |
| [mapContainsValue](/ko/sql-reference/functions/tuple-map-functions#mapContainsValue)           | 예                        | 모두                                                       | 예       |
| [mapContainsKeyLike](/ko/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)       | 예                        | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | 예       |
| [mapContainsValueLike](/ko/sql-reference/functions/tuple-map-functions#mapContainsValueLike)   | 예                        | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | 예       |
| [has](/ko/sql-reference/functions/array-functions.md/#has)                                     | 예                        | `array`                                                  | 예       |
| [hasAny](/ko/sql-reference/functions/array-functions.md/#hasAny)                               | 예                        | `array`                                                  | 예       |
| [hasAll](/ko/sql-reference/functions/array-functions.md/#hasAll)                               | 예                        | `array`                                                  | 예       |

¹ `LIKE` 및 `match`는 나열된 토크나이저에 대해 직접 읽기를 힌트로 사용하며, 그렇지 않으면 brute-force scan으로 폴백됩니다.
또한 `LIKE`는 전처리기나 후처리기 없이 `splitByNonAlpha` 및 `array` 토크나이저에 대해 *직접 읽기(힌트 없이)*도 지원합니다(`use_text_index_like_evaluation_by_dictionary_scan`을 통해 활성화됨).

² `ILIKE`는 직접 읽기(힌트 없이)로만 지원됩니다(`use_text_index_like_evaluation_by_dictionary_scan = 1`, `splitByNonAlpha` 또는 `array` 토크나이저).
인덱스를 힌트로 사용하는 폴백은 없습니다. 설정이 비활성화되어 있거나 토크나이저가 지원 대상이 아니면 `ILIKE`에 인덱스가 사용되지 않습니다.
전처리기가 있는 경우 `lower` 또는 `upper`여야 하며, 후처리기는 지원되지 않습니다.

**실험적 기능: Positions 인수(선택 사항)**.

실험적 매개변수 `positions`(기본값: `0`)는 인덱스가 토큰 위치를 저장할지 여부를 제어합니다.
`1`로 설정하면 인덱스가 위치 데이터(`.pos` 파일에 저장됨)도 추가로 저장하며, 이로 인해 [`hasPhrase`](#functions-example-hasphrase) 함수에서 직접 읽기를 통해 정확한 구문 일치를 수행할 수 있습니다.
위치를 저장하면 인덱스의 디스크상 크기와 쓰기 비용이 증가하므로, 이 기능은 명시적으로 선택해야 합니다.
디스크상 포맷은 아직 안정되지 않았으므로, 이 매개변수는 실험적이며 향후 릴리스에서 변경될 수 있습니다.
따라서 `positions = 1`로 인덱스를 생성하려면 MergeTree 설정 [`allow_experimental_text_index_positions`](/ko/operations/settings/merge-tree-settings#allow_experimental_text_index_positions)이 활성화되어 있어야 합니다.
포스팅 리스트만 저장하는 방식을 유지하려면 `positions = 0`(기본값)으로 설정하십시오. 이 인수 없이 생성된 텍스트 인덱스에는 위치 정보가 저장되지 않습니다.

:::warning
이 인수는 실험적이므로 테스트 용도로만 사용해야 합니다.
위치 저장을 활성화하려면 MergeTree 설정 [`allow_experimental_text_index_positions`](/ko/operations/settings/merge-tree-settings#allow_experimental_text_index_positions)을 설정하십시오.
:::

<details markdown="1">
  <summary>선택적 고급 매개변수</summary>

  다음 고급 매개변수의 기본값은 거의 모든 상황에서 잘 동작합니다.
  이 값을 변경하는 것은 권장하지 않습니다.

  선택적 매개변수 `dictionary_block_size`(기본값: 512)는 딕셔너리 블록의 크기를 행 수 기준으로 지정합니다.

  선택적 매개변수 `dictionary_block_frontcoding_compression`(기본값: 1)은 딕셔너리 블록이 압축 방식으로 front coding을 사용할지 지정합니다.

  선택적 매개변수 `posting_list_block_size`(기본값: 1048576)는 포스팅 리스트 블록의 크기를 행 수 기준으로 지정합니다.

  선택적 매개변수 `posting_list_codec`(기본값: `none`)는 포스팅 리스트에 사용할 코덱을 지정합니다:

  * `none` - 포스팅 리스트를 추가 압축 없이 저장합니다.
  * `bitpacking` - [차분(delta) 코딩](https://en.wikipedia.org/wiki/Delta_encoding)을 적용한 다음 [비트 패킹](https://dev.to/madhav_baby_giraffe/bit-packing-the-secret-to-optimizing-data-storage-and-transmission-m70)을 적용합니다(각각 고정 크기 블록 내에서 수행됨). SELECT 쿼리가 느려지므로 현재는 권장하지 않습니다.

  위의 고급 매개변수는 해당 MergeTree 설정을 통해 테이블 수준에서도 설정할 수 있습니다: [`text_index_dictionary_block_size`](/ko/operations/settings/merge-tree-settings#text_index_dictionary_block_size), [`text_index_dictionary_block_frontcoding_compression`](/ko/operations/settings/merge-tree-settings#text_index_dictionary_block_frontcoding_compression), [`text_index_posting_list_block_size`](/ko/operations/settings/merge-tree-settings#text_index_posting_list_block_size), [`text_index_posting_list_codec`](/ko/operations/settings/merge-tree-settings#text_index_posting_list_codec).
  이렇게 설정하면 매개변수를 명시적으로 지정하지 않은 해당 테이블의 모든 텍스트 인덱스에 적용됩니다.

  테이블 수준 설정의 주요 사용 사례는 모든 table parts에서 텍스트 인덱스를 삭제 후 다시 생성하지 않고도 기존 테이블의 인덱스 매개변수를 변경하는 것입니다.
  테이블 수준 설정을 변경하면 새 매개변수는 새 파트용으로 빌드되는 텍스트 인덱스에만 적용되며, 기존 파트는 현재 layout을 유지합니다.

  예를 들어 인덱스 정의에 지정된 인수는 테이블 설정보다 우선합니다:

  ```sql
  CREATE TABLE table(
      s String,
      -- 이 인덱스는 아래의 테이블 수준 기본값을 재정의하여 'bitpacking'을 사용합니다:
      INDEX idx_a s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'),
      -- 이 인덱스는 테이블 설정에서 'none'을 상속받습니다:
      INDEX idx_b lower(s) TYPE text(tokenizer = 'splitByNonAlpha'))
  ENGINE = MergeTree()
  ORDER BY tuple()
  SETTINGS text_index_posting_list_codec = 'none';
  ```
</details>

*인덱스 세분화 수준.*
ClickHouse에서 텍스트 인덱스는 [스킵 인덱스](/ko/engines/table-engines/mergetree-family/mergetree.md/#skip-index-types)의 한 유형으로 구현됩니다.
하지만 다른 스킵 인덱스와 달리 텍스트 인덱스는 무한 세분화 수준(1억)을 사용합니다.
이는 텍스트 인덱스의 테이블 정의에서 확인할 수 있습니다.

예시:

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

매우 큰 인덱스 세분화 수준으로 인해 텍스트 인덱스는 파트 전체에 대해 생성됩니다.
명시적으로 지정한 인덱스 세분화 수준은 무시됩니다.

<div id="using-a-text-index">
  ## 텍스트 인덱스 사용
</div>

`SELECT` 쿼리에서 텍스트 인덱스를 사용하는 것은 간단합니다. 일반적인 문자열 검색 함수가 자동으로 인덱스를 활용하기 때문입니다.
컬럼 또는 테이블 파트(table part)에 인덱스가 없으면 문자열 검색 함수는 느린 브루트 포스(전체 스캔)로 폴백됩니다.

:::note
텍스트 인덱스를 검색할 때는 함수 `hasAnyTokens` 및 `hasAllTokens`를 사용하는 것을 권장합니다. 자세한 내용은 [아래](#functions-example-hasanytokens-hasalltokens)를 참조하십시오.
이 함수들은 사용 가능한 모든 토크나이저와 가능한 모든 전처리기 및 후처리기 표현식에서 작동합니다.
반면 다른 지원 함수들은 역사적으로 텍스트 인덱스보다 먼저 도입되었기 때문에, 많은 경우 기존 동작을 유지해야 했습니다(예: 전처리기 또는 후처리기 지원 없음).
:::

<div id="functions-support">
  ### 지원되는 함수
</div>

`WHERE` 절 또는 `PREWHERE` 절에서 텍스트 함수를 사용하는 경우 텍스트 인덱스를 사용할 수 있습니다.

```sql
SELECT [...]
FROM [...]
WHERE string_search_function(column_with_text_index)
```

<div id="functions-example-equals">
  #### `=`
</div>

`=` ([equals](/ko/sql-reference/functions/comparison-functions.md/#equals))는 주어진 검색어 전체와 정확히 일치합니다.

예시:

```sql
SELECT * from table WHERE str = 'Hello';
```

<div id="functions-example-in">
  #### `IN`
</div>

`IN`([in](/ko/sql-reference/functions/in-functions))은 `equals`와 비슷하지만 모든 검색어에 일치합니다.

예시:

```sql
SELECT * from table WHERE str IN ('Hello', 'World');
```

:::note
텍스트 인덱스는 `NOT IN` (`notIn`)을 지원하지 않습니다.
:::

<div id="functions-example-like-match">
  #### `LIKE` 및 `match`
</div>

:::note
이 함수들은 현재 인덱스 토크나이저가 `splitByNonAlpha`, `ngrams`, `sparseGrams` 중 하나인 경우에만 필터링에 텍스트 인덱스를 사용합니다.
:::

:::note
`NOT LIKE` (`notLike`)는 텍스트 인덱스에서 지원되지 않습니다.
:::

텍스트 인덱스와 함께 `LIKE` ([like](/ko/sql-reference/functions/string-search-functions.md/#like)) 및 [match](/ko/sql-reference/functions/string-search-functions.md/#match) 함수를 사용하려면 ClickHouse가 검색어에서 완전한 토큰을 추출할 수 있어야 합니다.
`ngrams` 토크나이저를 사용하는 인덱스에서는 와일드카드 사이의 검색 문자열 길이가 ngram 길이와 같거나 더 길면 이 조건이 충족됩니다.

`splitByNonAlpha` 토크나이저를 사용하는 텍스트 인덱스의 예시:

```sql
SELECT count() FROM table WHERE comment LIKE 'support%';
```

예시의 `support`는 `support`, `supports`, `supporting` 등에 일치할 수 있습니다.
이러한 쿼리는 부분 문자열 쿨리이므로 텍스트 인덱스로는 속도를 높일 수 없습니다.

LIKE 쿼리에서 텍스트 인덱스를 활용하려면 LIKE 패턴을 다음과 같이 재작성해야 합니다:

```sql
SELECT count() FROM table WHERE comment LIKE ' support %'; -- or `% support %`
```

`support`의 왼쪽과 오른쪽 공백은 해당 용어가 토큰으로 추출되도록 합니다.

다행히도 ClickHouse가 역인덱스를 활용해 LIKE 쿼리 속도를 크게 높일 수 있는 특별한 경우가 있습니다.

자세한 내용은 [LIKE/ILIKE 성능 튜닝 섹션](#like-ilike-queries-perf)을 참조하십시오.

<div id="functions-example-multisearchany-multimatchany">
  #### `multiSearchAny` 및 `multiMatchAny`
</div>

[multiSearchAny](/ko/sql-reference/functions/string-search-functions.md/#multiSearchAny)와 해당 UTF-8 변형인 [multiSearchAnyUTF8](/ko/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8)는 여러 리터럴 부분 문자열 중 하나라도 haystack에 존재하는지 확인하며, [multiMatchAny](/ko/sql-reference/functions/string-search-functions.md/#multiMatchAny)는 여러 정규식 중 하나라도 일치하는지 확인합니다.
이 함수들은 `LIKE` 및 `match`와 동일한 조건에서 텍스트 인덱스를 사용합니다(위 참조). 즉, ClickHouse가 각 needle에서 완전한 토큰을 추출할 수 있어야 하며, needles 목록은 상수여야 합니다.
어떤 needle이라도 포함되어 있을 가능성이 있으면 해당 granule을 읽습니다.

`multiMatchAny`의 경우, 단일 패턴을 토큰 요구 사항으로 축약할 수 없으면(예: 모든 document와 일치하는 `.*`) 텍스트 인덱스를 사용할 수 없으며 쿼리는 full scan으로 폴백됩니다.

`LIKE` 및 `match`와 마찬가지로, 부분 문자열 및 정규식 검색은 `ngrams` 및 `sparseGrams` 토크나이저와 함께 사용할 때 가장 효과적입니다.
이 토크나이저는 서로 겹치는 문자 n-그램에 인덱스를 생성하므로, needle은 부분 문자열로 나타나는 모든 위치에서 인덱스에 존재하는 n-그램으로 분해됩니다. 이는 needle이 단어의 중간에서 시작하거나 끝나는 경우에도 마찬가지입니다.
따라서 needle의 길이가 n-그램 크기 이상이면 그대로 사용할 수 있습니다.

`ngrams` 토크나이저를 사용하는 텍스트 인덱스 예시:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, ['clickhouse', 'support']);
```

반면 `splitByNonAlpha` 토크나이저는 완전한 토큰(전체 단어)만 인덱싱합니다.
검색 문자열은 단어의 중간에서 시작하거나 끝날 수 있으므로, ClickHouse는 각 검색 문자열의 앞뒤 토큰을 삭제합니다. 따라서 인덱스는 완전한 토큰만 사용해 그래뉼을 가지치기할 수 있습니다.
부분 문자열 및 정규 표현식 검색에서 `splitByNonAlpha`와 함께 인덱스를 사용하려면 각 검색 문자열을 구분자 문자(예: 공백)로 감싸 하나 이상의 완전한 토큰이 되도록 하십시오.

`splitByNonAlpha` 토크나이저를 사용하는 텍스트 인덱스 예시:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, [' clickhouse ', ' support ']);
```

<div id="functions-example-startswith-endswith">
  #### `startsWith` and `endsWith`
</div>

`LIKE`와 마찬가지로 [startsWith](/ko/sql-reference/functions/string-functions.md/#startsWith) 및 [endsWith](/ko/sql-reference/functions/string-functions.md/#endsWith) 함수는 검색어에서 완전한 토큰을 추출할 수 있을 때만 텍스트 인덱스를 사용할 수 있습니다.
`ngrams` 토크나이저를 사용하는 인덱스에서는 와일드카드 사이의 검색 문자열 길이가 ngram 길이와 같거나 더 길면 이 조건을 충족합니다.
텍스트 인덱스가 후처리기를 사용하는 경우에도, 추출된 힌트 토큰이 정규화 후 비어 있지 않으면 이러한 함수는 Hint 모드에서 인덱스를 사용할 수 있습니다. 정규화 과정에서 모든 힌트 토큰이 제거되면 해당 프레디케이트에는 인덱스가 사용되지 않습니다.

`splitByNonAlpha` 토크나이저를 사용하는 텍스트 인덱스 예시:

```sql
SELECT count() FROM table WHERE startsWith(comment, 'clickhouse support');
```

예시에서는 `clickhouse`만 토큰으로 간주됩니다.
`support`는 `support`, `supports`, `supporting` 등에 매칭될 수 있으므로 토큰으로 간주되지 않습니다.

`clickhouse supports`로 시작하는 모든 행을 찾으려면 검색 패턴 끝에 공백을 하나 추가하십시오:

```sql
startsWith(comment, 'clickhouse supports ')`
```

마찬가지로 `endsWith`는 앞에 공백을 포함해 사용해야 합니다:

```sql
SELECT count() FROM table WHERE endsWith(comment, ' olap engine');
```

<div id="functions-example-hastoken">
  #### `hasToken`
</div>

:::note
`hasToken`은 non-`splitByNonAlpha` 토크나이저 및/또는 전처리기/후처리기 표현식을 사용하는 텍스트 인덱스에서 lookup에 사용할 때 몇 가지 주의할 점이 있습니다.
대신 `hasAnyTokens`와 `hasAllTokens`를 사용하는 것이 좋습니다.

대소문자를 구분하지 않는 변형인 `hasTokenCaseInsensitive` 및 `hasTokenCaseInsensitiveOrNull`은 텍스트 인덱스를 인식하지 못하므로, 텍스트 인덱스가 적용된 컬럼에서도 항상 전체 행 스캔으로 실행됩니다. 대소문자를 구분하지 않는 매칭이 필요하면 `lower(...)` 전처리기 또는 후처리기를 사용하고, 이를 `hasToken` / `hasAllTokens` / `hasAnyTokens`와 함께 사용하십시오.
:::

Function [hasToken](/ko/sql-reference/functions/string-search-functions.md/#hasToken)은 지정된 단일 토큰과 매칭합니다.

앞서 설명한 함수들과 달리 이 함수는 검색어를 토큰화하지 않습니다(입력이 단일 토큰이라고 가정합니다).

예시:

```sql
SELECT count() FROM table WHERE hasToken(comment, 'clickhouse');
```

<div id="functions-example-hasanytokens-hasalltokens">
  #### `hasAnyTokens` and `hasAllTokens`
</div>

함수 [hasAnyTokens](/ko/sql-reference/functions/string-search-functions.md/#hasAnyTokens) 및 [hasAllTokens](/ko/sql-reference/functions/string-search-functions.md/#hasAllTokens)는 주어진 토큰 중 하나 또는 전체와 일치하는지 확인합니다.

이 두 함수는 검색 토큰을 문자열 또는 이미 처리된 토큰의 배열로 받습니다. 문자열을 전달하면 인덱스 컬럼에 사용된 것과 동일한 토크나이저로 토큰화되며, 배열을 전달하면 검색 전에 토큰화가 적용되지 않습니다.
자세한 내용은 함수 문서를 참조하십시오.

예시:

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

함수 [hasPhrase](/ko/sql-reference/functions/string-search-functions.md/#hasPhrase)는 구문과 일치하는지 확인합니다. 즉, 모든 토큰이 검색 문자열에 있는 순서 그대로 연속해서 나타나야 합니다.

모든 토큰이 어딘가에 존재하기만 하면 되는 `hasAllTokens`와 달리, `hasPhrase`는 토큰이 연속된 시퀀스로 나타나야 합니다.
검색 구문은 인덱스 컬럼에 구성된 것과 동일한 토크나이저를 사용해 토큰화됩니다.
텍스트 인덱스가 후처리기를 사용하는 경우, 검색 구문도 인덱스 lookup 전에 정규화됩니다.
이 함수는 `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` 토크나이저 중 하나를 사용해야 합니다.

예시:

```sql
-- Matches: 'clickhouse' and 'olap' must appear consecutively in that order
SELECT count() FROM table WHERE hasPhrase(comment, 'clickhouse olap');

-- Does NOT match a row containing 'olap clickhouse' (wrong order)
-- Does NOT match a row containing 'clickhouse fast olap' (non-consecutive)
```

<div id="functions-example-has">
  #### `has`
</div>

배열 함수 [has](/ko/sql-reference/functions/array-functions#has)는 문자열 배열에서 단일 토큰과 일치하는지 확인합니다.

예시:

```sql
SELECT count() FROM table WHERE has(array, 'clickhouse');
```

<div id="functions-example-hasany-hasall">
  #### `hasAny` 및 `hasAll`
</div>

배열 함수 [hasAny](/ko/sql-reference/functions/array-functions#hasAny) 및 [hasAll](/ko/sql-reference/functions/array-functions#hasAll)은 인덱스가 적용된 배열 컬럼에 상수 문자열 집합의 일부 또는 전체가 포함되어 있는지 확인합니다.

예시:

```sql
SELECT count() FROM table WHERE hasAny(tags, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAll(tags, ['clickhouse', 'olap']);
```

<div id="functions-example-mapcontains">
  #### `mapContains`
</div>

함수 [mapContains](/ko/sql-reference/functions/tuple-map-functions#mapContainsKey)는 `mapContainsKey`의 별칭(alias)으로, 맵의 키에서 검색 대상 문자열로부터 추출한 토큰과 일치하는지 확인합니다.
동작은 `String` 컬럼에 사용하는 `equals` 함수와 유사합니다.
텍스트 인덱스는 `mapKeys(map)` 표현식에 생성된 경우에만 사용됩니다.

예시:

```sql
SELECT count() FROM table WHERE mapContainsKey(map, 'clickhouse');
-- OR
SELECT count() FROM table WHERE mapContains(map, 'clickhouse');
```

<div id="functions-example-mapcontainsvalue">
  #### `mapContainsValue`
</div>

함수 [mapContainsValue](/ko/sql-reference/functions/tuple-map-functions#mapContainsValue)는 맵의 값에 있는 검색 문자열에서 추출된 토큰과 일치하는 항목을 찾습니다.
동작은 `String` 컬럼에 대해 `equals` 함수를 사용하는 경우와 유사합니다.
텍스트 인덱스는 `mapValues(map)` 표현식에 생성된 경우에만 사용됩니다.

예시:

```sql
SELECT count() FROM table WHERE mapContainsValue(map, 'clickhouse');
```

<div id="functions-example-mapcontainslike">
  #### `mapContainsKeyLike` 및 `mapContainsValueLike`
</div>

함수 [mapContainsKeyLike](/ko/sql-reference/functions/tuple-map-functions#mapContainsKeyLike) 및 [mapContainsValueLike](/ko/sql-reference/functions/tuple-map-functions#mapContainsValueLike)는 맵의 모든 키 또는 값(각각)에 대해 패턴 일치를 검사합니다.

예시:

```sql
SELECT count() FROM table WHERE mapContainsKeyLike(map, '% clickhouse %');
SELECT count() FROM table WHERE mapContainsValueLike(map, '% clickhouse %');
```

<div id="functions-example-access-operator">
  #### `operator[]`
</div>

액세스 [operator[]](/ko/sql-reference/operators#access-operators)는 텍스트 인덱스와 함께 사용하여 키와 값을 필터링할 수 있습니다. 텍스트 인덱스는 `mapKeys(map)` 표현식이나 `mapValues(map)` 표현식 또는 둘 다에 생성된 경우에만 사용됩니다.

예시:

```sql
SELECT count() FROM table WHERE map['engine'] = 'clickhouse';
```

텍스트 인덱스에서 `Array(T)` 및 `Map(K, V)` 유형의 컬럼을 사용하는 예시는 다음과 같습니다.

<div id="text-index-example-array">
  ### 배열(Array)(String) 컬럼 인덱싱
</div>

작성자가 키워드로 블로그 게시물을 분류하는 블로깅 플랫폼을 떠올려 보십시오.
사용자가 주제를 검색하거나 클릭해 관련 콘텐츠를 찾을 수 있어야 합니다.

다음 테이블 정의를 살펴보십시오:

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

텍스트 인덱스가 없으면 특정 키워드(예: `clickhouse`)가 포함된 게시물을 찾으려면 모든 항목을 스캔해야 합니다:

```sql
SELECT count() FROM posts WHERE has(keywords, 'clickhouse'); -- slow full-table scan - checks every keyword in every post
```

플랫폼 규모가 커질수록 쿼리가 각 행의 모든 `keywords` 배열을 검사해야 하므로 점점 느려집니다.
이 성능 문제를 해결하기 위해 `keywords` 컬럼에 텍스트 인덱스를 정의합니다:

```sql
ALTER TABLE posts ADD INDEX keywords_idx(keywords) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE posts MATERIALIZE INDEX keywords_idx; -- Don't forget to rebuild the index for existing data
```

<div id="text-index-example-map">
  ### 맵 컬럼 인덱싱
</div>

많은 관측성 활용 사례에서는 로그 메시지를 &quot;구성 요소&quot;로 나누어 각 요소에 맞는 데이터 타입으로 저장합니다. 예를 들어 timestamp에는 date time 값을, 로그 레벨에는 enum을 사용합니다.
메트릭 필드는 key-value 쌍으로 저장하는 것이 가장 좋습니다.
운영 팀은 디버깅, 보안 사고 대응, 모니터링을 위해 로그를 효율적으로 검색해야 합니다.

다음 로그 테이블을 살펴보겠습니다:

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

텍스트 인덱스가 없으면 [맵](/ko/sql-reference/data-types/map.md) 데이터를 검색할 때 전체 테이블 스캔이 필요합니다:

```sql
-- Finds all logs with rate limiting data:
SELECT * FROM logs WHERE has(mapKeys(attributes), 'rate_limit'); -- slow full-table scan

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- slow full-table scan
```

로그 양이 증가할수록 이러한 쿼리는 느려집니다.

해결 방법은 [맵](/ko/sql-reference/data-types/map.md)의 키와 값에 텍스트 인덱스를 생성하는 것입니다.
필드 이름이나 속성 타입으로 로그를 찾아야 하는 경우, [mapKeys](/ko/sql-reference/functions/tuple-map-functions.md/#mapKeys)를 사용해 텍스트 인덱스를 생성하십시오:

```sql
ALTER TABLE logs ADD INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_keys_idx;
```

속성의 실제 내용을 검색해야 한다면 [mapValues](/ko/sql-reference/functions/tuple-map-functions.md/#mapValues)를 사용하여 텍스트 인덱스를 생성하십시오:

```sql
ALTER TABLE logs ADD INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_vals_idx;
```

쿼리 예시:

```sql
-- Find all rate-limited requests:
SELECT * FROM logs WHERE mapContainsKey(attributes, 'rate_limit'); -- fast

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- fast

-- Finds all logs where any attribute includes an error:
SELECT * FROM logs WHERE mapContainsValueLike(attributes, '% error %'); -- fast
```

<div id="text-index-example-json">
  ### JSON 컬럼 인덱싱
</div>

텍스트 인덱스는 `JSON` 컬럼에 대해 세 가지 방식으로 사용할 수 있습니다:

1. **특정 서브컬럼에 대한 인덱스** — 일반 컬럼과 마찬가지로 알려진 JSON 경로에 텍스트 인덱스를 생성합니다. 이렇게 하면 해당 경로의 *값*이 인덱싱됩니다.
2. **[JSONAllPaths](/ko/sql-reference/functions/json-functions.md/#JSONAllPaths)를 사용하는 경로 기반 인덱스** — 각 그래뉼에 존재하는 *모든 경로*를 인덱싱하여, 질의한 경로를 포함할 수 없는 그래뉼을 건너뛸 수 있게 합니다. `Map` 컬럼과 유사합니다.
3. **[JSONAllValues](/ko/sql-reference/functions/json-functions.md#JSONAllValues)를 사용하는 값 기반 인덱스** — 모든 JSON 경로에 있는 *모든 값*을 인덱싱하여, 단일 인덱스로 모든 JSON 서브컬럼에 대한 전문 검색을 가속화합니다.

<div id="json-indexes-on-subcolumns">
  #### 특정 서브컬럼의 인덱스
</div>

일반 컬럼과 동일한 구문을 사용하여 모든 JSON 서브컬럼에 스킵 인덱스를 생성할 수 있습니다.

인덱스 표현식에서 JSON 서브컬럼을 참조하는 방법은 두 가지입니다:

* **JSON 타입 힌트에 선언된 타입 경로** — 이름으로 직접 접근합니다: `json.a`.
* **명시적 캐스트를 사용하는 동적 경로** — `::` 캐스트 구문을 사용합니다: `json.b::String`.

예시 인덱스 정의:

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

예시 쿼리:

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

쿼리 예시:

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
  #### JSONAllPaths를 사용한 경로 기반 인덱스
</div>

`Map` 컬럼과 마찬가지로, [`JSONAllPaths`](/ko/sql-reference/functions/json-functions.md/#JSONAllPaths)를 사용하면 [JSON](/ko/sql-reference/data-types/newjson.md) 컬럼에도 텍스트 인덱스를 생성할 수 있습니다.
이 인덱스는 각 그래뉼에 존재하는 JSON 경로 집합을 저장하며, 쿼리한 경로가 없는 그래뉼은 건너뛰는 데 사용합니다.

예시 인덱스 정의:

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

`EXPLAIN indexes = 1`을 사용하면 스킵 인덱스가 실제로 사용되는지 확인할 수 있습니다.
경로가 한 파트에만 존재하면 인덱스는 다른 파트를 건너뜁니다.

예시:

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

어느 파트에도 해당 경로가 없으면 모든 파트와 그래뉼을 건너뜁니다.

예시:

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

`IS NOT NULL`도 인덱스를 사용합니다 — `경로`가 없으면 값이 `NULL`이 되므로 해당 그래뉼을 건너뜁니다:

예시:

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
  #### JSONAllValues를 사용한 값 기반 인덱스
</div>

텍스트 인덱스는 [`JSONAllValues`](/ko/sql-reference/functions/json-functions.md#JSONAllValues) 함수를 사용해 [JSON](/ko/sql-reference/data-types/newjson.md) 컬럼 검색을 가속할 수 있습니다.

`JSONAllValues`는 JSON 컬럼의 모든 값을 `Array(String)`으로 반환합니다.
문자열이 아닌 데이터 타입의 값(예: 정수, 배열)은 텍스트 표현으로 변환됩니다.
`JSONAllValues`로 빌드한 텍스트 인덱스는 각 행의 모든 JSON 경로에 있는 이러한 텍스트 표현을 인덱싱합니다.
이 인덱스는 이후 개별 JSON 서브컬럼을 기준으로 필터링하는 쿼리를 가속할 수 있습니다.
쿼리가 특정 서브컬럼(예: `data.user_name = 'alice'`)을 기준으로 필터링할 때, 텍스트 인덱스는 어떤 JSON 값에도 검색 토큰이 없는 행(및 그래뉼)을 빠르게 건너뛸 수 있습니다.

:::note
서로 다른 JSON 경로에 동일한 토큰이 포함된 경우, 인덱스에서 거짓 양성이 발생할 수 있습니다.
예를 들어, 행 1에 `{"a": "hello", "b": "world"}`가 있고 쿼리가 `data.a = 'world'`를 검색하면, 텍스트 인덱스는 `world`가 경로 `a`가 아니라 `b`에 속한다는 점을 구분할 수 없습니다.
이 경우 인덱스는 해당 행을 건너뛰지 않으며, 실제 컬럼 데이터에 대한 필터가 최종 평가를 수행합니다.
이 동작은 인덱스가 빠른 사전 필터 역할을 하는 다른 텍스트 인덱스 사용 사례와 동일합니다.
:::

<div id="json-all-values-creating-the-index">
  ##### 인덱스 생성
</div>

인덱스 정의 예시:

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
  ##### 지원되는 쿼리 패턴
</div>

인덱스가 생성되면 `String` 컬럼에 사용하는 것과 동일한 함수와, 모든 컬럼에서 사용할 수 있는 `equals` 함수를 사용해 JSON 서브컬럼에 대한 쿼리 성능을 높일 수 있습니다.

서브컬럼 접근:

```sql
SELECT * FROM events WHERE data.user_name = 'alice';
SELECT * FROM events WHERE data.message LIKE '% error %';
SELECT * FROM events WHERE startsWith(data.status, 'fail');
SELECT * FROM events WHERE hasToken(data.title, 'clickhouse');
```

명시적 `CAST`를 사용한 서브컬럼 액세스:

```sql
SELECT * FROM events WHERE hasAllTokens(data.message::String, 'connection timeout');
SELECT * FROM events WHERE data.status_code::UInt64 = 404;
SELECT * FROM events WHERE has(data.tags::Array(String), 'bug')
```

`IN` 연산자:

```sql
SELECT * FROM events WHERE data.level IN ('error', 'critical');
```

<div id="text-index-phrase-search">
  ### 구문 검색
</div>

예를 들어, 일반적인 텍스트 인덱스 검색은 다음과 같습니다.

```sql
SELECT *
FROM tab
WHERE hasAllTokens(col, 'weather in Tokyo')
```

주어진 토큰을 임의의 순서로 포함하는 모든 행과 일치합니다.
예시에서 `While she stayed in Tokyo, the weather was great.` 행은 필터와 일치합니다.

반면 구문 검색은 주어진 순서대로 토큰이 일치하는 것을 의미합니다.
예를 들어,

```sql
SELECT *
FROM tab
WHERE hasPhrase(col, 'weather in Tokyo')
```

`How is the weather in Tokyo?`와 같이 토큰 시퀀스 `weather in Tokyo`를 포함하는 모든 행과 일치합니다.

텍스트 인덱스는 구문에 포함된 모든 토큰의 포스팅 리스트를 교집합해 후보 그래뉼을 식별함으로써 구문 검색 속도를 높입니다.
그런 다음 ClickHouse는 해당 그래뉼 내에서 토큰이 정확히 인접해 있는지 확인합니다.
이 과정은 비교적 비용이 크며 일반적인 텍스트 검색 쿼리보다 느립니다.
구문 검색 쿼리의 속도를 높이려면 텍스트 인덱스에서 위치 저장을 활성화하십시오(위의 `Optional parameters` 참조).

`hasPhrase`는 토크나이저 `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK`와 함께 사용할 수 있습니다.
지정된 구문 문자열은 인덱스의 토크나이저를 사용해 토큰화됩니다.
구문 내 구분 문자는 무시됩니다. `splitByNonAlpha`를 토크나이저로 사용한다고 가정하면 `hasPhrase(text, 'quick+brown')`는 `hasPhrase(text, 'quick brown')`와 동일합니다.

<div id="text-index-phrase-search-example">
  #### 예시
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

2행(`'New weather in York'`)은 토큰 순서가 올바르지 않으므로 일치하지 않습니다.
3행(`'weather in New Orleans'`)은 `'York'` 토큰이 포함되어 있지 않으므로 일치하지 않습니다.

<div id="performance-tuning">
  ## 성능 튜닝
</div>

<div id="direct-read">
  ### 직접 읽기
</div>

일부 텍스트 쿼리는 &quot;직접 읽기&quot;라는 최적화를 통해 속도를 크게 높일 수 있습니다.

예시:

```sql
SELECT column_a, column_b, ...
FROM [...]
WHERE string_search_function(column_with_text_index)
```

직접 읽기 최적화는 기본 텍스트 컬럼에 접근하지 않고 텍스트 인덱스(즉, 텍스트 인덱스 조회)만 사용해 쿼리를 처리합니다.
텍스트 인덱스 조회는 읽는 데이터 양이 비교적 적기 때문에 ClickHouse의 일반적인 스킵 인덱스(스킵 인덱스 조회를 수행한 뒤 남은 그래뉼을 로드하고 필터링함)보다 훨씬 빠릅니다.

직접 읽기는 두 가지 설정으로 제어됩니다:

* 설정 [query&#95;plan&#95;direct&#95;read&#95;from&#95;text&#95;index](../../../operations/settings/settings#query_plan_direct_read_from_text_index)는(기본값: true) 직접 읽기를 전반적으로 활성화할지 지정합니다.
* 설정 [use&#95;skip&#95;indexes&#95;on&#95;data&#95;read](../../../operations/settings/settings#use_skip_indexes_on_data_read)는 ClickHouse 버전 &lt; 26.4에서 직접 읽기를 사용하기 위한 필수 조건이었습니다.

**지원되는 함수**

직접 읽기 최적화는 `hasToken`, `hasAllTokens`, `hasAnyTokens` 함수를 지원합니다.
텍스트 인덱스가 `array` 토크나이저로 정의된 경우 `equals`, `has`, `hasAny`, `hasAll`, `mapContainsKey`, `mapContainsValue` 함수도 직접 읽기에서 지원됩니다.
이 함수들은 `AND`, `OR`, `NOT` 연산자로 조합할 수도 있습니다.
`WHERE` 또는 `PREWHERE` 절에는 추가적인 비텍스트 검색 함수 필터(텍스트 컬럼이나 다른 컬럼에 대한 필터)도 포함될 수 있습니다. 이 경우에도 직접 읽기 최적화는 사용되지만 효율은 다소 떨어집니다(지원되는 텍스트 검색 함수에만 적용됨).

쿼리가 직접 읽기를 사용하는지 확인하려면 `EXPLAIN PLAN actions = 1`과 함께 쿼리를 실행하십시오.
예시로, 직접 읽기가 비활성화된 쿼리는

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 0, -- disable direct read
```

반환값

```text
[...]
Filter ((WHERE + Change column names to column identifiers))
Filter column: hasToken(__table1.col, 'some_token'_String) (removed)
Actions: INPUT : 0 -> col String : 0
         COLUMN Const(String) -> 'some_token'_String String : 1
         FUNCTION hasToken(col :: 0, 'some_token'_String :: 1) -> hasToken(__table1.col, 'some_token'_String) UInt8 : 2
[...]
```

반면 `query_plan_direct_read_from_text_index = 1`로 동일한 쿼리를 실행하면

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 1, -- enable direct read
```

반환값

```text
[...]
Expression (Before GROUP BY)
Positions:
  Filter
  Filter column: __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 (removed)
  Actions: INPUT :: 0 -> __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 UInt8 : 0
[...]
```

두 번째 EXPLAIN PLAN 출력에는 가상 컬럼 `__text_index_<index_name>_<function_name>_<id>`이 포함됩니다.
이 컬럼이 있으면 직접 읽기가 사용된 것입니다.

WHERE 필터 절에 텍스트 검색 함수만 포함된 경우, 쿼리는 컬럼 데이터를 전혀 읽지 않고 처리될 수 있으며 직접 읽기를 통해 가장 큰 성능상 이점을 얻을 수 있습니다.
하지만 쿼리의 다른 부분에서 텍스트 컬럼에 접근하더라도 직접 읽기는 여전히 성능 개선을 제공합니다.

**힌트로 사용하는 직접 읽기**

힌트로 사용하는 직접 읽기는 일반적인 직접 읽기와 동일한 원리를 따르지만, 기본 텍스트 컬럼을 제거하는 대신 텍스트 인덱스 데이터로부터 추가 필터를 생성합니다.
이는 텍스트 인덱스만 읽을 경우 거짓 양성이 발생할 수 있는 함수에 사용됩니다.

지원되는 함수는 `like`, `startsWith`, `endsWith`, `equals`, `has`, `hasPhrase`, `mapContainsKey`, `mapContainsValue`입니다.

이 추가 필터는 다른 필터와 결합할 때 결과 집합을 더욱 제한하는 추가 선택도를 제공하여, 다른 컬럼에서 읽어야 하는 데이터 양을 줄이는 데 도움이 됩니다.

힌트로 사용하는 직접 읽기는 [query&#95;plan&#95;text&#95;index&#95;add&#95;hint](../../../operations/settings/settings#query_plan_text_index_add_hint) 설정으로 제어됩니다(기본적으로 활성화됨).

힌트가 없는 쿼리 예시:

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE (col LIKE '%some-token%') AND (d >= today())
SETTINGS query_plan_text_index_add_hint = 0
FORMAT TSV
```

반환값

```text
[...]
Prewhere filter column: and(like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

반면 `query_plan_text_index_add_hint = 1`을 설정해 동일한 쿼리를 실행하면

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE col LIKE '%some-token%'
SETTINGS query_plan_text_index_add_hint = 1
```

반환값

```text
[...]
Prewhere filter column: and(__text_index_idx_col_like_d306f7c9c95238594618ac23eb7a3f74, like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

두 번째 EXPLAIN PLAN 출력에서는 필터 조건에 추가 결합 조건(`__text_index_...`)이 들어간 것을 확인할 수 있습니다.
[PREWHERE](/ko/sql-reference/statements/select/prewhere) 최적화 덕분에 필터 조건은 3개의 개별 결합 조건으로 분해되며, 계산 복잡도가 낮은 것부터 높은 것 순서로 적용됩니다.
이 쿼리에서는 먼저 `__text_index_...`를 적용하고, 그다음 `greaterOrEquals(...)`, 마지막으로 `like(...)`를 적용합니다.
이 순서 덕분에 `WHERE` 절 이후 쿼리에서 사용하는 무거운 컬럼을 읽기 전에, 텍스트 인덱스와 원래 필터만으로 스키핑되는 그래뉼보다 더 많은 데이터 그래뉼을 스키핑할 수 있어 읽어야 하는 데이터 양이 더욱 줄어듭니다.

<div id="like-ilike-queries-perf">
  ### LIKE/ILIKE 쿼리
</div>

LIKE/ILIKE 쿼리 패턴이 `%<alpha-numeric-characters-without-spaces>%`이고 텍스트 인덱스 토크나이저가 `splitByNonAlpha` 또는 `array`인 경우, ClickHouse는 inverted index를 활용해 LIKE/ILIKE 쿼리 속도를 크게 높입니다. 이를 위해 ClickHouse는 일치하는 패턴을 찾을 때 전체 테이블 스캔 대신 inverted index 딕셔너리를 스캔합니다.

이 최적화가 활성화되면 LIKE/ILIKE 쿼리는 전체 테이블 스캔보다 훨씬 빨라집니다. 하지만 패턴이 딕셔너리 토큰 대부분과 일치하면 전체 테이블 스캔보다 성능이 더 나빠질 수 있습니다. 다행히 이를 방지하는 폴백 메커니즘이 있습니다.

이 최적화는 다음 설정으로 제어됩니다:

* [use&#95;text&#95;index&#95;like&#95;evaluation&#95;by&#95;dictionary&#95;scan](../../../operations/settings/settings#use_text_index_like_evaluation_by_dictionary_scan)

폴백 메커니즘은 다음 두 가지 설정으로 제어됩니다:

* [text&#95;index&#95;like&#95;min&#95;pattern&#95;length](../../../operations/settings/settings#text_index_like_min_pattern_length)
* [text&#95;index&#95;like&#95;max&#95;postings&#95;to&#95;read](../../../operations/settings/settings#text_index_like_max_postings_to_read)

이 최적화는 `like` 및 `ilike` 함수만 지원합니다.

<div id="caching">
  ### 캐싱
</div>

텍스트 인덱스의 일부를 메모리에 버퍼링하기 위한 서버 전역 캐시가 여러 개 있습니다([구현 세부 사항](#implementation) 섹션 참조).
현재 I/O를 줄이기 위해 텍스트 인덱스의 역직렬화된 헤더, 토큰, 포스팅 리스트용 캐시가 제공됩니다.
설정 [use&#95;text&#95;index&#95;header&#95;cache](/ko/operations/settings/settings#use_text_index_header_cache), [use&#95;text&#95;index&#95;tokens&#95;cache](/ko/operations/settings/settings#use_text_index_tokens_cache), [use&#95;text&#95;index&#95;postings&#95;cache](/ko/operations/settings/settings#use_text_index_postings_cache)를 사용하면 쿼리의 개별 캐시 읽기 및 쓰기를 비활성화할 수 있습니다.

캐시를 지우려면 [SYSTEM CLEAR TEXT INDEX CACHES](../../../sql-reference/statements/system#drop-text-index-caches) SQL 문을 사용하십시오.

캐시를 구성하려면 다음 서버 설정을 참조하십시오.

<div id="caching-tokens">
  #### 토큰 캐시 설정
</div>

| 설정                                                                                                                                                  | 설명                                          |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| [text&#95;index&#95;tokens&#95;cache&#95;policy](/ko/operations/server-configuration-parameters/settings#text_index_tokens_cache_policy)               | 텍스트 인덱스 토큰 캐시 정책 이름입니다.                     |
| [text&#95;index&#95;tokens&#95;cache&#95;size](/ko/operations/server-configuration-parameters/settings#text_index_tokens_cache_size)                   | 바이트 단위의 최대 캐시 크기입니다.                        |
| [text&#95;index&#95;tokens&#95;cache&#95;max&#95;entries](/ko/operations/server-configuration-parameters/settings#text_index_tokens_cache_max_entries) | 캐시에 저장할 수 있는 역직렬화된 토큰의 최대 개수입니다.            |
| [text&#95;index&#95;tokens&#95;cache&#95;size&#95;ratio](/ko/operations/server-configuration-parameters/settings#text_index_tokens_cache_size_ratio)   | 텍스트 인덱스 토큰 캐시에서 전체 캐시 크기 대비 보호 큐의 크기 비율입니다. |

<div id="caching-header">
  #### 헤더 캐시 설정
</div>

| Setting                                                                                                                                             | Description                              |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| [text&#95;index&#95;header&#95;cache&#95;policy](/ko/operations/server-configuration-parameters/settings#text_index_header_cache_policy)               | 텍스트 인덱스 헤더 캐시 정책의 이름입니다.                 |
| [text&#95;index&#95;header&#95;cache&#95;size](/ko/operations/server-configuration-parameters/settings#text_index_header_cache_size)                   | 바이트 단위의 최대 캐시 크기입니다.                     |
| [text&#95;index&#95;header&#95;cache&#95;max&#95;entries](/ko/operations/server-configuration-parameters/settings#text_index_header_cache_max_entries) | 캐시에 저장되는 역직렬화된 헤더의 최대 개수입니다.             |
| [text&#95;index&#95;header&#95;cache&#95;size&#95;ratio](/ko/operations/server-configuration-parameters/settings#text_index_header_cache_size_ratio)   | 텍스트 인덱스 헤더 캐시에서 전체 캐시 크기 대비 보호 큐의 크기입니다. |

<div id="caching-posting-lists">
  #### 포스팅 리스트 캐시 설정
</div>

| 설정                                                                                                                                                      | 설명                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| [text&#95;index&#95;postings&#95;cache&#95;policy](/ko/operations/server-configuration-parameters/settings#text_index_postings_cache_policy)               | 텍스트 인덱스 포스팅 리스트 캐시 정책 이름입니다.                       |
| [text&#95;index&#95;postings&#95;cache&#95;size](/ko/operations/server-configuration-parameters/settings#text_index_postings_cache_size)                   | 캐시의 최대 크기(바이트)입니다.                                 |
| [text&#95;index&#95;postings&#95;cache&#95;max&#95;entries](/ko/operations/server-configuration-parameters/settings#text_index_postings_cache_max_entries) | 캐시에 저장할 수 있는 역직렬화된 포스팅의 최대 개수입니다.                  |
| [text&#95;index&#95;postings&#95;cache&#95;size&#95;ratio](/ko/operations/server-configuration-parameters/settings#text_index_postings_cache_size_ratio)   | 텍스트 인덱스 포스팅 리스트 캐시에서 전체 캐시 크기 대비 보호 큐가 차지하는 크기입니다. |

<div id="limitations">
  ## 제한 사항
</div>

현재 텍스트 인덱스에는 다음과 같은 제한 사항이 있습니다:

* 토큰 수가 많은 텍스트 인덱스(예: 100억 개의 토큰)를 머티리얼라이즈하는 과정에서 상당한 양의 메모리가 사용될 수 있습니다. 텍스트
  인덱스 머티리얼라이즈는 직접적으로(`ALTER TABLE <table> MATERIALIZE INDEX <index>`) 발생할 수도 있고, 간접적으로 파트 병합 중에 발생할 수도 있습니다.
* 4,294,967,296개(= 2^32 = 약 42억) 이상의 행이 있는 파트에서는 텍스트 인덱스를 구체화할 수 없습니다. 구체화된 텍스트 인덱스가 없으면 쿼리는 해당 파트 내에서 느린 brute-force 검색으로 폴백됩니다. 최악의 경우를 가정해 보면, 파트에 String 타입의 단일 컬럼만 포함되어 있고 MergeTree setting `max_bytes_to_merge_at_max_space_in_pool`(기본값: 150 GB)이 변경되지 않았다고 가정합니다. 이 경우 해당 컬럼의 행당 평균 문자 수가 29.5자 미만이면 이런 상황이 발생합니다. 실제로는 테이블에 다른 컬럼도 포함되므로 임계값은 이보다 몇 배 더 작아집니다(다른 컬럼의 개수, 유형, 크기에 따라 달라집니다).

<div id="text-index-vs-bloom-filter-indexes">
  ## 텍스트 인덱스와 블룸 필터 기반 인덱스 비교
</div>

문자열 프레디케이트는 텍스트 인덱스와 블룸 필터 기반 인덱스(인덱스 유형 `bloom_filter`, `ngrambf_v1`, `tokenbf_v1`, `sparse_grams`)를 사용해 더 빠르게 처리할 수 있지만, 두 방식은 설계와 의도된 사용 사례 측면에서 근본적으로 다릅니다:

**블룸 필터 인덱스**

* false positive를 발생시킬 수 있는 확률적 데이터 구조를 기반으로 합니다.
* 집합 멤버십 여부만 판별할 수 있습니다. 즉, 컬럼에 토큰 X가 포함되어 있을 수 있는지, 아니면 확실히 포함되어 있지 않은지만 알 수 있습니다.
* 쿼리 실행 중 큰 범위를 스키핑할 수 있도록 granule 수준의 정보를 저장합니다.
* 적절하게 튜닝하기가 어렵습니다([여기](mergetree#n-gram-bloom-filter)의 예시 참고).
* 비교적 크기가 작습니다(part당 수 킬로바이트 또는 수 메가바이트).

**텍스트 인덱스**

* 토큰에 대해 결정적인 inverted index를 구축합니다. 인덱스 자체로는 false positive가 발생하지 않습니다.
* 텍스트 검색 워크로드에 특화되어 있습니다.
* 효율적인 용어 lookup이 가능하도록 행 수준의 정보를 저장합니다.
* 비교적 크기가 큽니다(part당 수십~수백 메가바이트).

블룸 필터 기반 인덱스는 전문 검색을 &quot;부수 효과&quot;로만 지원합니다:

* 고급 tokenization 및 전처리를 지원하지 않습니다.
* 여러 토큰을 대상으로 하는 검색을 지원하지 않습니다.
* inverted index에서 기대하는 성능 특성을 제공하지 않습니다.

반면 텍스트 인덱스는 전문 검색에 맞게 특별히 설계되었습니다:

* tokenization 및 전처리를 제공합니다
* `hasAllTokens`, `LIKE`, `match` 및 이와 유사한 텍스트 검색 함수에 대해 효율적으로 지원합니다.
* 대규모 텍스트 코퍼스에서 훨씬 뛰어난 확장성을 제공합니다.

<div id="implementation">
  ## 구현 세부 사항
</div>

각 텍스트 인덱스는 2개의 (추상적인) 데이터 구조로 이루어집니다.

* 각 토큰을 포스팅 리스트에 매핑하는 딕셔너리
* 각각이 행 번호 집합을 나타내는 포스팅 리스트 집합

텍스트 인덱스는 파트 전체를 대상으로 빌드됩니다.
다른 스킵 인덱스와 달리 텍스트 인덱스는 데이터 파트가 머지될 때 다시 빌드하지 않고 머지할 수 있습니다(아래 참조).

인덱스를 생성하는 동안 (파트별로) 3개의 파일이 생성됩니다.

**딕셔너리 블록 파일 (.dct)**

텍스트 인덱스의 토큰은 정렬된 뒤, 각각 512개의 토큰을 담는 딕셔너리 블록에 저장됩니다(블록 크기는 `dictionary_block_size` 매개변수로 구성할 수 있습니다).
딕셔너리 블록 파일(.dct)은 한 파트에 있는 모든 인덱스 그래뉼의 모든 딕셔너리 블록으로 구성됩니다.

**인덱스 헤더 파일 (.idx)**

인덱스 헤더 파일에는 각 딕셔너리 블록별로 해당 블록의 첫 번째 토큰과 딕셔너리 블록 파일 내 상대 오프셋이 저장됩니다.

이 희소 인덱스 구조는 ClickHouse의 [희소 프라이머리 키 인덱스](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes))와 유사합니다.

**포스팅 리스트 파일 (.pst)**

모든 토큰의 포스팅 리스트는 포스팅 리스트 파일에 순차적으로 배치됩니다.
공간을 절약하면서도 빠른 교집합 및 합집합 연산을 지원하기 위해 포스팅 리스트는 [roaring bitmaps](https://roaringbitmap.org/) 형태로 저장됩니다.
포스팅 리스트가 `posting_list_block_size`보다 크면 여러 블록으로 분할되어 포스팅 리스트 파일에 순차적으로 저장됩니다.

**위치 파일 (.pos)**

선택 사항이며 인덱스 인수 `positions = 1`인 경우에만 생성됩니다.
일치하는 행 내에서 토큰의 위치를 저장합니다.

**텍스트 인덱스의 머지**

데이터 파트가 머지될 때 텍스트 인덱스를 처음부터 다시 빌드할 필요는 없습니다. 대신 머지 프로세스의 별도 단계에서 효율적으로 머지할 수 있습니다.
이 단계에서는 각 입력 파트의 텍스트 인덱스에 있는 정렬된 딕셔너리를 읽어 새롭게 통합된 딕셔너리로 결합합니다.
포스팅 리스트의 행 번호도 초기 머지 단계에서 생성된 기존 행 번호와 새 행 번호 간 매핑을 사용해, 머지된 데이터 파트에서의 새 위치를 반영하도록 다시 계산됩니다.
이러한 텍스트 인덱스 머지 방식은 `_part_offset` 컬럼이 있는 [프로젝션](/ko/docs/sql-reference/statements/alter/projection#projection-indexes)이 머지되는 방식과 유사합니다.
소스 파트에서 인덱스가 구체화되어 있지 않으면 인덱스를 빌드해 임시 파일에 기록한 뒤, 다른 파트의 인덱스 및 다른 임시 인덱스 파일의 인덱스와 함께 머지합니다.

**디버깅**

텍스트 인덱스를 검사하는 데 테이블 함수 [mergeTreeTextIndex](../../../sql-reference/table-functions/mergeTreeTextIndex.md)를 사용할 수 있습니다.

<div id="hacker-news-dataset">
  ## 예시: Hackernews 데이터셋
</div>

텍스트가 많은 대규모 데이터셋에서 텍스트 인덱스의 성능 개선 효과를 살펴보겠습니다.
인기 있는 Hacker News 웹사이트의 댓글 2,870만 행을 사용하겠습니다.
다음은 텍스트 인덱스가 없는 테이블입니다:

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

S3에 있는 Parquet 파일에 2,870만 개의 행이 들어 있습니다. 이제 이를 `hackernews` 테이블에 삽입하겠습니다:

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

`ALTER TABLE`을 사용해 comment 컬럼에 텍스트 인덱스를 추가한 다음 구체화합니다:

```sql
-- Add the index
ALTER TABLE hackernews ADD INDEX comment_idx comment TYPE text(tokenizer = splitByNonAlpha);

-- Materialize the index for existing data
ALTER TABLE hackernews MATERIALIZE INDEX comment_idx SETTINGS mutations_sync = 2;
```

이제 `hasToken`, `hasAnyTokens`, `hasAllTokens` 함수를 사용해 쿼리를 실행해 보겠습니다.
다음 예시에서는 일반적인 인덱스 스캔과 직접 읽기 최적화 사이에 큰 성능 차이가 있음을 보여줍니다.

<div id="using-hasToken">
  ### 1. `hasToken` 사용
</div>

`hasToken`은 텍스트에 특정 단일 토큰이 포함되어 있는지 확인합니다.
대소문자를 구분하는 토큰 &#39;ClickHouse&#39;를 검색하겠습니다.

**직접 읽기 비활성화 (표준 스캔)**
기본적으로 ClickHouse는 스킵 인덱스를 사용해 그래뉼을 필터링한 다음, 해당 그래뉼의 컬럼 데이터를 읽습니다.
직접 읽기를 비활성화하면 이 동작을 시뮬레이션할 수 있습니다.

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

**직접 읽기 활성화(빠른 인덱스 읽기)**
이제 직접 읽기가 활성화된 상태(기본값)에서 동일한 쿼리를 실행합니다.

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

직접 읽기 쿼리는 인덱스만 읽기 때문에 45배 이상 더 빠르며(0.362초 대비 0.008초), 처리하는 데이터 양도 현저히 적습니다(9.51 GB 대비 3.15 MB).

<div id="using-hasAnyTokens">
  ### 2. `hasAnyTokens` 사용하기
</div>

`hasAnyTokens`는 텍스트에 지정된 토큰 중 하나 이상이 포함되어 있는지 확인합니다.
&#39;love&#39; 또는 &#39;ClickHouse&#39;가 포함된 댓글을 검색해 보겠습니다.

**직접 읽기 비활성화(표준 스캔)**

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

**직접 읽기 활성화(빠른 인덱스 읽기)**

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

이 일반적인 &quot;OR&quot; 검색에서는 속도 향상 폭이 훨씬 더 큽니다.
전체 컬럼 스캔을 피하면 쿼리 속도가 거의 89배 빨라집니다(1.329초 대 0.015초).

<div id="using-hasAllTokens">
  ### 3. `hasAllTokens` 사용하기
</div>

`hasAllTokens`는 텍스트에 지정된 모든 토큰이 포함되어 있는지 확인합니다.
`love`와 `ClickHouse`가 모두 포함된 댓글을 검색하겠습니다.

**직접 읽기 비활성화(표준 스캔)**
직접 읽기가 비활성화되어 있어도 기본 스킵 인덱스는 여전히 효과적입니다.
28.7M개의 행을 147.46K개의 행으로 필터링할 수 있지만, 여전히 컬럼에서 57.03 MB를 읽어야 합니다.

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

**직접 읽기 활성화됨 (빠른 인덱스 읽기)**
직접 읽기는 인덱스 데이터를 기반으로 쿼리를 처리하므로 147.46 KB만 읽습니다.

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

이 &quot;AND&quot; 검색에서는 직접 읽기 최적화가 일반적인 스킵 인덱스 스캔보다 26배 이상 빠릅니다(0.184초 대비 0.007초).

<div id="compound-search">
  ### 4. 복합 검색: OR, AND, NOT, ...
</div>

직접 읽기 최적화는 복합 불리언 표현식에도 적용됩니다.
여기에서는 &#39;ClickHouse&#39; OR &#39;clickhouse&#39;를 대상으로 대소문자를 구분하지 않는 검색을 수행합니다.

**직접 읽기 비활성화(표준 스캔)**

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

**직접 읽기 활성화(빠른 인덱스 읽기)**

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

인덱스의 결과를 조합하면 직접 읽기 쿼리는 34배 더 빨라지고(0.450초 대비 0.013초), 9.58 GB의 컬럼 데이터를 읽지 않아도 됩니다.
이 경우에는 `hasAnyTokens(comment, ['ClickHouse', 'clickhouse'])`를 사용하는 것이 더 효율적이며 권장되는 구문입니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse 전문 검색 일반 제공 발표](https://clickhouse.com/blog/full-text-search-ga-release)
* 블로그: [객체 스토리지를 위한 고성능 전문 검색 구축](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)
* 동영상: [ClickHouse 전문 검색 소개](https://www.youtube.com/watch?v=9zPmf1a_heU)
* 동영상: [내부 들여다보기: ClickHouse의 규모와 속도에 맞춘 전문 검색](https://www.youtube.com/watch?v=8JbqE_ubfkU)
* 발표 자료: [ClickHouse 전문 검색의 내부: 빠르고 네이티브하며 열 지향적인 구조](https://github.com/ClickHouse/clickhouse-presentations/blob/master/2025-tumuchdata-munich/ClickHouse_%20full-text%20search%20-%2011.11.2025%20Munich%20Database%20Meetup.pdf)
* 발표 자료: [역방향 데이터베이스 인덱스: 왜 필요한지, 무엇인지, 어떻게 구현하는지, FOSDEM 2026](https://presentations.clickhouse.com/2026-fosdem-inverted-index/Inverted_indexes_the_what_the_why_the_how.pdf)

**구버전 자료**

* 블로그: [ClickHouse의 역인덱스 소개](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
* 블로그: [ClickHouse 전문 검색의 내부: 빠르고 네이티브하며 열 지향적인 구조](https://clickhouse.com/blog/clickhouse-full-text-search)
* 동영상: [전문 검색 인덱스: 설계와 실험](https://www.youtube.com/watch?v=O_MnyUkrIq8)