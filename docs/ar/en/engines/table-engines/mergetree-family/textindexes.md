---
description: 'اعثر بسرعة على عبارات البحث في النص.'
keywords: ['البحث النصي الكامل', 'فهرس نصي', 'فهرس', 'فهارس']
sidebar_label: 'البحث النصي الكامل باستخدام الفهارس النصية'
slug: /engines/table-engines/mergetree-family/textindexes
title: 'البحث النصي الكامل باستخدام الفهارس النصية'
doc_type: 'مرجع'
---

تتيح الفهارس النصية (المعروفة أيضًا باسم [الفهارس المعكوسة](https://en.wikipedia.org/wiki/Inverted_index)) إجراء بحث نصي كامل سريع في البيانات النصية.
ويخزّن الفهرس النصي تعيينًا يربط بين الرموز وأرقام الصفوف التي تحتوي على كل رمز.
وتُولَّد هذه الرموز عبر عملية تُسمى تقسيم النص إلى رموز.
فعلى سبيل المثال، يحوّل مُقسِّم الرموز الافتراضي في ClickHouse الجملة الإنجليزية &quot;The cat likes mice.&quot; إلى الرموز [&quot;The&quot;, &quot;cat&quot;, &quot;likes&quot;, &quot;mice&quot;].

على سبيل المثال، افترض وجود جدول يتكون من عمود واحد وثلاثة صفوف

```result
1: The cat likes mice.
2: Mice are afraid of dogs.
3: I have two dogs and a cat.
```

الرموز المقابلة هي:

```result
1: The, cat, likes, mice
2: Mice, are, afraid, of, dogs
3: I, have, two, dogs, and, a, cat
```

نفضّل عادةً إجراء البحث دون مراعاة حالة الأحرف، لذلك نحوّل الرموز إلى أحرف صغيرة:

```result
1: the, cat, likes, mice
2: mice, are, afraid, of, dogs
3: i, have, two, dogs, and, a, cat
```

سنزيل أيضًا الكلمات الشائعة مثل &quot;I&quot; و&quot;the&quot; و&quot;and&quot; لأنها تظهر في كل صف تقريبًا:

```result
1: cat, likes, mice
2: mice, afraid, dogs
3: have, two, dogs, cat
```

يحتوي الفهرس النصي حينئذٍ (من حيث المبدأ) على المعلومات التالية:

```result
afraid : [2]
cat    : [1, 3]
dogs   : [2, 3]
have   : [3]
likes  : [1]
mice   : [1]
two    : [3]
```

باستخدام رمز بحث، تتيح بنية الفهرس هذه العثور بسرعة على جميع الصفوف المطابقة.

<div id="creating-a-text-index">
  ## إنشاء فهرس نصي
</div>

أصبحت الفهارس النصية متاحة للاستخدام العام (GA) في ClickHouse الإصدار 26.2 والإصدارات الأحدث.
في هذه الإصدارات، لا حاجة إلى تهيئة أي إعدادات خاصة لاستخدام الفهرس النصي.
نوصي بشدة باستخدام إصدارات ClickHouse &gt;= 26.2 في حالات استخدام الإنتاج.

:::note
يمكن استخدام الفهارس النصية مع أي إصدار من ClickHouse &gt;= 26.2، بغض النظر عن إعداد [compatibility](../../../operations/settings/settings#compatibility).
:::

لإنشاء فهرس نصي، استخدم الصياغة التالية:

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

يمكن تعريف فهارس نصية على أعمدة من الأنواع التالية:

* [String](/ar/sql-reference/data-types/string.md) و[FixedString](/ar/sql-reference/data-types/fixedstring.md)،
* [Array(String)](/ar/sql-reference/data-types/array.md) و[Array(FixedString)](/ar/sql-reference/data-types/array.md)،
* [Map](/ar/sql-reference/data-types/map.md) (باستخدام الدالتين [mapKeys](/ar/sql-reference/functions/tuple-map-functions.md/#mapKeys) و[mapValues](/ar/sql-reference/functions/tuple-map-functions.md/#mapValues))، و
* [JSON](/ar/sql-reference/data-types/newjson.md) (باستخدام الدالتين [JSONAllPaths](/ar/sql-reference/functions/json-functions.md/#JSONAllPaths) و[`JSONAllValues`](/ar/sql-reference/functions/json-functions.md#JSONAllValues)).

الأعمدة من النوع [Nullable(T)](/ar/sql-reference/data-types/nullable.md) و[LowCardinality()](/ar/sql-reference/data-types/lowcardinality.md) مدعومة أيضًا، بما في ذلك `Array(Nullable(String or FixedString))`.

بدلًا من ذلك، لإضافة فهرس نصي إلى جدول موجود:

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

إذا أضفت فهرسًا إلى جدول موجود، فنوصي بتجسيد الفهرس لأجزاء الجدول الحالية (وإلا فسيعود البحث في الأجزاء غير المفهرسة إلى عمليات فحص بالقوة الغاشمة بطيئة).

```sql title="Query"
ALTER TABLE table MATERIALIZE INDEX text_idx SETTINGS mutations_sync = 2;
```

لإزالة فهرس نصي، يُرجى تنفيذ

```sql title="Query"
ALTER TABLE table DROP INDEX text_idx;
```

**وسيطة مُقسِّم الرموز (إلزامية)**. تحدد وسيطة `tokenizer` مُقسِّم الرموز المستخدم:

* `splitByNonAlpha` يقسم السلاسل النصية عند محارف ASCII غير الأبجدية الرقمية (راجع الدالة [splitByNonAlpha](/ar/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha)).
* `splitByString(S)` يقسم السلاسل النصية باستخدام سلاسل فصل محددة يعرّفها المستخدم `S` (راجع الدالة [splitByString](/ar/sql-reference/functions/splitting-merging-functions.md/#splitByString)).
  يمكن تحديد الفواصل باستخدام معلمة اختيارية، على سبيل المثال: `tokenizer = splitByString([', ', '; ', '\n', '\\'])`.
  لاحظ أن كل سلسلة يمكن أن تتكون من عدة محارف (`', '` في المثال).
  إذا لم تُحدَّد قائمة الفواصل الافتراضية صراحةً (على سبيل المثال، `tokenizer = splitByString`)، فستكون مسافة بيضاء واحدة `[' ']`.
* `asciiCJK` يقسم السلاسل النصية إلى رموز باستخدام قواعد حدود الكلمات في Unicode (على نحو مشابه لـ [Unicode Text Segmentation (UAX #29)](https://unicode.org/reports/tr29/)). تُشكِّل محارف ASCII الأبجدية الرقمية والشرطات السفلية رموزًا مع الموصلات (ASCII `:` للحروف، و`.` و`'` للمحارف من النوع نفسه). أما محارف Unicode غير التابعة لـ ASCII، بما في ذلك محارف [CJK](https://en.wikipedia.org/wiki/CJK_characters)، فتتحول إلى رموز من محرف واحد.
* `ngrams(N)` يقسم السلاسل النصية إلى `N`-grams متساوية الطول (راجع الدالة [ngrams](/ar/sql-reference/functions/splitting-merging-functions.md/#ngrams)).
  يمكن تحديد طول ngram باستخدام معلمة عددية صحيحة اختيارية بين 1 و8، على سبيل المثال: `tokenizer = ngrams(3)`.
  إذا لم يُحدَّد حجم ngram الافتراضي صراحةً (على سبيل المثال، `tokenizer = ngrams`)، فسيكون 3.
* `sparseGrams(min_length, max_length, min_cutoff_length)` يقسم السلاسل النصية إلى n-grams متغيرة الطول لا يقل طولها عن `min_length` ولا يزيد على `max_length` (شاملًا) من المحارف (راجع الدالة [sparseGrams](/ar/sql-reference/functions/string-functions#sparseGrams)).
  ما لم يُحدَّد ذلك صراحةً، تكون القيم الافتراضية لـ `min_length` و`max_length` هي 3 و100.
  إذا جرى توفير المعلمة `min_cutoff_length`، فلن تُعاد إلا n-grams التي يكون طولها أكبر من أو مساويًا لـ `min_cutoff_length`.
  بالمقارنة مع `ngrams(N)`، يُنتج مُقسِّم الرموز `sparseGrams` N-grams متغيرة الطول، مما يتيح تمثيلًا أكثر مرونة للنص الأصلي.
  على سبيل المثال، `tokenizer = sparseGrams(3, 5, 4)` يولّد داخليًا 3-grams و4-grams و5-grams من سلسلة الإدخال، لكن لا تُعاد إلا 4-grams و5-grams.
* `array` لا يُجري أي تقسيم إلى رموز، أي إن قيمة كل صف تُعد رمزًا (راجع الدالة [array](/ar/sql-reference/functions/array-functions.md/#array)).

جميع مُقسِّمات الرموز المتاحة مُدرجة في [system.tokenizers](../../../operations/system-tables/tokenizers.md).

:::note
يطبّق مُقسِّم الرموز `splitByString` فواصل التقسيم من اليسار إلى اليمين.
وقد يؤدي ذلك إلى التباس.
فعلى سبيل المثال، ستتسبب سلاسل الفواصل `['%21', '%']` في تقسيم `%21abc` إلى الرموز `['abc']`، بينما سيؤدي تبديل سلسلتي الفواصل إلى `['%', '%21']` إلى إخراج `['21abc']`.
في معظم الحالات، ستحتاج إلى أن تُفضِّل المطابقة الفواصل الأطول أولًا.
ويمكن تحقيق ذلك عمومًا بتمرير سلاسل الفواصل بترتيب تنازلي حسب الطول.
إذا كانت سلاسل الفواصل تشكل [prefix code](https://en.wikipedia.org/wiki/Prefix_code)، فيمكن تمريرها بأي ترتيب.
:::

لفهم كيفية تقسيم مُقسِّم الرموز لسلسلة الإدخال، يمكنك استخدام الدالتين [tokens](/ar/sql-reference/functions/splitting-merging-functions.md/#tokens) و[tokensForLikePattern](/ar/sql-reference/functions/splitting-merging-functions.md/#tokensForLikePattern):

مثال:

```sql title="Query"
SELECT tokens('abc def', 'ngrams', 3);
```

```result title="Response"
['abc','bc ','c d',' de','def']
```

*التعامل مع مدخلات غير ASCII.*
يمكن إنشاء الفهارس النصية استنادًا إلى بيانات نصية بأي لغة وبأي مجموعة محارف.
بالنسبة إلى النصوص غير ASCII، يُوصى باستخدام المقسِّم `asciiCJK` لأنه يتعامل بصورة صحيحة مع حدود الكلمات في Unicode، بما في ذلك محارف CJK.
:::

**وسيطة المعالج المسبق (اختيارية)**. يشير المعالج المسبق إلى تعبير يُطبَّق على سلسلة الإدخال قبل تقسيمها إلى رموز.

من حالات الاستخدام الشائعة لوسيطة المعالج المسبق:

1. التحويل إلى الأحرف الصغيرة/الكبيرة، أو توحيد حالة الأحرف لتمكين المطابقة غير الحساسة لحالة الأحرف، مثل [lower](/ar/sql-reference/functions/string-functions.md/#lower)، [lowerUTF8](/ar/sql-reference/functions/string-functions.md/#lowerUTF8)، [caseFoldUTF8](/ar/sql-reference/functions/string-functions.md/#caseFoldUTF8).
2. تطبيع UTF-8، مثل [normalizeUTF8NFC](/ar/sql-reference/functions/string-functions.md/#normalizeUTF8NFC)، [normalizeUTF8NFD](/ar/sql-reference/functions/string-functions.md/#normalizeUTF8NFD)، [normalizeUTF8NFKC](/ar/sql-reference/functions/string-functions.md/#normalizeUTF8NFKC)، [normalizeUTF8NFKD](/ar/sql-reference/functions/string-functions.md/#normalizeUTF8NFKD)، [normalizeUTF8NFKCCasefold](/ar/sql-reference/functions/string-functions.md/#normalizeUTF8NFKCCasefold)، [toValidUTF8](/ar/sql-reference/functions/string-functions.md/#toValidUTF8).
3. إزالة الأحرف أو السلاسل الفرعية غير المرغوب فيها أو تحويلها، مثل إزالة علامات التشكيل، باستخدام [extractTextFromHTML](/ar/sql-reference/functions/string-functions.md/#extractTextFromHTML)، [substring](/ar/sql-reference/functions/string-functions.md/#substring)، [idnaEncode](/ar/sql-reference/functions/string-functions.md/#idnaEncode)، [translate](/ar/sql-reference/functions/string-replace-functions.md/#translate)، [removeDiacriticsUTF8](/ar/sql-reference/functions/string-functions.md/#removeDiacriticsUTF8).

يجب أن يحوّل تعبير المعالج المسبق قيمة إدخال من النوع [String](/ar/sql-reference/data-types/string.md) أو [FixedString](/ar/sql-reference/data-types/fixedstring.md) إلى قيمة من النوع نفسه.
إذا كان الفهرس النصي قد بُني على عمود من النوع `Nullable(T)` أو `LowCardinality(T)`، فيجب أن يقبل تعبير المعالج المسبق القيم القابلة لأن تكون NULL أو منخفضة الكاردينالية (أي ألّا يُثير استثناءً).

أمثلة:

* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(col, '\n', 1))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(extractTextFromHTML(col)))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(col)))`

كذلك، يجب ألا يشير تعبير المعالج المسبق إلا إلى العمود أو التعبير الذي عُرِّف الفهرس النصي عليه.

أمثلة:

* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(lower(col)))`
* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(col), lower(col)))`
* غير مسموح: `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(col, col))`

استخدام الدوال غير الحتمية محظور.

:::note
المعالِجات المسبقة تعادل من حيث المبدأ إحاطة عمود الفهرس أو التعبير بتعبير المعالج المسبق.
على سبيل المثال، يمكن محاكاة المعالج المسبق `lower` في `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))` باستخدام `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha')`.
ويكمن عيب هذا الشكل الأخير في أن المعالج المسبق المُحاكى لا يُطبَّق إلا إذا طابق شرط التصفية في عبارة WHERE.
فعلى سبيل المثال، تتطابق `WHERE hasAllTokens(lower(col), [...])` بينما لا تتطابق `WHERE hasAllTokens(col, [...])`.
لذلك، نوصي باستخدام تعبيرات المعالج المسبق للحصول على أفضل تجربة استخدام.
:::

تستخدم الدوال [hasToken](/ar/sql-reference/functions/string-search-functions.md/#hasToken) و[hasAllTokens](/ar/sql-reference/functions/string-search-functions.md/#hasAllTokens) و[hasAnyTokens](/ar/sql-reference/functions/string-search-functions.md/#hasAnyTokens) و[hasPhrase](/ar/sql-reference/functions/string-search-functions.md/#hasPhrase) المعالج المسبق لتحويل مصطلح البحث أولًا قبل تقسيمه إلى tokens.
لاحظ أنه نظرًا إلى أن المعالج المسبق لا يُطبَّق إلا على مسار الفهرس النصي، فقد تختلف نتائج هذه الدوال بين الاستعلامات التي تستخدم الفهرس النصي والاستعلامات التي لا تستخدمه (مثل `SETTINGS use_skip_indexes = 0`).

على سبيل المثال،

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

يعادل ما يلي:

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

في هذه الحالة، يحوِّل تعبير المعالجة المسبقة عناصر المصفوفة كل عنصر على حدة.

مثال:

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

لتعريف معالج مسبق في فهرس نصي عند إنشائه على أعمدة من النوع [Map](/ar/sql-reference/data-types/map.md)، يحتاج المستخدمون إلى تحديد ما إذا كان الفهرس
مبنيًا على مفاتيح الخريطة أم على قيمها.

مثال:

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

**وسيطة معالج لاحق (اختيارية)**. يشير معالج لاحق إلى تعبير يُطبَّق على كل token ناتج بعد tokenization.

وعلى خلاف معالج مسبق، الذي يحوّل سلسلة الإدخال بالكامل قبل أن يقسمها tokenizer إلى tokens، يعمل معالج لاحق على الـ tokens نفسها، واحدةً تلو الأخرى.
وهذا هو الموضع الطبيعي للتحويلات التي تكون بطبيعتها على مستوى الـ token.

تشمل حالات الاستخدام المعتادة لوسيطة معالج لاحق ما يلي:

1. **تصفية stop words (tokens شديدة التكرار)**. إن الـ tokens الشائعة جدًا مثل &quot;the&quot; و&quot;a&quot; و&quot;is&quot; تحمل قيمة محدودة جدًا من حيث صلة البحث وتؤدي إلى تضخيم الفهرس.
   يمكنك استخدام معالج لاحق لاستبعادها بتحويلها إلى tokens فارغة — ويتم تجاهل tokens الفارغة، أي لا تُضاف إلى الفهرس.
   Example: `if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)`
2. **إزالة timestamp**. غالبًا ما تبدأ أسطر Log بطابع زمني منظَّم مثل `2024-01-15T10:23:45` أو تتضمنه.
   يؤدي indexing لـ tokens الخاصة بالطابع الزمني إلى تضخيم الفهرس بسلاسل لا تحمل أي صلة بالبحث.
   توجد طريقتان متكاملتان لتجاهل timestamps:
   * **نهج معالج لاحق**: استخدم tokenizer ‏`splitByString` (التقسيم حسب المسافات البيضاء) بحيث يصبح الطابع الزمني بالكامل token واحدًا، ثم استخدم `parseDateTimeOrNull` لاكتشافه وحذفه.
     Example: `if(isNull(parseDateTimeOrNull(str, '%Y-%m-%dT%H:%i:%S')), str, '')`
     بالنسبة إلى timestamps التي تتضمن timezone offsets أو fractional seconds، استخدم `parseDateTimeBestEffortOrNull(str)` من دون format string صريح.
   * **نهج معالج مسبق**: أزل الطابع الزمني من سطر Log الكامل *قبل* tokenization باستخدام تعبير نمطي.
     Example: `replaceRegexpAll(str, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')`
     يعمل هذا مع أي tokenizer، وهو أكثر كفاءة لأن محارف الطابع الزمني لا تخضع لـ tokenization أصلًا.
     ويمكن الجمع بين النهجين: يزيل معالج مسبق الطابع الزمني، بينما يقوم معالج لاحق بتطبيع الـ tokens المتبقية أو تصفيتها (مثل التحويل إلى أحرف صغيرة + حذف كلمات الشدة مثل `ERROR` أو `INFO`).
3. **Stemming**. يؤدي ربط كل token بجذره إلى تحسين استرجاع البحث عبر مطابقة المتغيرات الصرفية التي تشترك في الجذر نفسه.
   فعلى سبيل المثال، مع stemming للغة الإنجليزية، تُختزل &quot;running&quot; و&quot;runs&quot; و&quot;run&quot; جميعًا إلى &quot;run&quot;، لذا فإن query لأي من هذه المتغيرات يطابقها كلها.
   يوفّر ClickHouse دالة [stem](/ar/sql-reference/functions/string-functions.md/#stem) مضمّنة لعدة لغات.
   Example: `stem(str, 'en')`
4. **تطبيع حالة الأحرف**. تحويل الـ tokens إلى أحرف صغيرة أو كبيرة لتمكين case-insensitive matching، مثل [lower](/ar/sql-reference/functions/string-functions.md/#lower) و[lowerUTF8](/ar/sql-reference/functions/string-functions.md/#lowerUTF8).
   بالنسبة للتحويل إلى الأحرف الصغيرة والكبيرة، نوصي باستخدام معالج مسبق بدلًا من معالج لاحق.

يحوّل تعبير معالج لاحق tokens من النوع [String](/ar/sql-reference/data-types/string.md) إلى tokens من النوع نفسه.
كما يجب أن يشير تعبير معالج لاحق فقط إلى العمود أو التعبير الذي يُعرَّف فهرس النص على أساسه.
وعندما يكون العمود من النوع `Array(String)`، يظل معالج لاحق يعمل على الـ tokens الفردية بوصفها قيم `String` عادية.

يُحظر استخدام الدوال غير الحتمية.

يُطبَّق المعالج اللاحق على كل رمز يتم توليده أثناء بناء الفهرس (وبالنسبة إلى مُقسِّم الرموز `array`، يُعدّ كل عنصر في المصفوفة رمزًا). عند وقت الاستعلام، يعتمد السلوك على الدالة:

* بالنسبة إلى `hasToken` و`hasAllTokens` و`hasAnyTokens` و`hasPhrase` (مع أي مُقسِّم رموز مدعوم): يُطبَّق المعالج اللاحق على كلٍّ من رموز النص المُراد البحث فيه وعبارة البحث، مما يتيح مطابقة مُطبَّعة بالكامل (مثل البحث غير الحساس لحالة الأحرف). وبالنسبة إلى `hasPhrase`، تُوضَع الرموز بعد المعالجة اللاحقة بشكل متجاور، لذلك إذا حذف المعالج اللاحق رمزًا فلن يترك فجوة موضعية، وستظل العبارة تتطابق عبره — على سبيل المثال، مع معالج لاحق للكلمات الشائعة يحذف `the`، فإن `hasPhrase(col, 'see cat')` يطابق مستندًا يحتوي على `see the cat`.
* بالنسبة إلى جميع الدوال الأخرى (`=`, `IN`, `has`, `hasAny`, `hasAll`, `mapContains*`): لا تُطبَّق المعالجة اللاحقة إلا على عبارة البحث لأغراض lookup لتلميح الفهرس؛ أما predicate على مستوى الصف فيظل يقارن بقيم العمود الأصلية.

أمثلة:

* إزالة الكلمات الشائعة باستخدام تعبير معالج لاحق:

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

* أزل الطوابع الزمنية باستخدام تعبير المعالج اللاحق:

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

* أزل العلامات الزمنية باستخدام تعبير معالج مسبق:

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

* أزل الطوابع الزمنية باستخدام تعبير مدمج لمعالج مسبق ومعالج لاحق:

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

* أرجِع الرموز إلى جذورها باستخدام تعبير المعالج اللاحق:

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

**دعم الدوال**.

بالنسبة إلى المسندات التي تستخدم فهرس النص، يُطبَّق المعالج المسبق والمعالج اللاحق على قيمة البحث قبل الفحص على مستوى الحبيبة، بحيث يستخدم البحث في الفهرس الرموز نفسها التي خُزِّنت عند بناء الفهرس.
وبالنسبة إلى معظم الدوال (`=`, `IN`, `startsWith`, `endsWith`, `LIKE`, `mapContains*`)، لا يُستخدم فهرس النص إلا لتخطّي كتل البيانات غير ذات الصلة؛ ولا يزال ClickHouse يتحقق من كل صف متبقٍّ باستخدام المسند الأصلي في مقابل بيانات العمود الأصلية.
وبالنسبة إلى دوال البحث عن الرموز (`hasToken`, `hasAllTokens`, `hasAnyTokens`)، يكون فهرس النص هو مسار التقييم الأساسي: إذ يطبّع ClickHouse `needle` باستخدام المعالج المسبق ومُقسِّم الرموز والمعالج اللاحق أنفسهم الذين طُبّقوا وقت بناء الفهرس، ويستخدم هذه الصيغة المُطبَّعة لكلٍّ من أجزاء الجدول المفهرسة وغير المفهرسة. ومع وجود معالج لاحق، تُطبَّع أيضًا رموز `haystack` وقت الاستعلام (مع أي مُقسِّم رموز، وليس فقط `array`)، بحيث يُحوَّل جانبا المقارنة بصورة متّسقة ولا تعتمد النتيجة على ما إذا كان الفهرس يُقرأ مباشرةً (الإعداد `query_plan_direct_read_from_text_index`) أو ما إذا كان جزء معيّن يحتوي على فهرس مُجسَّد — على سبيل المثال، تمكين المطابقة غير الحساسة لحالة الأحرف لـ `hasAllTokens(col, ['FOO'])` باستخدام معالج لاحق `lower`.
ومن دون `positions`، تستخدم `hasPhrase` الفهرس كتلميح فقط وتتحقق من كل صف متبقٍّ باستخدام المسند الأصلي؛ كما يطبّع المعالج اللاحق أيضًا كلاً من العبارة ورموز `haystack` بالطريقة نفسها، بحيث تكون النتيجة مستقلة عن مسار القراءة، ولا يؤدّي إسقاط الرموز بواسطة المعالج اللاحق إلى كسر تجاور العبارة. ومع `positions = 1`، تستخدم `hasPhrase` قراءات مباشرة دقيقة (مع الاستمرار في تطبيق المعالج اللاحق، إن وجد).
تُتجاهَل رموز البحث التي يحوّلها المعالج اللاحق إلى سلسلة فارغة، أي تُعامَل كما لو كانت غير موجودة في عبارة البحث.

| الدالة                                                                                      | يدعم معالجًا مسبقًا                       | مُقسِّمات الرموز المتوافقة                               | يدعم معالجًا لاحقًا |
| ------------------------------------------------------------------------------------------- | ----------------------------------------- | -------------------------------------------------------- | ------------------- |
| `=`                                                                                         | نعم                                       | جميعها                                                   | نعم                 |
| `IN`                                                                                        | نعم                                       | جميعها                                                   | نعم                 |
| [hasToken](/ar/sql-reference/functions/string-search-functions.md/#hasToken)                   | نعم                                       | جميعها (مصممة لـ `splitByNonAlpha`)                      | نعم                 |
| [hasAnyTokens(col, str)](/ar/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | نعم                                       | جميعها                                                   | نعم                 |
| [hasAllTokens(col, str)](/ar/sql-reference/functions/string-search-functions.md/#hasAllTokens) | نعم                                       | جميعها                                                   | نعم                 |
| [hasAnyTokens(col, arr)](/ar/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | لا (تُستخدَم عناصر المصفوفة كرموز كما هي) | جميعها                                                   | نعم                 |
| [hasAllTokens(col, arr)](/ar/sql-reference/functions/string-search-functions.md/#hasAllTokens) | لا (تُستخدَم عناصر المصفوفة كرموز كما هي) | جميعها                                                   | نعم                 |
| [hasPhrase](/ar/sql-reference/functions/string-search-functions.md/#hasPhrase)                 | نعم                                       | `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` | نعم                 |
| [startsWith](/ar/sql-reference/functions/string-functions.md/#startsWith)                      | نعم                                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | نعم                 |
| [endsWith](/ar/sql-reference/functions/string-functions.md/#endsWith)                          | نعم                                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | نعم                 |
| [like](/ar/sql-reference/functions/string-search-functions.md/#like)                           | نعم¹                                      | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | نعم¹                |
| [match](/ar/sql-reference/functions/string-search-functions.md/#match)                         | نعم¹                                      | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | نعم¹                |
| [ilike](/ar/sql-reference/functions/string-search-functions.md/#like)                          | نعم² (`lower`/`upper` فقط)                | `splitByNonAlpha`, `array`²                              | لا²                 |
| [mapContainsKey](/ar/sql-reference/functions/tuple-map-functions#mapContainsKey)               | نعم                                       | جميعها                                                   | نعم                 |
| [mapContainsValue](/ar/sql-reference/functions/tuple-map-functions#mapContainsValue)           | نعم                                       | جميعها                                                   | نعم                 |
| [mapContainsKeyLike](/ar/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)       | نعم                                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | نعم                 |
| [mapContainsValueLike](/ar/sql-reference/functions/tuple-map-functions#mapContainsValueLike)   | نعم                                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | نعم                 |
| [has](/ar/sql-reference/functions/array-functions.md/#has)                                     | نعم                                       | `array`                                                  | نعم                 |
| [hasAny](/ar/sql-reference/functions/array-functions.md/#hasAny)                               | نعم                                       | `array`                                                  | نعم                 |
| [hasAll](/ar/sql-reference/functions/array-functions.md/#hasAll)                               | نعم                                       | `array`                                                  | نعم                 |

¹ تستخدم `LIKE` و`match` القراءة المباشرة كتلميح مع مُقسِّمات الرموز المذكورة؛ وإلا فتعودان إلى الفحص بالقوة الغاشمة.
ويدعم `LIKE` أيضًا *القراءة المباشرة (من دون تلميح)* (مُمكّنة عبر `use_text_index_like_evaluation_by_dictionary_scan`) مع مُقسِّمَي الرموز `splitByNonAlpha` و`array` من دون معالج مسبق أو لاحق.

² لا يُدعَم `ILIKE` إلا عبر القراءة المباشرة (من دون تلميح) (`use_text_index_like_evaluation_by_dictionary_scan = 1`، مع مُقسِّم الرموز `splitByNonAlpha` أو `array`).
ولا يوجد مسار احتياطي لاستخدام الفهرس كتلميح: إذا كان هذا الإعداد معطّلًا أو لم يكن مُقسِّم الرموز ضمن المجموعة المدعومة، فلن يُستخدم الفهرس مع `ILIKE`.
ويجب أن يكون المعالج المسبق، إن وُجد، هو `lower` أو `upper`؛ أما المعالجات اللاحقة فغير مدعومة.

**تجريبي: الوسيطة Positions (اختيارية)**.

المَعلمة التجريبية `positions` (الافتراضي: `0`) تتحكم في ما إذا كان الفهرس يخزّن مواضع الرموز.
عند ضبطها على `1`، يخزّن الفهرس أيضًا بيانات المواضع (في ملف `.pos`)، مما يتيح المطابقة الدقيقة للعبارات عبر القراءة المباشرة للدالة [`hasPhrase`](#functions-example-hasphrase).
يؤدي تخزين المواضع إلى زيادة حجم الفهرس على القرص وارتفاع تكلفة الكتابة، لذا فهو خيار يتطلب التفعيل صراحةً.
تنسيق التخزين على القرص ليس مستقرًا بعد، لذا فهذه المَعلمة تجريبية وقد تتغير في إصدار مستقبلي.
لذلك، فإن إنشاء فهرس باستخدام `positions = 1` يتطلب تمكين إعداد MergeTree [`allow_experimental_text_index_positions`](/ar/operations/settings/merge-tree-settings#allow_experimental_text_index_positions).
اضبط `positions = 0` (الافتراضي) للإبقاء على التخزين المعتمد على posting list فقط؛ وستظل فهارس النص التي أُنشئت بدون هذه الوسيطة بلا مواضع.

:::warning
هذه الوسيطة تجريبية ويجب استخدامها للاختبار فقط.
اضبط إعداد MergeTree [`allow_experimental_text_index_positions`](/ar/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) لتمكين تخزين المواضع.
:::

<details markdown="1">
  <summary>مَعلمات متقدمة اختيارية</summary>

  ستعمل القيم الافتراضية للمَعلمات المتقدمة التالية جيدًا في جميع الحالات تقريبًا.
  لا نوصي بتغييرها.

  المَعلمة الاختيارية `dictionary_block_size` (الافتراضي: 512) تحدد حجم كتل القاموس بالصفوف.

  المَعلمة الاختيارية `dictionary_block_frontcoding_compression` (الافتراضي: 1) تحدد ما إذا كانت كتل القاموس تستخدم front coding كآلية Compression.

  المَعلمة الاختيارية `posting_list_block_size` (الافتراضي: 1048576) تحدد حجم كتل posting list بالصفوف.

  المَعلمة الاختيارية `posting_list_codec` (الافتراضي: `none`) تحدد codec المستخدم لـ posting list:

  * `none` - تُخزَّن posting lists بدون Compression إضافي.
  * `bitpacking` - يطبّق ترميز [تفاضلي (دلتا)](https://en.wikipedia.org/wiki/Delta_encoding)، يتبعه [bit-packing](https://dev.to/madhav_baby_giraffe/bit-packing-the-secret-to-optimizing-data-storage-and-transmission-m70) (كلٌّ منهما ضمن كتل ذات حجم ثابت). يؤدي ذلك إلى إبطاء استعلامات SELECT، ولا يُنصح به حاليًا.

  يمكن بدلاً من ذلك ضبط المَعلمات المتقدمة أعلاه على مستوى الجدول من خلال إعدادات MergeTree المقابلة: [`text_index_dictionary_block_size`](/ar/operations/settings/merge-tree-settings#text_index_dictionary_block_size)، و[`text_index_dictionary_block_frontcoding_compression`](/ar/operations/settings/merge-tree-settings#text_index_dictionary_block_frontcoding_compression)، و[`text_index_posting_list_block_size`](/ar/operations/settings/merge-tree-settings#text_index_posting_list_block_size)، و[`text_index_posting_list_codec`](/ar/operations/settings/merge-tree-settings#text_index_posting_list_codec).
  وتنطبق هذه الإعدادات على كل فهرس نصي في الجدول لا يحدد المَعلمة صراحةً.

  تتمثل حالة الاستخدام الرئيسية للإعدادات على مستوى الجدول في تغيير مَعلمات الفهرس لجدول موجود بدون إسقاط الفهرس النصي وإعادة إنشائه على جميع table parts.
  ويؤدي تغيير إعداد على مستوى الجدول إلى تطبيق المَعلمات الجديدة فقط على فهارس النص المبنية للأجزاء الجديدة؛ أما الأجزاء الحالية فتحتفظ بالبنية الحالية لها.

  تأخذ الوسيطة المحددة في تعريف الفهرس الأسبقية على إعداد الجدول، على سبيل المثال:

  ```sql
  CREATE TABLE table(
      s String,
      -- يستخدم هذا الفهرس 'bitpacking'، متجاوزًا القيمة الافتراضية على مستوى الجدول أدناه:
      INDEX idx_a s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'),
      -- يرث هذا الفهرس 'none' من إعداد الجدول:
      INDEX idx_b lower(s) TYPE text(tokenizer = 'splitByNonAlpha'))
  ENGINE = MergeTree()
  ORDER BY tuple()
  SETTINGS text_index_posting_list_codec = 'none';
  ```
</details>

*درجة دقة الفهرس.*
تُنفَّذ فهارس النص داخل ClickHouse كنوع من [skip indexes](/ar/engines/table-engines/mergetree-family/mergetree.md/#skip-index-types).
ومع ذلك، وعلى عكس skip indexes الأخرى، تستخدم فهارس النص درجة دقة لا نهائية (100 مليون).
ويمكن ملاحظة ذلك في تعريف جدول الفهرس النصي.

مثال:

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

تضمن قيمة دقة الفهرس الكبيرة جدًا إنشاء الفهرس النصي للجزء بأكمله.
ويُتجاهل أيّ مقدار لدقة الفهرس يُحدَّد صراحةً.

<div id="using-a-text-index">
  ## استخدام فهرس نصي
</div>

يُعد استخدام فهرس نصي في استعلامات SELECT أمرًا مباشرًا، إذ تستفيد دوال البحث النصي الشائعة في السلاسل النصية من الفهرس تلقائيًا.
إذا لم يكن هناك فهرس على عمود أو جزء من الجدول، فستعود دوال البحث في السلاسل النصية إلى عمليات فحص بالقوة الغاشمة بطيئة.

:::note
نوصي باستخدام الدالتين `hasAnyTokens` و`hasAllTokens` للبحث في الفهرس النصي؛ يُرجى الاطلاع على [أدناه](#functions-example-hasanytokens-hasalltokens).
تعمل هاتان الدالتان مع جميع مُقسِّم الرموز المتاحة وجميع تعبيرات المعالجة المسبقة واللاحقة الممكنة.
ونظرًا إلى أن الدوال المدعومة الأخرى سبقت الفهرس النصي تاريخيًا، فقد كان عليها الاحتفاظ بسلوكها القديم في كثير من الحالات (على سبيل المثال، عدم دعم المعالجة المسبقة أو اللاحقة).
:::

<div id="functions-support">
  ### الدوال المدعومة
</div>

يمكن استخدام الفهرس النصي إذا استُخدمت دوال النص في عبارة `WHERE` أو عبارات `PREWHERE`:

```sql
SELECT [...]
FROM [...]
WHERE string_search_function(column_with_text_index)
```

<div id="functions-example-equals">
  #### `=`
</div>

`=` ([equals](/ar/sql-reference/functions/comparison-functions.md/#equals)) يطابق كامل مصطلح البحث المُعطى.

مثال:

```sql
SELECT * from table WHERE str = 'Hello';
```

<div id="functions-example-in">
  #### `IN`
</div>

`IN` ([in](/ar/sql-reference/functions/in-functions)) تشبه `equals`، لكنها تطابق جميع مصطلحات البحث.

مثال:

```sql
SELECT * from table WHERE str IN ('Hello', 'World');
```

:::note
`NOT IN` (`notIn`) غير مدعوم في الفهرس النصي.
:::

<div id="functions-example-like-match">
  #### `LIKE` و `match`
</div>

:::note
تستخدم هذه الدوال حاليًا فهرس النص للتصفية فقط إذا كان مُقسِّم الرموز للفهرس هو `splitByNonAlpha` أو `ngrams` أو `sparseGrams`.
:::

:::note
لا يدعم فهرس النص `NOT LIKE` (`notLike`).
:::

لاستخدام `LIKE` ([like](/ar/sql-reference/functions/string-search-functions.md/#like)) والدالة [match](/ar/sql-reference/functions/string-search-functions.md/#match) مع فهارس النص، يجب أن يتمكن ClickHouse من استخراج رموز كاملة من عبارة البحث.
وبالنسبة إلى الفهرس الذي يستخدم مُقسِّم الرموز `ngrams`، يتحقق ذلك إذا كان طول السلاسل النصية المطلوب البحث عنها بين محارف البدل مساويًا لطول ngram أو أكبر منه.

مثال على فهرس النص الذي يستخدم مُقسِّم الرموز `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE comment LIKE 'support%';
```

يمكن أن يطابق `support` في المثال كلاً من `support` و`supports` و`supporting` وما إلى ذلك.
هذا النوع من الاستعلامات هو استعلام substring، ولا يمكن تسريعه باستخدام فهرس نصي.

للاستفادة من فهرس نصي في استعلامات LIKE، يجب إعادة كتابة نمط LIKE على النحو التالي:

```sql
SELECT count() FROM table WHERE comment LIKE ' support %'; -- or `% support %`
```

تضمن المسافات الواقعة إلى يسار `support` ويمينه إمكانية استخراج هذا المصطلح بوصفه token.

لحسن الحظ، توجد حالة خاصة يمكن فيها لـ ClickHouse الاستفادة من الفهرس المعكوس لتسريع استعلامات LIKE بدرجة كبيرة.

راجع [قسم ضبط أداء LIKE/ILIKE](#like-ilike-queries-perf) للاطلاع على التفاصيل.

<div id="functions-example-multisearchany-multimatchany">
  #### `multiSearchAny` and `multiMatchAny`
</div>

تتحقق [multiSearchAny](/ar/sql-reference/functions/string-search-functions.md/#multiSearchAny) ومتغيرها UTF-8 [multiSearchAnyUTF8](/ar/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8) مما إذا كانت أيٌّ من عدة سلاسل فرعية حرفية موجودة في السلسلة المُراد البحث فيها، بينما تتحقق [multiMatchAny](/ar/sql-reference/functions/string-search-functions.md/#multiMatchAny) مما إذا كان أيٌّ من عدة تعبيرات نمطية يطابق.
تستخدم هذه الدوال الفهرس النصي وفق الشروط نفسها المستخدمة مع `LIKE` و`match` (انظر أعلاه): يجب أن يتمكن ClickHouse من استخراج رموز كاملة من كل نمط بحث، ويجب أن تكون قائمة أنماط البحث ثابتة.
تُقرأ granule إذا كان من الممكن أن يوجد فيها أي نمط بحث.

بالنسبة إلى `multiMatchAny`، إذا تعذر اختزال نمط واحد إلى متطلب رمز (على سبيل المثال `.*`، الذي يطابق أي نص)، فلا يمكن استخدام الفهرس النصي، ويعود الاستعلام إلى full scan.

وكما هو الحال مع `LIKE` و`match`، يعمل البحث بالسلاسل الفرعية والتعبيرات النمطية بأفضل صورة مع مُقسِّم الرموز `ngrams` و`sparseGrams`.
وتفهرس مُقسِّمات الرموز هذه n-grams متداخلة من المحارف، لذلك يُفكَّك نمط البحث إلى n-grams موجودة في الفهرس أينما ظهر كسلسلة فرعية، بغض النظر عما إذا كان يبدأ أو ينتهي في منتصف كلمة.
لذلك يمكن استخدام نمط البحث كما هو، ما دام طوله لا يقل عن حجم n-gram.

مثال على الفهرس النصي باستخدام مُقسِّم الرموز `ngrams`:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, ['clickhouse', 'support']);
```

على النقيض من ذلك، لا يقوم مُقسِّم الرموز `splitByNonAlpha` إلا بفهرسة الرموز الكاملة (أي الكلمات الكاملة).
ونظرًا إلى أن `needle` قد يبدأ أو ينتهي في منتصف كلمة، فإن ClickHouse يستبعد الرمزين الأول والأخير من كل `needle`، بحيث لا يمكن للفهرس استبعاد granules إلا بالاعتماد على الرموز الكاملة.
ولجعل البحث باستخدام `substring` والتعبيرات النمطية يستفيد من الفهرس مع `splitByNonAlpha`، أحِط كل `needle` بمحارف فاصلة (مثل المسافات) بحيث يُشكِّل رمزًا كاملًا واحدًا أو أكثر.

مثال على الفهرس النصي مع مُقسِّم الرموز `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, [' clickhouse ', ' support ']);
```

<div id="functions-example-startswith-endswith">
  #### ‏`startsWith` و `endsWith`
</div>

على غرار `LIKE`، لا يمكن للدالتين [startsWith](/ar/sql-reference/functions/string-functions.md/#startsWith) و[endsWith](/ar/sql-reference/functions/string-functions.md/#endsWith) استخدام فهرس نصي إلا إذا أمكن استخراج رموز كاملة من عبارة البحث.
وبالنسبة إلى الفهرس الذي يستخدم مُقسِّم الرموز ‏`ngrams`، يتحقق ذلك إذا كان طول السلاسل المطلوب البحث عنها بين أحرف البدل مساويًا لطول ngram أو أكبر منه.
وعندما يستخدم الفهرس النصي postprocessor، يمكن لهاتين الدالتين أيضًا استخدام الفهرس في وضع Hint إذا ظلت رموز التلميح المستخرجة غير فارغة بعد التطبيع. وإذا أدى التطبيع إلى إسقاط جميع رموز التلميح، فلن يُستخدم الفهرس لهذا الشرط.

مثال على الفهرس النصي الذي يستخدم مُقسِّم الرموز ‏`splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE startsWith(comment, 'clickhouse support');
```

في هذا المثال، لا يُعدّ سوى `clickhouse` رمزًا.
أما `support` فلا يُعدّ رمزًا لأنه يمكن أن يطابق `support` و`supports` و`supporting` وغيرها.

للعثور على جميع الصفوف التي تبدأ بـ `clickhouse supports`، يُرجى إنهاء نمط البحث بمسافة في آخره:

```sql
startsWith(comment, 'clickhouse supports ')`
```

وبالمثل، ينبغي استخدام `endsWith` مع مسافة في البداية:

```sql
SELECT count() FROM table WHERE endsWith(comment, ' olap engine');
```

<div id="functions-example-hastoken">
  #### `hasToken`
</div>

:::note
تنطوي الدالة `hasToken` على بعض المحاذير عند استخدامها في عمليات lookup ضمن فهارس النص مع مُقسِّمات الرموز غير `splitByNonAlpha` و/أو تعبيرات المعالجة المسبقة/اللاحقة.
نوصي باستخدام `hasAnyTokens` و`hasAllTokens` بدلًا من ذلك.

أما الصيغ غير الحساسة لحالة الأحرف `hasTokenCaseInsensitive` و`hasTokenCaseInsensitiveOrNull` فلا تراعي فهارس النص — إذ تُنفَّذ دائمًا على شكل فحص كامل للصفوف حتى على الأعمدة المفهرسة نصيًا. وللمطابقة غير الحساسة لحالة الأحرف، استخدم معالجًا مسبقًا أو لاحقًا مثل `lower(...)` وادمجه مع `hasToken` / `hasAllTokens` / `hasAnyTokens`.
:::

تُجري الدالة [hasToken](/ar/sql-reference/functions/string-search-functions.md/#hasToken) مطابقة مع رمز واحد محدد.

وعلى خلاف الدوال المذكورة سابقًا، فهي لا تُجزِّئ مصطلح البحث إلى رموز (إذ تفترض أن المُدخل رمز واحد).

مثال:

```sql
SELECT count() FROM table WHERE hasToken(comment, 'clickhouse');
```

<div id="functions-example-hasanytokens-hasalltokens">
  #### `hasAnyTokens` and `hasAllTokens`
</div>

تُطابق الدالتان [hasAnyTokens](/ar/sql-reference/functions/string-search-functions.md/#hasAnyTokens) و[hasAllTokens](/ar/sql-reference/functions/string-search-functions.md/#hasAllTokens) واحدًا من الرموز المحددة أو جميعها.

تقبل هاتان الدالتان رموز البحث إما على هيئة سلسلة نصية ستُجزَّأ إلى رموز باستخدام أداة تقسيم الرموز نفسها المستخدمة لعمود الفهرس، أو على هيئة مصفوفة من الرموز المُعالجة مسبقًا، بحيث لا يُطبَّق عليها أي تقسيم إلى رموز قبل البحث.
راجِع توثيق الدالة لمزيد من المعلومات.

مثال:

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

تُطابِق الدالة [hasPhrase](/ar/sql-reference/functions/string-search-functions.md/#hasPhrase) عبارةً: إذ يجب أن تظهر جميع الرموز متتاليةً وبالترتيب نفسه الوارد في سلسلة البحث.

وعلى خلاف `hasAllTokens`، التي تكتفي بوجود جميع الرموز في أي موضع، فإن `hasPhrase` تشترط ظهورها كتسلسل متصل.
تُقسَّم عبارة البحث إلى رموز باستخدام مُقسِّم الرموز نفسه المُعَدّ لعمود الفهرس.
وعندما يستخدم الفهرس النصي postprocessor، تُطبَّع عبارة البحث أيضًا قبل البحث في الفهرس.
لاحظ أن الدالة تتطلب أحد مُقسِّمات الرموز `splitByNonAlpha` أو `splitByString` أو `ngrams` أو `asciiCJK`.

مثال:

```sql
-- Matches: 'clickhouse' and 'olap' must appear consecutively in that order
SELECT count() FROM table WHERE hasPhrase(comment, 'clickhouse olap');

-- Does NOT match a row containing 'olap clickhouse' (wrong order)
-- Does NOT match a row containing 'clickhouse fast olap' (non-consecutive)
```

<div id="functions-example-has">
  #### `has`
</div>

تُطابق دالة المصفوفات [has](/ar/sql-reference/functions/array-functions#has) مع `رمز` واحد ضمن مصفوفة من السلاسل النصية.

مثال:

```sql
SELECT count() FROM table WHERE has(array, 'clickhouse');
```

<div id="functions-example-hasany-hasall">
  #### `hasAny` و `hasAll`
</div>

تتحقق دالتا المصفوفات [hasAny](/ar/sql-reference/functions/array-functions#hasAny) و [hasAll](/ar/sql-reference/functions/array-functions#hasAll) مما إذا كان عمود المصفوفة المفهرس يحتوي على أي من سلاسل البحث الثابتة أو عليها جميعًا.

مثال:

```sql
SELECT count() FROM table WHERE hasAny(tags, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAll(tags, ['clickhouse', 'olap']);
```

<div id="functions-example-mapcontains">
  #### `mapContains`
</div>

تُطابِق الدالة [mapContains](/ar/sql-reference/functions/tuple-map-functions#mapContainsKey) (وهي اسم مستعار لـ `mapContainsKey`) الرموز المستخرجة من السلسلة النصية المُراد البحث فيها مع مفاتيح الخريطة.
يشبه هذا السلوك الدالة `equals` عند استخدامها مع عمود `String`.
لا يُستخدم الفهرس النصي إلا إذا كان قد أُنشئ على التعبير `mapKeys(map)`.

مثال:

```sql
SELECT count() FROM table WHERE mapContainsKey(map, 'clickhouse');
-- OR
SELECT count() FROM table WHERE mapContains(map, 'clickhouse');
```

<div id="functions-example-mapcontainsvalue">
  #### `mapContainsValue`
</div>

تطابق الدالة [mapContainsValue](/ar/sql-reference/functions/tuple-map-functions#mapContainsValue) مع الرموز المستخرجة من السلسلة النصية المطلوب البحث فيها داخل قيم الخريطة.
ويشبه هذا السلوك دالة `equals` عند استخدامها مع عمود `String`.
ولا يُستخدم الفهرس النصي إلا إذا أُنشئ على التعبير `mapValues(map)`.

مثال:

```sql
SELECT count() FROM table WHERE mapContainsValue(map, 'clickhouse');
```

<div id="functions-example-mapcontainslike">
  #### `mapContainsKeyLike` و `mapContainsValueLike`
</div>

تُطبِّق الدالتان [mapContainsKeyLike](/ar/sql-reference/functions/tuple-map-functions#mapContainsKeyLike) و [mapContainsValueLike](/ar/sql-reference/functions/tuple-map-functions#mapContainsValueLike) نمطًا على جميع مفاتيح خريطة أو قيمها (على الترتيب).

مثال:

```sql
SELECT count() FROM table WHERE mapContainsKeyLike(map, '% clickhouse %');
SELECT count() FROM table WHERE mapContainsValueLike(map, '% clickhouse %');
```

<div id="functions-example-access-operator">
  #### `operator[]`
</div>

يمكن استخدام [عامل الوصول operator[]](/ar/sql-reference/operators#access-operators) مع الفهرس النصي لتصفية المفاتيح والقيم. ولا يُستخدم الفهرس النصي إلا إذا كان مُنشأً على التعبيرين `mapKeys(map)` أو `mapValues(map)`، أو على كليهما.

مثال:

```sql
SELECT count() FROM table WHERE map['engine'] = 'clickhouse';
```

راجع الأمثلة التالية حول استخدام الأعمدة من النوع `Array(T)` و`Map(K, V)` مع الفهرس النصي.

<div id="text-index-example-array">
  ### فهرسة أعمدة Array(String)
</div>

تخيّل منصة تدوين، حيث يصنّف المؤلفون منشوراتهم باستخدام الكلمات المفتاحية.
نريد أن يتمكّن المستخدمون من اكتشاف محتوى ذي صلة عبر البحث عن الموضوعات أو النقر عليها.

لننظر في تعريف الجدول التالي:

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

من دون فهرس نصي، يتطلب العثور على المنشورات التي تتضمن كلمة محددة (مثل `clickhouse`) فحص جميع السجلات:

```sql
SELECT count() FROM posts WHERE has(keywords, 'clickhouse'); -- slow full-table scan - checks every keyword in every post
```

مع توسّع المنصة، يزداد هذا بطئًا لأن الاستعلام يجب أن يفحص كل مصفوفة `keywords` في كل صف.
وللتغلّب على مشكلة الأداء هذه، نعرّف فهرسًا نصيًا للعمود `keywords`:

```sql
ALTER TABLE posts ADD INDEX keywords_idx(keywords) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE posts MATERIALIZE INDEX keywords_idx; -- Don't forget to rebuild the index for existing data
```

<div id="text-index-example-map">
  ### فهرسة أعمدة Map
</div>

في العديد من حالات استخدام قابلية الرصد، تُقسَّم رسائل السجل إلى &quot;مكوّنات&quot; وتُخزَّن باستخدام أنواع البيانات المناسبة، مثل التاريخ والوقت لـ `timestamp`، و`enum` لمستوى السجل، وهكذا.
من الأفضل تخزين حقول المقاييس كأزواج مفتاح/قيمة.
تحتاج فرق العمليات إلى البحث بكفاءة في السجلات لأغراض استكشاف الأخطاء وإصلاحها، والتحقيق في الحوادث الأمنية، والمراقبة.

ضع في اعتبارك جدول السجلات التالي:

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

من دون فهرس نصي، يتطلب البحث في بيانات [Map](/ar/sql-reference/data-types/map.md) إجراء عمليات مسح كاملة للجدول:

```sql
-- Finds all logs with rate limiting data:
SELECT * FROM logs WHERE has(mapKeys(attributes), 'rate_limit'); -- slow full-table scan

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- slow full-table scan
```

كلما ازداد حجم السجلات، أصبحت هذه الاستعلامات بطيئة.

يتمثل الحل في إنشاء فهرس نصي لمفاتيح [Map](/ar/sql-reference/data-types/map.md) وقيمها.
استخدم [mapKeys](/ar/sql-reference/functions/tuple-map-functions.md/#mapKeys) لإنشاء فهرس نصي عندما تحتاج إلى العثور على السجلات بحسب أسماء الحقول أو أنواع السمات:

```sql
ALTER TABLE logs ADD INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_keys_idx;
```

استخدم [mapValues](/ar/sql-reference/functions/tuple-map-functions.md/#mapValues) لإنشاء فهرس نصي عندما تحتاج إلى البحث ضمن المحتوى الفعلي للسمات:

```sql
ALTER TABLE logs ADD INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_vals_idx;
```

أمثلة لاستعلامات:

```sql
-- Find all rate-limited requests:
SELECT * FROM logs WHERE mapContainsKey(attributes, 'rate_limit'); -- fast

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- fast

-- Finds all logs where any attribute includes an error:
SELECT * FROM logs WHERE mapContainsValueLike(attributes, '% error %'); -- fast
```

<div id="text-index-example-json">
  ### فهرسة أعمدة JSON
</div>

يمكن استخدام الفهارس النصية مع أعمدة `JSON` بثلاث طرق:

1. **فهارس على أعمدة فرعية محددة** — أنشئ فهرسًا نصيًا على مسار JSON معروف، تمامًا كما تفعل مع عمود عادي. يؤدي ذلك إلى فهرسة *القيم* الموجودة في ذلك المسار.
2. **فهارس قائمة على المسارات باستخدام [JSONAllPaths](/ar/sql-reference/functions/json-functions.md/#JSONAllPaths)** — تُفهرِس *جميع المسارات* الموجودة في كل حبيبة لتخطّي الحبيبات التي لا يمكن أن تحتوي على المسار المطلوب في الاستعلام. وهي مشابهة لأعمدة `Map`.
3. **فهارس قائمة على القيم باستخدام [JSONAllValues](/ar/sql-reference/functions/json-functions.md#JSONAllValues)** — تُفهرِس *جميع القيم* عبر كل مسارات JSON لتسريع البحث النصي الكامل في أي عمود فرعي من JSON باستخدام فهرس واحد.

<div id="json-indexes-on-subcolumns">
  #### فهارس على أعمدة فرعية محددة
</div>

يمكنك إنشاء فهرس تخطٍّ على أي عمود JSON فرعي باستخدام البنية نفسها المستخدمة مع الأعمدة العادية.

هناك طريقتان للإشارة إلى عمود JSON فرعي في تعبير الفهرس:

* **مسار ذو نوع محدد** مُعرَّف في تلميح نوع JSON — ويمكن الوصول إليه مباشرةً بالاسم: `json.a`.
* **مسار ديناميكي** مع تحويل نوع صريح — استخدم صيغة التحويل `::`: `json.b::String`.

مثال على تعريف الفهرس:

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

مثال لاستعلام:

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

مثال لاستعلام:

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
  #### فهارس تعتمد على المسارات باستخدام JSONAllPaths
</div>

على غرار أعمدة `Map`، يمكن إنشاء فهارس نصية على أعمدة [JSON](/ar/sql-reference/data-types/newjson.md) باستخدام [`JSONAllPaths`](/ar/sql-reference/functions/json-functions.md/#JSONAllPaths).
ويخزّن الفهرس مجموعة مسارات JSON الموجودة في كل حبيبة، ويستخدمها لتخطي الحبيبات التي يغيب عنها المسار المُستعلَم عنه.

مثال على تعريف الفهرس:

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

يمكنك استخدام `EXPLAIN indexes = 1` للتحقق من استخدام فهرس التخطي.
عندما يوجد المسار في جزء واحد فقط، يتخطى الفهرس الجزء الآخر.

مثال:

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

إذا لم يكن المسار موجودًا في أي جزء، فستُتخطى جميع الأجزاء والحبيبات.

مثال:

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

يستخدم `IS NOT NULL` أيضًا الفهرس — إذ يتجاوز الحبيبات التي يغيب فيها المسار (لأن القيمة ستكون `NULL`):

مثال:

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
  #### الفهارس المعتمدة على القيم باستخدام JSONAllValues
</div>

يمكن استخدام الفهارس النصية لتسريع البحث في أعمدة [JSON](/ar/sql-reference/data-types/newjson.md) عبر الدالة [`JSONAllValues`](/ar/sql-reference/functions/json-functions.md#JSONAllValues).

تعيد `JSONAllValues` جميع القيم من عمود JSON بصيغة `Array(String)`.
وتُحوَّل قيم أنواع البيانات غير النصية (مثل الأعداد الصحيحة والمصفوفات) إلى تمثيلها النصي.
ويقوم الفهرس النصي المُنشأ باستخدام `JSONAllValues` بفهرسة هذه التمثيلات النصية عبر جميع مسارات JSON في كل صف.
ويمكن لهذا الفهرس بعد ذلك تسريع الاستعلامات التي تطبّق عامل تصفية على أعمدة JSON الفرعية الفردية.
وعندما يطبّق استعلام عامل تصفية على عمود فرعي محدد (مثل `data.user_name = 'alice'`)، يمكن للفهرس النصي تخطي الصفوف (والحبيبات) التي لا تحتوي أيٌّ من قيم JSON فيها على رموز البحث بسرعة.

:::note
قد ينتج عن الفهرس نتائج إيجابية كاذبة عندما تحتوي مسارات JSON مختلفة على الرموز نفسها.
فعلى سبيل المثال، إذا كان الصف 1 يحتوي على `{"a": "hello", "b": "world"}` وكان الاستعلام يبحث عن `data.a = 'world'`، فلن يتمكن الفهرس النصي من التمييز بين كون `world` تنتمي إلى المسار `b` لا إلى `a`.
في مثل هذه الحالات، لن يتخطى الفهرس هذا الصف، وسيتولى عامل التصفية على بيانات العمود الفعلية إجراء التقييم النهائي.
وهذا هو السلوك نفسه في حالات استخدام الفهرس النصي الأخرى، حيث يعمل الفهرس كعامل تصفية مسبق سريع.
:::

<div id="json-all-values-creating-the-index">
  ##### إنشاء الفهرس
</div>

مثال على تعريف الفهرس:

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
  ##### أنماط الاستعلام المدعومة
</div>

بمجرد إنشاء الفهرس، يمكنه تسريع الاستعلامات على الأعمدة الفرعية في JSON باستخدام الدوال نفسها المستخدمة مع أعمدة `String`، والدالة `equals` لجميع الأعمدة.

الوصول إلى الأعمدة الفرعية:

```sql
SELECT * FROM events WHERE data.user_name = 'alice';
SELECT * FROM events WHERE data.message LIKE '% error %';
SELECT * FROM events WHERE startsWith(data.status, 'fail');
SELECT * FROM events WHERE hasToken(data.title, 'clickhouse');
```

الوصول إلى العمود الفرعي باستخدام `CAST` الصريح:

```sql
SELECT * FROM events WHERE hasAllTokens(data.message::String, 'connection timeout');
SELECT * FROM events WHERE data.status_code::UInt64 = 404;
SELECT * FROM events WHERE has(data.tags::Array(String), 'bug')
```

عامل التشغيل `IN`:

```sql
SELECT * FROM events WHERE data.level IN ('error', 'critical');
```

<div id="text-index-phrase-search">
  ### البحث بالعبارة
</div>

بحث عادي باستخدام فهرس نصي، على سبيل المثال

```sql
SELECT *
FROM tab
WHERE hasAllTokens(col, 'weather in Tokyo')
```

يطابق جميع الصفوف التي تحتوي على الرموز المحددة بأي ترتيب.
في المثال، الصف `While she stayed in Tokyo, the weather was great.` يطابق شرط التصفية.

في المقابل، يعني البحث بالعبارة مطابقة الرموز بالترتيب المحدد.
على سبيل المثال،

```sql
SELECT *
FROM tab
WHERE hasPhrase(col, 'weather in Tokyo')
```

يطابق أي صف يحتوي على تسلسل الرموز `weather in Tokyo`، مثل `How is the weather in Tokyo?`؟

يُسرِّع فهرس النص البحث عن العبارات من خلال تقاطع قوائم الظهور لجميع الرموز في العبارة لتحديد الحبيبات المرشحة.
وداخل هذه الحبيبات، يتحقق ClickHouse بعد ذلك من التجاور الدقيق بين الرموز.
تُعد هذه العملية مكلفة نسبيًا وأبطأ من استعلامات البحث النصي العادية.
ولتسريع استعلامات البحث عن العبارات، يُرجى تمكين تخزين المواضع في فهرس النص (راجع `Optional parameters` أعلاه).

يمكن استخدام `hasPhrase` مع مُقسِّمات الرموز `splitByNonAlpha` و`splitByString` و`ngrams` و`asciiCJK`.
تُقسَّم سلسلة العبارة المُعطاة إلى رموز باستخدام مُقسِّم الرموز الخاص بالفهرس.
تُتجاهل أحرف الفصل في العبارة: `hasPhrase(text, 'quick+brown')` مكافئة لـ `hasPhrase(text, 'quick brown')`، بافتراض استخدام `splitByNonAlpha` كمُقسِّم للرموز.

<div id="text-index-phrase-search-example">
  #### مثال
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

الصف 2 (`'New weather in York'`) لا يتطابق لأن الرموز النصية ليست بالترتيب الصحيح.
الصف 3 (`'weather in New Orleans'`) لا يتطابق لأنه لا يحتوي على الرمز النصي `'York'`.

<div id="performance-tuning">
  ## تحسين الأداء
</div>

<div id="direct-read">
  ### القراءة المباشرة
</div>

يمكن تسريع أنواع معيّنة من الاستعلامات النصية بشكل كبير بفضل تحسين يُسمّى &quot;القراءة المباشرة&quot;.

مثال:

```sql
SELECT column_a, column_b, ...
FROM [...]
WHERE string_search_function(column_with_text_index)
```

تُجيب آلية تحسين القراءة المباشرة عن الاستعلام بالاعتماد حصريًا على فهرس النص (أي عبر عمليات lookup في فهرس النص) من دون الوصول إلى عمود النص الأساسي.
وتقرأ عمليات lookup في فهرس النص قدرًا قليلًا نسبيًا من البيانات، لذا فهي أسرع بكثير من فهارس التخطي المعتادة في ClickHouse (التي تُجري lookup في فهرس التخطي، ثم تحميل الحبيبات المتبقية وتصفيتها).

تخضع القراءة المباشرة لإعدادين:

* الإعداد [query&#95;plan&#95;direct&#95;read&#95;from&#95;text&#95;index](../../../operations/settings/settings#query_plan_direct_read_from_text_index) (`true` افتراضيًا)، ويحدد ما إذا كانت القراءة المباشرة مُمكّنة بشكل عام.
* كان الإعداد [use&#95;skip&#95;indexes&#95;on&#95;data&#95;read](../../../operations/settings/settings#use_skip_indexes_on_data_read) شرطًا مسبقًا للقراءة المباشرة في إصدارات ClickHouse الأقدم من 26.4.

**الدوال المدعومة**

تدعم آلية تحسين القراءة المباشرة الدوال `hasToken` و`hasAllTokens` و`hasAnyTokens`.
إذا كان فهرس النص معرّفًا باستخدام tokenizer من النوع `array`، فستكون القراءة المباشرة مدعومة أيضًا للدوال `equals` و`has` و`hasAny` و`hasAll` و`mapContainsKey` و`mapContainsValue`.
ويمكن أيضًا دمج هذه الدوال باستخدام العوامل `AND` و`OR` و`NOT`.
كما يمكن أن تتضمن عبارتا `WHERE` أو `PREWHERE` عوامل تصفية إضافية لا تتعلق بدوال البحث النصي (لأعمدة النص أو الأعمدة الأخرى) - وفي هذه الحالة، ستظل آلية تحسين القراءة المباشرة مستخدمة ولكن بفاعلية أقل (إذ إنها تنطبق فقط على دوال البحث النصي المدعومة).

وللتأكد مما إذا كان الاستعلام يستخدم القراءة المباشرة، شغّل الاستعلام باستخدام `EXPLAIN PLAN actions = 1`.
وعلى سبيل المثال، استعلام مع تعطيل القراءة المباشرة

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 0, -- disable direct read
```

القيمة المُعادة

```text
[...]
Filter ((WHERE + Change column names to column identifiers))
Filter column: hasToken(__table1.col, 'some_token'_String) (removed)
Actions: INPUT : 0 -> col String : 0
         COLUMN Const(String) -> 'some_token'_String String : 1
         FUNCTION hasToken(col :: 0, 'some_token'_String :: 1) -> hasToken(__table1.col, 'some_token'_String) UInt8 : 2
[...]
```

في المقابل، عند تشغيل الاستعلام نفسه مع `query_plan_direct_read_from_text_index = 1`

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 1, -- enable direct read
```

القيمة المعادة

```text
[...]
Expression (Before GROUP BY)
Positions:
  Filter
  Filter column: __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 (removed)
  Actions: INPUT :: 0 -> __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 UInt8 : 0
[...]
```

يحتوي ناتج `EXPLAIN PLAN` الثاني على عمود افتراضي `__text_index_<index_name>_<function_name>_<id>`.
إذا كان هذا العمود موجودًا، فهذا يعني استخدام القراءة المباشرة.

إذا كان شرط `WHERE` يحتوي فقط على دوال البحث النصي، فيمكن للاستعلام تجنّب قراءة بيانات العمود بالكامل وتحقيق أكبر فائدة في الأداء من خلال القراءة المباشرة.
ومع ذلك، حتى إذا جرى الوصول إلى العمود النصي في موضع آخر من الاستعلام، فستظل القراءة المباشرة توفّر تحسينًا في الأداء.

**القراءة المباشرة كتلميح**

تعتمد القراءة المباشرة كتلميح على المبادئ نفسها التي تقوم عليها القراءة المباشرة العادية، لكنها تضيف بدلًا من ذلك عامل تصفية إضافيًا مُنشأً من بيانات فهرس النص، من دون الاستغناء عن العمود النصي الأساسي.
وتُستخدم مع الدوال التي قد تؤدي فيها القراءة من فهرس النص فقط إلى نتائج إيجابية كاذبة.

الدوال المدعومة هي: `like`, `startsWith`, `endsWith`, `equals`, `has`, `hasPhrase`, `mapContainsKey`, و`mapContainsValue`.

يمكن أن يوفّر عامل التصفية الإضافي قدرًا أكبر من الانتقائية لتقييد مجموعة النتائج أكثر عند دمجه مع عوامل تصفية أخرى، مما يساعد على تقليل كمية البيانات المقروءة من الأعمدة الأخرى.

يتم التحكم في القراءة المباشرة كتلميح عبر الإعداد [query&#95;plan&#95;text&#95;index&#95;add&#95;hint](../../../operations/settings/settings#query_plan_text_index_add_hint) (مُمكّن افتراضيًا).

مثال على استعلام بدون تلميح:

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE (col LIKE '%some-token%') AND (d >= today())
SETTINGS query_plan_text_index_add_hint = 0
FORMAT TSV
```

القيمة المُعادة

```text
[...]
Prewhere filter column: and(like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

في حين يُنفَّذ الاستعلام نفسه مع `query_plan_text_index_add_hint = 1`

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE col LIKE '%some-token%'
SETTINGS query_plan_text_index_add_hint = 1
```

القيمة المُعادة

```text
[...]
Prewhere filter column: and(__text_index_idx_col_like_d306f7c9c95238594618ac23eb7a3f74, like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

في ناتج `EXPLAIN PLAN` الثاني، يمكنك ملاحظة أنه أُضيف حدّ اقتراني إضافي (`__text_index_...`) إلى شرط التصفية.
وبفضل تحسين [PREWHERE](/ar/sql-reference/statements/select/prewhere)، يُقسَّم شرط التصفية إلى ثلاثة حدود اقترانية منفصلة، تُطبَّق بترتيب تصاعدي من حيث التعقيد الحسابي.
في هذا الاستعلام، يكون ترتيب التطبيق هو `__text_index_...`، ثم `greaterOrEquals(...)`، وأخيرًا `like(...)`.
ويتيح هذا الترتيب تخطي عدد أكبر من الحبيبات مقارنةً بتلك التي يتخطاها الفهرس النصي وشرط التصفية الأصلي، وذلك قبل قراءة الأعمدة الثقيلة المستخدمة في الاستعلام بعد عبارة `WHERE`، مما يقلل أكثر من كمية البيانات المطلوب قراءتها.

<div id="like-ilike-queries-perf">
  ### استعلامات LIKE/ILIKE
</div>

عندما يكون نمط استعلام LIKE/ILIKE هو `%<alpha-numeric-characters-without-spaces>%` ويكون مُقسِّم فهرس النص `splitByNonAlpha` أو `array`، يستفيد ClickHouse من الفهرس المعكوس لتسريع استعلامات LIKE/ILIKE بشكل ملحوظ. ولتحقيق ذلك، يفحص ClickHouse قاموس الفهرس المعكوس بدلًا من إجراء فحص كامل للجدول للعثور على النمط المطابق.

عند تمكين هذا التحسين، ينبغي أن تكون استعلامات LIKE/ILIKE أسرع بكثير من الفحص الكامل للجدول. ومع ذلك، إذا كان النمط يطابق معظم الرموز في القاموس، فقد يصبح الأداء أسوأ مقارنةً بالفحص الكامل للجدول. ولحسن الحظ، توجد آلية fallback لمنع ذلك.

يتم التحكم في هذا التحسين من خلال الإعداد:

* [use&#95;text&#95;index&#95;like&#95;evaluation&#95;by&#95;dictionary&#95;scan](../../../operations/settings/settings#use_text_index_like_evaluation_by_dictionary_scan)

ويتم التحكم في آلية fallback من خلال إعدادين:

* [text&#95;index&#95;like&#95;min&#95;pattern&#95;length](../../../operations/settings/settings#text_index_like_min_pattern_length)
* [text&#95;index&#95;like&#95;max&#95;postings&#95;to&#95;read](../../../operations/settings/settings#text_index_like_max_postings_to_read)

لا يدعم هذا التحسين سوى الدالتين `like` و`ilike`.

<div id="caching">
  ### التخزين المؤقت
</div>

توجد ذواكر تخزين مؤقت متعددة على مستوى الخادم لتخزين أجزاء من الفهرس النصي مؤقتًا في الذاكرة (راجع قسم [تفاصيل التنفيذ](#implementation)):
توجد حاليًا ذواكر تخزين مؤقت للترويسات بعد فك التسلسل، والرموز، وقوائم الإسناد الخاصة بالفهرس النصي لتقليل عمليات الإدخال/الإخراج.
استخدم الإعدادات [use&#95;text&#95;index&#95;header&#95;cache](/ar/operations/settings/settings#use_text_index_header_cache) و[use&#95;text&#95;index&#95;tokens&#95;cache](/ar/operations/settings/settings#use_text_index_tokens_cache) و[use&#95;text&#95;index&#95;postings&#95;cache](/ar/operations/settings/settings#use_text_index_postings_cache) لتعطيل قراءة الاستعلامات من ذواكر التخزين المؤقت المنفصلة هذه والكتابة إليها.

لمسح ذواكر التخزين المؤقت، استخدم العبارة [SYSTEM CLEAR TEXT INDEX CACHES](../../../sql-reference/statements/system#drop-text-index-caches)

يُرجى الرجوع إلى إعدادات الخادم التالية لضبط ذواكر التخزين المؤقت.

<div id="caching-tokens">
  #### إعدادات ذاكرة التخزين المؤقت للرموز
</div>

| الإعداد                                                                                                                                             | الوصف                                                                                                           |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;tokens&#95;cache&#95;policy](/ar/operations/server-configuration-parameters/settings#text_index_tokens_cache_policy)               | اسم سياسة ذاكرة التخزين المؤقت لرموز الفهرس النصي.                                                              |
| [text&#95;index&#95;tokens&#95;cache&#95;size](/ar/operations/server-configuration-parameters/settings#text_index_tokens_cache_size)                   | الحد الأقصى لحجم ذاكرة التخزين المؤقت بالبايت.                                                                  |
| [text&#95;index&#95;tokens&#95;cache&#95;max&#95;entries](/ar/operations/server-configuration-parameters/settings#text_index_tokens_cache_max_entries) | الحد الأقصى لعدد الرموز بعد فك التسلسل في ذاكرة التخزين المؤقت.                                                 |
| [text&#95;index&#95;tokens&#95;cache&#95;size&#95;ratio](/ar/operations/server-configuration-parameters/settings#text_index_tokens_cache_size_ratio)   | حجم قائمة الانتظار المحمية في ذاكرة التخزين المؤقت لرموز الفهرس النصي مقارنةً بإجمالي حجم ذاكرة التخزين المؤقت. |

<div id="caching-header">
  #### إعدادات ذاكرة التخزين المؤقت لترويسة الفهرس
</div>

| الإعداد                                                                                                                                             | الوصف                                                                                                             |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;header&#95;cache&#95;policy](/ar/operations/server-configuration-parameters/settings#text_index_header_cache_policy)               | اسم سياسة ذاكرة التخزين المؤقت لترويسة الفهرس النصي.                                                              |
| [text&#95;index&#95;header&#95;cache&#95;size](/ar/operations/server-configuration-parameters/settings#text_index_header_cache_size)                   | الحد الأقصى لحجم ذاكرة التخزين المؤقت بالبايت.                                                                    |
| [text&#95;index&#95;header&#95;cache&#95;max&#95;entries](/ar/operations/server-configuration-parameters/settings#text_index_header_cache_max_entries) | الحد الأقصى لعدد الترويسات التي جرى فك تسلسلها في ذاكرة التخزين المؤقت.                                           |
| [text&#95;index&#95;header&#95;cache&#95;size&#95;ratio](/ar/operations/server-configuration-parameters/settings#text_index_header_cache_size_ratio)   | حجم قائمة الانتظار المحمية في ذاكرة التخزين المؤقت لترويسة الفهرس النصي مقارنةً بإجمالي حجم ذاكرة التخزين المؤقت. |

<div id="caching-posting-lists">
  #### إعدادات ذاكرة التخزين المؤقت لقوائم الإسناد
</div>

| الإعداد                                                                                                                                                 | الوصف                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;postings&#95;cache&#95;policy](/ar/operations/server-configuration-parameters/settings#text_index_postings_cache_policy)               | اسم سياسة ذاكرة التخزين المؤقت لقوائم الإسناد الخاصة بالفهرس النصي.                                                   |
| [text&#95;index&#95;postings&#95;cache&#95;size](/ar/operations/server-configuration-parameters/settings#text_index_postings_cache_size)                   | الحد الأقصى لحجم ذاكرة التخزين المؤقت بالبايت.                                                                        |
| [text&#95;index&#95;postings&#95;cache&#95;max&#95;entries](/ar/operations/server-configuration-parameters/settings#text_index_postings_cache_max_entries) | الحد الأقصى لعدد عناصر قوائم الإسناد بعد فك التسلسل في ذاكرة التخزين المؤقت.                                         |
| [text&#95;index&#95;postings&#95;cache&#95;size&#95;ratio](/ar/operations/server-configuration-parameters/settings#text_index_postings_cache_size_ratio)   | حجم الطابور المحمي في ذاكرة التخزين المؤقت لقوائم الإسناد الخاصة بالفهرس النصي مقارنةً بإجمالي حجم ذاكرة التخزين المؤقت. |

<div id="limitations">
  ## القيود
</div>

للفهرس النصي حاليًا القيود التالية:

* يمكن أن تستهلك مادية الفهارس النصية التي تحتوي على عدد كبير من الرموز (مثل 10 مليارات رمز) قدرًا كبيرًا من الذاكرة. ويمكن أن تتم
  مادية الفهرس النصي مباشرةً (`ALTER TABLE <table> MATERIALIZE INDEX <index>`) أو بشكل غير مباشر أثناء عمليات دمج الأجزاء.
* لا يمكن مادية الفهارس النصية على الأجزاء التي تحتوي على أكثر من 4.294.967.296 (= 2^32 = نحو 4.2 مليارات) صف. ومن دون فهرس نصي مادي، تلجأ الاستعلامات إلى البحث الشامل البطيء داخل الجزء. وكتقدير لأسوأ الحالات، افترض أن الجزء يحتوي على عمود واحد من النوع String وأن إعداد MergeTree ‏`max_bytes_to_merge_at_max_space_in_pool` (القيمة الافتراضية: 150 GB) لم يتغير. في هذه الحالة، يحدث ذلك إذا كان العمود يحتوي في المتوسط على أقل من 29.5 حرفًا لكل صف. عمليًا، تحتوي الجداول أيضًا على أعمدة أخرى، وتكون العتبة أقل من ذلك بعدة مرات (بحسب عدد الأعمدة الأخرى ونوعها وحجمها).

<div id="text-index-vs-bloom-filter-indexes">
  ## الفهارس النصية مقابل الفهارس المستندة إلى Bloom filter
</div>

يمكن تسريع عبارات الشرط على السلاسل النصية باستخدام الفهارس النصية والفهارس المستندة إلى Bloom filter (نوع الفهرس `bloom_filter` و`ngrambf_v1` و`tokenbf_v1` و`sparse_grams`)، لكنهما يختلفان جذريًا من حيث التصميم وحالات الاستخدام المستهدفة:

**فهارس Bloom filter**

* تستند إلى هياكل بيانات احتمالية قد تنتج نتائج إيجابية كاذبة.
* لا يمكنها سوى الإجابة عن أسئلة الانتماء إلى مجموعة؛ أي إن العمود قد يحتوي على الرمز X أو أنه بالتأكيد لا يحتوي على X.
* تخزّن معلومات على مستوى الحبيبات، ما يتيح تخطي نطاقات واسعة أثناء تنفيذ الاستعلام.
* يصعب ضبطها بالشكل الصحيح (انظر [هنا](mergetree#n-gram-bloom-filter) للاطلاع على مثال).
* وهي مدمجة نسبيًا (بضعة كيلوبايتات أو ميغابايتات لكل جزء).

**الفهارس النصية**

* تُنشئ فهرسًا معكوسًا حتميًا للرموز. ولا يمكن أن ينتج عنها بحد ذاتها أي نتائج إيجابية كاذبة.
* مُحسّنة خصيصًا لأحمال عمل البحث النصي.
* تخزّن معلومات على مستوى الصفوف، ما يتيح بحثًا فعالًا عن المصطلحات.
* وهي كبيرة نسبيًا (من عشرات إلى مئات الميغابايتات لكل جزء).

لا تدعم الفهارس المستندة إلى Bloom filter البحث بالنص الكامل إلا كـ &quot;أثر جانبي&quot;:

* فهي لا تدعم التجزئة المتقدمة إلى رموز أو المعالجة المسبقة.
* ولا تدعم البحث عبر عدة رموز.
* ولا توفر خصائص الأداء المتوقعة من فهرس معكوس.

أما الفهارس النصية، فعلى النقيض، فهي مصممة خصيصًا للبحث بالنص الكامل:

* فهي توفر التجزئة إلى رموز والمعالجة المسبقة
* وتوفر دعمًا فعالًا للدوال `hasAllTokens` و`LIKE` و`match` وغيرها من دوال البحث النصي المشابهة.
* وتتمتع بقابلية توسع أفضل بكثير مع المجموعات النصية الكبيرة.

<div id="implementation">
  ## تفاصيل التنفيذ
</div>

يتكوّن كل فهرس نصي من بنيتَي بيانات (مجرّدتين):

* قاموس يربط كل رمز بقائمة إسناد، و
* مجموعة من قوائم الإسناد، تمثّل كل واحدة منها مجموعة من أرقام الصفوف.

يُنشأ الفهرس النصي للجزء بالكامل.
وعلى خلاف فهارس التخطي الأخرى، يمكن دمج الفهرس النصي بدلًا من إعادة بنائه عند دمج أجزاء البيانات (انظر أدناه).

أثناء إنشاء الفهرس، تُنشأ ثلاثة ملفات (لكل جزء):

**ملف كتل القاموس (.dct)**

تُرتَّب الرموز في الفهرس النصي وتُخزَّن في كتل قاموس، تضم كل كتلة 512 رمزًا (حجم الكتلة قابل للتهيئة عبر المعامل `dictionary_block_size`).
ويتكوّن ملف كتل القاموس (.dct) من جميع كتل القاموس لكل index granules في الجزء.

**ملف ترويسة الفهرس (.idx)**

يحتوي ملف ترويسة الفهرس، لكل كتلة قاموس، على أول رمز في الكتلة وإزاحته النسبية داخل ملف كتل القاموس.

تشبه بنية الفهرس sparse هذه [فهرس المفتاح الأساسي sparse](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes)) في ClickHouse.

**ملف قوائم الإسناد (.pst)**

تُرتَّب قوائم الإسناد لجميع الرموز ترتيبًا تسلسليًا داخل ملف قوائم الإسناد.
ولتوفير المساحة مع إتاحة عمليات intersect و union السريعة في الوقت نفسه، تُخزَّن قوائم الإسناد على هيئة [roaring bitmaps](https://roaringbitmap.org/).
إذا كانت قائمة الإسناد أكبر من `posting_list_block_size`، فسيتم تقسيمها إلى كتل متعددة تُخزَّن تسلسليًا في ملف قوائم الإسناد.

**ملف المواضع (.pos)**

اختياري، ويُنشأ فقط إذا كانت وسيطة الفهرس `positions = 1`.
يخزّن مواضع الرموز داخل الصفوف المطابقة.

**دمج الفهارس النصية**

عند دمج أجزاء البيانات، لا يلزم إعادة بناء الفهرس النصي من الصفر؛ بل يمكن دمجه بكفاءة في خطوة منفصلة من عملية الدمج.
وخلال هذه الخطوة، تُقرأ القواميس المرتبة للفهرس النصي من كل جزء إدخال وتُدمج في قاموس موحّد جديد.
كما يُعاد حساب أرقام الصفوف في قوائم الإسناد لتعكس مواضعها الجديدة في جزء البيانات المدمج، باستخدام mapping من أرقام الصفوف القديمة إلى الجديدة يُنشأ خلال Phase الدمج الأولية.
تشبه طريقة دمج الفهارس النصية هذه كيفية دمج [projections](/ar/docs/sql-reference/statements/alter/projection#projection-indexes) التي تحتوي على العمود `_part_offset`.
إذا لم يكن الفهرس materialized في الجزء المصدر، فسيتم بناؤه وكتابته إلى ملف مؤقت ثم دمجه مع الفهارس من الأجزاء الأخرى ومن ملفات الفهارس المؤقتة الأخرى.

**استكشاف الأخطاء وإصلاحها**

يمكن استخدام table function ‏[mergeTreeTextIndex](../../../sql-reference/table-functions/mergeTreeTextIndex.md) لفحص الفهارس النصية داخليًا.

<div id="hacker-news-dataset">
  ## مثال: مجموعة بيانات Hacker News
</div>

لنلقِ نظرة على تحسينات الأداء التي تحققها الفهارس النصية على مجموعة بيانات كبيرة تحتوي على كمية كبيرة من النصوص.
سنستخدم 28.7 مليون صف من التعليقات على موقع Hacker News الشهير.
فيما يلي الجدول من دون فهرس نصي:

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

توجد 28.7M صفًا في ملف Parquet على S3 - فلنُدرِجها في جدول `hackernews`:

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

سنستخدم `ALTER TABLE` لإضافة فهرس نصي إلى عمود comment، ثم نطبّقه ماديًا:

```sql
-- Add the index
ALTER TABLE hackernews ADD INDEX comment_idx comment TYPE text(tokenizer = splitByNonAlpha);

-- Materialize the index for existing data
ALTER TABLE hackernews MATERIALIZE INDEX comment_idx SETTINGS mutations_sync = 2;
```

الآن، لنُجرِ استعلامات باستخدام الدوال `hasToken` و`hasAnyTokens` و`hasAllTokens`.
ستُظهر الأمثلة التالية الفرق الكبير في الأداء بين فحص الفهرس القياسي وتحسين القراءة المباشرة.

<div id="using-hasToken">
  ### 1. استخدام `hasToken`
</div>

يتحقق `hasToken` مما إذا كان النص يحتوي على رمز واحد محدد.
سنبحث عن رمز حساس لحالة الأحرف وهو &#39;ClickHouse&#39;.

**القراءة المباشرة معطّلة (المسح القياسي)**
يستخدم ClickHouse افتراضيًا فهرس التخطي لتصفية الحبيبات، ثم يقرأ بيانات الأعمدة الخاصة بهذه الحبيبات.
يمكننا محاكاة هذا السلوك من خلال تعطيل القراءة المباشرة.

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

**القراءة المباشرة مفعّلة (قراءة الفهرس السريعة)**
الآن نشغّل الاستعلام نفسه مع تفعيل القراءة المباشرة (وهو الوضع الافتراضي).

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

استعلام `direct read` أسرع بأكثر من 45 مرة (0.362s مقابل 0.008s)، ويعالج كمية أقل بكثير من البيانات (9.51 GB مقابل 3.15 MB)، إذ يقرأ من الفهرس وحده.

<div id="using-hasAnyTokens">
  ### 2. استخدام `hasAnyTokens`
</div>

يتحقق `hasAnyTokens` مما إذا كان النص يحتوي على واحدة على الأقل من الرموز المحددة.
سنبحث عن تعليقات تحتوي على &#39;love&#39; أو &#39;ClickHouse&#39;.

**القراءة المباشرة معطّلة (الفحص القياسي)**

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

**القراءة المباشرة مفعّلة (القراءة السريعة من الفهرس)**

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

يكون تحسّن الأداء أكثر لفتًا للنظر في هذا البحث الشائع باستخدام &quot;OR&quot;.
ويصبح الاستعلام أسرع بنحو 89 مرة (1.329s مقابل 0.015s) بفضل تجنّب فحص العمود بالكامل.

<div id="using-hasAllTokens">
  ### 3. استخدام `hasAllTokens`
</div>

يتحقق `hasAllTokens` مما إذا كان النص يحتوي على جميع الرموز المحددة.
سنبحث عن تعليقات تتضمن كلًّا من &#39;love&#39; و&#39;ClickHouse&#39;.

**القراءة المباشرة معطلة (المسح القياسي)**
حتى مع تعطيل القراءة المباشرة، يظل فهرس التخطي القياسي فعّالًا.
فهو يضيّق 28.7 مليون صف إلى 147.46 ألف صف فقط، لكنه لا يزال بحاجة إلى قراءة 57.03 ميغابايت من العمود.

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

**القراءة المباشرة مفعّلة (قراءة سريعة من الفهرس)**
تعالج القراءة المباشرة الاستعلام بالاعتماد على بيانات الفهرس، فلا تقرأ سوى 147.46 كيلوبايت.

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

في بحث &quot;AND&quot; هذا، يُعدّ تحسين القراءة المباشرة أسرع بأكثر من 26 مرة (0.184s مقابل 0.007s) من المسح القياسي لفهرس التخطي.

<div id="compound-search">
  ### 4. البحث المركب: OR، AND، NOT، ...
</div>

ينطبق تحسين القراءة المباشرة أيضًا على التعبيرات المنطقية المركبة.
هنا، سنُجري بحثًا دون مراعاة حالة الأحرف عن &#39;ClickHouse&#39; OR &#39;clickhouse&#39;.

**تعطيل القراءة المباشرة (المسح القياسي)**

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

**تم تفعيل القراءة المباشرة (القراءة السريعة للفهرس)**

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

من خلال الجمع بين النتائج المستخرجة من الفهرس، يصبح استعلام القراءة المباشرة أسرع بمقدار 34 مرة (0.450s مقابل 0.013s)، مع تجنّب قراءة 9.58 GB من بيانات الأعمدة.
في هذه الحالة تحديدًا، ستكون الصياغة ‎`hasAnyTokens(comment, ['ClickHouse', 'clickhouse'])`‎ هي المفضلة والأكثر كفاءة.

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [الإعلان عن التوافر العام لميزة البحث بالنص الكامل في ClickHouse](https://clickhouse.com/blog/full-text-search-ga-release)
* مدونة: [بناء بحث بالنص الكامل عالي الأداء للتخزين الكائني](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)
* فيديو: [مقدمة إلى البحث بالنص الكامل في ClickHouse](https://www.youtube.com/watch?v=9zPmf1a_heU)
* فيديو: [ما وراء الكواليس: البحث بالنص الكامل في ClickHouse على نطاق واسع وبسرعة عالية](https://www.youtube.com/watch?v=8JbqE_ubfkU)
* عرض تقديمي: [نظرة داخلية على البحث بالنص الكامل في ClickHouse: سريع وأصلي وقائم على الأعمدة](https://github.com/ClickHouse/clickhouse-presentations/blob/master/2025-tumuchdata-munich/ClickHouse_%20full-text%20search%20-%2011.11.2025%20Munich%20Database%20Meetup.pdf)
* عرض تقديمي: [فهارس قواعد البيانات المعكوسة: لماذا، وما هي، وكيف تعمل، FOSDEM 2026](https://presentations.clickhouse.com/2026-fosdem-inverted-index/Inverted_indexes_the_what_the_why_the_how.pdf)

**مواد قديمة**

* مدونة: [تقديم الفهارس المعكوسة في ClickHouse](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
* مدونة: [نظرة داخلية على البحث بالنص الكامل في ClickHouse: سريع وأصلي وقائم على الأعمدة](https://clickhouse.com/blog/clickhouse-full-text-search)
* فيديو: [فهارس النص الكامل: التصميم والتجارب](https://www.youtube.com/watch?v=O_MnyUkrIq8)