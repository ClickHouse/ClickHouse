---
description: 'توثيق دوال AI'
sidebar_label: 'AI'
slug: /sql-reference/functions/ai-functions
title: 'دوال AI'
doc_type: 'reference'
---

دوال AI هي دوال مضمّنة في ClickHouse يمكنك استخدامها لاستدعاء AI أو إنشاء embeddings للعمل مع بياناتك، واستخراج المعلومات، وتصنيف البيانات، وغير ذلك...

:::note
دوال AI تجريبية. اضبط [`allow_experimental_ai_functions`](/ar/operations/settings/settings#allow_experimental_ai_functions) لتمكينها.
:::

:::note
قد تُرجع دوال AI مخرجات غير متوقعة. وتعتمد النتيجة بدرجة كبيرة على جودة prompt والنموذج المستخدم.
:::

تشترك جميع الدوال في بنية تحتية مشتركة توفّر ما يلي:

* **فرض الحصص**: حدود لكل استعلام على الرموز ([`ai_function_max_input_tokens_per_query`](/ar/operations/settings/settings#ai_function_max_input_tokens_per_query), [`ai_function_max_output_tokens_per_query`](/ar/operations/settings/settings#ai_function_max_output_tokens_per_query)) وعلى استدعاءات واجهة برمجة التطبيقات ([`ai_function_max_api_calls_per_query`](/ar/operations/settings/settings#ai_function_max_api_calls_per_query)).
* **إعادة المحاولة مع تأخير متزايد**: تُعاد محاولة حالات الفشل العابرة ([`ai_function_max_retries`](/ar/operations/settings/settings#ai_function_max_retries)) مع backoff أُسّي ([`ai_function_retry_initial_delay_ms`](/ar/operations/settings/settings#ai_function_retry_initial_delay_ms)).

<div id="configuration">
  ## الإعداد
</div>

تعتمد دوال AI على **مجموعة مسماة** تخزّن بيانات اعتماد المزوّد والإعدادات. ويمكن إنشاء مجموعات مسماة مختلفة واستخدامها مع دوال مختلفة أو مع استدعاءات مختلفة للدوال. على سبيل المثال، قد ترغب في تعريف مجموعة مسماة مختلفة لاستخدامها مع دوال النص (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`) في مقابل الدالة `aiEmbed`، إذ تتطلب نقاط نهاية مختلفة وتستخدم عادةً نماذج مختلفة.

في ما يلي مثال على تعليمة لإنشاء مجموعة مسماة تتضمن بيانات اعتماد المزوّد: واحدة مع نقطة نهاية للدردشة وأخرى مع نقطة نهاية للتضمين:

```sql
CREATE NAMED COLLECTION ai_text_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/chat/completions',
    model = 'gpt-4o-mini',
    api_key = 'sk-...';

CREATE NAMED COLLECTION ai_embedding_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/embeddings',
    model = 'text-embedding-3-small',
    api_key = 'sk-...';
```

<div id="named-collection-parameters">
  ### معلمات المجموعة المُسمّاة
</div>

| المعلمة       | النوع  | الافتراضي | الوصف                                                                                                                                       |
| ------------- | ------ | --------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `provider`    | String | —         | موفّر النموذج. القيم المدعومة: `'openai'`، `'anthropic'`. راجع الملاحظة أدناه.                                                              |
| `endpoint`    | String | —         | URL نقطة نهاية واجهة برمجة تطبيقات.                                                                                                         |
| `model`       | String | —         | اسم النموذج (مثل `'gpt-4o-mini'`، `'text-embedding-3-small'`).                                                                              |
| `api_key`     | String | —         | مفتاح المصادقة الخاص بالموفّر. اختياري: عند عدم تحديده، لا تُرسل ترويسة المصادقة، مما يتيح استهداف خوادم متوافقة مع OpenAI لا تتطلب مصادقة. |
| `max_tokens`  | UInt64 | `1024`    | الحد الأقصى لعدد الرموز الناتجة لكل استدعاء لواجهة برمجة تطبيقات.                                                                           |
| `api_version` | String | —         | سلسلة إصدار واجهة برمجة تطبيقات. تُستخدم مع Anthropic (`'2023-06-01'`).                                                                     |

:::note
يمكن استخدام أي واجهة برمجة تطبيقات متوافقة مع OpenAI (مثل vLLM وOllama وLiteLLM) من خلال تعيين `provider = 'openai'` وتوجيه `endpoint` إلى خدمتك.
:::

<div id="selecting-credentials">
  ### اختيار بيانات الاعتماد
</div>

تحدِّد الدالة المجموعة المُسمّاة التي ستُستخدم وفق الترتيب التالي:

1. المفتاح `credentials` في خريطة المعلمات الخاصة بها، إن وُجد؛
2. وإلا، فإعداد بيانات الاعتماد الافتراضية المناسب:
   * [`ai_function_text_default_credentials`](/ar/operations/settings/settings#ai_function_text_default_credentials) لدوال النص (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`);
   * [`ai_function_embedding_default_credentials`](/ar/operations/settings/settings#ai_function_embedding_default_credentials) للدالة `aiEmbed`.

إذا لم يُضبط أيٌّ منهما، يفشل الاستدعاء. تستخدم دوال النص ودوال التضمين إعدادات افتراضية منفصلة لأن نقطة النهاية والنموذج الخاصَّين بـ chat-completions يختلفان عن نظيريهما الخاصَّين بـ embeddings.

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

<div id="parameter-map">
  ### خريطة المعلمات
</div>

تقبل كل دالة وسيطة اختيارية أخيرة من النوع `Map(String, String)` للمعلمات. جميع القيم عبارة عن سلاسل نصية (ضع الأرقام بين علامات اقتباس، مثل `'0.2'`). تُرفَض أي مفاتيح غير معروفة. وأي مفتاح موجود يتجاوز القيمة المقابلة له في المجموعة المسماة؛ أما المفتاح غير الموجود فيعود إلى المجموعة المسماة (بالنسبة إلى `model`/`max_tokens`) أو إلى القيمة الافتراضية المضمّنة.

المعلمات التالية مشتركة بين جميع دوال AI:

| المفتاح       | الوصف                                            |
| ------------- | ------------------------------------------------ |
| `credentials` | المجموعة المسماة المطلوب استخدامها (انظر أعلاه). |
| `model`       | يتجاوز قيمة `model` الخاصة بالمجموعة.            |

تقبل كل دالة أيضًا معلمات إضافية خاصة بها (مثل `max_tokens` و`temperature` و`system_prompt` و`instructions` و`dimensions`). راجع المرجع الخاص بكل دالة أدناه لمعرفة المعلمات التي تقبلها والقيم الافتراضية لها.

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

<div id="query-level-settings">
  ### إعدادات على مستوى الاستعلام
</div>

جميع الإعدادات المتعلقة بالذكاء الاصطناعي مُدرجة في [Settings](/ar/operations/settings/settings) ضمن البادئة `ai_function_`.

<div id="restricting-endpoint-hosts">
  ### تقييد مضيفي نقطة النهاية
</div>

يمثل عنوان URL الخاص بـ `endpoint` في مجموعة مسماة للذكاء الاصطناعي وجهة صادرة يتصل بها الخادم بهويته الخاصة، وقد يتضمن، إذا تم تحديده، `api_key` الخاص بالمجموعة المسماة في رؤوس الطلب. افتراضيًا، يسمح ClickHouse بأي مضيف. لحصر الدوال على مجموعة محددة من المزوّدين، قم بتهيئة [`remote_url_allow_hosts`](/ar/operations/server-configuration-parameters/settings#remote_url_allow_hosts) في config الخادم، على سبيل المثال:

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

لاحظ أن هذا الإعداد على مستوى الخادم وينطبق على جميع الميزات التي تستخدم HTTP.

<div id="transport-security">
  ### أمان النقل (HTTP مقابل HTTPS)
</div>

يُحدَّد النقل حصريًا من خلال المخطط في عنوان URL الخاص بـ `endpoint`. لا يوجد تشفير على مستوى التطبيق لحمولة الطلب؛ فحماية البيانات أثناء النقل تعتمد بالكامل على هذا المخطط:

* `https://` — يستخدم الاتصال TLS. يُشفَّر نص الطلب (النص المُدخل والمطالبات) و`api_key` في رؤوس الطلب أثناء النقل، كما يجري التحقق من شهادة المزوّد. استخدم هذا مع أي مزوّد بعيد.
* `http://` — يكون الاتصال **غير مشفَّر**. يُرسَل نص الطلب و`api_key` بنص واضح. استخدم هذا فقط مع مزوّد موثوق على شبكة خاصة (مثل مثيل `vLLM` أو `Ollama` محلي).

لا تفرض دوال AI استخدام HTTPS: إذ يتم قبول `endpoint` بعنوان `http://` وتُرسَل البيانات من دون تشفير. لا يوجد حاليًا إعداد على جهة الخادم يرفض نقاط نهاية AI غير المشفَّرة — إذ يقيّد [`remote_url_allow_hosts`](/ar/operations/server-configuration-parameters/settings#remote_url_allow_hosts) مضيف الوجهة فقط ولا يفحص مخطط عنوان URL، لذا فإن `endpoint` بعنوان `http://` إلى مضيف مسموح به يمرّ أيضًا. لضمان نقلٍ مشفَّر، اضبط المجموعات المسماة باستخدام نقاط نهاية `https://`.

لاحظ أنه في كلتا الحالتين يتلقى المزوّد بيانات الإدخال بنص واضح بعد إنهاء TLS؛ إذ لا يحمي TLS البيانات إلا على مسار الشبكة بين الخادم والمزوّد.

<div id="supported-providers">
  ## المزوّدون المدعومون
</div>

| المزوّد   | قيمة `provider` | دوال الدردشة | ملاحظات                             |
| --------- | --------------- | ------------ | ----------------------------------- |
| OpenAI    | `'openai'`      | نعم          | المزوّد الافتراضي.                  |
| Anthropic | `'anthropic'`   | نعم          | يستخدم نقطة النهاية `/v1/messages`. |

<div id="observability">
  ## الرصد
</div>

يُتتبَّع نشاط AI function عبر [ProfileEvents](/ar/operations/system-tables/query_log) في ClickHouse:

| ProfileEvent      | الوصف                                                                                       |
| ----------------- | ------------------------------------------------------------------------------------------- |
| `AIAPICalls`      | عدد طلبات HTTP المرسلة إلى موفّر الذكاء الاصطناعي.                                          |
| `AIInputTokens`   | إجمالي رموز الإدخال المستهلكة.                                                              |
| `AIOutputTokens`  | إجمالي رموز الإخراج المستهلكة.                                                              |
| `AIRowsProcessed` | عدد الصفوف التي تلقّت نتيجة.                                                                |
| `AIRowsSkipped`   | عدد الصفوف التي تم تخطيها (تم تجاوز الحصة، أو حدث خطأ مع `ai_function_throw_on_error = 0`). |

استعلم عن هذه الأحداث:

```sql
SELECT
    ProfileEvents['AIAPICalls'] AS api_calls,
    ProfileEvents['AIInputTokens'] AS input_tokens,
    ProfileEvents['AIOutputTokens'] AS output_tokens
FROM system.query_log
WHERE query_id = 'query_id'
AND type = 'QueryFinish'
ORDER BY event_time DESC;
```

{/*
  يُستبدل المحتوى الداخلي للوسوم أدناه أثناء وقت البناء في إطار عمل التوثيق بـ
  مستندات مُولَّدة من system.functions. يُرجى عدم تعديل هذه الوسوم أو إزالتها.
  راجع: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }