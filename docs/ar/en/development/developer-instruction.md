---
description: 'المتطلبات الأساسية وإرشادات الإعداد لتطوير ClickHouse'
sidebar_label: 'المتطلبات الأساسية'
sidebar_position: 5
slug: /development/developer-instruction
title: 'المتطلبات الأساسية للمطورين'
doc_type: 'guide'
---

يمكن تجميع ClickHouse على Linux وFreeBSD وmacOS.
إذا كنت تستخدم Windows، فلا يزال بإمكانك تجميع ClickHouse داخل آلة افتراضية تعمل بنظام Linux، مثل [VirtualBox](https://www.virtualbox.org/) مع Ubuntu.

<div id="create-a-repository-on-github">
  ## أنشئ مستودعًا على GitHub
</div>

لبدء تطوير ClickHouse، ستحتاج إلى حساب على [GitHub](https://www.github.com/).
يُرجى أيضًا إنشاء مفتاح SSH محليًا (إذا لم يكن لديك واحد بالفعل) ورفع المفتاح العام إلى GitHub، لأن ذلك شرط أساسي للمساهمة بالتصحيحات.

بعد ذلك، أنشئ نسخة متفرعة من [مستودع ClickHouse](https://github.com/ClickHouse/ClickHouse/) ضمن حسابك الشخصي بالنقر على زر &quot;نسخة متفرعة&quot; في الزاوية العلوية اليمنى.

للمساهمة بالتغييرات، مثل إصلاح مشكلة أو إضافة ميزة، نفّذ commit لتغييراتك أولًا على فرع في نسختك المتفرعة، ثم أنشئ &quot;طلب سحب&quot; بهذه التغييرات إلى المستودع الرئيسي.

للعمل مع مستودعات Git، يُرجى تثبيت Git. على سبيل المثال، في Ubuntu، شغّل:

```sh
sudo apt update
sudo apt install git
```

يمكنك العثور على ورقة مرجعية سريعة لـ Git [هنا](https://education.github.com/git-cheat-sheet-education.pdf).
ويمكنك العثور على دليل Git مفصل [هنا](https://git-scm.com/book/en/v2).

<div id="clone-the-repository-to-your-development-machine">
  ## استنسخ المستودع إلى جهاز التطوير لديك
</div>

أولًا، نزّل ملفات المصدر إلى جهازك العامل، أي استنسخ المستودع:

```sh
git clone git@github.com:your_github_username/ClickHouse.git  # replace the placeholder with your GitHub user name
cd ClickHouse
```

ينشئ هذا الأمر دليلاً باسم `ClickHouse/` يحتوي على الشيفرة المصدرية والاختبارات وملفات أخرى.
يمكنك تحديد دليل مخصص لعملية checkout بعد عنوان الـ URL، لكن من المهم ألا يحتوي هذا المسار على مسافات، لأن ذلك قد يتسبب في تعطّل عملية البناء لاحقًا.

يستخدم المستودع الخاص بـ Git في ClickHouse وحدات فرعية لجلب مكتبات تابعة لجهات خارجية.
لا يتم إجراء checkout للوحدات الفرعية افتراضيًا.
يمكنك إما

* تشغيل `git clone` مع الخيار `--recurse-submodules`،

* إذا شغّلت `git clone` بدون `--recurse-submodules`، فشغّل `git submodule update --init --jobs <N>` لإجراء checkout لجميع الوحدات الفرعية بشكل صريح. (يمكن تعيين `<N>` مثلًا إلى `12` لتنزيلها بالتوازي.)

* إذا شغّلت `git clone` بدون `--recurse-submodules` وكنت ترغب في استخدام checkout سطحي [shallow](https://github.blog/2020-12-21-get-up-to-speed-with-partial-clone-and-shallow-clone/) للوحدات الفرعية لتجاوز history فيها وتوفير بعض المساحة، فشغّل `./contrib/update-submodules.sh`. يُستخدم هذا البديل في CI، لكنه غير موصى به للتطوير المحلي لأنه يجعل العمل مع الوحدات الفرعية أقل سلاسة وأبطأ.

للتحقق من status وحدات Git الفرعية، شغّل `git submodule status`.

إذا ظهرت لك رسالة error التالية

```bash
Permission denied (publickey).
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.
```

مفاتيح SSH اللازمة للاتصال بـ GitHub غير موجودة.
توجد هذه المفاتيح عادةً في `~/.ssh`.
لكي يقبل GitHub مفاتيح SSH، تحتاج إلى رفعها في إعداداته.

يمكنك أيضًا استنساخ المستودع عبر HTTPS:

```sh
git clone https://github.com/ClickHouse/ClickHouse.git
```

لكن هذا لن يتيح لك دفع تغييراتك إلى الخادم.
لا يزال بإمكانك استخدامه مؤقتًا ثم إضافة مفاتيح SSH لاحقًا، مع استبدال العنوان البعيد للمستودع باستخدام الأمر `git remote`.

يمكنك أيضًا إضافة عنوان مستودع ClickHouse الأصلي إلى مستودعك المحلي لسحب التحديثات من هناك:

```sh
git remote add upstream git@github.com:ClickHouse/ClickHouse.git
```

بعد تنفيذ هذا الأمر بنجاح، ستتمكن من سحب التحديثات من مستودع ClickHouse الرئيسي بتشغيل `git pull upstream master`.

:::tip
يُرجى عدم استخدام `git push` بصيغته المباشرة، فقد تدفع إلى الـ remote الخطأ و/أو الفرع الخطأ.
من الأفضل تحديد اسمَي الـ remote والفرع صراحةً، على سبيل المثال: `git push origin my_branch_name`.
:::”

<div id="writing-code">
  ## كتابة الشيفرة
</div>

فيما يلي بعض الروابط السريعة التي قد تكون مفيدة عند كتابة الشيفرة لـClickHouse:

* [معمارية ClickHouse](/ar/development/architecture/).
* [دليل أسلوب الشيفرة](/ar/development/style/).
* [مكتبات الطرف الثالث](/ar/development/contrib#adding-and-maintaining-third-party-libraries)
* [كتابة الاختبارات](/ar/development/tests/)
* [المشكلات المفتوحة](https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22)

<div id="ide">
  ### IDE
</div>

يُعدّ كلٌّ من [Visual Studio Code](https://code.visualstudio.com/) و[Neovim](https://neovim.io/) خيارين أثبتا نجاحهما سابقًا في تطوير ClickHouse. إذا كنت تستخدم VS Code، فنوصي باستخدام [إضافة clangd](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd) بدلًا من IntelliSense، لأنها أفضل منه بكثير من ناحية الأداء.

يُعدّ [CLion](https://www.jetbrains.com/clion/) بديلًا ممتازًا آخر. ومع ذلك، قد يكون أبطأ في المشاريع الكبيرة مثل ClickHouse. وفيما يلي بعض الأمور التي ينبغي وضعها في الاعتبار عند استخدام CLion:

* ينشئ CLion مسار `build` تلقائيًا، ويحدّد `debug` تلقائيًا كنوع build
* يستخدم إصدار CMake المعرّف داخل CLion، وليس الإصدار الذي ثبّتَّه أنت
* يستخدم CLion الأمر `make` لتشغيل مهام build بدلًا من `ninja` (وهذا سلوك طبيعي)

ومن بيئات التطوير المتكاملة الأخرى التي يمكنك استخدامها [Sublime Text](https://www.sublimetext.com/) و[Qt Creator](https://www.qt.io/product/development-tools) و[Kate](https://kate-editor.org/).

<div id="create-a-pull-request">
  ## أنشئ طلب سحب
</div>

انتقل إلى مستودع النسخة المتفرعة الخاص بك في واجهة مستخدم GitHub.
إذا كنت تطوّر على فرع، فعليك اختيار ذلك الفرع.
ستجد زر &quot;طلب سحب&quot; على الشاشة.
والمقصود بذلك ببساطة: &quot;إنشاء طلب لقبول تعديلاتي في المستودع الرئيسي&quot;.

يمكن إنشاء طلب سحب حتى إذا لم يكتمل العمل بعد.
في هذه الحالة، يُرجى وضع الكلمة &quot;WIP&quot; (قيد العمل) في بداية العنوان، ويمكن تغييرها لاحقًا.
وهذا مفيد للمراجعة التعاونية ومناقشة التغييرات، وكذلك لتشغيل جميع الاختبارات المتاحة.
ومن المهم أن تقدّم وصفًا موجزًا لتغييراتك، إذ سيُستخدم لاحقًا لإنشاء سجل تغييرات الإصدار.

سيبدأ الاختبار بمجرد أن يضيف موظفو ClickHouse إلى طلب السحب الخاص بك وسم &quot;can be tested&quot;.
ستظهر نتائج بعض عمليات التحقق الأولى (مثل أسلوب الشيفرة) خلال بضع دقائق.
وستصل نتائج التحقق من البناء خلال نصف ساعة.
أما المجموعة الرئيسية من الاختبارات فستُبلغ عن نتائجها خلال ساعة.

سيُعدّ النظام نُسخ ClickHouse التنفيذية الخاصة بطلب السحب لديك بشكل منفصل.
وللحصول على هذه النُسخ، انقر على الرابط &quot;Details&quot; بجوار الإدخال &quot;Builds&quot; في قائمة عمليات التحقق.
وهناك ستجد روابط مباشرة إلى حزم .deb المبنية لـ ClickHouse، ويمكنك نشرها حتى على خوادم الإنتاج لديك (إن لم تكن تخشى ذلك).

<div id="write-documentation">
  ## كتابة الوثائق
</div>

يجب أن يكون كل طلب سحب يضيف ميزة جديدة مصحوبًا بوثائق مناسبة.
إذا كنت ترغب في معاينة التغييرات التي أجريتها على الوثائق، فستجد في ملف README.md [هنا](https://github.com/ClickHouse/clickhouse-docs) إرشادات حول كيفية بناء صفحة الوثائق محليًا.
عند إضافة دالة جديدة إلى ClickHouse، يمكنك استخدام القالب أدناه كمرجع:

````markdown
# newFunctionName

A short description of the function goes here. It should describe briefly what it does and a typical usage case.

**Syntax**

\```sql
newFunctionName(arg1, arg2[, arg3])
\```

**Arguments**

- `arg1` — Description of the argument. [DataType](../data-types/float.md)
- `arg2` — Description of the argument. [DataType](../data-types/float.md)
- `arg3` — Description of optional argument (optional). [DataType](../data-types/float.md)

**Implementation Details**

A description of implementation details if relevant.

**Returned value**

- Returns {insert what the function returns here}. [DataType](../data-types/float.md)

**Example**

\```sql title="Query"
SELECT 'write your example query here';
\```

\```response title="Response"
┌───────────────────────────────────┐
│ the result of the query           │
└───────────────────────────────────┘
\```
````

<div id="using-test-data">
  ## استخدام بيانات الاختبار
</div>

غالبًا ما يتطلب تطوير ClickHouse تحميل مجموعات بيانات واقعية.
وتزداد أهمية ذلك خصوصًا عند اختبار الأداء.
لقد أعددنا خصيصًا مجموعة من بيانات تحليلات الويب مجهولة الهوية.
ويتطلب ذلك أيضًا نحو 3 جيجابايت إضافية من المساحة الخالية على القرص.

```sh
    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client
```

في clickhouse-client:

```sql
CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

```

استورد البيانات:

```bash
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv
```