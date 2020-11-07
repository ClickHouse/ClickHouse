---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u062F\u0633\u062A\u0648\u0631\u0627\u0644\u0639\u0645\u0644 \u062A\u0648\
  \u0633\u0639\u0647 \u062F\u0647\u0646\u062F\u0647 \u06A9\u0644\u06CC\u06A9 \u0645\
  \u0628\u062A\u062F\u06CC"
---

ساختمان از خانه کلیک بر روی لینوکس پشتیبانی, بورس و سیستم عامل مک ایکس.

# در صورت استفاده از ویندوز {#if-you-use-windows}

اگر شما استفاده از ویندوز, شما نیاز به ایجاد یک ماشین مجازی با اوبونتو. برای شروع کار با یک ماشین مجازی لطفا مجازی نصب کنید. شما می توانید اوبونتو را از وب سایت دانلود کنید: https://www.ubuntu.com/#download.لطفا یک ماشین مجازی از تصویر دانلود شده ایجاد کنید (شما باید حداقل 4 گیگابایت رم را رزرو کنید). برای اجرای یک ترمینال خط فرمان در اوبونتو, لطفا یک برنامه حاوی کلمه قرار “terminal” به نام (گنوم ترمینال, کنسول و غیره.) یا فقط کنترل را فشار دهید

# اگر از یک سیستم 32 بیتی استفاده می کنید {#if-you-use-a-32-bit-system}

تاتر نمی تواند کار کند و یا ساخت بر روی یک سیستم 32 بیتی. شما باید دسترسی به یک سیستم 64 بیتی کسب و شما می توانید ادامه مطلب.

# ایجاد یک مخزن در گیتهاب {#creating-a-repository-on-github}

برای شروع کار با مخزن کلیک شما یک حساب گیتهاب نیاز.

شما احتمالا در حال حاضر یکی, اما اگر اینکار را نکنید, لطفا ثبت نام در https://github.com. در صورتی که کلید های سش را ندارید باید تولید کنید و سپس در گیتهاب بارگذاری کنید. این برای ارسال بیش از تکه های خود را مورد نیاز است. همچنین ممکن است به استفاده از کلید همان جلسه که شما با هر سرور جلسه دیگر استفاده کنید - احتمالا شما در حال حاضر کسانی که.

ایجاد یک چنگال مخزن مخزن مخزن. برای انجام این کار لطفا بر روی کلیک کنید “fork” دکمه در گوشه سمت راست بالا در https://github.com/ClickHouse/ClickHouse. آن را به چنگال خود کپی ClickHouse/ClickHouse به حساب کاربری خود.

روند توسعه شامل اولین ارتکاب تغییرات در نظر گرفته شده را به چنگال خود را از خانه رعیتی و سپس ایجاد یک “pull request” برای این تغییرات پذیرفته می شود به مخزن اصلی (ClickHouse/ClickHouse).

برای کار با مخازن دستگاه گوارش, لطفا نصب کنید `git`.

برای انجام این کار در اوبونتو شما در ترمینال خط فرمان اجرا می کنید:

    sudo apt update
    sudo apt install git

کتابچه راهنمای مختصر در استفاده از دستگاه گوارش را می توان یافت: https://education.github.com/git-cheat-sheet-education.pdf.
برای یک کتابچه راهنمای دقیق در دستگاه گوارش را ببینید https://git-scm.com/book/en/v2.

# شبیه سازی یک مخزن به دستگاه توسعه خود را {#cloning-a-repository-to-your-development-machine}

بعد, شما نیاز به دانلود فایل های منبع بر روی دستگاه کار خود را. این است که به نام “to clone a repository” زیرا ایجاد یک کپی محلی از مخزن بر روی دستگاه کار خود را.

در خط فرمان ترمینال اجرا:

    git clone --recursive git@github.com:your_github_username/ClickHouse.git
    cd ClickHouse

توجه: لطفا جایگزین کنید *تغییر _نامهی تو* با چه مناسب است!

این دستور یک دایرکتوری ایجاد خواهد کرد `ClickHouse` حاوی کپی کار از پروژه.

مهم این است که مسیر به دایرکتوری کار شامل هیچ فضای سفید به عنوان ممکن است به مشکلات در حال اجرا سیستم ساخت منجر شود.

لطفا توجه داشته باشید که مخزن کلیک استفاده می کند `submodules`. That is what the references to additional repositories are called (i.e. external libraries on which the project depends). It means that when cloning the repository you need to specify the `--recursive` پرچم همانطور که در مثال بالا. اگر مخزن بدون زیر دستی مسدود شده باشد باید موارد زیر را دانلود کنید:

    git submodule init
    git submodule update

شما می توانید وضعیت را با فرمان بررسی کنید: `git submodule status`.

اگر پیغام خطای زیر را دریافت کنید:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

به طور کلی به این معنی است که کلید های برش برای اتصال به گیتهاب از دست رفته است. این کلید ها به طور معمول در واقع `~/.ssh`. برای کلید های جلسه پذیرفته می شود شما نیاز به ارسال در بخش تنظیمات رابط کاربر گیتهاب.

شما همچنین می توانید مخزن از طریق پروتکل قام کلون:

    git clone https://github.com/ClickHouse/ClickHouse.git

این, با این حال, نمی خواهد به شما اجازه تغییرات خود را به سرور ارسال. شما هنوز هم می توانید به طور موقت استفاده کنید و اضافه کردن کلید های جلسه بعد جایگزین نشانی از راه دور از مخزن با `git remote` فرمان.

شما همچنین می توانید نشانی اصلی مخزن مخزن محلی خود را اضافه کنید به جلو و به روز رسانی از وجود دارد:

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

پس از موفقیت در حال اجرا این دستور شما قادر خواهید بود به جلو و به روز رسانی از مخزن کلیک اصلی در حال اجرا خواهد بود `git pull upstream master`.

## کار با Submodules {#working-with-submodules}

کار با زیربول در دستگاه گوارش می تواند دردناک باشد. دستورات بعدی کمک خواهد کرد که برای مدیریت:

    # ! each command accepts --recursive
    # Update remote URLs for submodules. Barely rare case
    git submodule sync
    # Add new submodules
    git submodule init
    # Update existing submodules to the current state
    git submodule update
    # Two last commands could be merged together
    git submodule update --init

دستورات بعدی کمک خواهد کرد که شما را به تنظیم مجدد تمام زیربول به حالت اولیه (!هشدار! - هر گونه تغییر در داخل حذف خواهد شد):

    # Synchronizes submodules' remote URL with .gitmodules
    git submodule sync --recursive
    # Update the registered submodules with initialize not yet initialized
    git submodule update --init --recursive
    # Reset all changes done after HEAD
    git submodule foreach git reset --hard
    # Clean files from .gitignore
    git submodule foreach git clean -xfd
    # Repeat last 4 commands for all submodule
    git submodule foreach git submodule sync --recursive
    git submodule foreach git submodule update --init --recursive
    git submodule foreach git submodule foreach git reset --hard
    git submodule foreach git submodule foreach git clean -xfd

# ساخت سیستم {#build-system}

تاتر با استفاده از کیک و نینجا برای ساخت و ساز.

کیک-یک سیستم متا ساخت است که می تواند فایل های نینجا (ساخت وظایف) تولید کند.
نینجا-یک سیستم ساخت کوچکتر با تمرکز بر سرعت مورد استفاده برای اجرای این کارهای تولید کیک.

برای نصب در اوبونتو, دبیان و یا نعنا اجرا `sudo apt install cmake ninja-build`.

در حال بارگذاری `sudo yum install cmake ninja-build`.

اگر شما استفاده از قوس یا جنتو, شما احتمالا خودتان می دانید که چگونه به نصب کیک.

برای نصب کیک و نینجا در سیستم عامل مک ایکس اول گشتن نصب و سپس نصب هر چیز دیگری از طریق دم:

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew install cmake ninja

بعد, بررسی نسخه از کیک: `cmake --version`. اگر زیر 3.3, شما باید یک نسخه جدیدتر از وب سایت نصب: https://cmake.org/download/.

# کتابخانه های خارجی اختیاری {#optional-external-libraries}

تاتر با استفاده از چندین کتابخانه خارجی برای ساخت و ساز. همه آنها نمی نیاز به نصب به طور جداگانه به عنوان آنها ساخته شده است همراه با ClickHouse از منابع واقع در submodules. شما می توانید لیست را بررسی کنید `contrib`.

# ج ++ کامپایلر {#c-compiler}

کامپایلر شورای همکاری خلیج فارس با شروع از نسخه 10 و صدای شیپور نسخه 8 یا بالاتر برای ساخت و ساز خانه عروسکی پشتیبانی می کند.

یاندکس رسمی ایجاد شده در حال حاضر با استفاده از شورای همکاری خلیج فارس به دلیل تولید کد ماشین از عملکرد کمی بهتر (بازده تفاوت تا چند درصد با توجه به معیار ما). و کلانگ معمولا برای توسعه راحت تر است. هر چند, ادغام مداوم ما (سی) پلت فرم اجرا می شود چک برای حدود یک دوجین از ترکیب ساخت.

برای نصب شورای همکاری خلیج فارس در اوبونتو اجرای: `sudo apt install gcc g++`

بررسی نسخه شورای همکاری خلیج فارس: `gcc --version`. اگر زیر است 10, سپس دستورالعمل اینجا را دنبال کنید: https://clickhouse.tech/docs/fa/development/build/#install-gcc-10.

سیستم عامل مک ایکس ساخت فقط برای صدای جرنگ جرنگ پشتیبانی می شود. فقط فرار کن `brew install llvm`

اگر شما تصمیم به استفاده از صدای شیپور, شما همچنین می توانید نصب `libc++` و `lld`, اگر شما می دانید چه چیزی است. با استفاده از `ccache` همچنین توصیه می شود.

# روند ساخت و ساز {#the-building-process}

حالا که اماده ساخت خانه عروسکی هستید توصیه می کنیم یک دایرکتوری جداگانه ایجاد کنید `build` داخل `ClickHouse` که شامل تمام مصنوعات ساخت:

    mkdir build
    cd build

شما می توانید چندین دایرکتوری های مختلف (build_release, build_debug ، ) برای انواع مختلف ساخت.

در حالی که در داخل `build` فهرست, پیکربندی ساخت خود را با در حال اجرا کیک. قبل از اولین اجرا, شما نیاز به تعریف متغیرهای محیطی که کامپایلر را مشخص (نسخه 10 کامپایلر شورای همکاری خلیج فارس در این مثال).

لینوکس:

    export CC=gcc-10 CXX=g++-10
    cmake ..

سیستم عامل مک ایکس:

    export CC=clang CXX=clang++
    cmake ..

این `CC` متغیر کامپایلر برای ج مشخص (کوتاه برای کامپایلر ج), و `CXX` دستور متغیر که سی++ کامپایلر است که برای ساخت و ساز استفاده می شود.

برای ساخت سریع تر, شما می توانید به توسل `debug` نوع ساخت-ساخت بدون بهینه سازی. برای عرضه پارامتر زیر `-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

شما می توانید نوع ساخت را با اجرای این دستور در تغییر دهید `build` فهرست راهنما.

اجرای نینجا برای ساخت:

    ninja clickhouse-server clickhouse-client

فقط باینری مورد نیاز در حال رفتن به در این مثال ساخته شده است.

اگر شما نیاز به ساخت تمام فایل های باینری (تاسیسات و تست), شما باید نینجا بدون پارامتر اجرا:

    ninja

ساخت کامل نیاز به حدود 30 گیگابایت فضای دیسک رایگان یا 15 گیگابایت برای ساخت باینری اصلی دارد.

هنگامی که مقدار زیادی از رم در ساخت دستگاه در دسترس است شما باید تعداد وظایف ساخت به صورت موازی با اجرا محدود می کند `-j` پرم:

    ninja -j 1 clickhouse-server clickhouse-client

در ماشین با 4 گیگابایت رم, توصیه می شود برای مشخص 1, برای 8گیگابایت رم `-j 2` توصیه می شود.

اگر پیام را دریافت کنید: `ninja: error: loading 'build.ninja': No such file or directory`, به این معنی که تولید یک پیکربندی ساخت شکست خورده است و شما نیاز به بازرسی پیام بالا.

پس از شروع موفق از روند ساخت و ساز, شما پیشرفت ساخت را ببینید-تعداد کارهای پردازش شده و تعداد کل وظایف.

در حالی که ساختمان پیام در مورد protobuf فایل در libhdfs2 کتابخانه مانند `libprotobuf WARNING` ممکن است نشان دهد تا. هیچ چیز تاثیر می گذارد و امن نادیده گرفته می شود.

پس از ساخت موفق شما یک فایل اجرایی دریافت کنید `ClickHouse/<build_dir>/programs/clickhouse`:

    ls -l programs/clickhouse

# اجرای اجرایی ساخته شده از خانه کلیک {#running-the-built-executable-of-clickhouse}

برای اجرای سرور تحت کاربر فعلی شما نیاز به حرکت به `ClickHouse/programs/server/` (واقع در خارج از `build`) و اجرا:

    ../../build/programs/clickhouse server

در این مورد, تاتر خواهد فایل های پیکربندی واقع در دایرکتوری جاری استفاده. شما می توانید اجرا کنید `clickhouse server` از هر دایرکتوری مشخص کردن مسیر به یک فایل پیکربندی به عنوان یک پارامتر خط فرمان `--config-file`.

برای اتصال به ClickHouse با clickhouse-مشتری در یکی دیگر از ترمینال حرکت به `ClickHouse/build/programs/` و فرار کن `./clickhouse client`.

اگر شما `Connection refused` سعی کنید مشخص نشانی میزبان 127.0.0.1:

    clickhouse client --host 127.0.0.1

شما می توانید جایگزین تولید نسخه ClickHouse باینری در سیستم شما نصب شده خود را با سفارشی ساخته شده ClickHouse دودویی. برای انجام این کار نصب کلیک بر روی دستگاه خود را به دنبال دستورالعمل از وب سایت رسمی. بعد زیر را اجرا کنید:

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

توجه داشته باشید که `clickhouse-client`, `clickhouse-server` و دیگران به طور معمول به اشتراک گذاشته می شوند `clickhouse` دودویی.

شما همچنین می توانید خود را سفارشی ساخته شده ClickHouse دودویی با فایل پیکربندی از ClickHouse بسته نصب شده در سیستم شما:

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

# محیط توسعه یکپارچه) {#ide-integrated-development-environment}

اگر شما نمی دانید که محیط برنامه نویسی برای استفاده, توصیه می کنیم که شما با استفاده از کلون. کلوون نرم افزار تجاری است, اما 30 روز رایگان دوره محاکمه. این نیز رایگان برای دانشجویان. CLion می توان هم بر روی لینوکس و Mac OS X.

KDevelop و QTCreator دیگر از جایگزین های بسیار خوبی از یک IDE برای توسعه ClickHouse. توسعه و توسعه به عنوان یک محیط برنامه نویسی بسیار مفید هر چند ناپایدار. اگر توسعه پس از مدتی پس از باز کردن پروژه سقوط, شما باید کلیک کنید “Stop All” دکمه به محض این که لیستی از فایل های پروژه را باز کرده است. پس از انجام این کار کدولاپ باید خوب باشد برای کار با.

به عنوان ویراستاران کد ساده, شما می توانید متن والا و یا کد ویژوال استودیو استفاده, یا کیت (که همه در دسترس هستند در لینوکس).

فقط در مورد لازم به ذکر است که CLion ایجاد `build` مسیر خود را نیز در انتخاب خود `debug` برای ساخت نوع پیکربندی آن را با استفاده از یک نسخه از CMake که تعریف شده است در CLion و نه یک نصب شده توسط شما, و در نهایت, CLion استفاده خواهد کرد `make` برای اجرای وظایف ساخت به جای `ninja`. این رفتار طبیعی است, فقط نگه دارید که در ذهن برای جلوگیری از سردرگمی.

# نوشتن کد {#writing-code}

شرح ClickHouse معماری را می توان در اینجا یافت نشد: https://clickhouse.فناوری / اسناد/مهندسی / توسعه / معماری/

راهنمای سبک کد: https://clickhouse.فناوری / اسناد/در/توسعه / سبک/

تست نوشتن: https://clickhouse.فناوری / اسناد/توسعه/تست/

فهرست تکلیفها: https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22

# داده های تست {#test-data}

در حال توسعه تاتر اغلب نیاز به بارگذاری مجموعه داده های واقع بینانه است. این امر به ویژه برای تست عملکرد مهم است. ما یک مجموعه خاص تهیه شده از داده های ناشناس از یاندکس.متریکا این علاوه بر برخی از 3 گیگابایت فضای دیسک رایگان نیاز دارد. توجه داشته باشید که این داده ها مورد نیاز است برای به انجام رساندن بسیاری از وظایف توسعه.

    sudo apt install wget xz-utils

    wget https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz
    wget https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client

    CREATE DATABASE IF NOT EXISTS test

    CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

    CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv

# ایجاد درخواست کشش {#creating-pull-request}

حرکت به مخزن چنگال خود را در رابط کاربر گیتهاب است. اگر شما شده اند در حال توسعه در یک شاخه, شما نیاز به انتخاب کنید که شاخه. وجود خواهد داشت “Pull request” دکمه واقع بر روی صفحه نمایش. در اصل این به این معنی است “create a request for accepting my changes into the main repository”.

درخواست کشش را می توان ایجاد حتی اگر کار کامل نشده است. در این مورد لطفا کلمه را قرار دهید “WIP” (کار در حال پیشرفت) در ابتدای عنوان می تواند بعدا تغییر کند. این برای بررسی تعاونی و بحث در مورد تغییرات و همچنین برای اجرای تمام تست های موجود مفید است. این مهم است که شما شرح مختصری از تغییرات خود را فراهم, بعد برای تولید تغییرات انتشار استفاده خواهد شد.

تست شروع خواهد شد به عنوان به زودی به عنوان کارکنان یاندکس برچسب روابط عمومی خود را با یک برچسب “can be tested”. The results of some first checks (e.g. code style) will come in within several minutes. Build check results will arrive within half an hour. And the main set of tests will report itself within an hour.

این سیستم خواهد باینری کلیک ایجاد شده برای درخواست کشش خود را به صورت جداگانه تهیه. برای بازیابی این ایجاد کلیک کنید “Details” پیوند بعدی به “ClickHouse build check” ورود در لیست چک. وجود دارد شما لینک مستقیم به ساخته شده پیدا کنید .بسته دب از تاتر که شما می توانید حتی بر روی سرور تولید خود را استقرار (اگر شما هیچ ترس).

به احتمال زیاد برخی از ایجاد خواهد شد شکست در اولین بار. این به خاطر این واقعیت است که ما بررسی می کنیم ایجاد هر دو با شورای همکاری خلیج فارس و همچنین با صدای جرنگ, با تقریبا تمام هشدارهای موجود (همیشه با `-Werror` پرچم) را فعال کنید برای صدای جرنگ جرنگ. در همان صفحه, شما می توانید تمام سیاهههای مربوط ساخت پیدا به طوری که شما لازم نیست که برای ساخت خانه در تمام راه های ممکن.
