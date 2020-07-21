---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "Acemi ClickHouse Geli\u015Ftirici Talimat"
---

ClickHouse binası Linux, FreeBSD ve Mac OS X üzerinde desteklenmektedir.

# Windows Kullanıyorsanız {#if-you-use-windows}

Windows kullanıyorsanız, Ubuntu ile bir sanal makine oluşturmanız gerekir. Bir sanal makine ile çalışmaya başlamak için VirtualBox yükleyin. UB :unt :u'yu web sitesinden indirebilirsiniz: https://www.ubuntu.com/\#download. lütfen indirilen görüntüden bir sanal makine oluşturun (bunun için en az 4GB RAM ayırmalısınız). Ubuntu'da bir komut satırı terminali çalıştırmak için lütfen kelimeyi içeren bir program bulun “terminal” adına (gnome-terminal, konsole vb.)) veya sadece Ctrl+Alt+T tuşlarına basın.

# 32 bit sistem kullanıyorsanız {#if-you-use-a-32-bit-system}

ClickHouse çalışamaz veya 32-bit bir sistem üzerinde oluşturun. 64-bit bir sisteme erişim kazanmanız gerekir ve okumaya devam edebilirsiniz.

# Github'da bir depo oluşturma {#creating-a-repository-on-github}

ClickHouse repository ile çalışmaya başlamak için bir GitHub hesabına ihtiyacınız olacaktır.

Muhtemelen zaten bir tane var, ama yapmazsanız, lütfen kayıt olun https://github.com. SSH anahtarlarınız yoksa, bunları üretmeli ve daha sonra Github'a yüklemelisiniz. Bu yamalar üzerinden göndermek için gereklidir. Diğer SSH sunucularıyla kullandığınız aynı SSH anahtarlarını kullanmak da mümkündür - muhtemelen zaten bunlara sahipsiniz.

ClickHouse deposunun bir çatalı oluşturun. Bunu yapmak için lütfen tıklayın “fork” sağ üst köşedeki düğme https://github.com/ClickHouse/ClickHouse. bu hesabınıza ClickHouse / ClickHouse kendi kopyasını çatal olacaktır.

Geliştirme süreci ilk ClickHouse sizin çatal içine amaçlanan değişiklikleri işlemekle ve daha sonra bir oluşturma oluşur “pull request” bu değişikliklerin ana depoya kabul edilmesi için (ClickHouse/ClickHouse).

Git depoları ile çalışmak için lütfen yükleyin `git`.

Bunu Ubuntu'da yapmak için komut satırı terminalinde çalışırsınız:

    sudo apt update
    sudo apt install git

Git kullanımı ile ilgili kısa bir el kitabı burada bulunabilir: https://services.github.com/on-demand/downloads/github-git-cheat-sheet.pdf.
Git ile ilgili ayrıntılı bir el kitabı için bkz. https://git-scm.com/book/en/v2.

# Geliştirme Makinenize bir depo klonlama {#cloning-a-repository-to-your-development-machine}

Ardından, kaynak dosyaları çalışma makinenize indirmeniz gerekir. Bu denir “to clone a repository” çünkü çalışma makinenizde deponun yerel bir kopyasını oluşturur.

Komut satırında terminal Çalıştır:

    git clone --recursive git@github.com:your_github_username/ClickHouse.git
    cd ClickHouse

Not: lütfen, yerine *your\_github\_username* uygun olanı ile!

Bu komut bir dizin oluşturacaktır `ClickHouse` projenin çalışma kopyasını içeren.

Yapı sistemini çalıştırmakla ilgili sorunlara yol açabileceğinden, çalışma dizininin yolunun hiçbir boşluk içermemesi önemlidir.

ClickHouse deposunun kullandığını lütfen unutmayın `submodules`. That is what the references to additional repositories are called (i.e. external libraries on which the project depends). It means that when cloning the repository you need to specify the `--recursive` yukarıdaki örnekte olduğu gibi bayrak. Depo alt modüller olmadan klonlanmışsa, bunları indirmek için aşağıdakileri çalıştırmanız gerekir:

    git submodule init
    git submodule update

Komutu ile durumunu kontrol edebilirsiniz: `git submodule status`.

Aşağıdaki hata iletisini alırsanız:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

Genellikle Github'a bağlanmak için SSH anahtarlarının eksik olduğu anlamına gelir. Bu anahtarlar normalde `~/.ssh`. SSH anahtarlarının kabul edilmesi için bunları GitHub kullanıcı arayüzünün ayarlar bölümüne yüklemeniz gerekir.

Depoyu https protokolü aracılığıyla da klonlayabilirsiniz:

    git clone https://github.com/ClickHouse/ClickHouse.git

Ancak bu, değişikliklerinizi sunucuya göndermenize izin vermez. Yine de geçici olarak kullanabilir ve SSH anahtarlarını daha sonra deponun uzak adresini değiştirerek ekleyebilirsiniz `git remote` komut.

Oradan güncellemeleri çekmek için orijinal ClickHouse repo'nun adresini yerel deponuza da ekleyebilirsiniz:

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

Başarıyla bu komutu çalıştırdıktan sonra çalıştırarak ana ClickHouse repo güncellemeleri çekmek mümkün olacak `git pull upstream master`.

## Alt modüllerle çalışma {#working-with-submodules}

Git'teki alt modüllerle çalışmak acı verici olabilir. Sonraki komutlar onu yönetmeye yardımcı olacaktır:

    # ! each command accepts --recursive
    # Update remote URLs for submodules. Barely rare case
    git submodule sync
    # Add new submodules
    git submodule init
    # Update existing submodules to the current state
    git submodule update
    # Two last commands could be merged together
    git submodule update --init

Bir sonraki komutlar, tüm alt modülleri başlangıç durumuna sıfırlamanıza yardımcı olacaktır (!UYARI! - herhangi bir değişiklik içinde silinecektir):

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

# Yapı Sistemi {#build-system}

ClickHouse bina için Cmake ve Ninja kullanır.

Cmake-ninja dosyaları (yapı görevleri) üretebilir bir meta-yapı sistemi.
Ninja-bu cmake oluşturulan görevleri yürütmek için kullanılan hıza odaklanarak daha küçük bir yapı sistemi.

Ubuntu, Debian veya Mint run'a yüklemek için `sudo apt install cmake ninja-build`.

Centos'ta, RedHat koşusu `sudo yum install cmake ninja-build`.

Arch veya Gentoo kullanıyorsanız, muhtemelen cmake'i nasıl kuracağınızı kendiniz biliyorsunuz.

Mac OS X üzerinde cmake ve Ninja yüklemek için ilk homebrew yüklemek ve daha sonra demlemek yoluyla her şeyi yüklemek:

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew install cmake ninja

Ardından, cmake sürümünü kontrol edin: `cmake --version`. 3.3'ün altındaysa, web sitesinden daha yeni bir sürüm yüklemelisiniz: https://cmake.org/download/.

# İsteğe Bağlı Harici Kütüphaneler {#optional-external-libraries}

ClickHouse, bina için birkaç dış kütüphane kullanır. Alt modüllerde bulunan kaynaklardan ClickHouse ile birlikte oluşturuldukları için hepsinin ayrı olarak kurulması gerekmez. Listeyi kontrol edebilirsiniz `contrib`.

# C++ Derleyici {#c-compiler}

Derleyiciler gcc sürüm 9 ve Clang sürüm 8 veya üzeri başlayarak ClickHouse bina için desteklenmektedir.

Resmi Yandex şu anda GCC'Yİ kullanıyor çünkü biraz daha iyi performansa sahip makine kodu üretiyor (kriterlerimize göre yüzde birkaçına kadar bir fark yaratıyor). Ve Clang genellikle geliştirme için daha uygundur. Yine de, sürekli entegrasyon (CI) platformumuz yaklaşık bir düzine yapı kombinasyonunu denetler.

Ubuntu run GCC yüklemek için: `sudo apt install gcc g++`

Gcc sürümünü kontrol edin: `gcc --version`. 9'un altındaysa, buradaki talimatları izleyin: https://clickhouse.tech/docs/tr/development/build/#install-gcc-9.

Mac OS X build sadece Clang için desteklenir. Sadece koş `brew install llvm`

Eğer Clang kullanmaya karar verirseniz, ayrıca yükleyebilirsiniz `libc++` ve `lld` eğer ne olduğunu biliyorsan. Kullanım `ccache` ayrıca tavsiye edilir.

# İnşaat Süreci {#the-building-process}

Artık ClickHouse oluşturmaya hazır olduğunuza göre ayrı bir dizin oluşturmanızı öneririz `build` için `ClickHouse` bu, tüm yapı eserlerini içerecek:

    mkdir build
    cd build

Birkaç farklı dizine (build\_release, build\_debug, vb.) sahip olabilirsiniz.) farklı yapı türleri için.

İçinde iken `build` dizin, cmake çalıştırarak yapı yapılandırın. İlk çalıştırmadan önce, derleyici belirten ortam değişkenlerini tanımlamanız gerekir (bu örnekte sürüm 9 gcc derleyicisi).

Linux:

    export CC=gcc-9 CXX=g++-9
    cmake ..

Mac OS X:

    export CC=clang CXX=clang++
    cmake ..

Bu `CC` değişken C için derleyiciyi belirtir (C derleyicisi için kısa) ve `CXX` değişken, hangi C++ derleyicisinin bina için kullanılacağını bildirir.

Daha hızlı bir yapı için, `debug` yapı türü-hiçbir optimizasyonları ile bir yapı. Bunun için aşağıdaki parametreyi sağlayın `-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

Bu komutu çalıştırarak yapı türünü değiştirebilirsiniz. `build` dizin.

İnşa etmek için ninja çalıştırın:

    ninja clickhouse-server clickhouse-client

Bu örnekte yalnızca gerekli ikili dosyalar oluşturulacaktır.

Tüm ikili dosyaları (Yardımcı Programlar ve testler) oluşturmanız gerekiyorsa, ninja'yı parametre olmadan çalıştırmalısınız:

    ninja

Tam yapı, ana ikili dosyaları oluşturmak için yaklaşık 30GB boş disk alanı veya 15GB gerektirir.

Yapı makinesinde büyük miktarda RAM mevcut olduğunda, paralel olarak çalışan yapı görevlerinin sayısını sınırlamanız gerekir `-j` param:

    ninja -j 1 clickhouse-server clickhouse-client

4GB RAM'Lİ makinelerde, 8GB RAM için 1 belirtmeniz önerilir `-j 2` tavsiye edilir.

Mesajı alırsanız: `ninja: error: loading 'build.ninja': No such file or directory` bu, bir yapı yapılandırması oluşturmanın başarısız olduğu ve yukarıdaki mesajı incelemeniz gerektiği anlamına gelir.

Bina işleminin başarılı bir şekilde başlatılmasının ardından, yapı ilerlemesini görürsünüz-işlenmiş görevlerin sayısı ve toplam görev sayısı.

Libhdfs2 kütüphanesinde protobuf dosyaları hakkında mesajlar oluştururken `libprotobuf WARNING` ortaya çıkabilir. Hiçbir şeyi etkilemezler ve göz ardı edilmeleri güvenlidir.

Başarılı bir yapı üzerine yürütülebilir bir dosya alırsınız `ClickHouse/<build_dir>/programs/clickhouse`:

    ls -l programs/clickhouse

# Clickhouse'un yerleşik yürütülebilir dosyasını çalıştırma {#running-the-built-executable-of-clickhouse}

Sunucuyu geçerli kullanıcı altında çalıştırmak için aşağıdakilere gitmeniz gerekir `ClickHouse/programs/server/` (dışında bulunan `build`) ve koş:

    ../../build/programs/clickhouse server

Bu durumda, ClickHouse geçerli dizinde bulunan yapılandırma dosyalarını kullanır. Koş youabilirsiniz `clickhouse server` komut satırı parametresi olarak bir yapılandırma dosyasının yolunu belirten herhangi bir dizinden `--config-file`.

Başka bir terminalde clickhouse-client ile Clickhouse'a bağlanmak için `ClickHouse/build/programs/` ve koş `clickhouse client`.

Eğer alırsanız `Connection refused` Mac OS X veya Freebsd'de mesaj, ana bilgisayar adresi 127.0.0.1 belirtmeyi deneyin:

    clickhouse client --host 127.0.0.1

Sisteminizde yüklü olan ClickHouse binary'nin üretim sürümünü özel olarak oluşturulmuş ClickHouse binaryinizle değiştirebilirsiniz. Bunu yapmak için resmi web sitesinden talimatları izleyerek Makinenize ClickHouse yükleyin. Ardından, aşağıdakileri çalıştırın:

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

Not thate that `clickhouse-client`, `clickhouse-server` ve diğerleri yaygın olarak paylaşılan sembolik bağlardır `clickhouse` ikilik.

Ayrıca sisteminizde yüklü ClickHouse paketinden yapılandırma dosyası ile özel inşa ClickHouse ikili çalıştırabilirsiniz:

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

# IDE (entegre geliştirme ortamı) {#ide-integrated-development-environment}

Hangi IDE kullanmak bilmiyorsanız, clion kullanmanızı öneririz. CLion ticari bir yazılımdır, ancak 30 günlük ücretsiz deneme süresi sunar. Öğrenciler için de ücretsizdir. CLion Linux ve Mac OS X hem de kullanılabilir.

KDevelop ve QTCreator, ClickHouse geliştirmek için bir IDE'NİN diğer harika alternatifleridir. KDevelop kararsız olmasına rağmen çok kullanışlı bir IDE olarak geliyor. KDevelop projeyi açtıktan sonra bir süre sonra çökerse, tıklamanız gerekir “Stop All” proje dosyalarının listesini açar açmaz düğme. Bunu yaptıktan sonra KDevelop ile çalışmak iyi olmalıdır.

Basit kod editörleri olarak, Yüce metin veya Visual Studio kodunu veya Kate'i (hepsi Linux'ta kullanılabilir) kullanabilirsiniz.

Her ihtimale karşı, Clion'un yarattığını belirtmek gerekir `build` kendi başına yol, aynı zamanda kendi seçtikleri `debug` yapı türü için, yapılandırma için Clion'da tanımlanan ve sizin tarafınızdan yüklenmeyen bir cmake sürümünü kullanır ve son olarak CLion kullanacaktır `make` yerine yapı görevlerini çalıştırmak için `ninja`. Bu normal bir davranıştır, sadece karışıklığı önlemek için bunu aklınızda bulundurun.

# Kod Yazma {#writing-code}

Açıklaması ClickHouse mimarisi burada bulabilirsiniz: https://clickhouse.tech / doscs/TR / development / Arch /it /ec /ture/

Kod stili Kılavuzu: https://clickhouse.tech / doscs / TR / development / style/

Yazma testleri: https://clickhouse.teknoloji / doscs / TR / geliştirme / testler/

Görevlerin listesi: https://github.com/ClickHouse/ClickHouse/contribute

# Test Verileri {#test-data}

Clickhouse'un geliştirilmesi genellikle gerçekçi veri kümelerinin yüklenmesini gerektirir. Performans testi için özellikle önemlidir. Yandex'ten özel olarak hazırlanmış anonim veri setimiz var.Metrica. Ayrıca bazı 3GB boş disk alanı gerektirir. Bu verilerin geliştirme görevlerinin çoğunu gerçekleştirmek için gerekli olmadığını unutmayın.

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

# Çekme İsteği Oluşturma {#creating-pull-request}

Github'un kullanıcı arayüzünde çatal deposuna gidin. Bir dalda gelişiyorsanız, o Dalı seçmeniz gerekir. Bir olacak “Pull request” ekranda bulunan düğme. Özünde, bu demektir “create a request for accepting my changes into the main repository”.

Çalışma henüz tamamlanmamış olsa bile bir çekme isteği oluşturulabilir. Bu durumda lütfen kelimeyi koyun “WIP” (devam eden çalışma) başlığın başında, daha sonra değiştirilebilir. Bu, kooperatif Gözden geçirme ve değişikliklerin tartışılması ve mevcut tüm testlerin çalıştırılması için kullanışlıdır. Değişikliklerinizin kısa bir açıklamasını sağlamanız önemlidir, daha sonra sürüm değişiklikleri oluşturmak için kullanılacaktır.

Yandex çalışanları PR'NİZİ bir etiketle etiketlediğinde testler başlayacaktır “can be tested”. The results of some first checks (e.g. code style) will come in within several minutes. Build check results will arrive within half an hour. And the main set of tests will report itself within an hour.

Sistem, çekme isteğiniz için ayrı ayrı ClickHouse ikili yapıları hazırlayacaktır. Bu yapıları almak için tıklayın “Details” yanındaki bağlantı “ClickHouse build check” çekler listesinde giriş. Orada inşa doğrudan bağlantılar bulacaksınız .eğer üretim sunucularında bile dağıtabilirsiniz ClickHouse DEB paketleri (eğer hiçbir korku varsa).

Büyük olasılıkla bazı yapılar ilk kez başarısız olur. Bunun nedeni, hem gcc hem de clang ile, hemen hemen tüm mevcut uyarılarla (her zaman `-Werror` bayrak) clang için etkin. Aynı sayfada, tüm yapı günlüklerini bulabilirsiniz, böylece tüm olası yollarla ClickHouse oluşturmak zorunda kalmazsınız.
