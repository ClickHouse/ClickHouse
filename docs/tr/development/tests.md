---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 69
toc_title: "ClickHouse testleri nas\u0131l \xE7al\u0131\u015Ft\u0131r\u0131l\u0131\
  r"
---

# ClickHouse Testi {#clickhouse-testing}

## Fonksiyonel Testler {#functional-tests}

Fonksiyonel testler en basit ve kullanımı kolay olanlardır. ClickHouse özelliklerinin çoğu fonksiyonel testlerle test edilebilir ve bu şekilde test edilebilecek ClickHouse kodundaki her değişiklik için kullanılması zorunludur.

Her işlevsel test, çalışan ClickHouse sunucusuna bir veya birden çok sorgu gönderir ve sonucu referansla karşılaştırır.

Testler bulunur `queries` dizin. İki alt dizin var: `stateless` ve `stateful`. Durumsuz testler, önceden yüklenmiş test verileri olmadan sorguları çalıştırır - genellikle testin kendisinde anında küçük sentetik veri kümeleri oluştururlar. Durum bilgisi testleri, Yandex'ten önceden yüklenmiş test verileri gerektirir.Metrica ve halka açık değil. Biz sadece kullanmak eğilimindedir `stateless` testler ve yeni eklemekten kaçının `stateful` testler.

Her test iki tipten biri olabilir: `.sql` ve `.sh`. `.sql` test için borulu basit SQL komut dosyasıdır `clickhouse-client --multiquery --testmode`. `.sh` test kendisi tarafından çalıştırılan bir komut dosyasıdır.

Tüm testleri çalıştırmak için şunları kullanın `clickhouse-test` aracı. Bak `--help` Olası seçeneklerin listesi için. Sadece tüm testleri çalıştırmak veya test adı alt dize tarafından süzülmüş testlerin alt kümesini çalıştırabilirsiniz: `./clickhouse-test substring`.

Fonksiyonel testleri çağırmanın en basit yolu kopyalamaktır `clickhouse-client` -e doğru `/usr/bin/`, çalıştırmak `clickhouse-server` ve sonra koş `./clickhouse-test` kendi dizininden.

Yeni test eklemek için, bir `.sql` veya `.sh` dosya içinde `queries/0_stateless` dizin, elle kontrol edin ve sonra oluşturun `.reference` aşağıdaki şekilde dosya: `clickhouse-client -n --testmode < 00000_test.sql > 00000_test.reference` veya `./00000_test.sh > ./00000_test.reference`.

Testler yalnızca tabloları (create, drop, vb.) kullanmalıdır `test` önceden oluşturulduğu varsayılır veritabanı; ayrıca testler geçici tablolar kullanabilirsiniz.

İşlevsel testlerde dağıtılmış sorgular kullanmak istiyorsanız, kaldıraç `remote` tablo fonksiyonu ile `127.0.0.{1..2}` sunucunun kendisini sorgulaması için adresler; veya sunucu yapılandırma dosyasında önceden tanımlanmış test kümelerini kullanabilirsiniz `test_shard_localhost`.

Bazı testler ile işaretlenir `zookeeper`, `shard` veya `long` kendi adlarına.
`zookeeper` ZooKeeper kullanan testler içindir. `shard` testler içindir
dinlemek için sunucu gerektirir `127.0.0.*`; `distributed` veya `global` aynı var
anlama. `long` bir saniye biraz daha uzun süren testler içindir. Yapabilirsin
kullanarak bu test gruplarını devre dışı bırakın `--no-zookeeper`, `--no-shard` ve
`--no-long` sırasıyla seçenekler.

## Bilinen Hatalar {#known-bugs}

Fonksiyonel testlerle kolayca çoğaltılabilen bazı hatalar biliyorsak, hazırlanmış fonksiyonel testleri `tests/queries/bugs` dizin. Bu testler taşınacaktır `tests/queries/0_stateless` hatalar düzeltildiğinde.

## Entegrasyon Testleri {#integration-tests}

Entegrasyon testleri, kümelenmiş konfigürasyonda Clickhouse'u ve MySQL, Postgres, MongoDB gibi diğer sunucularla ClickHouse etkileşimini test etmeyi sağlar. Ağ bölmelerini, paket damlalarını vb. taklit etmek için kullanışlıdırlar. Bu testler Docker altında çalıştırılır ve çeşitli yazılımlarla birden fazla konteyner oluşturur.

Görmek `tests/integration/README.md` bu testlerin nasıl çalıştırılacağı hakkında.

Clickhouse'un üçüncü taraf sürücülerle entegrasyonunun sınanmadığını unutmayın. Ayrıca şu anda JDBC ve ODBC sürücülerimizle entegrasyon testlerimiz yok.

## Ünite Testleri {#unit-tests}

Birim testleri, Clickhouse'u bir bütün olarak değil, tek bir yalıtılmış kitaplık veya sınıfı test etmek istediğinizde kullanışlıdır. Etkinleştirebilir veya devre dışı bırakma ile testlerin yapı `ENABLE_TESTS` Cmake seçeneği. Birim testleri (ve diğer test programları) bulunur `tests` kodun alt dizinleri. Birim testlerini çalıştırmak için şunları yazın `ninja test`. Bazı testler kullanın `gtest`, ancak bazıları test başarısızlığında sıfır olmayan çıkış kodunu döndüren programlardır.

Kodun zaten işlevsel testler tarafından kapsanması durumunda birim testlerine sahip olmak zorunlu değildir (ve işlevsel testler genellikle kullanımı çok daha basittir).

## Performans Testleri {#performance-tests}

Performans testleri ölçmek ve sentetik sorguları ClickHouse bazı izole kısmının performansını karşılaştırmak için izin verir. Testler bulunur `tests/performance`. Her test ile temsil edilir `.xml` test durumunun açıklaması ile dosya. Testler ile çalıştırılır `clickhouse performance-test` Aracı (Bu gömülü `clickhouse` ikilik). Görmek `--help` çağırma için.

Her test, durdurma için bazı koşullarla (örneğin, bir döngüde bir veya birden fazla sorgu (muhtemelen parametre kombinasyonlarıyla) çalıştırır “maximum execution speed is not changing in three seconds”) ve sorgu performansı ile ilgili bazı metrikleri ölçün (örneğin “maximum execution speed”). Bazı testler önceden yüklenmiş test veri kümesinde Önkoşullar içerebilir.

Bazı senaryoda Clickhouse'un performansını artırmak istiyorsanız ve basit sorgularda iyileştirmeler gözlemlenebiliyorsa, bir performans testi yazmanız önerilir. Her zaman kullanmak mantıklı `perf top` testleriniz sırasında veya diğer perf araçları.

## Test araçları ve komut dosyaları {#test-tools-and-scripts}

Bazı programlar `tests` dizin testleri hazırlanmış değil, ancak test araçlarıdır. Örneğin, için `Lexer` bir araç var `src/Parsers/tests/lexer` bu sadece stdin'in tokenizasyonunu yapar ve renklendirilmiş sonucu stdout'a yazar. Bu tür araçları kod örnekleri olarak ve keşif ve manuel test için kullanabilirsiniz.

Ayrıca Çift Dosya yerleştirebilirsiniz `.sh` ve `.reference` aracı ile birlikte bazı önceden tanımlanmış giriş üzerinde çalıştırmak için-daha sonra komut sonucu karşılaştırılabilir `.reference` Dosya. Bu tür testler otomatik değildir.

## Çeşitli Testler {#miscellaneous-tests}

Bulunan dış sözlükler için testler vardır `tests/external_dictionaries` ve makine öğrenilen modeller için `tests/external_models`. Bu testler güncelleştirilmez ve tümleştirme testlerine aktarılmalıdır.

Çekirdek ekler için ayrı bir test var. Bu test, ayrı sunucularda ClickHouse kümesini çalıştırır ve çeşitli arıza durumlarını taklit eder: ağ bölünmesi, paket bırakma (ClickHouse düğümleri arasında, ClickHouse ve ZooKeeper arasında, ClickHouse sunucusu ve istemci arasında, vb.), `kill -9`, `kill -STOP` ve `kill -CONT` , istemek [Jepsen](https://aphyr.com/tags/Jepsen). Daha sonra test, kabul edilen tüm eklerin yazıldığını ve reddedilen tüm eklerin olmadığını kontrol eder.

Clickhouse açık kaynaklı önce çekirdek testi ayrı ekip tarafından yazılmıştır. Bu takım artık ClickHouse ile çalışmıyor. Test yanlışlıkla Java ile yazılmıştır. Bu nedenlerden dolayı, çekirdek testi yeniden yazılmalı ve entegrasyon testlerine taşınmalıdır.

## Manuel Test {#manual-testing}

Yeni bir özellik geliştirdiğinizde, el ile de test etmek mantıklıdır. Bunu aşağıdaki adımlarla yapabilirsiniz:

ClickHouse Oluşturun. Terminalden Clickhouse'u çalıştırın: dizini değiştir `programs/clickhouse-server` ve ile çalıştırın `./clickhouse-server`. Bu yapılandırma kullanacak (`config.xml`, `users.xml` ve içindeki dosyalar `config.d` ve `users.d` dizinler) geçerli dizinden varsayılan olarak. ClickHouse sunucusuna bağlanmak için, çalıştırın `programs/clickhouse-client/clickhouse-client`.

Tüm clickhouse araçlarının (sunucu, istemci, vb.) sadece tek bir ikili için symlinks olduğunu unutmayın `clickhouse`. Bu ikili bulabilirsiniz `programs/clickhouse`. Tüm araçlar olarak da çağrılabilir `clickhouse tool` yerine `clickhouse-tool`.

Alternatif olarak ClickHouse paketini yükleyebilirsiniz: Yandex deposundan kararlı sürüm veya kendiniz için paket oluşturabilirsiniz `./release` ClickHouse kaynakları kökünde. Ardından sunucuyu şu şekilde başlatın `sudo service clickhouse-server start` (veya sunucuyu durdurmak için durdurun). Günlükleri arayın `/etc/clickhouse-server/clickhouse-server.log`.

ClickHouse sisteminizde zaten yüklü olduğunda, yeni bir `clickhouse` ikili ve mevcut ikili değiştirin:

``` bash
$ sudo service clickhouse-server stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo service clickhouse-server start
```

Ayrıca sistem clickhouse-server durdurmak ve aynı yapılandırma ile ancak terminale günlüğü ile kendi çalıştırabilirsiniz:

``` bash
$ sudo service clickhouse-server stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Gdb ile örnek:

``` bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Sistem clickhouse-sunucu zaten çalışıyorsa ve bunu durdurmak istemiyorsanız, sizin port numaralarını değiştirebilirsiniz `config.xml` (veya bunları bir dosyada geçersiz kılma `config.d` dizin), uygun veri yolu sağlayın ve çalıştırın.

`clickhouse` ikili neredeyse hiçbir bağımlılıkları vardır ve Linux dağıtımları geniş genelinde çalışır. Hızlı ve kirli bir sunucuda değişikliklerinizi test etmek için, sadece yapabilirsiniz `scp` taze inşa `clickhouse` sunucunuza ikili ve daha sonra yukarıdaki örneklerde olduğu gibi çalıştırın.

## Test Ortamı {#testing-environment}

Kararlı olarak yayınlamadan önce test ortamında dağıtın. Test ortamı, 1/39 bölümünü işleyen bir kümedir [Üye.Metrica](https://metrica.yandex.com/) veriler. Test ortamımızı Yandex ile paylaşıyoruz.Metrica takımı. ClickHouse mevcut verilerin üstünde kesinti olmadan yükseltilir. İlk önce verilerin gerçek zamanlı olarak gecikmeden başarıyla işlendiğine bakıyoruz, çoğaltma çalışmaya devam ediyor ve Yandex tarafından görülebilen herhangi bir sorun yok.Metrica takımı. İlk kontrol aşağıdaki şekilde yapılabilir:

``` sql
SELECT hostName() AS h, any(version()), any(uptime()), max(UTCEventTime), count() FROM remote('example01-01-{1..3}t', merge, hits) WHERE EventDate >= today() - 2 GROUP BY h ORDER BY h;
```

Bazı durumlarda yandex'teki arkadaş ekiplerimizin test ortamına da dağıtım yapıyoruz: Pazar, Bulut, vb. Ayrıca geliştirme amacıyla kullanılan bazı donanım sunucularımız var.

## Yük Testi {#load-testing}

Test ortamına dağıtıldıktan sonra, üretim kümesinden gelen sorgularla yük testini çalıştırıyoruz. Bu elle yapılır.

Etkinleştirdiğinizden emin olun `query_log` üretim kümenizde.

Bir gün veya daha fazla sorgu günlüğü toplayın:

``` bash
$ clickhouse-client --query="SELECT DISTINCT query FROM system.query_log WHERE event_date = today() AND query LIKE '%ym:%' AND query NOT LIKE '%system.query_log%' AND type = 2 AND is_initial_query" > queries.tsv
```

Bu şekilde karmaşık bir örnektir. `type = 2` başarıyla yürütülen sorguları süzer. `query LIKE '%ym:%'` yandex'ten ilgili sorguları seçmektir.Metrica. `is_initial_query` yalnızca istemci tarafından başlatılan sorguları seçmektir, Clickhouse'un kendisi tarafından değil (dağıtılmış sorgu işlemenin parçaları olarak).

`scp` bu test kümenize günlük ve aşağıdaki gibi çalıştırın:

``` bash
$ clickhouse benchmark --concurrency 16 < queries.tsv
```

(muhtemelen de belirtmek istiyorum `--user`)

Sonra bir gece ya da hafta sonu için bırakın ve dinlenin.

Kontrol etmelisiniz `clickhouse-server` çökmez, bellek ayak izi sınırlıdır ve performans zamanla aşağılayıcı değildir.

Kesin sorgu yürütme zamanlamaları kaydedilmez ve sorguların ve ortamın yüksek değişkenliği nedeniyle karşılaştırılmaz.

## Yapı Testleri {#build-tests}

Yapı testleri, yapının çeşitli alternatif konfigürasyonlarda ve bazı yabancı sistemlerde bozulmadığını kontrol etmeyi sağlar. Testler bulunur `ci` dizin. Docker, Vagrant ve bazen de `qemu-user-static` Docker'ın içinde. Bu testler geliştirme aşamasındadır ve test çalıştırmaları otomatik değildir.

Motivasyon:

Normalde tüm testleri ClickHouse yapısının tek bir varyantında serbest bırakırız ve çalıştırırız. Ancak, iyice test edilmeyen alternatif yapı varyantları vardır. Örnekler:

-   FreeBSD üzerine inşa;
-   sistem paketlerinden kütüphaneler ile Debian üzerine inşa;
-   kütüphanelerin paylaşılan bağlantısı ile oluşturun;
-   AArch64 platformunda oluşturun;
-   PowerPc platformunda oluşturun.

Örneğin, sistem paketleri ile oluştur kötü bir uygulamadır, çünkü bir sistemin hangi paketlerin tam sürümüne sahip olacağını garanti edemeyiz. Ancak bu gerçekten Debian bakıcılarına ihtiyaç duyuyor. Bu nedenle en azından bu yapı varyantını desteklemeliyiz. Başka bir örnek: paylaşılan bağlantı ortak bir sorun kaynağıdır, ancak bazı Meraklılar için gereklidir.

Tüm yapı varyantlarında tüm testleri çalıştıramasak da, en azından çeşitli yapı varyantlarının bozulmadığını kontrol etmek istiyoruz. Bu amaçla yapı testlerini kullanıyoruz.

## Protokol uyumluluğu testi {#testing-for-protocol-compatibility}

ClickHouse ağ protokolünü genişlettiğimizde, eski clickhouse istemcisinin yeni clickhouse sunucusu ile çalıştığını ve yeni clickhouse istemcisinin eski clickhouse sunucusu ile çalıştığını (sadece ilgili paketlerden ikili dosyaları çalıştırarak) manuel olarak test ediyoruz.

## Derleyiciden yardım {#help-from-the-compiler}

Ana ClickHouse kodu (bu `dbms` dizin) ile inşa edilmiştir `-Wall -Wextra -Werror` ve bazı ek etkin uyarılar ile. Bu seçenekler üçüncü taraf kitaplıkları için etkin olmasa da.

Clang daha yararlı uyarılar vardır-Sen ile onları arayabilirsiniz `-Weverything` ve varsayılan oluşturmak için bir şey seçin.

Üretim yapıları için gcc kullanılır (hala clang'dan biraz daha verimli kod üretir). Geliştirme için, clang genellikle kullanımı daha uygundur. Hata ayıklama modu ile kendi makinenizde inşa edebilirsiniz (dizüstü bilgisayarınızın pilinden tasarruf etmek için), ancak derleyicinin daha fazla uyarı üretebileceğini lütfen unutmayın `-O3` daha iyi kontrol akışı ve prosedürler arası analiz nedeniyle. Clang ile inşa ederken, `libc++` yerine kullanılır `libstdc++` ve hata ayıklama modu ile oluştururken, hata ayıklama sürümü `libc++` çalışma zamanında daha fazla hata yakalamak için izin verir kullanılır.

## Dezenfektanlar {#sanitizers}

**Adres dezenfektanı**.
Biz başına taahhüt bazında ASan altında fonksiyonel ve entegrasyon testleri çalıştırın.

**Valgrind (Memcheck)**.
Bir gecede valgrind altında fonksiyonel testler yapıyoruz. Birden fazla saat sürer. Şu anda bilinen bir yanlış pozitif var `re2` kütüphane, bkz [bu makale](https://research.swtch.com/sparse).

**Tanımsız davranış dezenfektanı.**
Biz başına taahhüt bazında ASan altında fonksiyonel ve entegrasyon testleri çalıştırın.

**İplik dezenfektanı**.
Biz başına taahhüt bazında tsan altında fonksiyonel testler çalıştırın. Tsan altında hala taahhüt bazında entegrasyon testleri yapmıyoruz.

**Bellek temizleyici**.
Şu anda hala MSan kullanmıyoruz.

**Hata ayıklama ayırıcısı.**
Hata ayıklama sürümü `jemalloc` hata ayıklama oluşturmak için kullanılır.

## Fuzzing {#fuzzing}

ClickHouse fuzzing hem kullanılarak uygulanmaktadır [libFuzzer](https://llvm.org/docs/LibFuzzer.html) ve rastgele SQL sorguları.
Tüm fuzz testleri sanitizers (Adres ve tanımsız) ile yapılmalıdır.

LibFuzzer kütüphane kodu izole fuzz testi için kullanılır. Fuzzers test kodunun bir parçası olarak uygulanır ve “\_fuzzer” adı postfixes.
Fuzzer örneği bulunabilir `src/Parsers/tests/lexer_fuzzer.cpp`. LibFuzzer özgü yapılandırmalar, sözlükler ve corpus saklanır `tests/fuzz`.
Kullanıcı girişini işleyen her işlevsellik için fuzz testleri yazmanızı öneririz.

Fuzzers varsayılan olarak oluşturulmaz. Hem fuzzers inşa etmek `-DENABLE_FUZZING=1` ve `-DENABLE_TESTS=1` seçenekler ayarlanmalıdır.
Fuzzers oluştururken Jemalloc'u devre dışı bırakmanızı öneririz. ClickHouse fuzzing'i entegre etmek için kullanılan yapılandırma
Google OSS-Fuzz bulunabilir `docker/fuzz`.

Ayrıca rastgele SQL sorguları oluşturmak ve sunucunun bunları çalıştırarak ölmediğini kontrol etmek için basit fuzz testi kullanıyoruz.
İçinde bulabilirsiniz `00746_sql_fuzzy.pl`. Bu test sürekli olarak (gece ve daha uzun) çalıştırılmalıdır.

## Güvenlik Denetimi {#security-audit}

Yandex Güvenlik ekibinden insanlar güvenlik açısından ClickHouse yetenekleri bazı temel bakış yapmak.

## Statik Analizörler {#static-analyzers}

Koş weuyoruz `PVS-Studio` taahhüt bazında. Değerlendir havedik `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`. Sen kullanım talimatları bulacaksınız `tests/instructions/` dizin. Ayrıca okuyabilirsiniz [Rusça makale](https://habr.com/company/yandex/blog/342018/).

Kullanıyorsanız `CLion` bir IDE olarak, bazı kaldıraç `clang-tidy` kutudan kontrol eder.

## Sertleşme {#hardening}

`FORTIFY_SOURCE` varsayılan olarak kullanılır. Neredeyse işe yaramaz, ancak nadir durumlarda hala mantıklı ve bunu devre dışı bırakmıyoruz.

## Kod Stili {#code-style}

Kod stili kuralları açıklanmıştır [burada](https://clickhouse.tech/docs/en/development/style/).

Bazı ortak stil ihlallerini kontrol etmek için şunları kullanabilirsiniz `utils/check-style` komut.

Kodunuzun uygun stilini zorlamak için şunları kullanabilirsiniz `clang-format`. Dosya `.clang-format` kaynak rootlarında yer almaktadır. Çoğunlukla gerçek kod stilimizle karşılık gelir. Ancak uygulanması tavsiye edilmez `clang-format` varolan dosyalara biçimlendirmeyi daha da kötüleştirdiği için. Kullanabilirsiniz `clang-format-diff` eğer clang kaynak deposunda bulabilirsiniz aracı.

Alternatif olarak deneyebilirsiniz `uncrustify` kodunuzu yeniden biçimlendirmek için bir araç. Yapılandırma içinde `uncrustify.cfg` kaynaklarda kök. Daha az test edilmiştir `clang-format`.

`CLion` kod stilimiz için ayarlanması gereken kendi kod biçimlendiricisine sahiptir.

## Metrica B2B testleri {#metrica-b2b-tests}

Her ClickHouse sürümü Yandex Metrica ve AppMetrica motorları ile test edilir. Clickhouse'un Test ve kararlı sürümleri Vm'lerde dağıtılır ve Giriş verilerinin sabit örneğini işleyen Metrica motorunun küçük bir kopyasıyla çalışır. Daha sonra Metrica motorunun iki örneğinin sonuçları birlikte karşılaştırılır.

Bu testler ayrı ekip tarafından otomatikleştirilir. Yüksek sayıda hareketli parça nedeniyle, testler çoğu zaman tamamen ilgisiz nedenlerle başarısız olur, bu da anlaşılması çok zordur. Büyük olasılıkla bu testlerin bizim için negatif değeri var. Bununla birlikte, bu testlerin yüzlerce kişiden yaklaşık bir veya iki kez yararlı olduğu kanıtlanmıştır.

## Test Kapsamı {#test-coverage}

Temmuz 2018 itibariyle test kapsamını takip etmiyoruz.

## Test Otomasyonu {#test-automation}

Yandex dahili CI ve iş otomasyon sistemi ile testler yapıyoruz “Sandbox”.

Yapı işleri ve testler, taahhüt bazında sanal alanda çalıştırılır. Ortaya çıkan paketler ve test sonuçları Github'da yayınlanır ve doğrudan bağlantılar tarafından indirilebilir. Eserler sonsuza dek saklanır. Eğer GitHub bir çekme isteği gönderdiğinizde, biz olarak etiketlemek “can be tested” ve bizim CI sistemi sizin için ClickHouse paketleri (yayın, hata ayıklama, Adres dezenfektanı ile, vb) inşa edecek.

Travis CI, zaman ve hesaplama gücü sınırı nedeniyle kullanmıyoruz.
Jenkins'i kullanmayız. Daha önce kullanıldı ve şimdi Jenkins kullanmadığımız için mutluyuz.

[Orijinal makale](https://clickhouse.tech/docs/en/development/tests/) <!--hide-->
