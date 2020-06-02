---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "ClickHouse mimarisine genel bak\u0131\u015F"
---

# ClickHouse mimarisine genel bakış {#overview-of-clickhouse-architecture}

ClickHouse gerçek bir sütun yönelimli DBMS olduğunu. Veriler sütunlar tarafından ve dizilerin yürütülmesi sırasında (vektörler veya sütun parçaları) saklanır. Mümkün olduğunda, işlemler tek tek değerler yerine dizilere gönderilir. Buna denir “vectorized query execution,” ve gerçek veri işleme maliyetini düşürmeye yardımcı olur.

> Bu fikir yeni bir şey değil. Bu kadar uzanır `APL` programlama dili ve Tor andunları: `A +`, `J`, `K`, ve `Q`. Dizi programlama bilimsel veri işlemede kullanılır. Bu fikir ilişkisel veritabanlarında yeni bir şey değildir: örneğin, `Vectorwise` sistem.

Sorgu işlemeyi hızlandırmak için iki farklı yaklaşım vardır: vektörize sorgu yürütme ve çalışma zamanı kodu oluşturma. İkincisi, tüm Yönlendirme ve dinamik gönderimi kaldırır. Bu yaklaşımların hiçbiri diğerinden kesinlikle daha iyi değildir. Çalışma zamanı kodu üretimi, birçok işlemi birleştirdiğinde daha iyi olabilir, böylece CPU yürütme birimlerini ve boru hattını tam olarak kullanır. Vectorized sorgu yürütme daha az pratik olabilir, çünkü önbelleğe yazılması ve geri okunması gereken geçici vektörler içerir. Geçici veri L2 önbelleğinde uymuyorsa, bu bir sorun haline gelir. Ancak vektörize sorgu yürütme, CPU'nun SIMD yeteneklerini daha kolay kullanır. Bir [araştırma öd paperevi](http://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf) arkadaşlarımız tarafından yazıldı, her iki yaklaşımı birleştirmenin daha iyi olduğunu gösteriyor. ClickHouse vectorized sorgu yürütme kullanır ve çalışma zamanı kodu üretimi için başlangıç desteği sınırlıdır.

## Sütun {#columns}

`IColumn` arabirim, bellekteki sütunları temsil etmek için kullanılır (aslında, sütunların parçaları). Bu arayüz, çeşitli ilişkisel operatörlerin uygulanması için yardımcı yöntemler sağlar. Hemen hemen tüm işlemler değişmez: orijinal sütunu değiştirmezler, ancak yeni bir değiştirilmiş bir tane oluştururlar. Örneğin, `IColumn :: filter` yöntem bir filtre bayt maskesi kabul eder. Bu için kullanılır `WHERE` ve `HAVING` ilişkisel operatörler. Ek örnekler: `IColumn :: permute` desteklemek için yöntem `ORDER BY`, bu `IColumn :: cut` desteklemek için yöntem `LIMIT`.

Çeşitli `IColumn` uygulanışlar (`ColumnUInt8`, `ColumnString` ve benzeri) sütunların bellek düzeninden sorumludur. Bellek düzeni genellikle bitişik bir dizidir. Sütunların tamsayı türü için, sadece bir bitişik dizidir, örneğin `std :: vector`. İçin `String` ve `Array` sütunlar, iki vektördür: biri bitişik olarak yerleştirilmiş tüm dizi elemanları için ve her dizinin başlangıcına ait ofsetler için ikinci bir tane. Ayrıca var `ColumnConst` bu bellekte sadece bir değer depolar, ancak bir sütuna benziyor.

## Alan {#field}

Bununla birlikte, bireysel değerlerle de çalışmak mümkündür. Bireysel bir değeri temsil etmek için, `Field` kullanılır. `Field` sadece ayrımcılığa uğramış bir birlik mi `UInt64`, `Int64`, `Float64`, `String` ve `Array`. `IColumn` has the `operator[]` n - inci değerini bir olarak alma yöntemi `Field` ve... `insert` bir ekleme yöntemi `Field` bir sütunun sonuna. Bu yöntemler çok verimli değildir, çünkü geçici olarak uğraşmayı gerektirirler `Field` tek bir değeri temsil eden nesneler. Daha etkili yöntemleri vardır, mesela: `insertFrom`, `insertRangeFrom` ve bu yüzden.

`Field` bir tablo için belirli bir veri türü hakkında yeterli bilgiye sahip değildir. Mesela, `UInt8`, `UInt16`, `UInt32`, ve `UInt64` hepsi olarak temsil edilir `UInt64` in a `Field`.

## Sızdıran Soyutlamalar {#leaky-abstractions}

`IColumn` verilerin ortak ilişkisel dönüşümleri için yöntemler vardır, ancak tüm ihtiyaçları karşılamazlar. Mesela, `ColumnUInt64` iki sütunun toplamını hesaplamak için bir yöntem yoktur ve `ColumnString` bir alt dize araması çalıştırmak için bir yöntem yok. Bu sayısız rutinleri dışında uygulanmaktadır `IColumn`.

Sütunlar üzerinde çeşitli işlevler kullanarak genel, verimli olmayan bir şekilde uygulanabilir `IColumn` çıkarma yöntemleri `Field` değerleri veya belirli bir veri iç bellek düzeni bilgisini kullanarak özel bir şekilde `IColumn` uygulanış. Döküm fonksiyonları ile belirli bir `IColumn` yazın ve doğrudan iç temsil ile anlaşma. Mesela, `ColumnUInt64` has the `getData` bir iç diziye başvuru döndüren yöntem, daha sonra ayrı bir yordam okur veya bu diziyi doğrudan doldurur. Sahibiz “leaky abstractions” çeşitli rutinlerin verimli uzmanlıklarına izin vermek.

## Veri Türleri {#data_types}

`IDataType` seri hale getirme ve serileştirmeden sorumludur: ikili veya metin biçiminde sütunların veya bireysel değerlerin parçalarını okumak ve yazmak için. `IDataType` tablolardaki veri türlerine doğrudan karşılık gelir. Örneğin, `DataTypeUInt32`, `DataTypeDateTime`, `DataTypeString` ve böyle devam eder.

`IDataType` ve `IColumn` sadece gevşek birbirleriyle ilişkilidir. Farklı veri türleri bellekte aynı tarafından temsil edilebilir `IColumn` uygulanışlar. Mesela, `DataTypeUInt32` ve `DataTypeDateTime` her ikisi de tarafından temsil edilir `ColumnUInt32` veya `ColumnConstUInt32`. Buna ek olarak, aynı veri türü farklı tarafından temsil edilebilir `IColumn` uygulanışlar. Mesela, `DataTypeUInt8` tarafından temsil edilebilir `ColumnUInt8` veya `ColumnConstUInt8`.

`IDataType` yalnızca meta verileri depolar. Mesela, `DataTypeUInt8` hiçbir şey saklamıyor (vptr hariç) ve `DataTypeFixedString` mağazalar sadece `N` (sabit boyutlu dizelerin boyutu).

`IDataType` çeşitli veri formatları için yardımcı yöntemlere sahiptir. Örnekler, Olası Alıntı ile bir değeri serileştirmek, json için bir değeri serileştirmek ve XML formatının bir parçası olarak bir değeri serileştirmek için kullanılan yöntemlerdir. Veri formatlarına doğrudan yazışma yoktur. Örneğin, farklı veri biçimleri `Pretty` ve `TabSeparated` aynı kullanabilirsiniz `serializeTextEscaped` hel methodper yöntemi `IDataType` Arabirim.

## Engel {#block}

A `Block` bellekteki bir tablonun bir alt kümesini (yığın) temsil eden bir kapsayıcıdır. Bu sadece üçlü bir dizi: `(IColumn, IDataType, column name)`. Sorgu yürütme sırasında veri tarafından işlenir `Block`s. Eğer bir `Block`(bu yaptığımız verileri `IColumn` nesne), biz onun türü hakkında bilgi var (içinde `IDataType`) bu bize bu sütunla nasıl başa çıkacağımızı söyler ve sütun adına sahibiz. Tablodan orijinal sütun adı veya hesaplamaların geçici sonuçlarını almak için atanan bazı yapay ad olabilir.

Bir bloktaki sütunlar üzerinde bazı işlevleri hesapladığımızda, bloğa sonucu olan başka bir sütun ekleriz ve işlemler değişmez olduğu için işlevin argümanları için sütunlara dokunmayız. Daha sonra, gereksiz sütunlar bloktan kaldırılabilir, ancak değiştirilemez. Ortak alt ifadelerin ortadan kaldırılması için uygundur.

İşlenen her veri yığını için bloklar oluşturulur. Aynı hesaplama türü için, sütun adları ve türleri farklı bloklar için aynı kalır ve yalnızca sütun verileri değişir unutmayın. Küçük blok boyutları shared\_ptrs ve sütun adlarını kopyalamak için geçici dizeleri yüksek bir ek yükü olduğundan blok üstbilgisinden blok verileri bölmek daha iyidir.

## Blok Akışları {#block-streams}

Blok akışları veri işleme içindir. Bir yerden veri okumak, veri dönüşümleri gerçekleştirmek veya bir yere veri yazmak için blok akışları kullanıyoruz. `IBlockInputStream` has the `read` mevcut iken bir sonraki bloğu getirme yöntemi. `IBlockOutputStream` has the `write` bloğu bir yere itmek için yöntem.

Akar responsibles areular sorumludur:

1.  Bir tabloya okuma veya yazma. Tablo sadece okuma veya yazma blokları için bir akış döndürür.
2.  Veri formatlarının uygulanması. Örneğin, bir terminale veri çıkışı yapmak istiyorsanız `Pretty` biçim, blokları ittiğiniz bir blok çıkış akışı oluşturursunuz ve bunları biçimlendirir.
3.  Veri dönüşümleri gerçekleştirme. Diyelim ki var `IBlockInputStream` ve filtrelenmiş bir akış oluşturmak istiyorum. Yarat createtığınız `FilterBlockInputStream` ve akışı ile başlatın. Sonra bir blok çektiğinizde `FilterBlockInputStream`, akışınızdan bir blok çeker, filtreler ve filtrelenmiş bloğu size döndürür. Sorgu yürütme boru hatları bu şekilde temsil edilir.

Daha sofistike dönüşümler var. Örneğin, çektiğiniz zaman `AggregatingBlockInputStream`, kaynağındaki tüm verileri okur, toplar ve sizin için toplanmış bir veri akışı döndürür. Başka bir örnek: `UnionBlockInputStream` yapıcıdaki birçok giriş kaynağını ve ayrıca bir dizi iş parçacığını kabul eder. Birden çok iş parçacığı başlatır ve paralel olarak birden fazla kaynaktan okur.

> Blok akışları “pull” akışı kontrol etme yaklaşımı: ilk akıştan bir blok çektiğinizde, gerekli blokları iç içe geçmiş akışlardan çeker ve tüm yürütme boru hattı çalışır. Ne “pull” ne “push” en iyi çözümdür, çünkü kontrol akışı örtükdür ve bu, birden fazla sorgunun eşzamanlı yürütülmesi (birçok boru hattının birlikte birleştirilmesi) gibi çeşitli özelliklerin uygulanmasını sınırlar. Bu sınırlama, coroutines ile veya sadece birbirlerini bekleyen ekstra iş parçacıkları çalıştırarak aşılabilir. Kontrol akışını açık hale getirirsek daha fazla olasılığa sahip olabiliriz: verileri bir hesaplama biriminden diğerine bu hesaplama birimlerinin dışında geçirme mantığını bulursak. Re thisad this [makale](http://journal.stuffwithstuff.com/2013/01/13/iteration-inside-and-out/) daha fazla düşünce için.

Sorgu yürütme boru hattının her adımda geçici veri oluşturduğuna dikkat etmeliyiz. Blok boyutunu yeterince küçük tutmaya çalışıyoruz, böylece geçici veriler CPU önbelleğine sığıyor. Bu varsayımla, geçici veri yazmak ve okumak, diğer hesaplamalarla karşılaştırıldığında neredeyse ücretsizdir. Boru hattındaki birçok operasyonu bir araya getirmek için bir alternatif düşünebiliriz. Boru hattını mümkün olduğunca kısa hale getirebilir ve geçici verilerin çoğunu kaldırabilir, bu da bir avantaj olabilir, ancak dezavantajları da vardır. Örneğin, bölünmüş bir boru hattı, Ara verileri önbelleğe almayı, aynı anda çalışan benzer sorgulardan Ara verileri çalmayı ve benzer sorgular için boru hatlarını birleştirmeyi kolaylaştırır.

## Biçimliler {#formats}

Veri formatları blok akışları ile uygulanır. Var “presentational” sadece müşteriye veri çıkışı için uygun biçimler, örneğin `Pretty` sadece sağlayan biçim `IBlockOutputStream`. Ve gibi giriş / çıkış biçimleri vardır `TabSeparated` veya `JSONEachRow`.

Satır akışları da vardır: `IRowInputStream` ve `IRowOutputStream`. Verileri bloklarla değil, tek tek satırlarla çekmenize/itmenize izin verirler. Ve sadece satır yönelimli formatların uygulanmasını basitleştirmek için gereklidir. Sarıcı `BlockInputStreamFromRowInputStream` ve `BlockOutputStreamFromRowOutputStream` satır yönelimli akışları normal blok yönelimli akışlara dönüştürmenize izin verin.

## I/O {#io}

Bayt yönelimli giriş / çıkış için, `ReadBuffer` ve `WriteBuffer` soyut sınıflar. C++yerine kullanılırlar `iostream`s. merak etmeyin: her olgun C++ projesi başka bir şey kullanıyor `iostream`s iyi nedenlerden dolayı.

`ReadBuffer` ve `WriteBuffer` sadece bitişik bir tampon ve bu tampondaki konuma işaret eden bir imleç. Uygulamalar, arabellek belleğine sahip olabilir veya sahip olmayabilir. Arabelleği aşağıdaki verilerle doldurmak için sanal bir yöntem vardır (for `ReadBuffer`) veya tamponu bir yere yıkamak için (için `WriteBuffer`). Sanal yöntemler nadiren denir.

Uygulamaları `ReadBuffer`/`WriteBuffer` sıkıştırma uygulamak için dosyalar ve dosya tanımlayıcıları ve ağ soketleri ile çalışmak için kullanılır (`CompressedWriteBuffer` is initialized with another WriteBuffer and performs compression before writing data to it), and for other purposes – the names `ConcatReadBuffer`, `LimitReadBuffer`, ve `HashingWriteBuffer` kendileri için konuşuyoruz.

Read / WriteBuffers sadece baytlarla ilgilenir. Fonksiyonları vardır `ReadHelpers` ve `WriteHelpers` başlık dosyaları biçimlendirme giriş/çıkış ile yardımcı olmak için. Örneğin, ondalık biçimde bir sayı yazmak için yardımcılar vardır.

Bir sonuç kümesi yazmak istediğinizde neler olduğuna bakalım `JSON` stdout için biçimlendirin. Eğer bir sonuç getirilecek hazır set var `IBlockInputStream`. Yarat createtığınız `WriteBufferFromFileDescriptor(STDOUT_FILENO)` STDOUT için bayt yazmak için. Yarat createtığınız `JSONRowOutputStream` bununla başlatıldı `WriteBuffer`, satır yazmak için `JSON` stdout. Yarat createtığınız `BlockOutputStreamFromRowOutputStream` bu da yetmiyormuş gibi göstermek için `IBlockOutputStream`. Sonra Ara `copyData` veri aktarmak için `IBlockInputStream` -e doğru `IBlockOutputStream` ve her şey çalışıyor. İçten, `JSONRowOutputStream` çeşitli json sınırlayıcıları yazacak ve `IDataType::serializeTextJSON` bir referans ile yöntem `IColumn` ve satır numarası argüman olarak. Sonuç olarak, `IDataType::serializeTextJSON` bir yöntem çağırır `WriteHelpers.h`: mesela, `writeText` sayısal türler ve `writeJSONString` için `DataTypeString`.

## Tablolar {#tables}

Bu `IStorage` arayüz tabloları temsil eder. Bu arayüzün farklı uygulamaları farklı tablo motorlarıdır. Örnekler şunlardır `StorageMergeTree`, `StorageMemory` ve bu yüzden. Bu sınıfların örnekleri sadece tablolardır.

Anahtar `IStorage` yöntemler şunlardır `read` ve `write`. Ayrıca vardır `alter`, `rename`, `drop` ve bu yüzden. Bu `read` yöntem aşağıdaki bağımsız değişkenleri kabul eder: bir tablodan okunacak sütun kümesi, `AST` dikkate alınması gereken sorgu ve döndürülmesi gereken akış sayısı. Bir veya birden fazla döndürür `IBlockInputStream` nesneler ve sorgu yürütme sırasında bir tablo altyapısı içinde tamamlanan veri işleme aşaması hakkında bilgi.

Çoğu durumda, read yöntemi yalnızca belirtilen sütunları bir tablodan okumaktan sorumludur, daha fazla veri işleme için değil. Tüm diğer veri işleme sorgu yorumlayıcısı tarafından yapılır ve sorumluluk dışında `IStorage`.

Ancak önemli istisnalar var:

-   AST sorgusu için geçirilir `read` yöntemi ve tablo altyapısı dizin kullanımını türetmek ve bir tablodan daha az veri okumak için kullanabilirsiniz.
-   Bazen tablo motoru verileri belirli bir aşamaya kadar işleyebilir. Mesela, `StorageDistributed` uzak sunuculara sorgu gönderebilir, farklı uzak sunuculardan gelen verilerin birleştirilebileceği bir aşamaya veri işlemelerini isteyebilir ve bu önceden işlenmiş verileri döndürebilir. Sorgu yorumlayıcısı daha sonra verileri işlemeyi tamamlar.

Tablo `read` yöntem birden çok döndürebilir `IBlockInputStream` nesneleri paralel veri işleme izin vermek için. Bu çoklu blok giriş akışları bir tablodan paralel olarak okuyabilir. Ardından, bu akışları bağımsız olarak hesaplanabilen çeşitli dönüşümlerle (ifade değerlendirme veya filtreleme gibi) sarabilir ve bir `UnionBlockInputStream` bunların üzerine, paralel olarak birden fazla akıştan okumak için.

Ayrıca vardır `TableFunction`s. bunlar geçici olarak dönen işlevlerdir `IStorage` içinde kullanılacak nesne `FROM` bir sorgu yan tümcesi.

Tablo motorunuzu nasıl uygulayacağınıza dair hızlı bir fikir edinmek için, basit bir şeye bakın `StorageMemory` veya `StorageTinyLog`.

> Bu sonucu `read` yöntem, `IStorage` dönüşler `QueryProcessingStage` – information about what parts of the query were already calculated inside storage.

## Ayrıştırıcılar {#parsers}

Elle yazılmış özyinelemeli iniş ayrıştırıcı bir sorgu ayrıştırır. Mesela, `ParserSelectQuery` sorgunun çeşitli bölümleri için temel ayrıştırıcıları yinelemeli olarak çağırır. Ayrıştırıcılar bir `AST`. Bu `AST` örnekleri olan düğüm bylerle temsil edilir `IAST`.

> Ayrıştırıcı jeneratörler tarihsel nedenlerle kullanılmaz.

## Tercümanlar {#interpreters}

Sorgu yürütme kanalının oluşturulmasından tercümanlar sorumludur. `AST`. Gibi basit tercümanlar vardır `InterpreterExistsQuery` ve `InterpreterDropQuery` veya daha sofistike `InterpreterSelectQuery`. Sorgu yürütme boru hattı, blok giriş veya çıkış akışlarının birleşimidir. Örneğin, yorumlama sonucu `SELECT` sorgu olduğunu `IBlockInputStream` sonuç kümesini okumak için; INSERT sorgusunun sonucu `IBlockOutputStream` ekleme için veri yazmak ve yorumlama sonucu `INSERT SELECT` sorgu olduğunu `IBlockInputStream` bu, ilk okumada boş bir sonuç kümesi döndürür, ancak verileri kopyalar `SELECT` -e doğru `INSERT` aynı zamanda.

`InterpreterSelectQuery` kullanma `ExpressionAnalyzer` ve `ExpressionActions` sorgu analizi ve dönüşümler için makine. Bu, kural tabanlı sorgu iyileştirmelerinin çoğunun yapıldığı yerdir. `ExpressionAnalyzer` oldukça dağınık ve yeniden yazılmalıdır: modüler dönüşümlere veya sorguya izin vermek için ayrı sınıflara çeşitli sorgu dönüşümleri ve optimizasyonlar çıkarılmalıdır.

## İşlevler {#functions}

Sıradan fonksiyonlar ve toplam fonksiyonlar vardır. Toplama işlevleri için bir sonraki bölüme bakın.

Ordinary functions don't change the number of rows – they work as if they are processing each row independently. In fact, functions are not called for individual rows, but for `Block`'s vectorized sorgu yürütme uygulamak için veri.

Gibi bazı çeşitli fonksiyonlar vardır [blockSize](../sql-reference/functions/other-functions.md#function-blocksize), [rowNumberİnBlock](../sql-reference/functions/other-functions.md#function-rownumberinblock), ve [runningAccumulate](../sql-reference/functions/other-functions.md#function-runningaccumulate), blok işlemeyi istismar eden ve satırların bağımsızlığını ihlal eden.

Clickhouse'un güçlü yazımı var, bu yüzden örtük tür dönüşümü yok. Bir işlev belirli bir tür kombinasyonunu desteklemiyorsa, bir istisna atar. Ancak, birçok farklı tür kombinasyonu için işlevler çalışabilir (aşırı yüklenebilir). Örneğin, `plus` fonksiyonu (uygulamak için `+` operatör) sayısal türlerin herhangi bir kombinasyonu için çalışır: `UInt8` + `Float32`, `UInt16` + `Int8` ve bu yüzden. Ayrıca, bazı variadic işlevleri gibi bağımsız değişkenlerin herhangi bir sayıda kabul edebilir `concat` İşlev.

Bir işlev açıkça desteklenen veri türlerini gönderir ve desteklenen çünkü bir işlev uygulamak biraz rahatsız edici olabilir `IColumns`. Örneğin, `plus` işlev, sayısal türlerin ve sabit veya sabit olmayan sol ve sağ bağımsız değişkenlerin her birleşimi için bir C++ şablonunun örneklendirilmesiyle oluşturulan koda sahiptir.

Bu şablon kodu kabartmak önlemek için çalışma zamanı kodu nesil uygulamak için mükemmel bir yerdir. Ayrıca, kaynaşmış çarpma-ekleme gibi kaynaşmış işlevler eklemeyi veya bir döngü yinelemesinde birden fazla karşılaştırma yapmayı mümkün kılar.

Vektörize sorgu yürütme nedeniyle, işlevler kısa devre değildir. Örneğin, yazarsanız `WHERE f(x) AND g(y)`, her iki taraf da satırlar için bile hesaplanır `f(x)` sıfırdır (hariç `f(x)` sıfır sabit bir ifadedir). Ama eğer seçicilik `f(x)` durum yüksek ve hesaplama `f(x)` çok daha ucuzdur `g(y)`, çok geçişli hesaplama uygulamak daha iyidir. İlk önce hesaplayacaktı `f(x)`, daha sonra sonucu sütunları süzün ve sonra hesaplayın `g(y)` sadece daha küçük, filtrelenmiş veri parçaları için.

## Toplama Fonksiyonları {#aggregate-functions}

Toplama işlevleri durumsal işlevlerdir. Geçirilen değerleri bir duruma biriktirir ve bu durumdan sonuç almanıza izin verir. İle Yönet theyil theirler. `IAggregateFunction` Arabirim. Devletler oldukça basit olabilir (devlet `AggregateFunctionCount` sadece bir tek `UInt64` değeri) veya oldukça karmaşık (devlet `AggregateFunctionUniqCombined` doğrusal bir dizi, bir karma tablo ve bir kombinasyonudur `HyperLogLog` olasılıksal veri yapısı).

Devletler tahsis edilir `Arena` (bir bellek havuzu) yüksek önemlilik yürütürken birden çok durumla başa çıkmak için `GROUP BY` sorgu. Devletler önemsiz olmayan bir yapıcı ve yıkıcı olabilir: örneğin, karmaşık toplama durumları ek belleği kendileri tahsis edebilir. Devletlerin yaratılmasına ve yok edilmesine ve mülkiyet ve yıkım düzeninin doğru bir şekilde geçmesine biraz dikkat gerektirir.

Toplama durumları serileştirilmiş ve dağıtılmış sorgu yürütme sırasında ağ üzerinden geçmek veya bunları yeterli RAM olmadığı diskte yazmak için serileştirilmiş. Hatta bir tablo ile saklanabilir `DataTypeAggregateFunction` verilerin artımlı toplanmasına izin vermek için.

> Toplu işlev durumları için seri hale getirilmiş veri biçimi şu anda sürümlendirilmemiş. Toplama durumları yalnızca geçici olarak saklanırsa sorun olmaz. Ama biz var `AggregatingMergeTree` artan toplama için tablo motoru ve insanlar zaten üretimde kullanıyor. Gelecekte herhangi bir toplama işlevi için seri hale getirilmiş biçimi değiştirirken geriye dönük uyumluluğun gerekli olmasının nedeni budur.

## Hizmetçi {#server}

Sunucu birkaç farklı arayüz uygular:

-   Herhangi bir yabancı istemciler için bir HTTP arabirimi.
-   Dağıtılmış sorgu yürütme sırasında yerel ClickHouse istemcisi ve sunucular arası iletişim için bir TCP arabirimi.
-   Çoğaltma için veri aktarımı için bir arabirim.

DAHİLİ olarak, coroutines veya lifler olmadan sadece ilkel bir çok iş parçacıklı sunucudur. Sunucu, yüksek oranda basit sorguları işlemek için değil, nispeten düşük oranda karmaşık sorguları işlemek için tasarlandığından, her biri analitik için çok miktarda veri işleyebilir.

Sunucu başlatır `Context` sorgu yürütme için gerekli ortama sahip sınıf: kullanılabilir veritabanlarının, kullanıcıların ve erişim haklarının, ayarların, kümelerin, işlem listesinin, sorgu günlüğünün vb. listesi. Tercümanlar bu ortamı kullanır.

Sunucu TCP protokolü için tam geriye ve ileriye dönük uyumluluk sağlıyoruz: eski istemciler yeni sunucularla konuşabilir ve yeni istemciler eski sunucularla konuşabilir. Ancak sonsuza dek korumak istemiyoruz ve yaklaşık bir yıl sonra eski sürümler için destek kaldırıyoruz.

!!! note "Not"
    Çoğu harici uygulama için, HTTP arayüzünü kullanmanızı öneririz, çünkü basit ve kullanımı kolaydır. TCP protokolü, iç veri yapılarına daha sıkı bir şekilde bağlanır: veri bloklarını geçirmek için bir iç biçim kullanır ve sıkıştırılmış veriler için özel çerçeveleme kullanır. Bu protokol için bir C kütüphanesi yayınlamadık çünkü pratik olmayan ClickHouse kod tabanının çoğunu bağlamayı gerektiriyor.

## Dağıtılmış Sorgu Yürütme {#distributed-query-execution}

Bir küme kurulumundaki sunucular çoğunlukla bağımsızdır. Sen-ebilmek yaratmak a `Distributed` bir kümedeki bir veya tüm sunucularda tablo. Bu `Distributed` table does not store data itself – it only provides a “view” Bir kümenin birden çok düğümündeki tüm yerel tablolara. Bir seçtiğinizde `Distributed` tablo, bu sorguyu yeniden yazar, Yük Dengeleme ayarlarına göre uzak düğümleri seçer ve sorguyu onlara gönderir. Bu `Distributed` tablo, farklı sunuculardan gelen Ara sonuçların birleştirilebileceği bir aşamaya kadar bir sorguyu işlemek için uzak sunuculardan ister. Sonra Ara sonuçları alır ve onları birleştirir. Dağıtılmış tablo, uzak sunuculara mümkün olduğunca fazla çalışma dağıtmaya çalışır ve ağ üzerinden çok fazla ara veri göndermez.

In veya JOIN yan tümcelerinde alt sorgular olduğunda işler daha karmaşık hale gelir ve her biri bir `Distributed` Tablo. Bu sorguların yürütülmesi için farklı stratejilerimiz var.

Dağıtılmış sorgu yürütme için genel bir sorgu planı yoktur. Her düğüm, işin kendi kısmı için yerel sorgu planına sahiptir. Biz sadece basit tek geçişli dağıtılmış sorgu yürütme var: biz uzak düğümler için sorgular göndermek ve sonra sonuçları birleştirmek. Ancak bu, yüksek önemlilik grubu BYs ile veya JOIN için büyük miktarda geçici veri içeren karmaşık sorgular için mümkün değildir. Bu gibi durumlarda, gerek “reshuffle” ek koordinasyon gerektiren sunucular arasındaki veriler. ClickHouse bu tür bir sorgu yürütülmesini desteklemiyor ve üzerinde çalışmamız gerekiyor.

## Ağaç Birleştirme {#merge-tree}

`MergeTree` birincil anahtarla dizin oluşturmayı destekleyen bir depolama altyapısı ailesidir. Birincil anahtar, isteğe bağlı bir sütun veya ifade kümesi olabilir. Veri `MergeTree` tablo saklanır “parts”. Her bölüm verileri birincil anahtar sırasına göre saklar, böylece veriler birincil anahtar tuple tarafından lexicographically sıralanır. Tüm tablo sütunları ayrı olarak saklanır `column.bin` bu kısımlardaki dosyalar. Dosyalar sıkıştırılmış bloklardan oluşur. Her blok, ortalama değer boyutuna bağlı olarak genellikle 64 KB ila 1 MB sıkıştırılmamış veridir. Bloklar, birbiri ardına bitişik olarak yerleştirilmiş sütun değerlerinden oluşur. Sütun değerleri her sütun için aynı sıradadır (birincil anahtar siparişi tanımlar), bu nedenle birçok sütun tarafından yineleme yaptığınızda, karşılık gelen satırlar için değerler alırsınız.

Birincil anahtarın kendisi “sparse”. Her satır Adres yok ama verilerin sadece biraz değişir. Ayıran `primary.idx` dosya, n'nin çağrıldığı her N-inci satır için birincil anahtarın değerine sahiptir `index_granularity` (genellikle, n = 8192). Ayrıca, her sütun için, biz var `column.mrk` dosyaları ile “marks,” veri dosyasındaki her N-inci satıra ofset olan. Her işaret bir çifttir: dosyadaki ofset sıkıştırılmış bloğun başlangıcına ve sıkıştırılmış bloktaki ofset verilerin başlangıcına. Genellikle, sıkıştırılmış bloklar işaretlerle hizalanır ve sıkıştırılmış bloktaki ofset sıfırdır. İçin veri `primary.idx` her zaman bellekte bulunur ve veri `column.mrk` dosyalar önbelleğe alınır.

Bir kısm aından bir şey okuy readacağımız zaman `MergeTree` bak biz `primary.idx` veri ve istenen verileri içerebilecek aralıkları bulun, ardından `column.mrk` veri ve bu aralıkları okumaya başlamak için nerede için uzaklıklar hesaplayın. Çünkü seyrek, fazla veri okunabilir. ClickHouse, basit nokta sorgularının yüksek bir yükü için uygun değildir, çünkü tüm Aralık `index_granularity` her anahtar için satırlar okunmalı ve her sütun için sıkıştırılmış bloğun tamamı sıkıştırılmalıdır. Dizin için fark edilebilir bellek tüketimi olmadan tek bir sunucu başına trilyonlarca satır tutabilmemiz gerektiğinden dizini seyrek yaptık. Ayrıca, birincil anahtar seyrek olduğundan, benzersiz değildir: ekleme zamanında tablodaki anahtarın varlığını denetleyemez. Bir tabloda aynı anahtara sahip birçok satır olabilir.

Ne zaman sen `INSERT` içine veri bir demet `MergeTree`, bu grup birincil anahtar sırasına göre sıralanır ve yeni bir bölüm oluşturur. Bazı parçaları periyodik olarak seçen ve parça sayısını nispeten düşük tutmak için bunları tek bir sıralanmış parçaya birleştiren arka plan iş parçacıkları vardır. Bu yüzden denir `MergeTree`. Tabii ki, birleştirme yol açar “write amplification”. Tüm parçalar değişmez: sadece oluşturulur ve silinir, ancak değiştirilmez. SELECT yürütüldüğünde, tablonun bir anlık görüntüsünü (bir parça kümesi) tutar. Birleştirildikten sonra, arızadan sonra iyileşmeyi kolaylaştırmak için eski parçaları bir süre tutuyoruz, bu nedenle birleştirilmiş bir parçanın muhtemelen kırıldığını görürsek, kaynak parçalarıyla değiştirebiliriz.

`MergeTree` içermediği için bir lsm ağacı değildir “memtable” ve “log”: inserted data is written directly to the filesystem. This makes it suitable only to INSERT data in batches, not by individual row and not very frequently – about once per second is ok, but a thousand times a second is not. We did it this way for simplicity's sake, and because we are already inserting data in batches in our applications.

> MergeTree tabloları yalnızca bir (birincil) dizine sahip olabilir: herhangi bir ikincil dizin yoktur. Bir mantıksal tablo altında birden fazla fiziksel gösterime izin vermek, örneğin verileri birden fazla fiziksel sırayla depolamak veya hatta orijinal verilerle birlikte önceden toplanmış verilerle gösterimlere izin vermek güzel olurdu.

Arka plan birleştirmeleri sırasında ek iş yapan MergeTree motorları vardır. Örnekler şunlardır `CollapsingMergeTree` ve `AggregatingMergeTree`. Bu, güncellemeler için özel destek olarak kabul edilebilir. Kullanıcıların genellikle arka plan birleştirmeleri yürütüldüğünde zaman üzerinde hiçbir kontrole sahip çünkü bu gerçek güncellemeler olmadığını unutmayın, ve bir veri `MergeTree` tablo hemen hemen her zaman tamamen birleştirilmiş formda değil, birden fazla bölümde saklanır.

## Çoğalma {#replication}

Clickhouse çoğaltma başına tablo bazında yapılandırılabilir. Aynı sunucuda bazı çoğaltılmış ve bazı çoğaltılmamış tablolar olabilir. Ayrıca, iki faktörlü çoğaltmaya sahip bir tablo ve üç faktörlü bir tablo gibi farklı şekillerde çoğaltılmış tablolar da olabilir.

Çoğaltma uygulanır `ReplicatedMergeTree` depolama motoru. The path in `ZooKeeper` depolama altyapısı için bir parametre olarak belirtilir. Aynı yolu olan tüm tablolar `ZooKeeper` birbirlerinin kopyaları haline gelir: verilerini senkronize eder ve tutarlılığı korurlar. Yinelemeler, bir tablo oluşturarak veya bırakarak dinamik olarak eklenebilir ve kaldırılabilir.

Çoğaltma, zaman uyumsuz bir çoklu ana düzeni kullanır. Bir oturum olan herhangi bir yinelemeye veri ekleyebilirsiniz. `ZooKeeper` ve veriler diğer tüm yinelemelere zaman uyumsuz olarak çoğaltılır. ClickHouse güncelleştirmeleri desteklemediğinden, çoğaltma çakışmaz. Eklerin çekirdek onayı olmadığından, bir düğüm başarısız olursa, yalnızca eklenen veriler kaybolabilir.

Çoğaltma için meta veri zookeeper saklanır. Hangi eylemlerin yapılacağını listeleyen bir çoğaltma günlüğü vardır. Eylemler şunlardır: parça al; parçaları Birleştir; bir bölüm bırak vb. Her çoğaltma, çoğaltma günlüğünü kendi kuyruğuna kopyalar ve sonra da sıradaki eylemleri yürütür. Örneğin, ekleme, “get the part” eylem günlüğüne oluşturulur ve her çoğaltma bu bölümü indirir. Birleştirmeler, baytla aynı sonuçları elde etmek için yinelemeler arasında koordine edilir. Tüm parçalar tüm kopyalarda aynı şekilde birleştirilir. Bir kopyayı lider olarak seçerek elde edilir ve bu çoğaltma birleştirir ve yazar “merge parts” günlük eylemler.

Çoğaltma fiziksel: yalnızca sıkıştırılmış parçalar sorgular değil düğümler arasında aktarılır. Birleştirmeler, çoğu durumda ağ amplifikasyonundan kaçınarak ağ maliyetlerini düşürmek için her yinelemede bağımsız olarak işlenir. Büyük birleştirilmiş parçalar, yalnızca önemli çoğaltma gecikmesi durumunda ağ üzerinden gönderilir.

Ayrıca, her bir kopya, ZooKeeper içindeki durumunu parça seti ve sağlama toplamı olarak depolar. Yerel dosya sistemindeki durum ZooKeeper referans durumundan ayrıldığında, kopya diğer kopyalardan eksik ve bozuk parçaları indirerek tutarlılığını geri yükler. Yerel dosya sisteminde beklenmeyen veya bozuk bazı veriler olduğunda, ClickHouse onu kaldırmaz, ancak ayrı bir dizine taşır ve unutur.

!!! note "Not"
    ClickHouse kümesi bağımsız parçalardan oluşur ve her parça kopyalardan oluşur. Küme **elastik değil**, böylece yeni bir parça ekledikten sonra, veriler otomatik olarak kırıklar arasında yeniden dengelenmez. Bunun yerine, küme yükünün eşit olmayan şekilde ayarlanması gerekiyor. Bu uygulama size daha fazla kontrol sağlar ve onlarca düğüm gibi nispeten küçük kümeler için uygundur. Ancak üretimde kullandığımız yüzlerce düğüm içeren kümeler için bu yaklaşım önemli bir dezavantaj haline gelir. Kümeler arasında otomatik olarak bölünebilen ve dengelenebilen dinamik olarak çoğaltılmış bölgelerle kümeye yayılan bir tablo altyapısı uygulamalıyız.

{## [Orijinal makale](https://clickhouse.tech/docs/en/development/architecture/) ##}
