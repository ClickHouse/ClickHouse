---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 68
toc_title: "C++ kodu nas\u0131l yaz\u0131l\u0131r"
---

# C++ kodu nasıl yazılır {#how-to-write-c-code}

## Genel Öneriler {#general-recommendations}

**1.** Aşağıdakiler önerilerdir, gereksinimler değildir.

**2.** Kod düzenliyorsanız, varolan kodun biçimlendirmesini takip etmek mantıklıdır.

**3.** Kod stili tutarlılık için gereklidir. Tutarlılık, kodu okumayı kolaylaştırır ve aynı zamanda kodu aramayı da kolaylaştırır.

**4.** Kuralların çoğunun mantıklı nedenleri yoktur, yerleşik uygulamalar tarafından belirlenir.

## Biçimlendirir {#formatting}

**1.** Biçimlendirme çoğu tarafından otomatik olarak yapılacaktır `clang-format`.

**2.** Girintiler 4 boşluk vardır. Bir sekme dört boşluk ekleyecek şekilde geliştirme ortamınızı yapılandırın.

**3.** Kıvırcık parantezlerin açılması ve kapatılması ayrı bir satırda olmalıdır.

``` cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** Tüm fonksiyon gövdesi tek ise `statement`, tek bir satır üzerine yerleştirilebilir. Yer (satır sonunda boşluk dışında) etrafında ayraç boşluk.

``` cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** Fonksiyonlar için. Parantezlerin etrafına boşluk koymayın.

``` cpp
void reinsert(const Value & x)
```

``` cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** İçinde `if`, `for`, `while` ve diğer ifadeler, açılış braketinin önüne bir boşluk yerleştirilir (işlev çağrılarının aksine).

``` cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** İkili operatörlerin etrafına boşluk Ekle (`+`, `-`, `*`, `/`, `%`, …) and the ternary operator `?:`.

``` cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** Bir satır beslemesi girilirse, operatörü yeni bir satıra koyun ve girintiyi ondan önce artırın.

``` cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** İsterseniz, bir çizgi içinde hizalama için boşluk kullanabilirsiniz.

``` cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** Operatörler etrafında boşluk kullanmayın `.`, `->`.

Gerekirse, operatör bir sonraki satıra sarılabilir. Bu durumda, önündeki ofset artar.

**11.** Tekli operatörleri ayırmak için boşluk kullanmayın (`--`, `++`, `*`, `&`, …) from the argument.

**12.** Virgülden sonra bir boşluk koyun, ancak ondan önce değil. Aynı kural, bir içindeki noktalı virgül için de geçerlidir `for` İfade.

**13.** Ayırmak için boşluk kullanmayın `[]` operatör.

**14.** İn a `template <...>` ifade, arasında bir boşluk kullanın `template` ve `<`; sonra boşluk yok `<` ya da önce `>`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** Sınıflarda ve yapılarda, yazın `public`, `private`, ve `protected` aynı seviyede `class/struct` ve kodun geri kalanını girinti.

``` cpp
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;
    ...
}
```

**16.** Eğer aynı `namespace` tüm dosya için kullanılır ve başka önemli bir şey yoktur, içinde bir ofset gerekli değildir `namespace`.

**17.** Eğer blok için bir `if`, `for`, `while` veya başka bir ifade tek bir `statement`, kıvırcık parantez isteğe bağlıdır. Place the `statement` bunun yerine ayrı bir satırda. Bu kural iç içe geçmiş için de geçerlidir `if`, `for`, `while`, …

Ama eğer iç `statement` kıvırcık parantez içerir veya `else`, dış blok kıvırcık parantez içinde yazılmalıdır.

``` cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** Çizgilerin uçlarında boşluk olmamalıdır.

**19.** Kaynak dosyalar UTF-8 kodlanmıştır.

**20.** ASCII olmayan karakterler dize değişmezlerinde kullanılabilir.

``` cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** Tek bir satırda birden çok ifade yazmayın.

**22.** Fonksiyonların içindeki kod bölümlerini gruplandırın ve bunları birden fazla boş satırla ayırın.

**23.** Bir veya iki boş satırla ayrı işlevler, sınıflar vb.

**24.** `A const` (bir değerle ilgili) tür adından önce yazılmalıdır.

``` cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** Bir işaretçi veya başvuru bildirirken, `*` ve `&` semboller her iki taraftaki boşluklarla ayrılmalıdır.

``` cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** Şablon türlerini kullanırken, `using` anahtar kelime (en basit durumlar hariç).

Başka bir deyişle, şablon parametreleri yalnızca `using` ve kodda tekrarlanmıyor.

`using` bir işlevin içinde olduğu gibi yerel olarak bildirilebilir.

``` cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** Bir ifadede farklı türde birkaç değişken bildirmeyin.

``` cpp
//incorrect
int x, *y;
```

**28.** C tarzı yayınları kullanmayın.

``` cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** Sınıflarda ve yapılarda, grup üyeleri ve işlevleri her görünürlük kapsamı içinde ayrı ayrı.

**30.** Küçük sınıflar ve yapılar için, yöntem bildirimini uygulamadan ayırmak gerekli değildir.

Aynı şey, herhangi bir sınıf veya yapıdaki küçük yöntemler için de geçerlidir.

Templated sınıflar ve yapılar için, yöntem bildirimlerini uygulamadan ayırmayın(aksi takdirde aynı çeviri biriminde tanımlanmaları gerekir).

**31.** Satırları 80 yerine 140 karakterle sarabilirsiniz.

**32.** Postfix gerekli değilse, her zaman önek artış / azaltma işleçlerini kullanın.

``` cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

## Yorumlar {#comments}

**1.** Kodun önemsiz olmayan tüm bölümleri için yorum eklediğinizden emin olun.

Bu çok önemli. Yorumu yazmak, kodun gerekli olmadığını veya yanlış tasarlandığını anlamanıza yardımcı olabilir.

``` cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** Yorumlar gerektiği kadar ayrıntılı olabilir.

**3.** Açıklama yaptıkları koddan önce yorumları yerleştirin. Nadir durumlarda, yorumlar aynı satırda koddan sonra gelebilir.

``` cpp
/** Parses and executes the query.
*/
void executeQuery(
    ReadBuffer & istr, /// Where to read the query from (and data for INSERT, if applicable)
    WriteBuffer & ostr, /// Where to write the result
    Context & context, /// DB, tables, data types, engines, functions, aggregate functions...
    BlockInputStreamPtr & query_plan, /// Here could be written the description on how query was executed
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete /// Up to which stage process the SELECT query
    )
```

**4.** Yorumlar sadece İngilizce olarak yazılmalıdır.

**5.** Bir kitaplık yazıyorsanız, ana başlık dosyasına açıklayan ayrıntılı yorumları ekleyin.

**6.** Ek bilgi vermeyen yorumlar eklemeyin. Özellikle, bu gibi boş yorumlar bırakmayın:

``` cpp
/*
* Procedure Name:
* Original procedure name:
* Author:
* Date of creation:
* Dates of modification:
* Modification authors:
* Original file name:
* Purpose:
* Intent:
* Designation:
* Classes used:
* Constants:
* Local variables:
* Parameters:
* Date of creation:
* Purpose:
*/
```

Örnek kaynaktan ödünç alınmıştır http://home.tamk.fi / ~ jaalto / kurs / kodlama stili / doc/ulaşılamaz-kod/.

**7.** Çöp yorum yazmayın (yazar, oluşturma tarihi ..) her dosyanın başında.

**8.** Tek satırlı yorumlar üç eğik çizgi ile başlar: `///` ve çok satırlı yorumlar ile başlar `/**`. Bu yorumlar dikkate alınır “documentation”.

Not: bu yorumlardan belgeler oluşturmak için Doxygen kullanabilirsiniz. Ancak DOXYGEN genellikle kullanılmaz, çünkü IDE'DEKİ kodda gezinmek daha uygundur.

**9.** Çok satırlı açıklamaların başında ve sonunda (çok satırlı bir açıklamayı kapatan satır hariç) boş satırları olmamalıdır.

**10.** Kodu yorumlamak için temel yorumları kullanın, değil “documenting” yorumlar.

**11.** İşlem yapmadan önce kodun yorumlanan kısımlarını silin.

**12.** Yorumlarda veya kodda küfür kullanmayın.

**13.** Büyük harf kullanmayın. Aşırı noktalama kullanmayın.

``` cpp
/// WHAT THE FAIL???
```

**14.** Sınırlayıcılar yapmak için yorum kullanmayın.

``` cpp
///******************************************************
```

**15.** Yorumlarda tartışmalara başlamayın.

``` cpp
/// Why did you do this stuff?
```

**16.** Ne hakkında olduğunu açıklayan bir bloğun sonunda bir yorum yazmaya gerek yok.

``` cpp
/// for
```

## Adlar {#names}

**1.** Değişkenlerin ve sınıf üyelerinin adlarında alt çizgi içeren küçük harfler kullanın.

``` cpp
size_t max_block_size;
```

**2.** İşlevlerin (yöntemlerin) adları için, küçük harfle başlayan camelCase kullanın.

``` cpp
std::string getName() const override { return "Memory"; }
```

**3.** Sınıfların (yapıların) adları için büyük harfle başlayan CamelCase kullanın. Ben dışındaki önekler arayüzler için kullanılmaz.

``` cpp
class StorageMemory : public IStorage
```

**4.** `using` sınıf aslarla aynı şekilde adlandırılır veya `_t` ucunda.

**5.** Şablon Türü argümanlarının isimleri: basit durumlarda, kullanın `T`; `T`, `U`; `T1`, `T2`.

Daha karmaşık durumlar için, sınıf adları için kuralları izleyin veya öneki ekleyin `T`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Şablon sabit argümanlarının adları: değişken adları için kurallara uyun veya kullanın `N` basit durumlarda.

``` cpp
template <bool without_www>
struct ExtractDomain
```

**7.** Soyut sınıflar (arayüzler) için şunları ekleyebilirsiniz `I` önek.

``` cpp
class IBlockInputStream
```

**8.** Yerel olarak bir değişken kullanırsanız, kısa adı kullanabilirsiniz.

Diğer tüm durumlarda, anlamı açıklayan bir isim kullanın.

``` cpp
bool info_successfully_loaded = false;
```

**9.** İsimleri `define`s ve genel sabitler alt çizgi ile ALL_CAPS kullanın.

``` cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** Dosya adları, içerikleriyle aynı stili kullanmalıdır.

Bir dosya tek bir sınıf içeriyorsa, dosyayı sınıfla aynı şekilde adlandırın (CamelCase).

Dosya tek bir işlev içeriyorsa, dosyayı işlevle aynı şekilde adlandırın (camelCase).

**11.** İsim bir kısaltma içeriyorsa, o zaman:

-   Değişken adları için kısaltma küçük harfler kullanmalıdır `mysql_connection` (değil `mySQL_connection`).
-   Sınıfların ve işlevlerin adları için, büyük harfleri kısaltmada tutun`MySQLConnection` (değil `MySqlConnection`).

**12.** Yalnızca sınıf üyelerini başlatmak için kullanılan yapıcı bağımsız değişkenleri, sınıf üyeleri ile aynı şekilde, ancak sonunda bir alt çizgi ile adlandırılmalıdır.

``` cpp
FileQueueProcessor(
    const std::string & path_,
    const std::string & prefix_,
    std::shared_ptr<FileHandler> handler_)
    : path(path_),
    prefix(prefix_),
    handler(handler_),
    log(&Logger::get("FileQueueProcessor"))
{
}
```

Bağımsız değişken yapıcı gövdesinde kullanılmazsa, alt çizgi soneki atlanabilir.

**13.** Yerel değişkenlerin ve sınıf üyelerinin adlarında fark yoktur (önek gerekmez).

``` cpp
timer (not m_timer)
```

**14.** Bir de SAB theitler için `enum`, büyük harfle CamelCase kullanın. ALL_CAPS da kabul edilebilir. Eğer... `enum` yerel olmayan, bir `enum class`.

``` cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** Tüm isimler İngilizce olmalıdır. Rusça kelimelerin çevirisi izin verilmez.

    not Stroka

**16.** Kısaltmalar iyi biliniyorsa kabul edilebilir (kısaltmanın anlamını Wikipedia'da veya bir arama motorunda kolayca bulabilirsiniz).

    `AST`, `SQL`.

    Not `NVDH` (some random letters)

Kısaltılmış versiyon ortak kullanım ise eksik kelimeler kabul edilebilir.

Yorumlarda tam ad yanında yer alıyorsa bir kısaltma da kullanabilirsiniz.

**17.** C++ kaynak kodu ile dosya adları olmalıdır `.cpp` uzantı. Başlık dosyaları olmalıdır `.h` uzantı.

## Kod nasıl yazılır {#how-to-write-code}

**1.** Bellek yönetimi.

El ile bellek ayırma (`delete`) sadece kütüphane kodunda kullanılabilir.

Kütüphane kod inunda, `delete` operatör yalnızca yıkıcılarda kullanılabilir.

Uygulama kodunda, bellek sahibi olan nesne tarafından serbest bırakılmalıdır.

Örnekler:

-   En kolay yol, bir nesneyi yığına yerleştirmek veya onu başka bir sınıfın üyesi yapmaktır.
-   Çok sayıda küçük nesne için kapları kullanın.
-   Öbekte bulunan az sayıda nesnenin otomatik olarak ayrılması için şunları kullanın `shared_ptr/unique_ptr`.

**2.** Kaynak yönetimi.

Kullanmak `RAII` ve yukarıya bakın.

**3.** Hata işleme.

İstisnaları kullanın. Çoğu durumda, yalnızca bir istisna atmanız gerekir ve onu yakalamanız gerekmez (çünkü `RAII`).

Çevrimdışı veri işleme uygulamalarında, istisnaları yakalamamak genellikle kabul edilebilir.

Kullanıcı isteklerini işleyen sunucularda, bağlantı işleyicisinin en üst düzeyindeki istisnaları yakalamak genellikle yeterlidir.

İş parçacığı işlevlerinde, bunları ana iş parçacığında yeniden taramak için tüm istisnaları yakalamalı ve tutmalısınız `join`.

``` cpp
/// If there weren't any calculations yet, calculate the first block synchronously
if (!started)
{
    calculate();
    started = true;
}
else /// If calculations are already in progress, wait for the result
    pool.wait();

if (exception)
    exception->rethrow();
```

İşleme olmadan istisnaları asla gizlemeyin. Sadece körü körüne log tüm istisnaları koymak asla.

``` cpp
//Not correct
catch (...) {}
```

Eğer bazı özel durumlar göz ardı etmek gerekiyorsa, sadece özel olanlar için bunu yapmak ve diğerleri yeniden oluşturma.

``` cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

Yanıt kodlarıyla işlevleri kullanırken veya `errno`, her zaman sonucu kontrol edin ve hata durumunda bir istisna atın.

``` cpp
if (0 != close(fd))
    throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);
```

`Do not use assert`.

**4.** İstisna türleri.

Uygulama kodunda karmaşık özel durum hiyerarşisini kullanmaya gerek yoktur. Özel durum metni bir sistem yöneticisi için anlaşılabilir olmalıdır.

**5.** Yıkıcılardan istisnalar atmak.

Bu tavsiye edilmez, ancak izin verilir.

Aşağıdaki seçenekleri kullanın:

-   Bir işlev oluşturma (`done()` veya `finalize()`) bu, bir istisnaya yol açabilecek tüm işleri önceden yapacaktır. Bu işlev çağrıldıysa, daha sonra yıkıcıda istisna olmamalıdır.
-   Çok karmaşık olan görevler (ağ üzerinden ileti gönderme gibi), sınıf kullanıcısının imha edilmeden önce çağırması gereken ayrı bir yöntemle yerleştirilebilir.
-   Yıkıcıda bir istisna varsa, onu gizlemek yerine günlüğe kaydetmek daha iyidir (logger mevcutsa).
-   Basit uygulamalarda, güvenmek kabul edilebilir `std::terminate` bu (vakaların `noexcept` varsayılan olarak C++11) istisnaları işlemek için.

**6.** Anonim kod blokları.

Belirli değişkenleri yerel hale getirmek için tek bir işlevin içinde ayrı bir kod bloğu oluşturabilirsiniz, böylece bloktan çıkarken yıkıcılar çağrılır.

``` cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** Multithreading.

Çevrimdışı veri işleme programlarında:

-   Tek bir CPU çekirdeğinde mümkün olan en iyi performansı elde etmeye çalışın. Daha sonra gerekirse kodunuzu parallelize edebilirsiniz.

Sunucu uygulamalarında:

-   İstekleri işlemek için iş parçacığı havuzunu kullanın. Bu noktada, userspace bağlam değiştirme gerektiren herhangi bir görevimiz olmadı.

Çatal paralelleştirme için kullanılmaz.

**8.** İş parçacıklarını senkronize etme.

Genellikle farklı iş parçacıklarının farklı bellek hücreleri kullanmasını sağlamak mümkündür (daha da iyisi: farklı önbellek çizgileri) ve herhangi bir iş parçacığı senkronizasyonu kullanmamak (hariç `joinAll`).

Senkronizasyon gerekiyorsa, çoğu durumda, mutex altında kullanmak yeterlidir `lock_guard`.

Diğer durumlarda sistem senkronizasyonu ilkellerini kullanın. Meşgul bekleme kullanmayın.

Atomik işlemler sadece en basit durumlarda kullanılmalıdır.

Birincil uzmanlık alanınız olmadığı sürece kilitsiz veri yapılarını uygulamaya çalışmayın.

**9.** İşaretçiler vs referanslar.

Çoğu durumda, referansları tercih edin.

**10.** const.

Sabit referanslar, sabitler için işaretçiler kullanın, `const_iterator` ve const yöntemleri.

Düşünmek `const` varsayılan olmak ve olmayan kullanmak-`const` sadece gerektiğinde.

Değişkenleri değere göre geçirirken, `const` genellikle mantıklı değil.

**11.** imzasız.

Kullanmak `unsigned` gerekirse.

**12.** Sayısal türleri.

Türleri kullanın `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, ve `Int64` gibi `size_t`, `ssize_t`, ve `ptrdiff_t`.

Bu türleri sayılar için kullanmayın: `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** Argümanları geçmek.

Karmaşık değerleri referansla geçirin (dahil `std::string`).

Bir işlev öbekte oluşturulan bir nesnenin sahipliğini yakalarsa, bağımsız değişken türünü yapın `shared_ptr` veya `unique_ptr`.

**14.** Değerleri döndürür.

Çoğu durumda, sadece kullanın `return`. Yaz domayın `[return std::move(res)]{.strike}`.

İşlev öbek üzerinde bir nesne ayırır ve döndürürse, şunları kullanın `shared_ptr` veya `unique_ptr`.

Nadir durumlarda, değeri bir argüman aracılığıyla döndürmeniz gerekebilir. Bu durumda, argüman bir referans olmalıdır.

``` cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Allows creating an aggregate function by its name.
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** ad.

Ayrı bir kullanmaya gerek yoktur `namespace` uygulama kodu için.

Küçük kütüphanelerin de buna ihtiyacı yok.

Orta ve büyük kütüphaneler için her şeyi bir `namespace`.

Kütüphan theede `.h` dosya, kullanabilirsiniz `namespace detail` uygulama kodu için gerekli olmayan uygulama ayrıntılarını gizlemek için.

İn a `.cpp` dosya, bir kullanabilirsiniz `static` veya sembolleri gizlemek için anonim ad alanı.

Ayrıca, bir `namespace` bir için kullanılabilir `enum` ilgili isimlerin harici bir yere düşmesini önlemek için `namespace` (ama kullanmak daha iyidir `enum class`).

**16.** Ertelenmiş başlatma.

Başlatma için bağımsız değişkenler gerekiyorsa, normalde varsayılan bir yapıcı yazmamalısınız.

Daha sonra başlatmayı geciktirmeniz gerekiyorsa, geçersiz bir nesne oluşturacak varsayılan bir yapıcı ekleyebilirsiniz. Veya, az sayıda nesne için şunları kullanabilirsiniz `shared_ptr/unique_ptr`.

``` cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** Sanal fonksiyonlar.

Sınıf polimorfik kullanım için tasarlanmamışsa, işlevleri sanal hale getirmeniz gerekmez. Bu aynı zamanda yıkıcı için de geçerlidir.

**18.** Kodlamalar.

Her yerde UTF-8 kullanın. Kullanmak `std::string`ve`char *`. Kullanmayın `std::wstring`ve`wchar_t`.

**19.** Günlük.

Koddaki her yerde örneklere bakın.

Taahhütte bulunmadan önce, tüm anlamsız ve hata ayıklama günlüğünü ve diğer hata ayıklama çıktı türlerini silin.

İzleme düzeyinde bile döngülerde oturum açmaktan kaçınılmalıdır.

Günlükleri herhangi bir günlük düzeyinde okunabilir olmalıdır.

Günlük kaydı yalnızca uygulama kodunda, çoğunlukla kullanılmalıdır.

Günlük mesajları İngilizce olarak yazılmalıdır.

Günlük, tercihen Sistem Yöneticisi için anlaşılabilir olmalıdır.

Günlüğünde küfür kullanmayın.

Günlüğünde UTF-8 kodlamasını kullanın. Nadir durumlarda, günlüğünde ASCII olmayan karakterler kullanabilirsiniz.

**20.** Giriş-çıkış.

Kullanmayın `iostreams` uygulama performansı için kritik olan iç döngülerde (ve asla kullanmayın `stringstream`).

Kullan... `DB/IO` kütüphane yerine.

**21.** Tarih ve zaman.

Görmek `DateLUT` kitaplık.

**22.** İçermek.

Her zaman kullanın `#pragma once` korumaları dahil etmek yerine.

**23.** kullanım.

`using namespace` kullanılmaz. Kullanabilirsiniz `using` özel bir şeyle. Ancak bir sınıf veya işlev içinde yerel yapın.

**24.** Kullanmayın `trailing return type` gerekli olmadıkça fonksiyonlar için.

``` cpp
[auto f() -&gt; void;]{.strike}
```

**25.** Değişkenlerin bildirimi ve başlatılması.

``` cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** Sanal işlevler için yaz `virtual` temel sınıfta, ama yaz `override` yerine `virtual` soyundan gelen sınıflarda.

## C++ ' ın kullanılmayan özellikleri {#unused-features-of-c}

**1.** Sanal devralma kullanılmaz.

**2.** C++03 özel durum belirteçleri kullanılmaz.

## Platform {#platform}

**1.** Belirli bir platform için kod yazıyoruz.

Ama diğer şeyler eşit olmak, çapraz platform veya taşınabilir kod tercih edilir.

**2.** Dil: C++20.

**3.** Derleyici: `gcc`. Şu anda (Ağustos 2020), kod sürüm 9.3 kullanılarak derlenmiştir. (Ayrıca kullanılarak derlenebilir `clang 8`.)

Standart kütüphane kullanılır (`libc++`).

**4.**OS: Linux UB .untu, daha eski değil.

**5.**Kod x86_64 CPU mimarisi için yazılmıştır.

CPU komut seti, sunucularımız arasında desteklenen minimum kümedir. Şu anda, sse 4.2.

**6.** Kullanmak `-Wall -Wextra -Werror` derleme bayrakları.

**7.** Statik olarak bağlanması zor olanlar hariç tüm kitaplıklarla statik bağlantı kullanın (bkz. `ldd` komut).

**8.** Kod geliştirilmiş ve yayın ayarları ile ayıklanır.

## Araçlar {#tools}

**1.** KDevelop iyi bir IDE.

**2.** Hata ayıklama için kullanın `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...`, veya `tcmalloc_minimal_debug`.

**3.** Profilleme için kullanın `Linux Perf`, `valgrind` (`callgrind`), veya `strace -cf`.

**4.** Kaynaklar Git'te.

**5.** Montaj kullanımları `CMake`.

**6.** Programlar kullanılarak serbest bırakılır `deb` paketler.

**7.** Ana taahhüt yapı kırmak gerekir.

Sadece seçilen revizyonlar uygulanabilir olarak kabul edilir.

**8.** Kod yalnızca kısmen hazır olsa bile, mümkün olduğunca sık taahhüt yapın.

Bu amaçla dalları kullanın.

Eğer kod inunuz `master` şube henüz imara değil, önce inşa onu hariç `push`. Bunu bitirmek veya birkaç gün içinde kaldırmak gerekir.

**9.** Non-önemsiz değişiklik, kullanım şubeleri ve sunucu bunları yayımlamak.

**10.** Kullanılmayan kod depodan kaldırılır.

## Kitaplık {#libraries}

**1.** C++20 standart Kütüphanesi kullanılır (deneysel uzantılara izin verilir) ve `boost` ve `Poco` çerçeveler.

**2.** Gerekirse, OS paketinde bulunan iyi bilinen kütüphaneleri kullanabilirsiniz.

Zaten mevcut olan iyi bir çözüm varsa, başka bir kütüphane yüklemeniz gerektiği anlamına gelse bile kullanın.

(Ancak kötü kütüphaneleri koddan kaldırmaya hazır olun .)

**3.** Paketlerde ihtiyacınız olan şey yoksa veya eski bir sürüme veya yanlış derleme türüne sahip değilseniz, paketlerde olmayan bir kitaplık yükleyebilirsiniz.

**4.** Kütüphane küçükse ve kendi karmaşık yapı sistemine sahip değilse, kaynak dosyaları `contrib` klasör.

**5.** Tercih her zaman zaten kullanımda olan kütüphanelere verilir.

## Genel Öneriler {#general-recommendations-1}

**1.** Mümkün olduğunca az kod yazın.

**2.** En basit çözümü deneyin.

**3.** Nasıl çalışacağını ve iç döngünün nasıl çalışacağını bilene kadar kod yazmayın.

**4.** En basit durumlarda, kullanın `using` sınıflar veya yapılar yerine.

**5.** Mümkünse, kopya oluşturucuları, atama işleçleri, yıkıcılar (sınıf en az bir sanal işlev içeriyorsa, sanal bir işlev dışında) yazmayın, oluşturucuları taşıyın veya atama işleçlerini taşıyın. Başka bir deyişle, derleyici tarafından oluşturulan işlevleri düzgün çalışması gerekir. Kullanabilirsiniz `default`.

**6.** Kod sadeleştirme teşvik edilir. Mümkünse kodunuzun boyutunu azaltın.

## Ek Öneriler {#additional-recommendations}

**1.** Açıkça belirtme `std::` türleri için `stddef.h`

tavsiye edilmez. Başka bir deyişle, yazmanızı öneririz `size_t` yerine `std::size_t` daha kısa olduğu için.

Eklemek kabul edilebilir `std::`.

**2.** Açıkça belirtme `std::` standart C kitap fromlığından fonksiyonlar için

tavsiye edilmez. Başka bir deyişle, yazın `memcpy` yerine `std::memcpy`.

Bunun nedeni, aşağıdaki gibi benzer standart dışı işlevlerin olmasıdır `memmem`. Bu işlevleri zaman zaman kullanıyoruz. Bu işlevler mevcut değil `namespace std`.

Yazar yousan `std::memcpy` yerine `memcpy` her yerde, o zaman `memmem` olarak `std::` garip görünecek.

Yine de, hala kullanabilirsiniz `std::` eğer tercih ederseniz edin.

**3.** Aynı olanlar standart C++ kütüphanesinde mevcut olduğunda C'den işlevleri kullanma.

Daha verimli ise bu kabul edilebilir.

Örneğin, kullanın `memcpy` yerine `std::copy` büyük bellek parçalarını kopyalamak için.

**4.** Çok satırlı fonksiyon argümanları.

Aşağıdaki sarma stillerinden herhangi birine izin verilir:

``` cpp
function(
  T1 x1,
  T2 x2)
```

``` cpp
function(
  size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

``` cpp
function(size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

``` cpp
function(size_t left, size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

``` cpp
function(
      size_t left,
      size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

[Orijinal makale](https://clickhouse.tech/docs/en/development/style/) <!--hide-->
