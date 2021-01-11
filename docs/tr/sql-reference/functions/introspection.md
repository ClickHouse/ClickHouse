---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 65
toc_title: "\u0130\xE7g\xF6zlem"
---

# İç Gözlem Fonksiyonları {#introspection-functions}

İç gözlem için bu bölümde açıklanan işlevleri kullanabilirsiniz [ELF](https://en.wikipedia.org/wiki/Executable_and_Linkable_Format) ve [DWARF](https://en.wikipedia.org/wiki/DWARF) sorgu profilleme için.

!!! warning "Uyarıcı"
    Bu işlevler yavaştır ve güvenlik konuları getirebilir.

İç gözlem fonksiyonlarının düzgün çalışması için:

-   Yüklemek `clickhouse-common-static-dbg` paket.

-   Ayarla... [allow_introspection_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) ayar 1.

        For security reasons introspection functions are disabled by default.

ClickHouse için profiler raporları kaydeder [trace_log](../../operations/system-tables.md#system_tables-trace_log) sistem tablosu. Tablo ve profiler düzgün yapılandırıldığından emin olun.

## addressToLine {#addresstoline}

ClickHouse sunucu işleminin içindeki sanal bellek adresini dosya adına ve clickhouse kaynak kodundaki satır numarasına dönüştürür.

Resmi ClickHouse paketleri kullanırsanız, yüklemeniz gerekir `clickhouse-common-static-dbg` paket.

**Sözdizimi**

``` sql
addressToLine(address_of_binary_instruction)
```

**Parametre**

-   `address_of_binary_instruction` ([Uİnt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**Döndürülen değer**

-   Kaynak kodu dosya adı ve bu dosyadaki satır numarası iki nokta üst üste ile sınırlandırılmıştır.

        For example, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`, where `199` is a line number.

-   Işlev hata ayıklama bilgilerini bulamadıysanız, bir ikili adı.

-   Adres geçerli değilse, boş dize.

Tür: [Dize](../../sql-reference/data-types/string.md).

**Örnek**

İç gözlem işlevlerini etkinleştirme:

``` sql
SET allow_introspection_functions=1
```

İlk dizeyi seçme `trace_log` sistem tablosu:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:              2019-11-19
event_time:              2019-11-19 18:57:23
revision:                54429
timer_type:              Real
thread_number:           48
query_id:                421b6855-1858-45a5-8f37-f383409d6d72
trace:                   [140658411141617,94784174532828,94784076370703,94784076372094,94784076361020,94784175007680,140658411116251,140658403895439]
```

Bu `trace` alan, örnekleme anında yığın izini içerir.

Tek bir adres için kaynak kodu dosya adını ve satır numarasını alma:

``` sql
SELECT addressToLine(94784076370703) \G
```

``` text
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
```

İşlevin tüm yığın izine uygulanması:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
\G
```

Bu [arrayMap](higher-order-functions.md#higher_order_functions-array-map) işlev, her bir elemanın işlenmesini sağlar `trace` ar arrayray by the `addressToLine` İşlev. Gördüğünüz bu işlemin sonucu `trace_source_code_lines` çıktı sütunu.

``` text
Row 1:
──────
trace_source_code_lines: /lib/x86_64-linux-gnu/libpthread-2.27.so
/usr/lib/debug/usr/bin/clickhouse
/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.h:155
/usr/include/c++/9/bits/atomic_base.h:551
/usr/lib/debug/usr/bin/clickhouse
/lib/x86_64-linux-gnu/libpthread-2.27.so
/build/glibc-OTsEL5/glibc-2.27/misc/../sysdeps/unix/sysv/linux/x86_64/clone.S:97
```

## addressToSymbol {#addresstosymbol}

Clickhouse sunucu işlemi içindeki sanal bellek adresini ClickHouse nesne dosyalarından gelen simgeye dönüştürür.

**Sözdizimi**

``` sql
addressToSymbol(address_of_binary_instruction)
```

**Parametre**

-   `address_of_binary_instruction` ([Uİnt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**Döndürülen değer**

-   ClickHouse nesne dosyalarından sembol.
-   Adres geçerli değilse, boş dize.

Tür: [Dize](../../sql-reference/data-types/string.md).

**Örnek**

İç gözlem işlevlerini etkinleştirme:

``` sql
SET allow_introspection_functions=1
```

İlk dizeyi seçme `trace_log` sistem tablosu:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-20
event_time:    2019-11-20 16:57:59
revision:      54429
timer_type:    Real
thread_number: 48
query_id:      724028bf-f550-45aa-910d-2af6212b94ac
trace:         [94138803686098,94138815010911,94138815096522,94138815101224,94138815102091,94138814222988,94138806823642,94138814457211,94138806823642,94138814457211,94138806823642,94138806795179,94138806796144,94138753770094,94138753771646,94138753760572,94138852407232,140399185266395,140399178045583]
```

Bu `trace` alan, örnekleme anında yığın izini içerir.

Tek bir adres için sembol alma:

``` sql
SELECT addressToSymbol(94138803686098) \G
```

``` text
Row 1:
──────
addressToSymbol(94138803686098): _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
```

İşlevin tüm yığın izine uygulanması:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
\G
```

Bu [arrayMap](higher-order-functions.md#higher_order_functions-array-map) işlev, her bir elemanın işlenmesini sağlar `trace` ar arrayray by the `addressToSymbols` İşlev. Gördüğünüz bu işlemin sonucu `trace_symbols` çıktı sütunu.

``` text
Row 1:
──────
trace_symbols: _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
_ZNK2DB10Aggregator21executeWithoutKeyImplERPcmPNS0_28AggregateFunctionInstructionEPNS_5ArenaE
_ZN2DB10Aggregator14executeOnBlockESt6vectorIN3COWINS_7IColumnEE13immutable_ptrIS3_EESaIS6_EEmRNS_22AggregatedDataVariantsERS1_IPKS3_SaISC_EERS1_ISE_SaISE_EERb
_ZN2DB10Aggregator14executeOnBlockERKNS_5BlockERNS_22AggregatedDataVariantsERSt6vectorIPKNS_7IColumnESaIS9_EERS6_ISB_SaISB_EERb
_ZN2DB10Aggregator7executeERKSt10shared_ptrINS_17IBlockInputStreamEERNS_22AggregatedDataVariantsE
_ZN2DB27AggregatingBlockInputStream8readImplEv
_ZN2DB17IBlockInputStream4readEv
_ZN2DB26ExpressionBlockInputStream8readImplEv
_ZN2DB17IBlockInputStream4readEv
_ZN2DB26ExpressionBlockInputStream8readImplEv
_ZN2DB17IBlockInputStream4readEv
_ZN2DB28AsynchronousBlockInputStream9calculateEv
_ZNSt17_Function_handlerIFvvEZN2DB28AsynchronousBlockInputStream4nextEvEUlvE_E9_M_invokeERKSt9_Any_data
_ZN14ThreadPoolImplI20ThreadFromGlobalPoolE6workerESt14_List_iteratorIS0_E
_ZZN20ThreadFromGlobalPoolC4IZN14ThreadPoolImplIS_E12scheduleImplIvEET_St8functionIFvvEEiSt8optionalImEEUlvE1_JEEEOS4_DpOT0_ENKUlvE_clEv
_ZN14ThreadPoolImplISt6threadE6workerESt14_List_iteratorIS0_E
execute_native_thread_routine
start_thread
clone
```

## demangle {#demangle}

Kullanarak alabileceğiniz bir sembolü dönüştürür [addressToSymbol](#addresstosymbol) C++ işlev adına işlev.

**Sözdizimi**

``` sql
demangle(symbol)
```

**Parametre**

-   `symbol` ([Dize](../../sql-reference/data-types/string.md)) — Symbol from an object file.

**Döndürülen değer**

-   C++ işlevinin adı.
-   Bir sembol geçerli değilse boş dize.

Tür: [Dize](../../sql-reference/data-types/string.md).

**Örnek**

İç gözlem işlevlerini etkinleştirme:

``` sql
SET allow_introspection_functions=1
```

İlk dizeyi seçme `trace_log` sistem tablosu:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-20
event_time:    2019-11-20 16:57:59
revision:      54429
timer_type:    Real
thread_number: 48
query_id:      724028bf-f550-45aa-910d-2af6212b94ac
trace:         [94138803686098,94138815010911,94138815096522,94138815101224,94138815102091,94138814222988,94138806823642,94138814457211,94138806823642,94138814457211,94138806823642,94138806795179,94138806796144,94138753770094,94138753771646,94138753760572,94138852407232,140399185266395,140399178045583]
```

Bu `trace` alan, örnekleme anında yığın izini içerir.

Tek bir adres için bir işlev adı alma:

``` sql
SELECT demangle(addressToSymbol(94138803686098)) \G
```

``` text
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
```

İşlevin tüm yığın izine uygulanması:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
```

Bu [arrayMap](higher-order-functions.md#higher_order_functions-array-map) işlev, her bir elemanın işlenmesini sağlar `trace` ar arrayray by the `demangle` İşlev. Gördüğünüz bu işlemin sonucu `trace_functions` çıktı sütunu.

``` text
Row 1:
──────
trace_functions: DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
DB::Aggregator::executeWithoutKeyImpl(char*&, unsigned long, DB::Aggregator::AggregateFunctionInstruction*, DB::Arena*) const
DB::Aggregator::executeOnBlock(std::vector<COW<DB::IColumn>::immutable_ptr<DB::IColumn>, std::allocator<COW<DB::IColumn>::immutable_ptr<DB::IColumn> > >, unsigned long, DB::AggregatedDataVariants&, std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >&, std::vector<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >, std::allocator<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> > > >&, bool&)
DB::Aggregator::executeOnBlock(DB::Block const&, DB::AggregatedDataVariants&, std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >&, std::vector<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >, std::allocator<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> > > >&, bool&)
DB::Aggregator::execute(std::shared_ptr<DB::IBlockInputStream> const&, DB::AggregatedDataVariants&)
DB::AggregatingBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::ExpressionBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::ExpressionBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::AsynchronousBlockInputStream::calculate()
std::_Function_handler<void (), DB::AsynchronousBlockInputStream::next()::{lambda()#1}>::_M_invoke(std::_Any_data const&)
ThreadPoolImpl<ThreadFromGlobalPool>::worker(std::_List_iterator<ThreadFromGlobalPool>)
ThreadFromGlobalPool::ThreadFromGlobalPool<ThreadPoolImpl<ThreadFromGlobalPool>::scheduleImpl<void>(std::function<void ()>, int, std::optional<unsigned long>)::{lambda()#3}>(ThreadPoolImpl<ThreadFromGlobalPool>::scheduleImpl<void>(std::function<void ()>, int, std::optional<unsigned long>)::{lambda()#3}&&)::{lambda()#1}::operator()() const
ThreadPoolImpl<std::thread>::worker(std::_List_iterator<std::thread>)
execute_native_thread_routine
start_thread
clone
```
