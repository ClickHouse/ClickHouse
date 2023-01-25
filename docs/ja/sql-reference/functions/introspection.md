---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 65
toc_title: "\u5185\u7701"
---

# 内観関数 {#introspection-functions}

この章で説明する関数を使用して、イントロスペクトできます [ELF](https://en.wikipedia.org/wiki/Executable_and_Linkable_Format) と [DWARF](https://en.wikipedia.org/wiki/DWARF) クエリプロファイル用。

!!! warning "警告"
    これらの機能は、が必要となる場合があり安全に配慮し

内観機能の適切な操作のため:

-   インストール `clickhouse-common-static-dbg` パッケージ。

-   セット [allow_introspection_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) 1に設定します。

        For security reasons introspection functions are disabled by default.

ClickHouseはプロファイラレポートを [trace_log](../../operations/system-tables.md#system_tables-trace_log) システムテーブル。 のテーブルプロファイラで設定されます。

## アドレストリン {#addresstoline}

ClickHouse serverプロセス内の仮想メモリアドレスを、ClickHouseソースコード内のファイル名と行番号に変換します。

公式のClickHouseパッケージを使用する場合は、 `clickhouse-common-static-dbg` パッケージ。

**構文**

``` sql
addressToLine(address_of_binary_instruction)
```

**パラメータ**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**戻り値**

-   ソースコードのファイル名とこのファイル内の行番号をコロンで区切ります。

        For example, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`, where `199` is a line number.

-   関数がデバッグ情報を見つけられなかった場合のバイナリの名前。

-   アドレスが無効な場合は、空の文字列。

タイプ: [文字列](../../sql-reference/data-types/string.md).

**例**

イントロスペクション機能の有効化:

``` sql
SET allow_introspection_functions=1
```

から最初の文字列を選択する `trace_log` システム表:

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

その `trace` 分野のスタックトレースを瞬時にサンプリングします。

単一のアドレスのソースコードのファイル名と行番号の取得:

``` sql
SELECT addressToLine(94784076370703) \G
```

``` text
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
```

スタックトレース全体への関数の適用:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
\G
```

その [arrayMap](higher-order-functions.md#higher_order_functions-array-map) 機能はの各々の個々の要素を処理することを割り当てます `trace` による配列 `addressToLine` 機能。 この処理の結果は `trace_source_code_lines` 出力の列。

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

に変換する仮想メモリアドレス内ClickHouseサーバプロセスのシンボルからClickHouseオブジェクトファイルです。

**構文**

``` sql
addressToSymbol(address_of_binary_instruction)
```

**パラメータ**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**戻り値**

-   ClickHouseオブジェクトファイルからの記号。
-   アドレスが無効な場合は、空の文字列。

タイプ: [文字列](../../sql-reference/data-types/string.md).

**例**

イントロスペクション機能の有効化:

``` sql
SET allow_introspection_functions=1
```

から最初の文字列を選択する `trace_log` システム表:

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

その `trace` 分野のスタックトレースを瞬時にサンプリングします。

単一のアドレスのシンボルの取得:

``` sql
SELECT addressToSymbol(94138803686098) \G
```

``` text
Row 1:
──────
addressToSymbol(94138803686098): _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
```

スタックトレース全体への関数の適用:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
\G
```

その [arrayMap](higher-order-functions.md#higher_order_functions-array-map) 機能はの各々の個々の要素を処理することを割り当てます `trace` による配列 `addressToSymbols` 機能。 この処理の結果は `trace_symbols` 出力の列。

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

## デマングル {#demangle}

を使用して取得できるシンボルを変換します [addressToSymbol](#addresstosymbol) C++関数名への関数。

**構文**

``` sql
demangle(symbol)
```

**パラメータ**

-   `symbol` ([文字列](../../sql-reference/data-types/string.md)) — Symbol from an object file.

**戻り値**

-   C++関数の名前。
-   シンボルが無効な場合は空の文字列。

タイプ: [文字列](../../sql-reference/data-types/string.md).

**例**

イントロスペクション機能の有効化:

``` sql
SET allow_introspection_functions=1
```

から最初の文字列を選択する `trace_log` システム表:

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

その `trace` 分野のスタックトレースを瞬時にサンプリングします。

単一のアドレスの関数名の取得:

``` sql
SELECT demangle(addressToSymbol(94138803686098)) \G
```

``` text
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
```

スタックトレース全体への関数の適用:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
```

その [arrayMap](higher-order-functions.md#higher_order_functions-array-map) 機能はの各々の個々の要素を処理することを割り当てます `trace` による配列 `demangle` 機能。 この処理の結果は `trace_functions` 出力の列。

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
