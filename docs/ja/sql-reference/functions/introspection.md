---
slug: /ja/sql-reference/functions/introspection
sidebar_position: 100
sidebar_label: インスペクション
---

# インスペクション関数

この章で説明されている関数を使用して、[ELF](https://en.wikipedia.org/wiki/Executable_and_Linkable_Format) および [DWARF](https://en.wikipedia.org/wiki/DWARF) をクエリプロファイリングのためにインスペクトすることができます。

:::note    
これらの関数は遅く、セキュリティ上の考慮が必要な場合があります。
:::

インスペクション関数を正しく操作するには次の要件があります：

- `clickhouse-common-static-dbg` パッケージをインストールします。

- [allow_introspection_functions](../../operations/settings/settings.md#allow_introspection_functions) 設定を1に設定します。

        セキュリティ上の理由から、インスペクション関数はデフォルトで無効になっています。

ClickHouse はプロファイラーレポートを [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) システムテーブルに保存します。このテーブルとプロファイラーが適切に設定されていることを確認してください。

## addressToLine

ClickHouseサーバープロセス内の仮想メモリアドレスを、ClickHouseソースコードのファイル名と行番号に変換します。

公式のClickHouseパッケージを使用している場合、`clickhouse-common-static-dbg` パッケージをインストールする必要があります。

**構文**

``` sql
addressToLine(address_of_binary_instruction)
```

**引数**

- `address_of_binary_instruction` ([UInt64](../data-types/int-uint.md)) — 実行中のプロセス内の命令アドレス。

**戻り値**

- ソースコードのファイル名と、このファイル内の行番号をコロンで区切ったもの。
  例えば、`/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199` のように `199` は行番号です。
- デバッグ情報が見つからない場合はバイナリ名。
- アドレスが無効な場合は空文字列。

タイプ: [String](../../sql-reference/data-types/string.md)。

**例**

インスペクション関数の有効化:

``` sql
SET allow_introspection_functions=1;
```

`trace_log` システムテーブルから最初の行を選択:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G;
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

`trace` フィールドにはサンプリング時のスタックトレースが含まれています。

単一のアドレスに対してソースコードのファイル名と行番号を取得:

``` sql
SELECT addressToLine(94784076370703) \G;
```

``` text
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
```

スタックトレース全体に関数を適用:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
\G
```

[arrayMap](../../sql-reference/functions/array-functions.md#array-map) 関数を使用すると、`trace` 配列の各要素を `addressToLine` 関数で処理できます。この処理結果は出力の `trace_source_code_lines` 列に表示されます。

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

## addressToLineWithInlines

`addressToLine` と似ていますが、すべてのインライン関数を含む配列を返します。そのため、`addressToLine` よりも遅いです。

:::note
公式のClickHouseパッケージを使用している場合、`clickhouse-common-static-dbg` パッケージをインストールする必要があります。
:::

**構文**

``` sql
addressToLineWithInlines(address_of_binary_instruction)
```

**引数**

- `address_of_binary_instruction` ([UInt64](../data-types/int-uint.md)) — 実行中のプロセス内の命令アドレス。

**戻り値**

- 最初の要素がソースコードのファイル名とファイル内の行番号をコロンで区切った配列。2番目以降の要素は、インライン関数のソースコードのファイル名、行番号、および関数名のリスト。デバッグ情報が見つからない場合は、バイナリ名と等しい単一要素を持つ配列が返され、アドレスが無効な場合は空の配列が返されます。[Array(String)](../data-types/array.md)。

**例**

インスペクション関数の有効化:

``` sql
SET allow_introspection_functions=1;
```

アドレスに関数を適用:

```sql
SELECT addressToLineWithInlines(531055181::UInt64);
```

``` text
┌─addressToLineWithInlines(CAST('531055181', 'UInt64'))────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ ['./src/Functions/addressToLineWithInlines.cpp:98','./build_normal_debug/./src/Functions/addressToLineWithInlines.cpp:176:DB::(anonymous namespace)::FunctionAddressToLineWithInlines::implCached(unsigned long) const'] │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

スタックトレース全体に関数を適用:

``` sql
SELECT
    ta, addressToLineWithInlines(arrayJoin(trace) as ta)
FROM system.trace_log
WHERE
    query_id = '5e173544-2020-45de-b645-5deebe2aae54';
```

[arrayJoin](../../sql-reference/functions/array-functions.md#array-functions-join) 関数は配列を行に分割します。

``` text
┌────────ta─┬─addressToLineWithInlines(arrayJoin(trace))───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 365497529 │ ['./build_normal_debug/./contrib/libcxx/include/string_view:252']                                                                                                                                                        │
│ 365593602 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:191']                                                                                                                                                                      │
│ 365593866 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365592528 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365591003 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:477']                                                                                                                                                                      │
│ 365590479 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:442']                                                                                                                                                                      │
│ 365590600 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:457']                                                                                                                                                                      │
│ 365598941 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365607098 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365590571 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:451']                                                                                                                                                                      │
│ 365598941 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365607098 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365590571 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:451']                                                                                                                                                                      │
│ 365598941 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365607098 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365590571 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:451']                                                                                                                                                                      │
│ 365598941 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365597289 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:807']                                                                                                                                                                      │
│ 365599840 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:1118']                                                                                                                                                                     │
│ 531058145 │ ['./build_normal_debug/./src/Functions/addressToLineWithInlines.cpp:152']                                                                                                                                                │
│ 531055181 │ ['./src/Functions/addressToLineWithInlines.cpp:98','./build_normal_debug/./src/Functions/addressToLineWithInlines.cpp:176:DB::(anonymous namespace)::FunctionAddressToLineWithInlines::implCached(unsigned long) const'] │
│ 422333613 │ ['./build_normal_debug/./src/Functions/IFunctionAdaptors.h:21']                                                                                                                                                          │
│ 586866022 │ ['./build_normal_debug/./src/Functions/IFunction.cpp:216']                                                                                                                                                               │
│ 586869053 │ ['./build_normal_debug/./src/Functions/IFunction.cpp:264']                                                                                                                                                               │
│ 586873237 │ ['./build_normal_debug/./src/Functions/IFunction.cpp:334']                                                                                                                                                               │
│ 597901620 │ ['./build_normal_debug/./src/Interpreters/ExpressionActions.cpp:601']                                                                                                                                                    │
│ 597898534 │ ['./build_normal_debug/./src/Interpreters/ExpressionActions.cpp:718']                                                                                                                                                    │
│ 630442912 │ ['./build_normal_debug/./src/Processors/Transforms/ExpressionTransform.cpp:23']                                                                                                                                          │
│ 546354050 │ ['./build_normal_debug/./src/Processors/ISimpleTransform.h:38']                                                                                                                                                          │
│ 626026993 │ ['./build_normal_debug/./src/Processors/ISimpleTransform.cpp:89']                                                                                                                                                        │
│ 626294022 │ ['./build_normal_debug/./src/Processors/Executors/ExecutionThreadContext.cpp:45']                                                                                                                                        │
│ 626293730 │ ['./build_normal_debug/./src/Processors/Executors/ExecutionThreadContext.cpp:63']                                                                                                                                        │
│ 626169525 │ ['./build_normal_debug/./src/Processors/Executors/PipelineExecutor.cpp:213']                                                                                                                                             │
│ 626170308 │ ['./build_normal_debug/./src/Processors/Executors/PipelineExecutor.cpp:178']                                                                                                                                             │
│ 626166348 │ ['./build_normal_debug/./src/Processors/Executors/PipelineExecutor.cpp:329']                                                                                                                                             │
│ 626163461 │ ['./build_normal_debug/./src/Processors/Executors/PipelineExecutor.cpp:84']                                                                                                                                              │
│ 626323536 │ ['./build_normal_debug/./src/Processors/Executors/PullingAsyncPipelineExecutor.cpp:85']                                                                                                                                  │
│ 626323277 │ ['./build_normal_debug/./src/Processors/Executors/PullingAsyncPipelineExecutor.cpp:112']                                                                                                                                 │
│ 626323133 │ ['./build_normal_debug/./contrib/libcxx/include/type_traits:3682']                                                                                                                                                       │
│ 626323041 │ ['./build_normal_debug/./contrib/libcxx/include/tuple:1415']                                                                                                                                                             │
└───────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```

## addressToSymbol

ClickHouseサーバープロセス内の仮想メモリアドレスを、ClickHouseオブジェクトファイルからのシンボルに変換します。

**構文**

``` sql
addressToSymbol(address_of_binary_instruction)
```

**引数**

- `address_of_binary_instruction` ([UInt64](../data-types/int-uint.md)) — 実行中のプロセス内の命令アドレス。

**戻り値**

- ClickHouseオブジェクトファイルからのシンボル。[String](../data-types/string.md)。
- アドレスが無効な場合は空文字列。[String](../data-types/string.md)。

**例**

インスペクション関数の有効化:

``` sql
SET allow_introspection_functions=1;
```

`trace_log` システムテーブルから最初の行を選択:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G;
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

`trace` フィールドにはサンプリング時のスタックトレースが含まれています。

単一のアドレスに対してシンボルを取得:

``` sql
SELECT addressToSymbol(94138803686098) \G;
```

``` text
Row 1:
──────
addressToSymbol(94138803686098): _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
```

スタックトレース全体に関数を適用:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
\G
```

[arrayMap](../../sql-reference/functions/array-functions.md#array-map) 関数を使用すると、`trace` 配列の各要素を `addressToSymbols` 関数で処理できます。この処理結果は出力の `trace_symbols` 列に表示されます。

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

## demangle

[addressToSymbol](#addresstosymbol) 関数を使用して取得できるシンボルを C++ 関数名に変換します。

**構文**

``` sql
demangle(symbol)
```

**引数**

- `symbol` ([String](../data-types/string.md)) — オブジェクトファイルからのシンボル。

**戻り値**

- C++ 関数の名前、またはシンボルが無効な場合は空文字列。[String](../data-types/string.md)。

**例**

インスペクション関数の有効化:

``` sql
SET allow_introspection_functions=1;
```

`trace_log` システムテーブルから最初の行を選択:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G;
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

`trace` フィールドにはサンプリング時のスタックトレースが含まれています。

単一のアドレスに対して関数名を取得:

``` sql
SELECT demangle(addressToSymbol(94138803686098)) \G;
```

``` text
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
```

スタックトレース全体に関数を適用:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
```

[arrayMap](../../sql-reference/functions/array-functions.md#array-map) 関数を使用すると、`trace` 配列の各要素を `demangle` 関数で処理できます。この処理結果は出力の `trace_functions` 列に表示されます。

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

## tid

現在の [Block](https://clickhouse.com/docs/ja/development/architecture/#block) が処理されているスレッドのIDを返します。

**構文**

``` sql
tid()
```

**戻り値**

- 現在のスレッドID。[Uint64](../data-types/int-uint.md#uint-ranges)。

**例**

クエリ:

``` sql
SELECT tid();
```

結果:

``` text
┌─tid()─┐
│  3878 │
└───────┘
```

## logTrace

各 [Block](https://clickhouse.com/docs/ja/development/architecture/#block) に対してサーバーログにトレースログメッセージを記録します。

**構文**

``` sql
logTrace('message')
```

**引数**

- `message` — サーバーログに記録されるメッセージ。[String](../data-types/string.md#string)。

**戻り値**

- 常に0を返します。

**例**

クエリ:

``` sql
SELECT logTrace('logTrace message');
```

結果:

``` text
┌─logTrace('logTrace message')─┐
│                            0 │
└──────────────────────────────┘
```
