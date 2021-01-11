---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 65
toc_title: "\u81EA\u7701"
---

# 内省功能 {#introspection-functions}

您可以使用本章中描述的函数来反省 [ELF](https://en.wikipedia.org/wiki/Executable_and_Linkable_Format) 和 [DWARF](https://en.wikipedia.org/wiki/DWARF) 用于查询分析。

!!! warning "警告"
    这些功能很慢，可能会强加安全考虑。

对于内省功能的正确操作:

-   安装 `clickhouse-common-static-dbg` 包。

-   设置 [allow_introspection_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) 设置为1。

        出于安全考虑，内省函数默认是关闭的。

ClickHouse将探查器报告保存到 [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) 系统表. 确保正确配置了表和探查器。

## addressToLine {#addresstoline}

将ClickHouse服务器进程内的虚拟内存地址转换为ClickHouse源代码中的文件名和行号。

如果您使用官方的ClickHouse软件包，您需要安装 `clickhouse-common-static-dbg` 包。

**语法**

``` sql
addressToLine(address_of_binary_instruction)
```

**参数**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 正在运行进程的指令地址。

**返回值**

-   源代码文件名和行号（用冒号分隔的行号）

        示例, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`, where `199` is a line number.

-   如果函数找不到调试信息，返回二进制文件的名称。

-   如果地址无效，返回空字符串。

类型: [字符串](../../sql-reference/data-types/string.md).

**示例**

启用内省功能:

``` sql
SET allow_introspection_functions=1
```

从中选择第一个字符串 `trace_log` 系统表:

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

该 `trace` 字段包含采样时的堆栈跟踪。

获取单个地址的源代码文件名和行号:

``` sql
SELECT addressToLine(94784076370703) \G
```

``` text
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
```

将函数应用于整个堆栈跟踪:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
\G
```

该 [arrayMap](higher-order-functions.md#higher_order_functions-array-map) 功能允许处理的每个单独的元素 `trace` 阵列由 `addressToLine` 功能。 这种处理的结果，你在看 `trace_source_code_lines` 列的输出。

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

将ClickHouse服务器进程内的虚拟内存地址转换为ClickHouse对象文件中的符号。

**语法**

``` sql
addressToSymbol(address_of_binary_instruction)
```

**参数**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**返回值**

-   来自ClickHouse对象文件的符号。
-   如果地址无效，返回空字符串。

类型: [字符串](../../sql-reference/data-types/string.md).

**示例**

启用内省功能:

``` sql
SET allow_introspection_functions=1
```

从中选择第一个字符串 `trace_log` 系统表:

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

该 `trace` 字段包含采样时的堆栈跟踪。

获取单个地址的符号:

``` sql
SELECT addressToSymbol(94138803686098) \G
```

``` text
Row 1:
──────
addressToSymbol(94138803686098): _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
```

将函数应用于整个堆栈跟踪:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
\G
```

该 [arrayMap](higher-order-functions.md#higher_order_functions-array-map) 功能允许处理的每个单独的元素 `trace` 阵列由 `addressToSymbols` 功能。 这种处理的结果，你在看 `trace_symbols` 列的输出。

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

转换一个符号，您可以使用 [addressToSymbol](#addresstosymbol) 函数到C++函数名。

**语法**

``` sql
demangle(symbol)
```

**参数**

-   `symbol` ([字符串](../../sql-reference/data-types/string.md)) — Symbol from an object file.

**返回值**

-   C++函数的名称。
-   如果符号无效，则为空字符串。

类型: [字符串](../../sql-reference/data-types/string.md).

**示例**

启用内省功能:

``` sql
SET allow_introspection_functions=1
```

从中选择第一个字符串 `trace_log` 系统表:

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

该 `trace` 字段包含采样时的堆栈跟踪。

获取单个地址的函数名称:

``` sql
SELECT demangle(addressToSymbol(94138803686098)) \G
```

``` text
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
```

将函数应用于整个堆栈跟踪:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
```

该 [arrayMap](higher-order-functions.md#higher_order_functions-array-map) 功能允许处理的每个单独的元素 `trace` 阵列由 `demangle` 功能。 这种处理的结果，你在看 `trace_functions` 列的输出。

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
