---
toc_priority: 65
toc_title: Introspection
---

# Introspection Functions {#introspection-functions}

You can use functions described in this chapter to introspect [ELF](https://en.wikipedia.org/wiki/Executable_and_Linkable_Format) and [DWARF](https://en.wikipedia.org/wiki/DWARF) for query profiling.

!!! warning "Warning"
    These functions are slow and may impose security considerations.

For proper operation of introspection functions:

-   Install the `clickhouse-common-static-dbg` package.

-   Set the [allow_introspection_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) setting to 1.

        For security reasons introspection functions are disabled by default.

ClickHouse saves profiler reports to the [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) system table. Make sure the table and profiler are configured properly.

## addressToLine {#addresstoline}

Converts virtual memory address inside ClickHouse server process to the filename and the line number in ClickHouse source code.

If you use official ClickHouse packages, you need to install the `clickhouse-common-static-dbg` package.

**Syntax**

``` sql
addressToLine(address_of_binary_instruction)
```

**Arguments**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**Returned value**

-   Source code filename and the line number in this file delimited by colon.

        For example, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`, where `199` is a line number.

-   Name of a binary, if the function couldn’t find the debug information.

-   Empty string, if the address is not valid.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Enabling introspection functions:

``` sql
SET allow_introspection_functions=1;
```

Selecting the first string from the `trace_log` system table:

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

The `trace` field contains the stack trace at the moment of sampling.

Getting the source code filename and the line number for a single address:

``` sql
SELECT addressToLine(94784076370703) \G;
```

``` text
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
```

Applying the function to the whole stack trace:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
\G
```

The [arrayMap](../../sql-reference/functions/array-functions.md#array-map) function allows to process each individual element of the `trace` array by the `addressToLine` function. The result of this processing you see in the `trace_source_code_lines` column of output.

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

Converts virtual memory address inside ClickHouse server process to the symbol from ClickHouse object files.

**Syntax**

``` sql
addressToSymbol(address_of_binary_instruction)
```

**Arguments**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**Returned value**

-   Symbol from ClickHouse object files.
-   Empty string, if the address is not valid.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Enabling introspection functions:

``` sql
SET allow_introspection_functions=1;
```

Selecting the first string from the `trace_log` system table:

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

The `trace` field contains the stack trace at the moment of sampling.

Getting a symbol for a single address:

``` sql
SELECT addressToSymbol(94138803686098) \G;
```

``` text
Row 1:
──────
addressToSymbol(94138803686098): _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
```

Applying the function to the whole stack trace:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
\G
```

The [arrayMap](../../sql-reference/functions/array-functions.md#array-map) function allows to process each individual element of the `trace` array by the `addressToSymbols` function. The result of this processing you see in the `trace_symbols` column of output.

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

Converts a symbol that you can get using the [addressToSymbol](#addresstosymbol) function to the C++ function name.

**Syntax**

``` sql
demangle(symbol)
```

**Arguments**

-   `symbol` ([String](../../sql-reference/data-types/string.md)) — Symbol from an object file.

**Returned value**

-   Name of the C++ function.
-   Empty string if a symbol is not valid.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Enabling introspection functions:

``` sql
SET allow_introspection_functions=1;
```

Selecting the first string from the `trace_log` system table:

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

The `trace` field contains the stack trace at the moment of sampling.

Getting a function name for a single address:

``` sql
SELECT demangle(addressToSymbol(94138803686098)) \G;
```

``` text
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
```

Applying the function to the whole stack trace:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
```

The [arrayMap](../../sql-reference/functions/array-functions.md#array-map) function allows to process each individual element of the `trace` array by the `demangle` function. The result of this processing you see in the `trace_functions` column of output.

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
## tid {#tid}

Returns id of the thread, in which current [Block](https://clickhouse.tech/docs/en/development/architecture/#block) is processed.

**Syntax**

``` sql
tid()
```

**Returned value**

-   Current thread id. [Uint64](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Example**

Query:

``` sql
SELECT tid();
```

Result:

``` text
┌─tid()─┐
│  3878 │
└───────┘
```

## logTrace {#logtrace}

Emits trace log message to server log for each [Block](https://clickhouse.tech/docs/en/development/architecture/#block).

**Syntax**

``` sql
logTrace('message')
```

**Arguments**

-   `message` — Message that is emitted to server log. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Always returns 0.

**Example**

Query:

``` sql
SELECT logTrace('logTrace message');
```

Result:

``` text
┌─logTrace('logTrace message')─┐
│                            0 │
└──────────────────────────────┘
```

