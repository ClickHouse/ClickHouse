---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 65
toc_title: "Introspecci\xF3n"
---

# Funciones de introspección {#introspection-functions}

Puede utilizar las funciones descritas en este capítulo para [ELF](https://en.wikipedia.org/wiki/Executable_and_Linkable_Format) y [DWARF](https://en.wikipedia.org/wiki/DWARF) para la creación de perfiles de consultas.

!!! warning "Advertencia"
    Estas funciones son lentas y pueden imponer consideraciones de seguridad.

Para el correcto funcionamiento de las funciones de introspección:

-   Instale el `clickhouse-common-static-dbg` paquete.

-   Establezca el [allow\_introspection\_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) a 1.

        For security reasons introspection functions are disabled by default.

ClickHouse guarda los informes del generador de perfiles [trace\_log](../../operations/system-tables.md#system_tables-trace_log) tabla del sistema. Asegúrese de que la tabla y el generador de perfiles estén configurados correctamente.

## addressToLine {#addresstoline}

Convierte la dirección de memoria virtual dentro del proceso del servidor ClickHouse en el nombre de archivo y el número de línea en el código fuente de ClickHouse.

Si utiliza paquetes oficiales de ClickHouse, debe instalar el `clickhouse-common-static-dbg` paquete.

**Sintaxis**

``` sql
addressToLine(address_of_binary_instruction)
```

**Parámetros**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**Valor devuelto**

-   Nombre de archivo del código fuente y el número de línea en este archivo delimitado por dos puntos.

        For example, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`, where `199` is a line number.

-   Nombre de un binario, si la función no pudo encontrar la información de depuración.

-   Cadena vacía, si la dirección no es válida.

Tipo: [Cadena](../../sql-reference/data-types/string.md).

**Ejemplo**

Habilitación de las funciones de introspección:

``` sql
SET allow_introspection_functions=1
```

Seleccionando la primera cadena de la `trace_log` tabla del sistema:

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

El `trace` campo contiene el seguimiento de la pila en el momento del muestreo.

Obtener el nombre de archivo del código fuente y el número de línea para una sola dirección:

``` sql
SELECT addressToLine(94784076370703) \G
```

``` text
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
```

Aplicando la función a todo el seguimiento de la pila:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
\G
```

El [arrayMap](higher-order-functions.md#higher_order_functions-array-map) permite procesar cada elemento individual de la `trace` matriz por el `addressToLine` función. El resultado de este procesamiento se ve en el `trace_source_code_lines` columna de salida.

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

Convierte la dirección de memoria virtual dentro del proceso del servidor ClickHouse en el símbolo de los archivos de objetos ClickHouse.

**Sintaxis**

``` sql
addressToSymbol(address_of_binary_instruction)
```

**Parámetros**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**Valor devuelto**

-   Símbolo de archivos de objetos ClickHouse.
-   Cadena vacía, si la dirección no es válida.

Tipo: [Cadena](../../sql-reference/data-types/string.md).

**Ejemplo**

Habilitación de las funciones de introspección:

``` sql
SET allow_introspection_functions=1
```

Seleccionando la primera cadena de la `trace_log` tabla del sistema:

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

El `trace` campo contiene el seguimiento de la pila en el momento del muestreo.

Obtener un símbolo para una sola dirección:

``` sql
SELECT addressToSymbol(94138803686098) \G
```

``` text
Row 1:
──────
addressToSymbol(94138803686098): _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
```

Aplicando la función a todo el seguimiento de la pila:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
\G
```

El [arrayMap](higher-order-functions.md#higher_order_functions-array-map) permite procesar cada elemento individual de la `trace` matriz por el `addressToSymbols` función. El resultado de este procesamiento se ve en el `trace_symbols` columna de salida.

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

Convierte un símbolo que puede obtener utilizando el [addressToSymbol](#addresstosymbol) función a la función de C++ nombre.

**Sintaxis**

``` sql
demangle(symbol)
```

**Parámetros**

-   `symbol` ([Cadena](../../sql-reference/data-types/string.md)) — Symbol from an object file.

**Valor devuelto**

-   Nombre de la función C++
-   Cadena vacía si un símbolo no es válido.

Tipo: [Cadena](../../sql-reference/data-types/string.md).

**Ejemplo**

Habilitación de las funciones de introspección:

``` sql
SET allow_introspection_functions=1
```

Seleccionando la primera cadena de la `trace_log` tabla del sistema:

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

El `trace` campo contiene el seguimiento de la pila en el momento del muestreo.

Obtener un nombre de función para una sola dirección:

``` sql
SELECT demangle(addressToSymbol(94138803686098)) \G
```

``` text
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
```

Aplicando la función a todo el seguimiento de la pila:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
```

El [arrayMap](higher-order-functions.md#higher_order_functions-array-map) permite procesar cada elemento individual de la `trace` matriz por el `demangle` función. El resultado de este procesamiento se ve en el `trace_functions` columna de salida.

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
