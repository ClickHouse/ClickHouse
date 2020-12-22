---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 65
toc_title: Introspection
---

# Fonctions D'Introspection {#introspection-functions}

Vous pouvez utiliser les fonctions décrites dans ce chapitre pour introspecter [ELF](https://en.wikipedia.org/wiki/Executable_and_Linkable_Format) et [DWARF](https://en.wikipedia.org/wiki/DWARF) pour le profilage de requête.

!!! warning "Avertissement"
    Ces fonctions sont lentes et peuvent imposer des considérations de sécurité.

Pour le bon fonctionnement des fonctions d'introspection:

-   Installer le `clickhouse-common-static-dbg` paquet.

-   Définir le [allow_introspection_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) réglage sur 1.

        For security reasons introspection functions are disabled by default.

Clickhouse enregistre les rapports du profileur [trace_log](../../operations/system-tables.md#system_tables-trace_log) système de table. Assurez-vous que la table et le profileur sont correctement configurés.

## addressToLine {#addresstoline}

Convertit l'adresse de mémoire virtuelle dans le processus de serveur ClickHouse en nom de fichier et en numéro de ligne dans le code source de ClickHouse.

Si vous utilisez des paquets clickhouse officiels, vous devez installer le `clickhouse-common-static-dbg` paquet.

**Syntaxe**

``` sql
addressToLine(address_of_binary_instruction)
```

**Paramètre**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**Valeur renvoyée**

-   Nom de fichier du code Source et le numéro de ligne dans ce fichier délimité par deux-points.

        For example, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`, where `199` is a line number.

-   Nom d'un binaire, si la fonction n'a pas pu trouver les informations de débogage.

-   Chaîne vide, si l'adresse n'est pas valide.

Type: [Chaîne](../../sql-reference/data-types/string.md).

**Exemple**

Activation des fonctions d'introspection:

``` sql
SET allow_introspection_functions=1
```

Sélection de la première chaîne de `trace_log` système de table:

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

Le `trace` champ contient la trace de pile au moment de l'échantillonnage.

Obtenir le nom de fichier du code source et le numéro de ligne pour une seule adresse:

``` sql
SELECT addressToLine(94784076370703) \G
```

``` text
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
```

Application de la fonction à la trace de la pile entière:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
\G
```

Le [arrayMap](higher-order-functions.md#higher_order_functions-array-map) permet de traiter chaque élément individuel de l' `trace` tableau par la `addressToLine` fonction. Le résultat de ce traitement que vous voyez dans l' `trace_source_code_lines` colonne de sortie.

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

## adressetosymbol {#addresstosymbol}

Convertit l'adresse de mémoire virtuelle dans le processus de serveur ClickHouse en symbole à partir des fichiers d'objets ClickHouse.

**Syntaxe**

``` sql
addressToSymbol(address_of_binary_instruction)
```

**Paramètre**

-   `address_of_binary_instruction` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Address of instruction in a running process.

**Valeur renvoyée**

-   Symbole des fichiers D'objets ClickHouse.
-   Chaîne vide, si l'adresse n'est pas valide.

Type: [Chaîne](../../sql-reference/data-types/string.md).

**Exemple**

Activation des fonctions d'introspection:

``` sql
SET allow_introspection_functions=1
```

Sélection de la première chaîne de `trace_log` système de table:

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

Le `trace` champ contient la trace de pile au moment de l'échantillonnage.

Obtenir un symbole pour une seule adresse:

``` sql
SELECT addressToSymbol(94138803686098) \G
```

``` text
Row 1:
──────
addressToSymbol(94138803686098): _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
```

Application de la fonction à la trace de la pile entière:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
\G
```

Le [arrayMap](higher-order-functions.md#higher_order_functions-array-map) permet de traiter chaque élément individuel de l' `trace` tableau par la `addressToSymbols` fonction. Le résultat de ce traitement que vous voyez dans l' `trace_symbols` colonne de sortie.

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

Convertit un symbole que vous pouvez obtenir en utilisant le [adressetosymbol](#addresstosymbol) fonction au nom de la fonction c++.

**Syntaxe**

``` sql
demangle(symbol)
```

**Paramètre**

-   `symbol` ([Chaîne](../../sql-reference/data-types/string.md)) — Symbol from an object file.

**Valeur renvoyée**

-   Nom de la fonction C++.
-   Chaîne vide si un symbole n'est pas valide.

Type: [Chaîne](../../sql-reference/data-types/string.md).

**Exemple**

Activation des fonctions d'introspection:

``` sql
SET allow_introspection_functions=1
```

Sélection de la première chaîne de `trace_log` système de table:

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

Le `trace` champ contient la trace de pile au moment de l'échantillonnage.

Obtenir un nom de fonction pour une seule adresse:

``` sql
SELECT demangle(addressToSymbol(94138803686098)) \G
```

``` text
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
```

Application de la fonction à la trace de la pile entière:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
```

Le [arrayMap](higher-order-functions.md#higher_order_functions-array-map) permet de traiter chaque élément individuel de l' `trace` tableau par la `demangle` fonction. Le résultat de ce traitement que vous voyez dans l' `trace_functions` colonne de sortie.

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
