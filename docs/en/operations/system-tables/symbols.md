---
description: 'System table useful for C++ experts and ClickHouse engineers containing
  information for introspection of the `clickhouse` binary.'
keywords: ['system table', 'symbols']
slug: /operations/system-tables/symbols
title: 'system.symbols'
doc_type: 'reference'
---

Contains information for introspection of `clickhouse` binary. It requires the introspection privilege to access.
This table is only useful for C++ experts and ClickHouse engineers.

Columns:

- `symbol` ([String](../../sql-reference/data-types/string.md)) — Symbol name in the binary. It is mangled. You can apply `demangle(symbol)` to obtain a readable name.
- `symbol_demangled` ([Nullable(String)](../../sql-reference/data-types/string.md)) — Demangled symbol used for XRay instrumentation.
- `function_id` ([Nullable(Int32)](../../sql-reference/data-types/int-uint.md)) — Function ID in the XRay instrumentation map.
- `address_begin` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Start address of the symbol in the binary.
- `address_end` ([UInt64](../../sql-reference/data-types/int-uint.md)) — End address of the symbol in the binary.
- `name` ([String](../../sql-reference/data-types/string.md)) — Alias for `event`.

**Example**

```sql
SELECT * FROM system.symbols WHERE function_id IS NOT NULL LIMIT 5 SETTINGS allow_introspection_functions = 1
```

```text
Row 1:
──────
symbol:           _Z15isClickhouseAppNSt3__117basic_string_viewIcNS_11char_traitsIcEEEERNS_6vectorIPcNS_9allocatorIS5_EEEE
symbol_demangled: isClickhouseApp(std::__1::basic_string_view<char, std::__1::char_traits<char>>, std::__1::vector<char*, std::__1::allocator<char*>>&)
function_id:      1
address_begin:    219229312 -- 219.23 million
address_end:      219231408 -- 219.23 million

Row 2:
──────
symbol:           main
symbol_demangled: main
function_id:      2
address_begin:    219231872 -- 219.23 million
address_end:      219233485 -- 219.23 million

Row 3:
──────
symbol:           _ZN12_GLOBAL__N_19printHelpEiPPc
symbol_demangled: (anonymous namespace)::printHelp(int, char**)
function_id:      3
address_begin:    219233536 -- 219.23 million
address_end:      219233902 -- 219.23 million

Row 4:
──────
symbol:           _ZNSt3__110filesystem4pathC2B8se210105IPcvEERKT_NS1_6formatE
symbol_demangled: std::__1::filesystem::path::path[abi:se210105]<char*, void>(char* const&, std::__1::filesystem::path::format)
function_id:      4
address_begin:    219234496 -- 219.23 million
address_end:      219234620 -- 219.23 million

Row 5:
──────
symbol:           _ZNSt3__113unordered_setINS_17basic_string_viewIcNS_11char_traitsIcEEEENS_4hashIS4_EENS_8equal_toIS4_EENS_9allocatorIS4_EEEC2ESt16initializer_listIS4_E
symbol_demangled: std::__1::unordered_set<std::__1::basic_string_view<char, std::__1::char_traits<char>>, std::__1::hash<std::__1::basic_string_view<char, std::__1::char_traits<char>>>, std::__1::equal_to<std::__1::basic_string_view<char, std::__1::char_traits<char>>>, std::__1::allocator<std::__1::basic_string_view<char, std::__1::char_traits<char>>>>::unordered_set(std::initializer_list<std::__1::basic_string_view<char, std::__1::char_traits<char>>>)
function_id:      5
address_begin:    219235584 -- 219.24 million
address_end:      219235708 -- 219.24 million
```
