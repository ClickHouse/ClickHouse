---
title : DWARF
slug : /en/interfaces/formats/DWARF
keywords : [DWARF]
---

## Description

Parses DWARF debug symbols from an ELF file (executable, library, or object file). Similar to `dwarfdump`, but much faster (hundreds of MB/s) and with SQL. Produces one row for each Debug Information Entry (DIE) in the `.debug_info` section. Includes "null" entries that the DWARF encoding uses to terminate lists of children in the tree.
Quick background: `.debug_info` consists of *units*, corresponding to compilation units. Each unit is a tree of *DIE*s, with a `compile_unit` DIE as its root. Each DIE has a *tag* and a list of *attributes*. Each attribute has a *name* and a *value* (and also a *form*, which specifies how the value is encoded). The DIEs represent things from the source code, and their *tag* tells what kind of thing it is. E.g. there are functions (tag = `subprogram`), classes/structs/enums (`class_type`/`structure_type`/`enumeration_type`), variables (`variable`), function arguments (`formal_parameter`). The tree structure mirrors the corresponding source code. E.g. a `class_type` DIE can contain `subprogram` DIEs representing methods of the class.

Outputs the following columns:
- `offset` - position of the DIE in the `.debug_info` section
- `size` - number of bytes in the encoded DIE (including attributes)
- `tag` - type of the DIE; the conventional "DW_TAG_" prefix is omitted
- `unit_name` - name of the compilation unit containing this DIE
- `unit_offset` - position of the compilation unit containing this DIE in the `.debug_info` section
- `ancestor_tags` - array of tags of the ancestors of the current DIE in the tree, in order from innermost to outermost
- `ancestor_offsets` - offsets of ancestors, parallel to `ancestor_tags`
- a few common attributes duplicated from the attributes array for convenience:
    - `name`
    - `linkage_name` - mangled fully-qualified name; typically only functions have it (but not all functions)
    - `decl_file` - name of the source code file where this entity was declared
    - `decl_line` - line number in the source code where this entity was declared
- parallel arrays describing attributes:
    - `attr_name` - name of the attribute; the conventional "DW_AT_" prefix is omitted
    - `attr_form` - how the attribute is encoded and interpreted; the conventional DW_FORM_ prefix is omitted
    - `attr_int` - integer value of the attribute; 0 if the attribute doesn't have a numeric value
    - `attr_str` - string value of the attribute; empty if the attribute doesn't have a string value

## Example Usage

Example: find compilation units that have the most function definitions (including template instantiations and functions from included header files):
```sql
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```
```text
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

## Format Settings




