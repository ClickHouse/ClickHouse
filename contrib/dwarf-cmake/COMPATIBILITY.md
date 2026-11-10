# DWARF input format: compatibility ledger

Status: sections 0-13 record the pre-cutover baseline behavior from source inspection, and sections 12.1 and
14 record the measured baseline behavior (`26.11.1.2095`) and the behavior implemented by `rust/workspace/dwarf`;
where section 14 updates an earlier row, section 14 is authoritative. Tests named `T-*` in section 11.2 that are
covered by the crate's Rust test suite are mapped in section 14.4.

## 0. Provenance and conventions

| Item | Value |
|---|---|
| Crate | `rust/workspace/dwarf` (`_ch_rust_dwarf` / `ch_rust::dwarf`) |
| Baseline (old parser source & oracle) | Pre-cutover `DWARFBlockInputFormat` / `Elf` source at `master` `427747d6890c`; measured with pre-cutover `build_arm_release` binary `26.11.1.2095` (SHA-256 `f668f0419cf716196809f5c03e0650f0c096cfdf89e44d94e7d43632989f7ba3`) and `x86_64` `RelWithDebInfo` legacy oracle binary |
| Bundled LLVM | `contrib/llvm-project` gitlink `140ad525c086cfe79c8dcf33302a4f168d5a187b`; version 22.1.1 from `contrib/llvm-project/cmake/Modules/LLVMVersion.cmake:4-10`. This SHA was not checked against the upstream `llvmorg-22.1.1` tag (`git describe --exact-match` found no tag). |
| Rust crates (locked) | `gimli` 0.33.0, `object` 0.39.1 (`rust/workspace/Cargo.lock:1742-1743, 2478-2479`; vendored under `contrib/rust_vendor/gimli-0.33.0`, `contrib/rust_vendor/object-0.39.1`) |
| `rust/workspace/dwarf` before this file | did not exist |

Source abbreviations (all paths relative to the worktree):

- `F.cpp` = `src/Processors/Formats/Impl/DWARFBlockInputFormat.cpp`, `F.h` = `…/DWARFBlockInputFormat.h`
- `Elf.cpp`, `Elf.h` = `src/Common/Elf.{cpp,h}`
- `LLVM:` = `contrib/llvm-project/llvm/`, so `LLVM:lib/DebugInfo/DWARF/DWARFFormValue.cpp` etc.

Decision vocabulary:

- **KEEP**: new parser is bit-compatible with the old one for this case.
- **TIGHTEN**: new parser rejects, with the named error class, before returning its first row. Allowed only for spec-invalid input that LLVM tolerates, mishandles with undefined behavior, or silently truncates.
- **DECIDE**: design choice resolved in sections 12.1 and 14. Rows whose ID is followed by `+` were added by this ledger.

"Basis" labels, so vendor quirks are not passed off as standard requirements:

- **SPEC**: stated by the cited standard text (DWARF 5 at `https://dwarfstd.org/doc/DWARF5.pdf`, DWARF 4 at `https://dwarfstd.org/doc/DWARF4.pdf`, ELF gABI at `https://refspecs.linuxfoundation.org/elf/gabi4+/`). Section numbers were checked against the fetched text unless a row says otherwise.
- **VENDOR**: GNU/LLVM extension, not part of DWARF 5. Cite is the upstream definition, not the standard.
- **LLVM-IMPL**: behavior of LLVM 22.1.1's implementation, not stated by any standard.
- **CH**: ClickHouse's own convention in `F.cpp` / `Elf.cpp`, not stated by any standard.
- **SAFETY**: a memory-safety, undefined-behavior, or overflow closure with no standard statement.

Error classes: `CANNOT_PARSE_ELF` (ELF container, section table, missing `.debug_info`/`.debug_abbrev`) and `CANNOT_PARSE_DWARF` (everything else). Only the class is frozen; message text is not.

Tests: the "Test" column refers to the pending test list in section 11.2. Existing tests are listed in section 11.1. No test in section 11.2 has been written.

## 1. Conflicts between the approved plan and the source (read first)

| # | Plan statement | Source observation | Effect on this ledger |
|---|---|---|---|
| C1 | "Replicate its [`DB::Elf`] magic, section-table, section-name `STRTAB`/NUL, and `phoff != 0 && phnum != 0` checks" and "ELF64 little-endian … as `DB::Elf` effectively requires". | `Elf::init` checks only size ≥ 64, the 4 magic bytes, section/program table presence and bounds, and the shstrtab (`Elf.cpp:41-90`). It never reads `e_ident[EI_CLASS]`, `EI_DATA`, `EI_VERSION`, `e_version`, `e_ehsize`, `e_shentsize`, or `e_phentsize`. The existing 04340 fixtures leave those bytes zero (`04340_malformed_elf_dwarf_bounds.sh:26`). | Old acceptance does not require ELF64-LE; a malformed or foreign-class file is accepted if the bytes happen to line up. The ELF64-LE requirement is therefore a TIGHTEN (rows E-02, E-03; D-02), not a replication. |
| C2 | Seed row "tighten … code above `UInt16`". | LLVM stores *tag, attribute, form* as 16-bit enums (`LLVM:include/llvm/BinaryFormat/Dwarf.h:105,125,132`) and casts the ULEB with `static_cast` (`DWARFAbbreviationDeclaration.cpp:50,71,75`), so values above 0xFFFF are silently truncated. The abbreviation *code* is a 32-bit key (`DWARFDebugAbbrev.cpp:33,49,61-62`), and the format separately rejects DIE codes above `UINT32_MAX` (`F.cpp:479`). | Ledger reads the row as "tag/attribute/form ID above 0xFFFF" plus "abbreviation code above `UINT32_MAX`" (row U-07). |
| C3 | Seed row "`DW_FORM_LLVM_addrx_offset`: decode in the ClickHouse attribute adapter". | That form is supported for `attr_int` only. For `ranges`, `parseAddress` accepts only `addr`, `addrx`, `addrx1..4` and throws `CANNOT_PARSE_DWARF` for `LLVM_addrx_offset` and `GNU_addr_index` (`F.cpp:390-401`). For a `DW_TAG_compile_unit` DIE, the pre-pass throws the same way on `DW_AT_low_pc` with such a form even when `ranges` is not projected (`F.cpp:504-511`). | Rows F-20 and R-01 record both behaviors; the adapter must not make `ranges` start accepting the form. |
| C4 | "`onCancel` stops new work and causes workers to return promptly." "`resetParser` … permits a fresh read." | `onCancel` only stores `is_stopped` with no notify (`F.h:35-38`); `parseEntries` never checks it, so cancellation granularity is one chunk, and workers parked on `wake_up_threads` or `read` parked on `deliver_chunk` are not woken (`F.cpp:300-305, 1023`). `resetParser` resets `elf` and `extractor` only (`F.cpp:1037-1038`); `debug_line_extractor`, `debug_addr_section`, `debug_rnglists_extractor`, `debug_ranges_extractor`, `dwarf_context`, `file_contents` keep the previous input's values (`F.h:93-98`), and each is re-assigned only when the new file has that section (`F.cpp:267-274`). | These are old-parser defects, not behavior to preserve. Rows X-04 and X-07 record that the new parser must be stricter than the oracle, and that differential comparison after a reset is not an oracle. |
| C5 | Gate 5: "Separately run `02896` and `03641` … in Debug, ASan, TSan, and UBSan configurations." | `02896_dwarf_format.sh:2` is tagged `no-ubsan` (also `05139_dwarf_format_header_types.sh:2`). `03641` has no `no-ubsan`/`no-fasttest` tag (`03641_…sh:2`). | The UBSan cell for `02896` does not exist today. Removing the tag is a decision (D-12). |
| C6 | Plan test inventory omits `05139_dwarf_format_header_types.sh` and the contract for requested-type conversion. | `F.cpp:854-863` converts every produced column to the header's requested type; `F.cpp:214-216` throws `THERE_IS_NO_COLUMN` for unknown names. `ci/scripts/find_symbols.py` consumes `ranges[1].1`, `decl_file`, `linkage_name`. | Section 2 freezes both; section 11.1 lists `05139` and the consumer. |
| C7 | "use the already-opened file descriptor where the `ReadBuffer` API exposes one". | Old code re-opens by path: `Elf(file_in->getFileName())` → `MMapReadBufferFromFile(path_, 0)` (`F.cpp:232`, `Elf.cpp:24-28`), and only when `isRegularLocalFile(&offset) && offset == 0` (`F.cpp:229-231`). | Old has a path-reopen race that the plan removes; not a ledger compatibility item (X-09). |
| C8 | Seed row "OOB irrelevant sections … Preserve valid ELF64-LE behavior; reject spec-invalid files". | Old skips every non-fitting section silently (`Elf.cpp:108-110`), including legitimate `SHT_NOBITS` sections whose `sh_size` exceeds the file. "Reject" cannot apply to NOBITS. | Ambiguous as written; ledger proposes the split in E-06 (D-03). |
| C9 | (comment in `F.cpp:373-379`) "`DWARFFormValue::skipValue()` fails on `DW_FORM_strx3`". | In the bundled LLVM, `skipValue` lists `strx3` (`DWARFFormValue.cpp:161-164, 177`) and `getFixedFormByteSize` returns 3 (`LLVM:lib/BinaryFormat/Dwarf.cpp:969-971`). The workaround is a no-op and has no behavioral consequence. | None; recorded so nobody re-derives a `strx3` quirk. |

## 2. Frozen output contract

### 2.1 Schema (`F.cpp:44-100`, registration `F.cpp:1065-1081`)

Format name `DWARF`; fixed-schema reader returns the header unchanged without touching the input (`F.cpp:1043-1063`); registered via `registerRandomAccessInputFormat`, `markFormatSupportsSubsetOfColumns("DWARF")`. Column order is the header order below.

| # | Column | Type | Old source of the value |
|---|---|---|---|
| 0 | `offset` | `UInt64` | DIE offset in `.debug_info` before reading its abbreviation code (`F.cpp:441-443`). |
| 1 | `size` | `UInt32` | `static_cast<UInt32>(offset after attributes − die offset)`; null DIE includes its 1+ code bytes (`F.cpp:461, 721`). Truncation above `UINT32_MAX` is preserved. |
| 2 | `tag` | `LowCardinality(String)` | Index = tag ID into a shared 65536-entry dictionary; index 0 = `""` for null DIE (`F.cpp:170-186, 463, 484`). |
| 3 | `unit_name` | `LowCardinality(String)` | Chunk-level constant: `unit.unit_name` as it stands when the chunk is finalized (`F.cpp:778-789`). Assigned by every `DW_TAG_compile_unit` DIE that has a string `DW_AT_name`, last assignment wins (`F.cpp:674-675`). |
| 4 | `unit_offset` | `LowCardinality(UInt64)` | `dwarf_unit->getOffset()` (the unit header offset), chunk constant (`F.cpp:790-801`). |
| 5 | `ancestor_tags` | `Array(LowCardinality(String))` | Stack of ancestors that have children, innermost first (`F.cpp:444-453, 749-750`). For a null DIE the ancestors are recorded **before** the pop, so the row of a terminator lists the parent it closes (`F.cpp:444-453` precede `458-475`). |
| 6 | `ancestor_offsets` | `Array(UInt64)` | Parallel to 5 (`F.cpp:449-450`). |
| 7 | `name` | `String` | First string-class `DW_AT_name` in the DIE, else `""` (`F.cpp:670-673, 725`). |
| 8 | `linkage_name` | `String` | First string-class `DW_AT_linkage_name` only (not `DW_AT_MIPS_linkage_name`) (`F.cpp:677-678, 726`). |
| 9 | `decl_file` | `LowCardinality(String)` | First in-range `DW_AT_decl_file` constant, looked up in the unit's filename table; else `""` (`F.cpp:585-591, 727`). |
| 10 | `decl_line` | `UInt32` | First `DW_AT_decl_line` with a constant-group form, `static_cast<UInt32>` of the raw 64-bit value (`F.cpp:569-570`). |
| 11 | `ranges` | `Array(Tuple(UInt64, UInt64))` | See section 8. |
| 12 | `attr_name` | `Array(LowCardinality(String))` | Attribute ID → name; one element per abbreviation attribute, in declaration order (`F.cpp:536-537`). |
| 13 | `attr_form` | `Array(LowCardinality(String))` | The **declared** form (`attr.Form`), never the form resolved through `DW_FORM_indirect` or `implicit_const` (`F.cpp:538-541`). |
| 14 | `attr_int` | `Array(UInt64)` | See section 5 (forms) and section 6 (adapters). |
| 15 | `attr_str` | `Array(LowCardinality(String))` | See section 5 (forms) and section 6 (adapters). |

Common attributes (7-11) are produced **in addition to** their `attr_*` entries (comment `F.cpp:55-65`). The four `attr_*` arrays and the per-row offsets always have equal element counts. `ancestor_*` and `attr_*` and `ranges` share one offsets vector each (`F.cpp:452, 470, 723, 747`).

Requested types: the format may be asked for types other than the native ones; every column is converted with `castColumnToRequestedType` (`F.cpp:854-863`), and an unknown column name throws `THERE_IS_NO_COLUMN` at construction (`F.cpp:214-216, 129-137`). Covered today by `05139_dwarf_format_header_types.sh`.

### 2.2 Projection, validation, ordering

- **Projection controls materialization, not validation.** The attribute loop runs `parseAttribute` (full value decode, string NUL/bounds resolution) for every attribute of every DIE regardless of `need[]` (`F.cpp:532-535, 652-667`). The `DW_TAG_compile_unit` pre-pass and the `DW_AT_stmt_list` filename-table parse also run regardless (`F.cpp:486-512, 543-549`). Only `ranges` decoding is projection-dependent (`F.cpp:626, 730-746`).
- Dependencies: any of `attr_form`/`attr_int`/`attr_str` forces `attr_name` bookkeeping; `ancestor_offsets` forces `ancestor_tags` bookkeeping (`F.cpp:411-412`).
- Row order inside one unit is encoded DIE order, including across chunks of the same unit (a unit being processed is out of the queue, `F.cpp:307-326`). Chunk order across units is unspecified.
- A chunk holds at most 65536 rows (`F.cpp:438`); a chunk ends early when the unit's tree closes (`F.cpp:753-758`). A unit with exactly 65536 rows yields one chunk and no empty trailer.
- **Unit shape**: a unit must be exactly one tree. After the stack empties, `offset == end_offset` is required (`F.cpp:753-757`); a childless root followed by anything else fails.

## 3. ELF container (E)

Old path: `initELF` (`F.cpp:224-244`) → `Elf::init` (`Elf.cpp:35-93`) → `findSectionByName` (`Elf.cpp:137-140`).

| ID | Case | Old behavior (source) | Decision | Basis | Test |
|---|---|---|---|---|---|
| E-01 | Size ≥ 64 and magic | `elf_size < sizeof(ElfHeader)` → ELF error; 4-byte magic memcmp (`Elf.cpp:42-48`). | KEEP | SPEC gABI ch4 `e_ident` | T-ELF-1 |
| E-02+ | Ident class/data/version, `e_version` | Not read (see C1). Class-32 and big-endian files are not rejected by ident; layout is read as ELF64-LE regardless. | TIGHTEN (D-02): require `EI_CLASS=ELFCLASS64 (2)`, `EI_DATA=ELFDATA2LSB (1)`, `EI_VERSION=EV_CURRENT`; error `CANNOT_PARSE_ELF`. Old result on real ELF32/BE files is **unmeasured** (M-ELF). | SPEC gABI ch4 `EI_CLASS`/`EI_DATA`/`EI_VERSION` | T-ELF-2; 04340 fixtures must be re-written (they zero these bytes) |
| E-03+ | `e_ehsize`, `e_shentsize`, `e_phentsize`, table alignment | Unchecked; table sizes use `sizeof` of the packed 64-bit structs (`Elf.cpp:54-56, 87-89`), so any `e_shentsize` is accepted. | TIGHTEN: `e_shentsize == 64`, `e_phentsize == 56`, `e_ehsize == 64`. Alignment: whatever `object` 0.39.1 requires for ELF64 tables, if the file is otherwise valid → measure first (D-04). | SPEC gABI ch4 (`e_ehsize`, `e_shentsize`, `e_phentsize`; natural alignment) | T-ELF-3 |
| E-04 | Section table present, in-bounds | `shoff != 0 && shnum != 0 && rangeWithin(shoff, shnum*64, size)` (`Elf.cpp:54-57`). `shnum*64` is computed in `size_t`, no overflow. | KEEP | SPEC gABI `e_shoff`/`e_shnum` | existing 04340 `overflow_shoff`, `no_sections` |
| E-05 | Extended numbering | `e_shnum == 0` with count in section 0 (`shnum >= SHN_LORESERVE`) and `e_shstrndx == SHN_XINDEX` are not supported: both fall into E-04 / E-07 rejection. | KEEP (reject; no extended-numbering support in this change). Spec-valid but unsupported. | SPEC gABI ch4 `e_shnum`, `e_shstrndx` | T-ELF-4 |
| E-06+ | Out-of-file section | `iterateSections` silently skips any section with `offset > size` or `length > size − offset` (`Elf.cpp:108-110`). A skipped `.debug_*` is later "missing"; a skipped shstrtab → "doesn't have string table". | DECIDE (D-03): proposed TIGHTEN — a non-`SHT_NOBITS` section extending beyond the file → `CANNOT_PARSE_ELF` even if irrelevant; `SHT_NOBITS` is exempt because it occupies no file space. | SPEC gABI ch4 `sh_offset`/`sh_size`/`SHT_NOBITS` | T-ELF-5 |
| E-07 | Section-name table | The section whose type is `STRTAB` **and** index `== e_shstrndx` must exist and be in-bounds; size ≥ 1 and last byte NUL (`Elf.cpp:62-80`). | KEEP | SPEC gABI `e_shstrndx`, `SHT_STRTAB` | T-ELF-6 (wrong shstrtab type per plan) |
| E-08 | Section-name offsets | `Section::name()` throws `CANNOT_PARSE_ELF` if `sh_name >= strtab size` (`Elf.cpp:215-225`); it is evaluated for every in-bounds section visited, including all of them in the LLVM section-map pass (`F.cpp:276-284`), so one bad name offset anywhere fails the open. | KEEP (ELF error class) | SPEC gABI `sh_name` | T-ELF-7 |
| E-09 | Program headers | Only `phoff != 0 && phnum != 0 && in-bounds` is checked (`Elf.cpp:84-90`); contents are never read. `ET_REL`/`.o`/`.dwo` normally fail here; a relocatable file with fake program headers would pass. Error text says "truncated" even for absence. | KEEP (`CANNOT_PARSE_ELF`); the rule is a ClickHouse convention, not an ELF requirement. No relocation processing. | CH (the gABI permits a file with no program header table: `e_phnum` is then zero, ch4 `e_phnum`) | existing 04340 `overflow_phoff`; T-ELF-8 (`ET_REL`) |
| E-10 | Debug section lookup | `.debug_info` then `.debug_abbrev` by exact `strcmp`, first match in section-header order; missing → `CANNOT_PARSE_ELF` (`F.cpp:256-261`). `.debug_line`, `.debug_addr`, `.debug_rnglists`, `.debug_ranges` optional at open (`F.cpp:267-274`). `sh_type` and flags are **not** inspected. | KEEP for presence/name rules. Section type/flags: see E-11, E-12. | CH | T-ELF-9 |
| E-11+ | `SHT_NOBITS` debug section | Not checked: if its conceptual offset/size lie inside the file, the file bytes there are read as DWARF. Old result **unmeasured**. | DECIDE (D-05): proposed TIGHTEN — treat as no data: missing required section → `CANNOT_PARSE_ELF`; missing optional section follows the "referenced" rules. | SPEC gABI `SHT_NOBITS`, `sh_size` | T-ELF-10 |
| E-12+ | `SHF_COMPRESSED` / `.zdebug_*` | The format passes raw section buffers through the StringMap constructor; no decompression is requested (`F.cpp:276-285`, `LLVM:lib/DebugInfo/DWARF/DWARFContext.cpp:2124-2141`). Compressed-input outcomes are **unmeasured**. | KEEP the accepted subset, do not add decompression. Qualify flags and names against the baseline before choosing rejection behavior (D-06); a valid accepted file must remain compatible. | SPEC gABI ch4 `SHF_COMPRESSED`; CH for the raw-buffer path | T-ELF-11 |
| E-13+ | Section name matching | LLVM's section map strips **one** leading dot and uses first-wins `try_emplace` (`F.cpp:276-284`), so `debug_info` and `.debug_info` collide inside LLVM, while the format's own lookups require the dotted spelling. | KEEP valid accepted behavior. Do not silently replace the two lookup rules with exact dotted matching. Qualify collisions against the baseline before implementing section selection; a difference requires an independent spec-invalidity basis and a regression test. | CH; ELF section names alone do not justify rejecting a collision | T-ELF-12 |
| E-14 | Section overlap, aliasing | Not checked. Zero-copy views over the same bytes. | KEEP | CH | — |
| E-15 | Input span ownership | Local regular file at offset 0 → mmap by path; otherwise whole input copied into `PODArray` via `copyData(*in, buf, is_stopped)` (`F.cpp:227-243`). Copy is **non-append** (`WriteBufferFromVector.h:37`), so a reused buffer is overwritten. | KEEP semantics (C7 for the path/fd difference). Concurrent mutation and truncation of a mapped file remain undefined; documented, not an oracle. | CH | T-EXEC-6 (piped vs file) |
| E-16 | Cancellation during copy | If `is_stopped` interrupts `copyData`, `elf.emplace` still runs on the partial buffer (`F.cpp:241-243`) and can throw an ELF parse error instead of returning empty. | Not preserved; new parser must return an empty result on cancel, never a spurious parse error. | CH | T-EXEC-3 |

## 4. Units, abbreviations, tree shape (U)

Old path: `DWARFContext::create(sections, 8)` (`F.cpp:285`); units from `info_section_units()` (`F.cpp:287-288`); one `UnitState` per unit constructed eagerly on the opening thread (`F.cpp:139-152`). LLVM's default warning/recoverable handlers print to stderr (`LLVM:include/llvm/DebugInfo/DWARF/DWARFContext.h:117-119`), so stderr noise is not part of the contract.

| ID | Case | Old behavior (source) | Decision | Basis | Test |
|---|---|---|---|---|---|
| U-01 | Bad or truncated unit header | `Parser` returns null after a warning, and the enumeration loop does `break` (`LLVM:…/DWARFUnit.cpp:83-88, 134-137`), so the units before it are processed and the rest are silently dropped. | **TIGHTEN** (plan): `CANNOT_PARSE_DWARF` during open; never a truncated unit set. | SPEC DWARF5 §7.5.1 | T-UNIT-1 |
| U-02 | Header fields accepted | initial length 32/64-bit; version 2–5 (`DWARFContext.h:405-408`); v5 `unit_type`/address size read; address size ∈ {2,4,8} (`DWARFContext.h:410-412`, `DWARFUnit.cpp:339-342`); unit end must be a valid offset (`DWARFUnit.cpp:307-312`). | KEEP these acceptance ranges. | SPEC for version/format (DWARF5 §7.4, §7.5.1); LLVM-IMPL for the address-size set (the standard does not enumerate sizes) | T-UNIT-2 |
| U-03 | Address size 1 (or 0, 3, 5…) | Rejected by LLVM (`DWARFUnit.cpp:339-342`). | KEEP rejection even though `gimli` may accept 1 (plan). | LLVM-IMPL | T-UNIT-3 |
| U-04+ | `unit_length` smaller than the header; reserved initial-length values `0xfffffff0..0xfffffffe` | Only `isValidOffset(next_unit − 1)` is checked (`DWARFUnit.cpp:307`); a length too short to contain the header is not rejected there. Reserved-value handling inside `getInitialLength` was not read. | TIGHTEN: reject both (DWARF5 §7.4/§7.5.1 define the length as covering the rest of the unit). Old result **unmeasured**. | SPEC DWARF5 §7.5.1.1 (`unit_length`) | T-UNIT-4 |
| U-05+ | Unknown v5 `unit_type` | Value stored, no validation; header layout then assumes the plain layout (`DWARFUnit.cpp:272-293`). | DECIDE (D-07): proposed TIGHTEN to the values defined by DWARF5 Table 7.2 (§7.5.1) and the user range; verify the table contents before coding (not re-read here). | SPEC DWARF5 §7.5.1 Table 7.2 | T-UNIT-5 |
| U-06 | Which units are enumerated | `.debug_info` only: includes DWARF5 type/skeleton/partial units stored there; DWARF4 `.debug_types` units are added to LLVM's table **after** `finishedInfoUnits()` (`DWARFContext.cpp:291-297`) and are expected to be excluded from `info_section_units()`. | KEEP; `.debug_types`, `.debug_info.dwo`, `.dwo`, `.dwp` stay out of scope (no support added). **Unmeasured** (M-UNITS). | CH | T-UNIT-6 |
| U-07 | Abbreviation-table codes and IDs | Code is a 32-bit key (`DWARFDebugAbbrev.cpp:33,49`); tag/attr/form ULEBs are truncated to 16 bits (`DWARFAbbreviationDeclaration.cpp:50,71,75`). DIE side rejects `abbrev_code > UINT32_MAX` and unknown codes (`F.cpp:478-480`). | TIGHTEN (see C2): reject tag/attr/form IDs above 0xFFFF, and abbreviation declaration codes above `UINT32_MAX`; keep the DIE-side checks. | SPEC DWARF5 §7.5.3 (ULEB-encoded codes, tags, names); the 16-bit width is LLVM-IMPL | T-UNIT-7 |
| U-08 | Duplicate abbreviation code in one table | Both stored; if codes are not consecutive the lookup is a linear scan that returns the **first** match (`DWARFDebugAbbrev.cpp:43-48, 60-73`). | **TIGHTEN** (plan): `CANNOT_PARSE_DWARF`. | SPEC DWARF5 §7.5.3 (the code identifies the declaration); no explicit "must be unique" sentence was verified | T-UNIT-8 |
| U-09 | Abbreviation with tag 0 | Rejected (`DWARFAbbreviationDeclaration.cpp:54-58`). | KEEP | SPEC DWARF5 §7.5.3 | T-UNIT-9 |
| U-10 | Children byte not 0/1 | `HasChildren = (byte == DW_CHILDREN_yes)`, so any other value means "no children" (`DWARFAbbreviationDeclaration.cpp:63`). | **TIGHTEN** (plan) | SPEC DWARF5 §7.5.3 + Table 7.4 (`DW_CHILDREN_no 0x00`, `DW_CHILDREN_yes 0x01`) | T-UNIT-10 |
| U-11 | Attribute/form pair rules | Both-zero ends the declaration; exactly one zero is an error; `implicit_const` is followed by an SLEB; an unterminated list or missing final code-0 is an error (`DWARFAbbreviationDeclaration.cpp:70-145`, `DWARFDebugAbbrev.cpp:34-51`). | KEEP | SPEC DWARF5 §7.5.3 | T-UNIT-11 |
| U-12 | Unknown form ID | Accepted at abbreviation parse; at use `extractValue` hits `llvm_unreachable("unsupported form")` (`DWARFFormValue.cpp:340-343`), i.e. undefined behavior in release. Only `DW_TAG_compile_unit` DIEs are guarded, by the `skipValue` pre-pass (`F.cpp:507`, `DWARFFormValue.cpp:209-211`). Also reachable through `DW_FORM_indirect`. | **TIGHTEN** (plan): `CANNOT_PARSE_DWARF`. Proposed timing: at first use, so an unused declaration with a vendor form still opens (D-08). | SAFETY | T-UNIT-12 |
| U-13 | Forms not gated by unit version | `strx` in a v4 unit, `GNU_*` in v5, etc. are decoded without a version check. | KEEP (no version gating). | LLVM-IMPL | T-FORM-1 |
| U-14 | Abbreviation offset | Offset ≥ `.debug_abbrev` size or a malformed table at that offset → `abbrevs == nullptr` → `CANNOT_PARSE_DWARF` at open (`DWARFDebugAbbrev.cpp:156-158`, `DWARFUnit.cpp:1061-1073`, `F.cpp:144-146`). Tables not referenced by any unit are never parsed. | KEEP (including "unreferenced malformed table is ignored"). | LLVM-IMPL | T-UNIT-13 |
| U-15 | Unit DIE unparseable at open | `tryExtractDIEsIfNeeded(CUDieOnly)` failure from the string-offsets contribution throws at open (`F.cpp:149-151`, `DWARFUnit.cpp:545-556`); other failures (bad abbreviation at the root, DIE past unit end) are only stderr warnings and leave an empty DIE array (`DWARFDebugInfoEntry.cpp:27-35, 43-66`), after which the walk proceeds anyway. | **TIGHTEN**: both are `CANNOT_PARSE_DWARF` at open. | SAFETY | T-UNIT-14 |
| U-16 | Empty unit / unclosed tree / cross-unit read | `parseEntries` starts at `header end` and reads through the whole-section extractor (`F.cpp:265, 455`), so a header-only unit or a tree missing its terminators consumes the next unit's bytes; failure only occurs when `eof()` is violated after the stack empties (`F.cpp:753-757`). | **TIGHTEN** (plan): reject at the declared unit end; every read (DIE header, attribute value, string, block) must stay inside the unit. An empty unit (no DIE) is rejected, not skipped (D-09). | SPEC DWARF5 §2.3 (sibling chains terminated by null entries), §7.5.1 | T-UNIT-15 |
| U-17 | Null DIE with empty stack | `CANNOT_PARSE_DWARF` "Stack underflow" (`F.cpp:472-473`). | KEEP | SPEC DWARF5 §2.3 | T-UNIT-16 |
| U-18 | Root tag | Any tag; the compile-unit special handling is keyed on the **tag value** `DW_TAG_compile_unit` of any DIE, not on being the root (`F.cpp:486`). | KEEP | CH | T-ATTR-8 |
| U-19 | Work bound | Zero-byte forms (`implicit_const`, `flag_present`) and zero-length DIEs consume the plan's checked work counter; DIE depth lives in an explicit vector (old: `std::vector<StackEntry>`, `F.h:63`). | KEEP | SAFETY | T-EXEC-5 |

## 5. Attribute forms (F)

Old decode: `parseAttribute` → `DWARFFormValue::extractValue` with the whole-`.debug_info` extractor and the unit's `FormParams` (`F.cpp:356-367`, `LLVM:…/DWARFFormValue.cpp:220-351`). Dispatch is on the **resolved** form (`val.getForm()`, after `indirect`) (`F.cpp:551`). Value columns come from four switch groups (`F.cpp:554-717`). "const group" below means the first group (`F.cpp:554-611`). Wire size is per `extractValue`; offset size 4 or 8 by 32/64-bit DWARF; "addr" is the unit address size.

| ID | Form(s) | Old wire decode | `attr_int` / `attr_str` (source) | Decision | Basis |
|---|---|---|---|---|---|
| F-01 | `addr` | `addr`-sized value (`DWARFFormValue.cpp:238-245`) | address group: `getAsAddress().value_or(0)`, `""` (`F.cpp:615-624`) | KEEP | SPEC DWARF5 §7.5.5 (classes), §7.5.6 Table 7.6 |
| F-02 | `data1/2/4/8`, `sdata`, `udata`, `flag`, `flag_present`, `loclistx`, `rnglistx`, `sec_offset`, `implicit_const` | fixed/LEB (`:263-309, 331-339`) | const group: `getRawUValue()`; `sdata`/`implicit_const` are the sign-extended bits (`F.cpp:554-567`); `attr_str` `""` except F-14 and filename cases | KEEP | SPEC DWARF5 Table 7.6. Note: `loclistx`/`rnglistx` are class `loclist`/`rnglist` in the standard but `SectionOffset` in LLVM's class table (`DWARFFormValue.cpp:68-69`), which matters for `toSectionOffset` (rows A-06, R-02). |
| F-03 | `data16` | 16-byte block, `uval = 16` (`:292-296`) | const group: `attr_int = 16`, `attr_str = ""` (plan seed) | KEEP; the numeric 16 is a ClickHouse/LLVM artifact, not the value of the 128-bit constant | CH; SPEC for the wire size (DWARF5 Table 7.6) |
| F-04 | `block`, `block1/2/4`, `exprloc` | length then bytes (`:246-262, 347-349`) | block group: `attr_str` = raw bytes (embedded NULs preserved), `attr_int = 0` (`F.cpp:638-648`) | KEEP | SPEC DWARF5 §7.5.5 |
| F-05 | `string` | NUL-terminated inline (`:314-316`) | string group: bytes up to NUL (`strlen`, `F.cpp:664-683`) | KEEP | SPEC |
| F-06 | `strp`, `line_strp` | offset-size offset; looked up in `.debug_str` / `.debug_line_str` (`DWARFFormValue.cpp:618-622`) | string group; offset beyond the section or no NUL → `CANNOT_PARSE_DWARF` (`F.cpp:664-667`) | KEEP | SPEC DWARF5 §7.5.6 |
| F-07 | `strx`, `strx1..4` | ULEB / 1..4 bytes, resolved via the unit's string-offsets contribution | string group | KEEP; see A-04 for contribution rules | SPEC DWARF5 §7.26 |
| F-08 | `GNU_str_index` (`0x1f02`) | ULEB, treated like `strx` (`DWARFFormValue.cpp:603-615`) | string group; in a non-DWO unit with no contribution it fails (A-04) | KEEP | VENDOR: "Extensions for Fission proposal" (`LLVM:include/llvm/BinaryFormat/Dwarf.def:726-728`) |
| F-09 | `strp_sup` (`0x1d`) | offset-size | resolved against the **local** `.debug_str` (`DWARFFormValue.cpp:618-622`, no supplementary file) (`F.cpp:655`) | KEEP (plan seed). The class is `string` in the standard, but DWARF5 says the offset is into the **supplementary object file's** `.debug_str` (§7.4 form-role table and §7.5.5 string class), so this result is a ClickHouse/LLVM approximation, not spec behavior. | SPEC for class; **CH** for the local lookup |
| F-10 | `GNU_strp_alt` (`0x1f21`) | offset-size | `getAsCString` returns "Unsupported form" → `CANNOT_PARSE_DWARF` (`DWARFFormValue.cpp:597-600`) | KEEP rejection (plan seed) | VENDOR: "Alternate debug sections proposal (output of dwz)" (`Dwarf.def:729-731`) |
| F-11 | `ref1/2/4/8`, `ref_udata` | unit-relative | `attr_int = unit_offset + value` (wrapping), `attr_str = ""` (`F.cpp:699-712`, `DWARFFormValue.cpp:668-681`) | KEEP | SPEC DWARF5 §7.5.5 (unit-relative references) |
| F-12 | `ref_addr` | `getRefAddrByteSize()`: address size in DWARF 2, offset size otherwise (`Dwarf.h:1121-1125`) | `attr_int = value` as is (already `.debug_info`-relative) | KEEP | SPEC DWARF5 §7.5.5, §7.5.6 Table 7.6 |
| F-13 | `ref_sig8`, `ref_sup4`, `ref_sup8`, `GNU_ref_alt` | 8 / 4 / 8 / offset-size bytes (`DWARFFormValue.cpp:282, 289, 323, 334-336`) | `attr_int = 0`, `attr_str = ""` (plan seed; neither `getAsRelativeReference` nor `getAsDebugInfoReference` matches, `F.cpp:702-707`) | KEEP (value is deliberately dropped) | SPEC for the wire forms (Table 7.6); **CH** for the 0. `GNU_ref_alt`: VENDOR (`Dwarf.def:729-731`) |
| F-14 | `DW_AT_language`, `DW_AT_encoding` with a const-group form | — | `attr_str` = `LanguageString` minus `DW_LANG_` / `AttributeEncodingString` minus `DW_ATE_` of `static_cast<uint32_t>(value)`; unknown → `""` (`F.cpp:600-609`) | KEEP; names generated per N-03 | CH |
| F-15 | `addrx`, `addrx1..4`, `GNU_addr_index` (`0x1f01`) | ULEB / 1..4 bytes | address group: `getAsAddress().value_or(0)` (below) | KEEP | SPEC DWARF5 §7.27 for `addrx*`; `GNU_addr_index` VENDOR (`Dwarf.def:726-728`) |
| F-16 | `indirect` | ULEB form code, repeated until a non-indirect form (`:317-320, 345`) | dispatch on the resolved form; `attr_form` shows the declared `indirect` (plan seed) | KEEP; loop is bounded by input bytes | SPEC DWARF5 Table 7.6 |
| F-17+ | `indirect` that resolves to `implicit_const` or an unknown form | `implicit_const` branch is a no-op that keeps the value `0` (`:337-339`); unknown → U-12 | Unknown forms follow U-12. Qualify `implicit_const` against the applicable specification and errata (D-10); no new rejection is authorized merely by a dependency difference. | DWARF5 §7.5.3 and applicable errata must establish any claimed spec-invalidity | T-FORM-3 |
| F-18 | `implicit_const` | SLEB read in the abbreviation, no bytes in the DIE (`DWARFAbbreviationDeclaration.cpp:96-101`; `F.cpp:360-361`) | const group | KEEP | SPEC DWARF5 §7.5.3 |
| F-19 | SLEB/ULEB decoding | `decodeULEB128`/`decodeSLEB128` reject encodings that overflow 64 bits and report past-end (`LLVM:include/llvm/Support/LEB128.h:130-200`); `skipValue` ignores LEB errors (`DWARFFormValue.cpp:196`) and can fail to advance. | KEEP rejection of overflow/past-end (checked reader); no "fail to advance" behavior. | SPEC DWARF5 §7.6; SAFETY | T-FORM-2 |
| F-20 | `LLVM_addrx_offset` (`0x2001`) | ULEB index `<< 32`, then a `u32` offset, packed into one `uval` (`DWARFFormValue.cpp:310-313`; skip: `199-202`) | address group: index = `uval >> 32` (as `uint32_t`), result = address-table entry + low 32 bits (`DWARFFormValue.cpp:644-657`); no addr base / out of range → `0`. **Not accepted by `parseAddress`**, so `ranges` and the compile-unit pre-pass throw (C3) | KEEP exactly this bit arithmetic, including the 32-bit truncation of an oversized ULEB index (D-11) | **VENDOR**: defined only by LLVM, `Dwarf.def:732-733` (`HANDLE_DW_FORM(0x2001, LLVM_addrx_offset, 0, LLVM)`, comment "LLVM addr+offset extension"), emitted by `LLVM:lib/CodeGen/AsmPrinter/DwarfCompileUnit.cpp:144`, exercised by `LLVM:test/DebugInfo/X86/ranges_always.ll:72`. Upstream permalink (not fetched): `https://github.com/llvm/llvm-project/blob/llvmorg-22.1.1/llvm/include/llvm/BinaryFormat/Dwarf.def#L733`. `gimli` 0.33.0 defines no such form (`DW_*` constants have no `0x2001` form entry; `constants.rs:520,1095` are unrelated `DW_AT`/`DW_LNCT`). |
| F-21 | Value read crosses the unit end | Reads use the whole-section extractor, so values may run into the next unit (`F.cpp:265`). | **TIGHTEN** (plan row "cross-unit walk") | SAFETY | T-UNIT-15 |
| F-22 | Block/`data16` length longer than the remaining bytes | `getBytes` error → `extractValue` false → `CANNOT_PARSE_DWARF` (`DWARFFormValue.cpp:347-350`, `F.cpp:362-365`) | KEEP | SAFETY | T-FORM-4 |

## 6. Attribute-to-column adapters (A)

| ID | Case | Old behavior (source) | Decision | Basis | Test |
|---|---|---|---|---|---|
| A-01 | Which occurrence wins | First occurrence wins for `name`, `linkage_name`, `decl_file`, `decl_line` (`std::exchange(need_*, false)`, `F.cpp:569, 589, 672, 677`); **last** wins for `low_pc`, `high_pc`, `ranges`, `ranges_rnglistx` (`F.cpp:574-581, 630-632`), and for `unit_name`/`unit.filename_table` first-built (A-05). Duplicate attributes are still all decoded and listed in `attr_*`. | KEEP exactly | CH | T-ATTR-1 |
| A-02 | Form class gating of dedicated columns | `name`/`linkage_name`: only string-group forms (`F.cpp:670-678`). `decl_line`/`decl_file`: only const-group forms (`F.cpp:569, 585`). A `DW_AT_name` with a non-string form yields `""` in `name`. | KEEP | CH | T-ATTR-2 |
| A-03 | `decl_file` / `DW_AT_call_file` lookup | If value `< filename_table_size`, index `value + 1` into the unit filename table; for `decl_file` the first hit fills `decl_file`, and both fill `attr_str`; out-of-range or no table → `""` (`F.cpp:585-597`). | KEEP | CH | T-LINE-1 |
| A-04 | String-offsets base, strx | Root non-DWO unit, version ≥ 5: `DW_AT_str_offsets_base` found independently of attribute order with a section-offset-class form (`DWARFUnit.cpp:545,1168`). Contribution header/size validated by `parseDWARF{32,64}StringOffsetsTableHeader` and `validateContributionSize` (`:1086-1163`); no base means no contribution, so `strx` fails (`:246-250`). `Index` is `uint32_t`, `Index * ItemSize` wraps at 32 bits, bounds are section-wide, and contribution validation casts `Base` to `uint32_t` (`:246-259,1095`). In v≤4 non-DWO units indexed strings fail. | KEEP LLVM 22.1.1's valid accepted behavior, including `strx` before the base. Qualify oversized/cross-contribution indexes (D-13); tightening requires an independent specification citation and a regression test. | SPEC DWARF5 §7.26 for contribution layout; LLVM-IMPL for index/cast quirks | T-ATTR-3 |
| A-05 | `unit_name` | Assigned for any DIE with tag `compile_unit` and a string `DW_AT_name`, last wins, visible in every later chunk of that unit; value taken at chunk end (`F.cpp:674-675, 778-789`). Units whose root is not `compile_unit` have `""`. | KEEP | CH | T-ATTR-8 |
| A-06 | `DW_AT_stmt_list` and the filename table | Parsed once per unit, at the **first** DIE with `DW_AT_stmt_list` whose resolved form is section-offset class (`F.cpp:543-549`; class table `DWARFFormValue.cpp:31-80, 771-798`: `sec_offset`, `strp`, `line_strp`, `loclistx`, `rnglistx`, and `data4`/`data8` only for version ≤ 3). Lookups only succeed after the table exists, so attributes of an earlier DIE, or attributes ordered before `stmt_list` within a DIE, miss it. Missing `.debug_line` → `CANNOT_PARSE_DWARF` (`F.cpp:870-871`). | KEEP exactly (order-dependent). | CH | T-LINE-2 |
| A-07 | `size`, `decl_line` truncation | `static_cast<UInt32>` of a 64-bit value (`F.cpp:461, 570, 721`). | KEEP (plan); a `size` above `UINT32_MAX` needs a ≥4 GiB DIE and is not testable; recorded here. | CH | T-ATTR-9 (decl_line only) |
| A-08 | `high_pc` bookkeeping | Const-group `DW_AT_high_pc` sets `high_pc` **and** `relative_high_pc = true` regardless of projection; address-group `high_pc` is stored only when `ranges` is projected and never resets the relative flag (`F.cpp:572-576, 626-633`). | KEEP exactly (only matters for duplicate or mixed-form `high_pc`). | CH | T-RANGE-8 |

## 7. Names (N)

| ID | Case | Old behavior (source) | Decision | Basis | Test |
|---|---|---|---|---|---|
| N-01 | Tag/attribute/form dictionaries | Built at construction for IDs `1..65535`; entry 0 = `""`; name = `TagString`/`AttributeString`/`FormEncodingString` with `DW_TAG_`/`DW_AT_`/`DW_FORM_` removed (only if the name is at least that long); unknown → `unknown: <id>` (`F.cpp:154-212`, `LLVM:lib/BinaryFormat/Dwarf.cpp:21-30, 72-81, 105-114` — each returns the empty `StringRef` for unknown). | KEEP | CH (names themselves come from LLVM 22.1.1 `Dwarf.def`) | T-NAME-1 |
| N-02 | Source of truth for the new names | Five sparse `(id, name)` tables generated from the bundled LLVM (gitlink in section 0) and checked in as `rust/workspace/dwarf/src/names_generated.rs`. | KEEP; generator test compares against LLVM when available. | CH | T-NAME-1 |
| N-03 | Language and encoding names | `LanguageString(uint32)` minus `DW_LANG_`, `AttributeEncodingString(uint32)` minus `DW_ATE_`; unknown → `""` (`F.cpp:600-609`). | KEEP | CH | T-NAME-2 |
| N-04 | Name uniqueness | `ColumnUnique` requires unique strings, hence `unknown: N` filler (`F.cpp:176-181`). | KEEP (C++ adapter concern). | CH | — |

## 8. Addresses and ranges (R)

Old state: `UnitState::{debug_addr_base, rnglists_base, base_address}` initialized to `UINT64_MAX` as "unset" (`F.h:57-60`). The `.debug_addr`, `.debug_ranges` and `.debug_rnglists` extractors are built with a **hard-coded address size of 8**, whatever the unit says (`F.cpp:265-274`); `fetchFromDebugAddr` also reads a fixed 8 bytes per entry (`F.cpp:905-918`).

| ID | Case | Old behavior (source) | Decision | Basis | Test |
|---|---|---|---|---|---|
| R-01 | Compile-unit pre-pass | For **every** DIE whose tag is `DW_TAG_compile_unit` (including malformed nested ones), before its normal decode: standard `DW_AT_addr_base` → `debug_addr_base`, `DW_AT_rnglists_base` → `rnglists_base` (value = `getRawUValue()` of whatever form, no form-class check), `DW_AT_low_pc` decoded after the loop with `parseAddress` into `base_address`; other attributes use `skipValue`, which fails with `CANNOT_PARSE_DWARF` on unknown forms (`F.cpp:486-512, 369-388`). `GNU_addr_base` is **not** read here. Runs regardless of projection. `low_pc` forms other than `addr`/`addrx*` throw (C3). | KEEP (plan seed "On every DIE tagged `DW_TAG_compile_unit`…"). Non-root occurrences rerun only this pre-pass, not LLVM's string/GNU-address initialization (plan seed). | CH | T-RANGE-1, T-RANGE-2 |
| R-02 | Root-unit address base for `attr_int` | Computed once for the root: standard `DW_AT_addr_base`, else `DW_AT_GNU_addr_base`, requiring section-offset-class forms (`DWARFUnit.cpp:524-531`); absent base yields `0`. LLVM uses a 32-bit index, unit address size, unchecked offset arithmetic, then a section-wide bounds check (`:223-244`). | KEEP unit address size and valid accepted values. Unresolved/out-of-bounds lookups yield `0`; use safe checked bounds. Wrapped legacy results require qualification (D-14), not an assumption that every wrap yields `0`. | LLVM-IMPL | T-ATTR-5 |
| R-03 | Address forms for `ranges` | `parseAddress`: `addr` raw; `addrx`/`addrx1..4` → `fetchFromDebugAddr(unit.debug_addr_base, raw)`; otherwise throw (`F.cpp:390-401`). `fetchFromDebugAddr` throws if `.debug_addr` is missing, `addr_base == UINT64_MAX`, or `idx >= (size − base) / 8` (`F.cpp:905-918`). | KEEP (fixed 8-byte entries; throwing missing-base / out-of-bounds, plan seed). For 2/4-byte address units this reads 8 bytes per entry, contrary to the table header's `address_size`. | SPEC DWARF5 §7.27 describes a per-table `address_size`; the fixed 8 is **CH** | T-RANGE-3 |
| R-04 | When low/high are decoded for ranges | Only when `ranges` is projected and the form is in the address group: `low_pc`/`high_pc` via `parseAddress` (`F.cpp:626-633`). | KEEP | CH | T-RANGE-4 |
| R-05 | Contiguous range assembly | If `DW_AT_ranges` present: list only. Else if `low_pc`: `[low, high)` with `high = *high_pc` (address form), `low + *high_pc` wrapping (constant form), `low + 1` when absent; `high_pc` without `low_pc` → none (`F.cpp:730-746`). | KEEP (plan seed). `low_pc` alone ⇒ `[low, low+1)` is a ClickHouse convention; DWARF treats a lone `low_pc` as a single address. | SPEC DWARF5 §2.17.1-§2.17.2 for the two documented shapes; the `+1` is **CH** | T-RANGE-5 |
| R-06 | `DW_AT_ranges` selection | Only const-group forms set `ranges`; `ranges_rnglistx = (resolved form == rnglistx)` (`F.cpp:578-582`). | KEEP | CH | T-RANGE-6 |
| R-07 | DWARF 2–4 `.debug_ranges` | `unit version < 5`: `DWARFDebugRangeList::extract` with address size 8 and `(start,end)` pairs; offset must be valid; the end-of-list test is `isEndOfListEntry` (declared in the header, not read here; DWARF4 defines it as start 0 and end 0); an entry that does not advance by 16 bytes is an error; start `== UINT64_MAX` is a base-address selection entry; entries with `LowPC == UINT64_MAX − 1` are dropped **before** the base is added; if the active base `== UINT64_MAX − 1` the entry is dropped; otherwise base added with 64-bit wrap; empty and inverted ranges kept; base defaults to the compile-unit `low_pc` (`unit.base_address`, unset sentinel `UINT64_MAX`) and, when unset, entries are **not** offset (`LLVM:lib/DebugInfo/DWARF/DWARFDebugRangeList.cpp:31-65, 87-119`; `F.cpp:930-941`). Missing `.debug_ranges` throws. | KEEP (plan seed "skip only the current tombstone, preserve 64-bit wrapping"). | SPEC DWARF4 §2.17.3 for entry shape, base selection entry, default base, end-of-list; the `UINT64_MAX−1` tombstone and "no base ⇒ no offset" are **LLVM-IMPL** | T-RANGE-7 |
| R-08 | DWARF 5 `.debug_rnglists` | Raw entries via `RangeListEntry::extract` (kinds 0–7 only; unknown kind → error); list must start at `offset < section size`, and a list that reaches the section end without `end_of_list` is an error (`LLVM:…/DWARFDebugRnglists.cpp:20-86`, `DWARFListTable.h:230-252`). Conversion `getAbsoluteRanges(base_addr, 8, lookup)` (`F.cpp:968-979`, `DWARFDebugRnglists.cpp:95-173`): `base_addressx`/`startx_*` resolve through `fetchFromDebugAddr` which **throws** (so LLVM's "unresolved" fallbacks never run) with the index first truncated to `uint32_t`; `base_address` sets the base; `offset_pair` adds the base only if one is set; `start_end` and `start_length` read their address operand with the fixed 8-byte extractor; an entry is dropped when its start (for `offset_pair`: before and after adding the base) equals `UINT64_MAX`, and `offset_pair` is also dropped when the base equals `UINT64_MAX`; no other filtering (empty/inverted kept). Base defaults to `unit.base_address`. Missing `.debug_rnglists` throws. | KEEP (plan seed: raw iteration, no `gimli` converter). Index truncation to `uint32_t`: DECIDE (D-11) between preserving and rejecting `> UINT32_MAX`. Reject unterminated lists and unknown kinds with `CANNOT_PARSE_DWARF` (same as old). | SPEC DWARF5 §2.17.3 and §7.25 (entry kinds, Table 7.30); the `UINT64_MAX` tombstone, "offset_pair without base", and 8-byte operands are **LLVM-IMPL/CH** | T-RANGE-9 |
| R-09 | `rnglistx` indirection | `rnglists_base` must be set; `idx` bounded as `idx < (size − base) / entry_size`; entry read little-endian with `entry_size` = unit offset size; `entry <= size − base`; `offset = base + entry` (`F.cpp:947-966`). | KEEP (this arithmetic is already checked) | SPEC DWARF5 §3.1.1 item 15 (base points at the offsets table after the header), §7.28 | T-RANGE-10 |
| R-10 | `DW_AT_ranges` with direct offset in v5 | `offset` taken as a raw `.debug_rnglists` offset; header ignored (`F.cpp:968-972`). | KEEP | SPEC DWARF5 Table 7.6 (`sec_offset` into `.debug_rnglists`) | T-RANGE-9 |
| R-11 | Values near wrap | `low + high`, `LowPC + base` wrap modulo 2^64 with no check. | KEEP (use wrapping arithmetic). | CH (no standard text was cited for range arithmetic) | T-RANGE-11 |

## 9. Line table and filenames (L)

Old path: `parseFilenameTable` (`F.cpp:868-903`) → `llvm::DWARFDebugLine::Prologue::parse` (`LLVM:lib/DebugInfo/DWARF/DWARFDebugLine.cpp:364-481`). Only the prologue is parsed; the line program is never run.

| ID | Case | Old behavior (source) | Decision | Basis | Test |
|---|---|---|---|---|---|
| L-01 | Filename table layout | Dictionary `["", ("<invalid>" if version ≤ 4), name_0, name_1, …]`, `size = len − 1`; v2–v4 file numbering is one-based and v5 zero-based (`F.cpp:883-902`, comment `885`). Directory is never joined: only the entry's name. | KEEP | SPEC DWARF5 §6.2.4 (file numbering) for numbering; the `<invalid>` placeholder and no-join are **CH** | T-LINE-3 |
| L-02 | Unsupported line version | Version outside 2–5 → hard error (`DWARFDebugLine.cpp:378-388`) → `CANNOT_PARSE_DWARF`. | KEEP | LLVM-IMPL | T-LINE-4 |
| L-03 | Recoverable failures | LLVM logs and continues on malformed directory/file tables, opcode base 0, v5 address-size mismatch, or trailing unknown prologue data (`DWARFDebugLine.cpp:394-410,426-479`); partial filename tables may be used (`F.cpp:874-878`). | TIGHTEN independently established malformed headers/tables/entries under the approved rule. Preserve valid accepted behavior; classify non-table recoverables before implementation (D-15). | SPEC DWARF5 §6.2.4; a warning alone is not proof of spec-invalidity | T-LINE-5 |
| L-04 | Filename string failure | `entry.Name.getAsCString()` error → name `"<error>"`, error consumed (`F.cpp:890-896`). Reachable for v5 names whose form is `line_strp`/`strp`/`strx*` pointing outside the string section, a non-string form, `GNU_strp_alt`, or `strx` with no contribution (`DWARFFormValue.cpp:591-632`). | KEEP only this failure → `<error>` (plan) | CH | T-LINE-6 |
| L-05+ | v5 entries with non-constant `directory_index`/`timestamp`/`size` forms | `*Value.getAsUnsignedConstant()` dereferences an empty `optional` (`DWARFDebugLine.cpp:330-338`), which is undefined behavior. | **TIGHTEN** | SAFETY; SPEC DWARF5 §6.2.4.1 lists the allowed forms | T-LINE-7 |
| L-06 | v5 forms inside the header | Forms read with `extractValue` using the line section extractor and the unit as context (`DWARFDebugLine.cpp:292, 319`), so `strx*` in a line header uses the unit's string offsets. | KEEP | LLVM-IMPL | T-LINE-8 |
| L-07 | `DW_AT_stmt_list` offset out of range, or no `.debug_line` | Out of range → prologue parse error → `CANNOT_PARSE_DWARF` (`F.cpp:880-881`); no section → `CANNOT_PARSE_DWARF` (`F.cpp:870-871`). | KEEP | CH | T-LINE-9 |

## 10. Execution and lifetime (X)

| ID | Case | Old behavior (source) | Decision | Basis | Test |
|---|---|---|---|---|---|
| X-01 | Chunking | ≤ 65536 rows per chunk; boundaries 65535/65536/65537 exercise the `while (num_rows < 65536)` cap and the end check (`F.cpp:438, 753-758`). | KEEP | CH | T-EXEC-1 |
| X-02 | Threads / backpressure | `num_threads` workers; a worker waits while `delivery_queue.size() > num_threads` (`F.cpp:292-327`); a unit being parsed leaves the queue and is pushed back to the **front** when not at EOF (`F.cpp:325-326`). | KEEP | CH | T-EXEC-2 |
| X-03 | `getApproxBytesReadForChunk` | `.debug_info` bytes the unit advanced for that chunk; the first chunk excludes the unit header (`F.cpp:313-322`, `F.h:62`). | KEEP | CH | T-EXEC-2 |
| X-04 | Cancellation | See C4. Old cancel is chunk-granular and does not notify. | New parser is stricter: cancel within 4096 work items; not a parity item. | CH | T-EXEC-3 |
| X-05 | Error delivery | A worker exception is stored and rethrown by the next `read()` **before** queued chunks are returned (`F.cpp:1007-1008`); chunks already produced by other workers are lost. Rows emitted before a later error are therefore not an oracle (plan). `read()` failure stops the workers (`F.cpp:995-1001`). | KEEP error class; rows before the error unspecified. | CH | T-EXEC-4 |
| X-06 | Open-time errors | `initializeIfNeeded` runs on the first `read()` (`F.cpp:246-336`), so every open error surfaces there, not at schema inference. | KEEP | CH | T-ELF-* |
| X-07 | Reset | See C4: stale optional members survive `resetParser`. | New parser must destroy all file/unit handles and have no stale state; differential comparison across a reset is not an oracle. | SAFETY | T-EXEC-7 |
| X-08 | Projection of unknown column names | `THERE_IS_NO_COLUMN` at construction. | KEEP | CH | existing 05139 |
| X-09 | File identity / path reopen | See C7. | New parser validates handle/file generation on every call. | SAFETY | T-EXEC-7 |

## 11. Tests

### 11.1 Existing tests that touch the format (state at baseline)

| Test | What it covers | Notes |
|---|---|---|
| `tests/queries/0_stateless/02896_dwarf_format.sh` + `.reference` | `select *` and every single column on `data_dwarf/tiny_dwarf4`, `tiny_dwarf5` | tags `no-fasttest, no-msan, no-ubsan` (C5) |
| `tests/queries/0_stateless/05139_dwarf_format_header_types.sh` + `.reference` | requested-type conversion, `THERE_IS_NO_COLUMN`, `INSERT … FROM INFILE` | tags `no-fasttest, no-msan, no-ubsan`; not in the plan's file list (C6) |
| `tests/queries/0_stateless/04340_malformed_elf_dwarf_bounds.sh` + `.reference` | `overflow_shoff`, `overflow_phoff`, `too_small`, `no_sections` → `CANNOT_PARSE_ELF` | fixtures zero `EI_CLASS/DATA/VERSION` (`:26`); must be rewritten before E-02/E-03 land; comments claim `DB::Elf` ownership |
| `tests/queries/0_stateless/03641_clickhouse_prefer_symbols_from_contrib_over_rust_crates.sh` | queries the running binary's own DWARF with `max_threads=64` | uses the format as an end-to-end large-file smoke; tags `long, no-msan, no-parallel` |
| Consumers | `ci/scripts/find_symbols.py:101-104` (`tag`, `linkage_name`, `ranges[1].1`, `decl_file`); `tests/integration/test_storage_kafka/test_batch_slow_5.py:91` lists `DWARF` among formats | confirms column names/types are part of external use |
| Fixtures | `tests/queries/0_stateless/data_dwarf/{tiny_dwarf4,tiny_dwarf5,tiny.cpp}` | DWARF 4 and 5 only |

### 11.2 Pending permanent tests (none exist; all are PENDING)

Each is a new functional test created with `./tests/queries/0_stateless/add-test`; fixtures are generated in the test by a small script, as `04340` does. Names are assigned at creation.

- **T-ELF-1..12**: magic/size; ident class/data/version; header entry sizes; extended-numbering rejection; OOB section (non-NOBITS vs NOBITS); shstrtab wrong type / not NUL-terminated; bad `sh_name`; `ET_REL` without program headers; section name matching; NOBITS `.debug_info`; compressed section; duplicate/aliased names. All assert error **class** only.
- **T-UNIT-1..16**: truncated and malformed unit headers; version 2/3/4/5; address sizes 2/4/8 accepted and 1 rejected; short `unit_length`; unit types; `.debug_types`; tag/attr/form above 0xFFFF; duplicate abbreviation codes; null tag; children byte 2; pair rules; unknown form (direct and via `indirect`); abbreviation offset out of range; unparseable unit DIE; empty unit / unclosed tree / cross-unit read; null DIE with empty stack.
- **T-FORM-1..4**: every form in section 5 for value and `attr_*` output; LEB overflow; `indirect → implicit_const`; oversized block.
- **T-ATTR-1..9**: duplicate attributes; non-string `DW_AT_name`; `strx` before and after `DW_AT_str_offsets_base` plus invalid contribution; root `addr_base` vs `GNU_addr_base`; indexed addresses in range / out of range / no base; non-root `compile_unit`; `unit_name` last-wins; `decl_line` truncation.
- **T-LINE-1..9**: `decl_file`/`call_file` lookup; `stmt_list` ordering and missing `.debug_line`; v2–v5 table layouts; unsupported line version; malformed tables; `<error>` names; non-constant v5 fields; stray forms; out-of-range `stmt_list`.
- **T-RANGE-1..11**: pre-pass on every `compile_unit` DIE; unsupported `low_pc` form; `.debug_addr` bounds; `low`/`high` combinations; no-`high_pc` `+1`; v4 lists (base selection, tombstone, wrap, empty/inverted); `high_pc` sticky flag; v5 entry kinds and tombstone; `rnglistx`; wrapping; unreferenced lists.
- **T-PROJ-1..**: every single column, dependency pairs (`attr_*` without `attr_name`, `ancestor_offsets` alone), all columns; output length equality and valid offsets in each chunk.
- **T-NAME-1..2**: exhaustive 65536-entry comparison of generated tables with LLVM 22.1.1 (when LLVM is built); language/encoding.
- **T-EXEC-1..7**: 65535/65536/65537-row units; multi-unit threading and `approx bytes`; cancel; error propagation; bounded work counter; mmap vs piped input; reset and reuse (including a second input that lacks `.debug_line`, `.debug_addr`, `.debug_ranges`, `.debug_rnglists`).

## 12. Unmeasured subset qualifications (nothing below has been run)

The pre-cutover `clickhouse local` oracle binary (`26.11.1.2095`, SHA-256 `f668f0419cf716196809f5c03e0650f0c096cfdf89e44d94e7d43632989f7ba3`) is used in section 12.1 and section 14.

- **M-ELF**: valid ELF32 and big-endian inputs; NOBITS `.debug_*`; `SHF_COMPRESSED`/`.zdebug_*` inputs; `ET_REL`, `.dwo`, `.dwp`; unaligned section/program header tables; extended section numbering; sections with duplicate or dotless names.
- **M-UNITS**: DWARF 2 and 3 (no checked-in fixture; only 4 and 5); 64-bit DWARF; address sizes 2 and 4 across `.debug_addr`, `.debug_ranges`, `.debug_rnglists`; v5 type, skeleton, partial and split units inside `.debug_info`; DWARF 4 `.debug_types`; GCC and Clang output (executable, shared object, separate debug file); supplementary files (`dwz`); GNU split-DWARF forms in a non-DWO unit.
- **M-REJECT**: old-parser behavior on each TIGHTEN row, in particular E-02, E-03, E-06, U-04, U-07 (tag/attr/form truncation), U-08, U-10, U-12, U-15, U-16, L-03, L-05.
- **M-LIMITS**: a DIE `size` above `UINT32_MAX` is impractical to fixture; row A-07 records the truncation by source only.


### 12.1 Measured baseline (2026-11-06)

Oracle: pre-cutover `build_arm_release` (`26.11.1.2095`, SHA-256 `f668f0419cf716196809f5c03e0650f0c096cfdf89e44d94e7d43632989f7ba3`), run against the fixtures generated by `04340_malformed_elf_dwarf_bounds.sh`. Each fixture differs from the accepted `valid` control (1 row) in exactly one field.

| Fixture | Old result | New result (reference) | Row |
|---|---|---|---|
| `valid` | 1 row | 1 row | control |
| `overflow_shoff`, `overflow_phoff`, `too_small`, `no_program_headers`, `shstrtab_not_strtab` | `CANNOT_PARSE_ELF` | `CANNOT_PARSE_ELF` | E-01, E-04, E-07, E-09 KEEP |
| `elf32_class`, `big_endian_data`, `bad_ident_version` | 1 row (ident ignored) | `CANNOT_PARSE_ELF` | E-02 TIGHTEN |
| `bad_shentsize` | 1 row (`e_shentsize` ignored) | `CANNOT_PARSE_ELF` | E-03 TIGHTEN |
| `oob_irrelevant_section` | 1 row (section skipped) | `CANNOT_PARSE_ELF` | E-06 TIGHTEN (D-03 resolved) |
| `nobits_debug_info` | 1 row (file bytes read as DWARF) | `CANNOT_PARSE_ELF` (no `.debug_info` data) | E-11 TIGHTEN (D-05 resolved) |
| `unclosed_tree` | `CANNOT_PARSE_DWARF` | `CANNOT_PARSE_DWARF` | U-16 KEEP class |

These tightenings apply in the Rust-backed `DWARF` parser.
## 13. Design decisions resolved during qualification

| ID | Question | Proposed default |
|---|---|---|
| D-01 | Confirm the C2 reading of "code above `UInt16`". | tag/attr/form > 0xFFFF and abbreviation code > `UINT32_MAX` rejected |
| D-02 | Tighten ELF ident to ELFCLASS64 / ELFDATA2LSB / EV_CURRENT (E-02) although old acceptance of other classes is unmeasured. | yes, per the plan's ELF64-LE subset |
| D-03 | Out-of-file non-NOBITS irrelevant section (E-06) | reject `CANNOT_PARSE_ELF`; NOBITS exempt |
| D-04 | Header table alignment (E-03) | measure baseline and `object` behavior; dependency rejection alone cannot authorize rejecting valid accepted inputs |
| D-05 | NOBITS `.debug_*` (E-11) | treat as no data |
| D-06 | `SHF_COMPRESSED`/`.zdebug_*` (E-12) | measure baseline first; do not add decompression support or reject a valid accepted input merely because the new dependency interprets its flags differently |
| D-07 | Unknown v5 `unit_type` (U-05) | reject values outside DWARF5 Table 7.2 and the user range; verify the table first |
| D-08 | Time to reject an unknown form (U-12) | at first use |
| D-09 | Empty (header-only) unit (U-16) | reject |
| D-10 | `indirect` resolving to `implicit_const`/unknown form (F-17) | unknown forms follow the approved rejection policy; qualify `implicit_const` against the applicable DWARF specification and errata before claiming it is spec-invalid |
| D-11 | ULEB address index above `UINT32_MAX` (F-20, R-08) | preserve current index truncation and range conversion per the approved plan; rejection is permitted only with an independent spec-invalidity basis and a regression test |
| D-12 | Remove `no-ubsan` from `02896`/`05139` once the new parser has no UB | yes, with evidence |
| D-13 | `strx` index beyond the contribution or wrapped in 32 bits (A-04) | preserve LLVM's valid accepted behavior; any tightening requires an independent specification citation and a regression test, not merely `gimli` disagreement |
| D-14 | Wrapped `.debug_addr` offset sum for `attr_int` (R-02) | use checked bounds for safety; qualify the old result rather than asserting that every wrapped lookup returns `0`; valid accepted values remain unchanged |
| D-15 | Non-table line-prologue recoverables (L-03) | preserve valid accepted behavior; tighten only independently established malformed encodings under the approved rule |

## 14. Rust crate: implemented behavior, measurements and deviations

Everything below was run against the baseline oracle (`26.11.1.2095`, see 12.1) with the crate built from this tree. "Identical" means
every row of every column (names, ids, strings as hex, arrays) is byte-equal to `select ... order by offset format TSV`.

### 14.1 Differential corpus (valid inputs: no value difference)

| Corpus | Result |
|---|---|
| `tiny_dwarf4`, `tiny_dwarf5` (2475 rows each) | identical; also equal to the 20 rows recorded in `02896_dwarf_format.reference` (`tests/tiny.rs`) |
| gcc 13.3 and clang, DWARF 2, 3, 4, 5, each `-O0`/`-O2` (aarch64 executables) | identical |
| 64-bit DWARF (`-gdwarf64`, DWARF 3/4/5, gcc and clang) | identical |
| `-fdebug-types-section` (DWARF 4 `.debug_types` and DWARF 5 type units in `.debug_info`), `-ffunction-sections --gc-sections` (tombstone ranges), `-g3`, `-g1`, `-gline-tables-only`, `-flto`, PIE, shared objects | identical |
| 7 MB shared objects linked from rustc output (`lib_ch_rust_dwarf.a` release, `librequired_libs.a` debug) | identical |
| ~120 synthetic inputs produced by the crate's tests (every form group, `strx`, `.debug_addr`, ranges v4/v5, line tables v2-v5, chunking) replayed through both binaries | identical, or a TIGHTEN/abort row below |

### 14.2 Measured outcomes for the open TIGHTEN/DECIDE rows

| Row | Old (measured) | New | Decision |
|---|---|---|---|
| E-02 ELF32 / big-endian / `EI_VERSION` 0 | accepted, rows returned | `CANNOT_PARSE_ELF` | TIGHTEN (D-02 resolved: yes) |
| E-03 `e_shentsize` 40 | accepted | `CANNOT_PARSE_ELF` | TIGHTEN |
| E-06 non-NOBITS section beyond EOF | accepted | `CANNOT_PARSE_ELF` | TIGHTEN (D-03 resolved) |
| E-11 `SHT_NOBITS` `.debug_info` | accepted | `CANNOT_PARSE_ELF` (absent) | TIGHTEN (D-05 resolved) |
| E-12 `SHF_COMPRESSED` `.debug_*` (ld `--compress-debug-sections=zlib`) | exit 0, **zero rows**, stderr warning `unsupported version 0` (the raw zlib header is read as a unit header) | `CANNOT_PARSE_DWARF` (unit header) | D-06 resolved: sections are passed through raw (no flag inspection, no decompression); the user-visible difference is U-01: silent empty result becomes an error. Compressed sections the format never reads (e.g. `.debug_loc`) stay irrelevant |
| E-13 `debug_info` vs `.debug_info` | not measurable with real toolchains | implemented as in E-13 (exact dotted names for format lookups; one leading dot stripped, first section wins, for LLVM's `.debug_abbrev`/`.debug_str`/`.debug_line_str`/`.debug_str_offsets`/indexed-address `.debug_addr`); a `debug_info` section that wins LLVM's map over the dotted `.debug_info` is `CANNOT_PARSE_ELF` because units and DIE bytes would come from different sections | TIGHTEN (unmeasured corner) |
| ET_REL (`.o`), `.dwo` | `CANNOT_PARSE_ELF` | `CANNOT_PARSE_ELF` | KEEP (measured, gcc and clang) |
| split DWARF skeleton in the linked executable | `CANNOT_PARSE_DWARF` | `CANNOT_PARSE_DWARF` (`Missing DW_AT_addr_base` / `rnglists_base`) | KEEP class |
| U-01 bad/truncated/unsupported-version/bad-address-size/reserved-length/past-section unit header, trailing bytes after the last unit | exit 0, units before it returned, stderr warning | `CANNOT_PARSE_DWARF` at `ch_dwarf_file_open` | TIGHTEN |
| U-05 unknown v5 `unit_type` (0x42) | accepted | `CANNOT_PARSE_DWARF` (`gimli` rejects every value outside DW_UT_compile/type/partial/skeleton/split_compile/split_type, **including the user range**; D-07 proposed default would have kept it) | TIGHTEN, flagged |
| U-07 abbreviation code > `UINT32_MAX` | accepted | `CANNOT_PARSE_DWARF` | TIGHTEN (measured) |
| U-08 duplicate abbreviation code | accepted | `CANNOT_PARSE_DWARF` | TIGHTEN (measured) |
| U-10 children byte 2 | accepted (no children) | `CANNOT_PARSE_DWARF` | TIGHTEN (measured) |
| U-11 abbreviation table without final 0 code | `CANNOT_PARSE_DWARF` | `CANNOT_PARSE_DWARF` | KEEP; `gimli` alone would accept, so `file.rs` detects it with `probe::Probe` |
| U-14 malformed table that no unit references | ignored | ignored | KEEP (test `abbreviation_table_failures_are_dwarf_errors`) |
| U-15/U-16 root DIE null/unknown abbreviation/undecodable, unclosed tree, bytes after the tree | DWARF error or silent corruption | `CANNOT_PARSE_DWARF` at `ch_dwarf_unit_open` (root) or at the chunk (rest); every read is bounded by the unit | TIGHTEN |
| F-16 `DW_FORM_indirect` form code above 16 bits | LLVM truncates to 16 bits | `CANNOT_PARSE_DWARF` | TIGHTEN (new row) |
| F-17 `indirect` -> `implicit_const` | value 0, no bytes | same | KEEP (D-10 resolved: no new rejection) |
| F-20, D-11 `LLVM_addrx_offset` | index and offset packed as in LLVM | same incl. 32-bit truncation of the index; `ranges` still rejects the form | KEEP |
| R-01 `DW_AT_addr_base`/`rnglists_base` with an inline `DW_FORM_string` value | LLVM stores a pointer in the integer (nondeterministic) | `UINT64_MAX`, i.e. a deterministic out-of-bounds error on use | new row, deterministic |
| R-02, D-14 indexed address arithmetic for `attr_int` | `Index * addr_size` in 32 bits, sum in 64 bits, section-wide bounds check | identical arithmetic with checked reads; a wrapped sum reads 0 like LLVM | KEEP |
| A-04, D-13 `strx` | 32-bit `Index * ItemSize`, contribution header validated as LLVM 22.1.1 (`valid_range` reproduces `isValidOffsetForDataOfSize`, including the zero-length-at-0 quirk) | identical (`unit.rs` exhaustive tests) | KEEP |
| L-03/D-15 line header: `minimum_instruction_length`, `maximum_operations_per_instruction` or `line_range` equal to 0 | accepted, rows returned | `CANNOT_PARSE_DWARF` (`gimli::LineProgramHeader`) | TIGHTEN (`gimli::LineProgramHeader` rejects zero step parameters that would divide by zero in line state machines) |
| L-03 `segment_selector_size` != 0 (v5) | accepted | `CANNOT_PARSE_DWARF` | **flagged**: valid on segmented targets per DWARF 5 section 6.2.4, unsupported by `gimli`; no real producer emits it |
| L-03 v5 `address_size` 4 (valid for 32-bit targets) | **old aborts the process** (`Program aborted due to an unhandled Error`: the recoverable-error lambda drops the `llvm::Error` unconsumed when `LOG_INFO` is disabled) | accepted (`gimli` takes 1, 2, 4, 8); sizes 0 and 3 are rejected (old: abort) | accept; old behavior is a defect |
| L-03 any other *recoverable* prologue problem (opcode base 0, header length too short or too long, trailing header bytes, unterminated file table) | **old aborts** (same cause) | trailing header padding is accepted; the others are `CANNOT_PARSE_DWARF` | rule from the plan: the oracle does not decide where it aborts |
| L-03 line `unit_length` beyond `.debug_line` | accepted (LLVM clamps) | `CANNOT_PARSE_DWARF` | TIGHTEN (SPEC: DWARF 5 section 6.2.4 `unit_length`) |
| L-05 v5 path/directory form not of the string class and not parseable by `gimli` (`addr`, `flag_present`, `ref*`) | accepted, name `<error>` | `CANNOT_PARSE_DWARF` | TIGHTEN (SPEC: DWARF 5 section 6.2.4.1 allowed path forms). `data1`/`udata` etc. still yield `<error>` as before |
| L-04 string forms in file names that cannot be resolved (`line_strp` out of range, `strx` without table, `GNU_strp_alt`) | `<error>` | `<error>` | KEEP (measured with `line_strp`) |

### 14.3 Design facts the ledger rows depend on

- Units are enumerated from the exact `.debug_info`; `.debug_types` and DWARF 4 type units stay out (identical to old on `-fdebug-types-section` output).
- The compile-unit pre-pass decodes every attribute strictly (old: `skipValue`); its failure set is a subset of the failures the following walk reports.
- `unit_name` is reported by every chunk (`ChDwarfProgress.unit_name_*`); `CHUNK_READY` always has rows > 0; a unit whose tree closes on the 65536th row yields `UNIT_DONE` with that row (no empty trailer).
- After any error or `CANCELLED`, the unit handle refuses further calls (`CH_DWARF_ERR_INVALID_ARG`).
- Work accounting: one item per DIE, per attribute and per range record; the cancel callback is polled once per call start and then every 4096 items (`control::WORK_QUANTUM`).

### 14.4 Tests in the crate that cover rows of section 11.2 (run with `cargo test`; the `.sh` functional tests listed there remain PENDING unless 04340/02896/05139 already cover them)

| Section 11.2 group | Covered by |
|---|---|
| T-ELF-1..12 | `tests/malformed.rs::{elf_container_failures, missing_and_nobits_debug_sections_are_elf_errors}` (also 04340 fixtures) |
| T-UNIT-1..16 | `tests/malformed.rs::{unit_header_failures_are_dwarf_errors, abbreviation_table_failures_are_dwarf_errors, die_level_failures_are_dwarf_errors, reads_never_cross_into_the_next_unit, root_dies_that_cannot_be_initialized_fail_when_the_unit_opens}` |
| T-FORM-1..4 | `tests/semantics.rs::constant_address_block_string_and_reference_groups`, `src/attribute.rs` unit tests |
| T-ATTR-1..9 | `tests/semantics.rs` (first/last wins, non-string name, `strx` order and contribution errors, `addr_base` vs `GNU_addr_base`, pool lookups, nested `compile_unit`, `unit_name`), `tests/ranges_lines.rs::nested_compile_unit_dies_reset_the_range_state` |
| T-LINE-1..9 | `tests/ranges_lines.rs::{line_table_*, stmt_list_errors_and_ignored_forms}` |
| T-RANGE-1..11 | `tests/ranges_lines.rs::{dwarf4_*, dwarf5_*, ranges_are_only_decoded_*}`, `src/range.rs` unit tests |
| T-PROJ | `tests/ranges_lines.rs::every_projection_matches_the_full_decode` (all 65,536 column subsets), `invalid_projections_are_rejected_*` |
| T-NAME-1..2 | `tests/callbacks.rs::name_tables_are_complete_and_unique` (completeness, uniqueness, spot values); the exhaustive LLVM comparison is still PENDING |
| T-EXEC-1,3,4,5 | `tests/ranges_lines.rs::{chunking_and_progress, chunks_restart_*}`, `tests/callbacks.rs` (cancel, reserve failure, poll frequency, growth, threads, reopen) |
| Layout/ABI | `tests/abi_layout.rs` (header constants, struct layout checked by the system C99/C++17 compilers) |
| Valid fixtures | `tests/tiny.rs` |
