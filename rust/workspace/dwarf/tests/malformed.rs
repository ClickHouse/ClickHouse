//! Malformed ELF/DWARF corpus: every case must be rejected with the right error class (ELF-open
//! versus DWARF) before the first row of the bad unit is returned, and a rejected input never
//! panics or reads out of bounds.
#![allow(clippy::cloned_ref_to_slice_refs)]

mod common;

use common::*;
use gimli::constants::*;

const CU: u16 = 0x11;
const VAR: u16 = 0x34;

fn at(a: gimli::DwAt) -> u16 {
    a.0
}
fn form(f: gimli::DwForm) -> u16 {
    f.0
}

/// A valid one-unit DWARF 4 file: one CU DIE with a `name`, no children.
fn valid_parts() -> (Vec<Vec<u8>>, Vec<u8>) {
    (
        vec![abbrev(
            1,
            CU,
            false,
            &[(at(DW_AT_name), form(DW_FORM_string), None)],
        )],
        die(1, &[&cstr("a")]),
    )
}

fn valid_elf() -> Elf {
    let (decls, dies) = valid_parts();
    Elf::new(unit(4, Fmt::Dwarf32, 8, 0, &dies), abbrev_table(&decls))
}

fn patch(mut bytes: Vec<u8>, off: usize, data: &[u8]) -> Vec<u8> {
    bytes[off..off + data.len()].copy_from_slice(data);
    bytes
}

/// Reads every unit; returns the first failing status of open_file / open_unit / parse_chunk.
fn first_failure(bytes: Vec<u8>) -> Option<i32> {
    let file = match TestFile::open(bytes) {
        Ok(f) => f,
        Err((rc, _)) => return Some(rc),
    };
    let mut st = Collector::new();
    for i in 0..file.unit_count() {
        if let Err((rc, _)) = read_unit(&file, i, ALL_COLS, 65536, &mut st) {
            return Some(rc);
        }
    }
    None
}

#[test]
fn the_control_file_is_valid() {
    let file = TestFile::open(valid_elf().build()).unwrap();
    assert_eq!(file.unit_count(), 1);
    assert_eq!(rows_of(&file).rows, 1);
    assert_eq!(first_failure(valid_elf().build()), None);
}

// Field offsets of the ELF64 header and of section header `i` in the files built by `Elf`.
const E_SHOFF: usize = 0x28;
const E_PHOFF: usize = 0x20;
const E_SHENTSIZE: usize = 0x3a;
const E_PHNUM: usize = 0x38;
const E_SHNUM: usize = 0x3c;
const E_SHSTRNDX: usize = 0x3e;

fn section_header_at(bytes: &[u8], index: usize) -> usize {
    let shoff = u64::from_le_bytes(bytes[E_SHOFF..E_SHOFF + 8].try_into().unwrap()) as usize;
    shoff + index * 64
}

#[test]
fn elf_container_failures() {
    let good = valid_elf().build();
    let shnum = u16::from_le_bytes(good[E_SHNUM..E_SHNUM + 2].try_into().unwrap()) as usize;
    let shstr = shnum - 1;
    let sec = |i| section_header_at(&good, i);
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("tiny", b"\x7fELF".to_vec()),
        ("truncated header", good[..63].to_vec()),
        ("bad magic", patch(good.clone(), 0, b"\x7fELG")),
        ("ELF32", patch(good.clone(), 4, &[1])),
        ("big endian", patch(good.clone(), 5, &[2])),
        ("ident version", patch(good.clone(), 6, &[0])),
        (
            "e_shentsize",
            patch(good.clone(), E_SHENTSIZE, &40u16.to_le_bytes()),
        ),
        (
            "e_shoff overflow",
            patch(
                good.clone(),
                E_SHOFF,
                &0xffff_ffff_ffff_fff0u64.to_le_bytes(),
            ),
        ),
        (
            "e_shoff zero",
            patch(good.clone(), E_SHOFF, &0u64.to_le_bytes()),
        ),
        (
            "e_shnum zero",
            patch(good.clone(), E_SHNUM, &0u16.to_le_bytes()),
        ),
        (
            "e_shnum too large",
            patch(good.clone(), E_SHNUM, &0x4000u16.to_le_bytes()),
        ),
        (
            "e_shstrndx out of range",
            patch(good.clone(), E_SHSTRNDX, &(shnum as u16).to_le_bytes()),
        ),
        (
            "e_shstrndx extended",
            patch(good.clone(), E_SHSTRNDX, &0xffffu16.to_le_bytes()),
        ),
        (
            "no program headers",
            patch(
                patch(good.clone(), E_PHNUM, &0u16.to_le_bytes()),
                E_PHOFF,
                &0u64.to_le_bytes(),
            ),
        ),
        (
            "e_phoff overflow",
            patch(
                good.clone(),
                E_PHOFF,
                &0xffff_ffff_ffff_fff0u64.to_le_bytes(),
            ),
        ),
        (
            "shstrtab type",
            patch(good.clone(), sec(shstr) + 4, &1u32.to_le_bytes()),
        ),
        (
            "shstrtab offset overflow",
            patch(
                good.clone(),
                sec(shstr) + 24,
                &0xffff_ffff_ffff_fff0u64.to_le_bytes(),
            ),
        ),
        (
            "shstrtab empty",
            patch(good.clone(), sec(shstr) + 32, &0u64.to_le_bytes()),
        ),
        (
            "section name offset",
            patch(good.clone(), sec(1), &0x7fff_ffffu32.to_le_bytes()),
        ),
        (
            "out-of-file section",
            patch(
                good.clone(),
                sec(2) + 24,
                &0xffff_ffff_ffff_fff0u64.to_le_bytes(),
            ),
        ),
    ];
    for (name, bytes) in cases {
        let rc = first_failure(bytes).unwrap_or_else(|| panic!("{name}: must be rejected"));
        assert!(
            is_elf_class(rc),
            "{name}: status {rc} is not an ELF-class error"
        );
    }
}

#[test]
fn missing_and_nobits_debug_sections_are_elf_errors() {
    // No .debug_info / no .debug_abbrev at all.
    let (decls, _) = valid_parts();
    let mut no_info = Elf::new(vec![], abbrev_table(&decls));
    no_info.extra.clear();
    let mut bytes = no_info.build();
    // Rename `.debug_info` so neither exact name is present.
    let pos = bytes.windows(11).position(|w| w == b".debug_info").unwrap();
    bytes[pos + 1] = b'x';
    assert_eq!(first_failure(bytes), Some(ERR_MISSING_REQUIRED_SECTION));
    let mut bytes = valid_elf().build();
    let pos = bytes
        .windows(13)
        .position(|w| w == b".debug_abbrev")
        .unwrap();
    bytes[pos + 1] = b'x';
    assert_eq!(first_failure(bytes), Some(ERR_MISSING_REQUIRED_SECTION));
    // A NOBITS .debug_info is treated as absent (section header type 8).
    let good = valid_elf().build();
    let info = patch(
        good.clone(),
        section_header_at(&good, 2) + 4,
        &8u32.to_le_bytes(),
    );
    assert_eq!(first_failure(info), Some(ERR_MISSING_REQUIRED_SECTION));
    // Both spellings of a debug section: a leading dotless `debug_info` makes the walk ambiguous.
    let mut elf = valid_elf();
    elf.extra.push(("debug_info".into(), vec![0; 4]));
    // The dotless duplicate comes after `.debug_info`, so LLVM's map and the format agree.
    assert_eq!(first_failure(elf.build()), None);
}

#[test]
fn unit_header_failures_are_dwarf_errors() {
    let (decls, dies) = valid_parts();
    let table = abbrev_table(&decls);
    let good_unit = unit(4, Fmt::Dwarf32, 8, 0, &dies);
    let with_unit = |u: Vec<u8>| Elf::new(u, table.clone()).build();
    let mut cases: Vec<(&str, Vec<u8>)> = vec![
        ("truncated header", good_unit[..6].to_vec()),
        (
            "truncated payload",
            good_unit[..good_unit.len() - 1].to_vec(),
        ),
        (
            "trailing bytes",
            [good_unit.clone(), vec![0, 0, 0]].concat(),
        ),
        (
            "version 1",
            patch(good_unit.clone(), 4, &1u16.to_le_bytes()),
        ),
        (
            "version 6",
            patch(good_unit.clone(), 4, &6u16.to_le_bytes()),
        ),
        ("address size 1", patch(good_unit.clone(), 10, &[1])),
        ("address size 3", patch(good_unit.clone(), 10, &[3])),
        ("address size 0", patch(good_unit.clone(), 10, &[0])),
        ("address size 16", patch(good_unit.clone(), 10, &[16])),
        (
            "length shorter than header",
            patch(good_unit.clone(), 0, &4u32.to_le_bytes()),
        ),
        (
            "length covers only the header",
            patch(good_unit.clone(), 0, &7u32.to_le_bytes()),
        ),
        (
            "reserved length",
            patch(good_unit.clone(), 0, &0xffff_fff0u32.to_le_bytes()),
        ),
        (
            "length past the section",
            patch(good_unit.clone(), 0, &1000u32.to_le_bytes()),
        ),
        (
            "abbrev offset outside .debug_abbrev",
            patch(good_unit.clone(), 6, &1000u32.to_le_bytes()),
        ),
    ];
    // A second, healthy unit does not rescue a broken first one, nor the other way round.
    cases.push((
        "bad second unit",
        [
            good_unit.clone(),
            patch(good_unit.clone(), 4, &9u16.to_le_bytes()),
        ]
        .concat(),
    ));
    for (name, u) in cases {
        let rc = first_failure(with_unit(u)).unwrap_or_else(|| panic!("{name}: must be rejected"));
        assert!(is_dwarf_class(rc), "{name}: status {rc}");
    }
    // DWARF 5 unit types: unknown types are rejected, type units need a sane type_offset.
    let v5 = unit(5, Fmt::Dwarf32, 8, 0, &dies);
    assert_eq!(first_failure(with_unit(v5.clone())), None);
    let unknown_type = patch(v5.clone(), 6, &[0x42]);
    assert!(is_dwarf_class(
        first_failure(with_unit(unknown_type)).unwrap()
    ));
    // Empty .debug_info is a valid file with no units.
    let empty = TestFile::open(Elf::new(vec![], table).build()).unwrap();
    assert_eq!(empty.unit_count(), 0);
}

#[test]
fn abbreviation_table_failures_are_dwarf_errors() {
    let (_, dies) = valid_parts();
    let name = at(DW_AT_name);
    let f = form(DW_FORM_string);
    let ok_decl = abbrev(1, CU, false, &[(name, f, None)]);
    let unit_bytes = unit(4, Fmt::Dwarf32, 8, 0, &dies);
    let build = |table: Vec<u8>| Elf::new(unit_bytes.clone(), table).build();

    let mut bad: Vec<(&str, Vec<u8>)> = Vec::new();
    bad.push(("no terminator", ok_decl.clone()));
    bad.push(("empty table", vec![]));
    // children = 2: patch the byte after code (1) and tag (0x11)
    let mut kids = abbrev_table(&[ok_decl.clone()]);
    kids[2] = 2;
    bad.push(("children byte 2 (patched)", kids));
    bad.push((
        "duplicate code",
        abbrev_table(&[ok_decl.clone(), abbrev(1, VAR, false, &[])]),
    ));
    bad.push(("null tag", abbrev_table(&[abbrev(1, 0, false, &[])])));
    bad.push((
        "tag above 16 bits",
        abbrev_table(&[[uleb(1), uleb(0x1_0000), vec![0, 0, 0]].concat()]),
    ));
    bad.push((
        "form above 16 bits",
        abbrev_table(&[[
            uleb(1),
            uleb(u64::from(CU)),
            vec![0],
            uleb(3),
            uleb(0x1_0000),
            vec![0, 0],
        ]
        .concat()]),
    ));
    bad.push((
        "attribute zero with a form",
        abbrev_table(&[[
            uleb(1),
            uleb(u64::from(CU)),
            vec![0],
            uleb(0),
            uleb(8),
            vec![0, 0],
        ]
        .concat()]),
    ));
    bad.push((
        "form zero with an attribute",
        abbrev_table(&[[
            uleb(1),
            uleb(u64::from(CU)),
            vec![0],
            uleb(3),
            uleb(0),
            vec![0, 0],
        ]
        .concat()]),
    ));
    bad.push((
        "code above u32",
        abbrev_table(&[abbrev(0x1_0000_0001, CU, false, &[(name, f, None)])]),
    ));
    bad.push(("truncated LEB", vec![0x81]));
    bad.push(("truncated declaration", ok_decl[..3].to_vec()));
    bad.push((
        "unterminated attribute list",
        [uleb(1), uleb(u64::from(CU)), vec![0], uleb(3), uleb(8)].concat(),
    ));
    for (n, table) in bad {
        let rc = first_failure(build(table)).unwrap_or_else(|| panic!("{n}: must be rejected"));
        assert!(is_dwarf_class(rc), "{n}: status {rc}");
    }

    // A malformed table that no unit references is never parsed.
    let mut table = abbrev_table(&[ok_decl.clone()]);
    let unreferenced_at = table.len();
    table.extend([0xff, 0xff, 0xff]);
    assert_eq!(first_failure(build(table.clone())), None);
    // ... but a unit pointing at it fails.
    let pointing = Elf::new(
        unit(4, Fmt::Dwarf32, 8, unreferenced_at as u64, &dies),
        table,
    )
    .build();
    assert!(is_dwarf_class(first_failure(pointing).unwrap()));
}

#[test]
fn die_level_failures_are_dwarf_errors() {
    let name = at(DW_AT_name);
    let build = |decls: &[Vec<u8>], dies: Vec<u8>| {
        Elf::new(unit(4, Fmt::Dwarf32, 8, 0, &dies), abbrev_table(decls)).build()
    };
    let leaf = abbrev(1, CU, false, &[(name, form(DW_FORM_string), None)]);
    let parent = abbrev(1, CU, true, &[(name, form(DW_FORM_string), None)]);
    let child = abbrev(
        2,
        VAR,
        false,
        &[(at(DW_AT_byte_size), form(DW_FORM_data4), None)],
    );
    let cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "unclosed tree",
            build(&[parent.clone()], die(1, &[&cstr("a")])),
        ),
        (
            "unclosed nested tree",
            build(
                &[parent.clone(), child.clone()],
                [die(1, &[&cstr("a")]), die(2, &[&le32(1)])].concat(),
            ),
        ),
        ("null root", build(&[leaf.clone()], vec![0])),
        (
            "bytes after the tree closed",
            build(&[leaf.clone()], [die(1, &[&cstr("a")]), vec![0]].concat()),
        ),
        (
            "unknown abbreviation code",
            build(&[leaf.clone()], die(2, &[&cstr("a")])),
        ),
        (
            "abbreviation code above u32",
            build(&[leaf.clone()], [uleb(0x1_0000_0001), cstr("a")].concat()),
        ),
        (
            "truncated attribute",
            build(
                &[parent.clone(), child.clone()],
                [die(1, &[&cstr("a")]), die(2, &[&[1, 2]])].concat(),
            ),
        ),
        (
            "unterminated inline string",
            build(&[leaf.clone()], [uleb(1), b"abc".to_vec()].concat()),
        ),
        (
            "unknown form",
            build(
                &[abbrev(1, CU, false, &[(name, 0x7777, None)])],
                die(1, &[&[0; 8]]),
            ),
        ),
        (
            "unknown form through indirect",
            build(
                &[abbrev(
                    1,
                    CU,
                    false,
                    &[(name, form(DW_FORM_indirect), None)],
                )],
                die(1, &[&uleb(0x7777), &[0; 8]]),
            ),
        ),
        (
            "truncated LEB attribute",
            build(
                &[abbrev(1, CU, false, &[(name, form(DW_FORM_udata), None)])],
                die(1, &[&[0x80]]),
            ),
        ),
        (
            "overflowing LEB attribute",
            build(
                &[abbrev(1, CU, false, &[(name, form(DW_FORM_udata), None)])],
                die(1, &[&[0xff; 11]]),
            ),
        ),
        (
            "block longer than the unit",
            build(
                &[abbrev(1, CU, false, &[(name, form(DW_FORM_block1), None)])],
                die(1, &[&[200, 1, 2, 3]]),
            ),
        ),
        (
            "indirect form code above 16 bits",
            build(
                &[abbrev(
                    1,
                    CU,
                    false,
                    &[(name, form(DW_FORM_indirect), None)],
                )],
                die(1, &[&uleb(0x1_0001)]),
            ),
        ),
        (
            "ancestor underflow",
            build(
                &[parent.clone()],
                [die(1, &[&cstr("a")]), vec![0, 0]].concat(),
            ),
        ),
    ];
    for (n, bytes) in cases {
        let rc = first_failure(bytes).unwrap_or_else(|| panic!("{n}: must be rejected"));
        assert!(is_dwarf_class(rc), "{n}: status {rc}");
    }
}

#[test]
fn reads_never_cross_into_the_next_unit() {
    // The first unit's block claims more bytes than the unit has; the bytes exist in the section
    // (they belong to the second unit), but the read must stop at the unit boundary.
    let decls = [abbrev(
        1,
        CU,
        false,
        &[(at(DW_AT_name), form(DW_FORM_block1), None)],
    )];
    let table = abbrev_table(&decls);
    let u1 = unit(4, Fmt::Dwarf32, 8, 0, &die(1, &[&[40]]));
    let u2 = unit(4, Fmt::Dwarf32, 8, 0, &die(1, &[&[3, 7, 8, 9]]));
    let file = TestFile::open(Elf::new([u1, u2].concat(), table.clone()).build()).unwrap();
    let mut st = Collector::new();
    assert!(is_dwarf_class(
        read_unit(&file, 0, ALL_COLS, 65536, &mut st)
            .err()
            .unwrap()
            .0
    ));
    // The healthy second unit is unaffected.
    let (d, _) = read_unit(&file, 1, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(d.attr_str.unwrap()[0], vec![vec![7, 8, 9]]);
    // An inline string without its NUL inside the unit, with the NUL in the next unit.
    let decls = [abbrev(
        1,
        CU,
        false,
        &[(at(DW_AT_name), form(DW_FORM_string), None)],
    )];
    let u1 = unit(4, Fmt::Dwarf32, 8, 0, &[uleb(1), b"ab".to_vec()].concat());
    let u2 = unit(4, Fmt::Dwarf32, 8, 0, &die(1, &[&cstr("ok")]));
    let file = TestFile::open(Elf::new([u1, u2].concat(), abbrev_table(&decls)).build()).unwrap();
    assert!(is_dwarf_class(
        read_unit(&file, 0, ALL_COLS, 65536, &mut st)
            .err()
            .unwrap()
            .0
    ));
}

#[test]
fn root_dies_that_cannot_be_initialized_fail_when_the_unit_opens() {
    let decls = [abbrev(
        1,
        CU,
        true,
        &[(at(DW_AT_name), form(DW_FORM_string), None)],
    )];
    let table = abbrev_table(&decls);
    for (n, dies) in [
        ("null root", vec![0u8]),
        ("unknown abbreviation", die(5, &[&cstr("x")])),
        ("truncated attribute", [uleb(1), b"x".to_vec()].concat()),
    ] {
        let file =
            TestFile::open(Elf::new(unit(4, Fmt::Dwarf32, 8, 0, &dies), table.clone()).build())
                .unwrap();
        let e = file
            .open_unit(0)
            .err()
            .unwrap_or_else(|| panic!("{n}: must fail to open"));
        assert!(is_dwarf_class(e.0), "{n}");
    }
    assert_eq!(
        TestFile::open(valid_elf().build())
            .unwrap()
            .open_unit(5)
            .err()
            .unwrap()
            .0,
        ERR_INVALID_ARG
    );
}
