//! Row semantics on synthetic DWARF: tree walk, dedicated columns, every form group, string
//! resolution, `.debug_addr` pools, string-offset tables. Expected values come from the behavior
//! of the old parser (see COMPATIBILITY.md).

mod common;

use _ch_rust_dwarf::attribute::DW_FORM_LLVM_addrx_offset;
use common::*;
use gimli::constants::*;

const CU: u16 = 0x11;
const SUBPROGRAM: u16 = 0x2e;
const VARIABLE: u16 = 0x34;

fn at(a: gimli::DwAt) -> u16 {
    a.0
}
fn form(f: gimli::DwForm) -> u16 {
    f.0
}

fn strs(rows: &[&str]) -> Vec<Vec<u8>> {
    rows.iter().map(|s| s.as_bytes().to_vec()).collect()
}

#[test]
fn tree_rows_ancestors_and_null_entries() {
    let decls = [
        abbrev(1, CU, true, &[(at(DW_AT_name), form(DW_FORM_string), None)]),
        abbrev(
            2,
            SUBPROGRAM,
            true,
            &[(at(DW_AT_name), form(DW_FORM_string), None)],
        ),
        abbrev(
            3,
            VARIABLE,
            false,
            &[(at(DW_AT_name), form(DW_FORM_string), None)],
        ),
    ];
    let dies = [
        die(1, &[&cstr("cu")]),
        die(2, &[&cstr("f")]),
        die(3, &[&cstr("v")]),
        vec![0],
        die(3, &[&cstr("w")]),
        vec![0],
    ]
    .concat();
    let file = one_unit(4, 8, &decls, &dies, vec![]);
    let mut st = Collector::new();
    let (d, infos) = read_unit(&file, 0, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(d.rows, 6);
    assert_eq!(d.offset.as_ref().unwrap(), &[11, 15, 18, 21, 22, 25]);
    assert_eq!(d.size.as_ref().unwrap(), &[4, 3, 3, 1, 3, 1]);
    assert_eq!(
        d.tag.as_ref().unwrap(),
        &[CU, SUBPROGRAM, VARIABLE, 0, VARIABLE, 0]
    );
    // Innermost ancestor first; a null entry lists the parent it closes.
    assert_eq!(
        d.ancestor_tags.as_ref().unwrap(),
        &[
            vec![],
            vec![CU],
            vec![SUBPROGRAM, CU],
            vec![SUBPROGRAM, CU],
            vec![CU],
            vec![CU]
        ]
    );
    assert_eq!(
        d.ancestor_offsets.as_ref().unwrap(),
        &[
            vec![],
            vec![11],
            vec![15, 11],
            vec![15, 11],
            vec![11],
            vec![11]
        ]
    );
    assert_eq!(
        d.name.as_ref().unwrap(),
        &strs(&["cu", "f", "v", "", "w", ""])
    );
    assert_eq!(d.attr_name.as_ref().unwrap()[3], Vec::<u16>::new());
    // The first chunk starts after the 11-byte header and covers the rest of the unit.
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].bytes_advanced, 26 - 11);
    assert_eq!(infos[0].unit_name, b"cu");
    assert_eq!(file.open_unit(0).unwrap().offset, 0);
}

#[test]
fn dedicated_columns_first_wins_and_arrays_keep_everything() {
    let decls = [
        abbrev(
            1,
            CU,
            true,
            &[
                (at(DW_AT_stmt_list), form(DW_FORM_sec_offset), None),
                (at(DW_AT_name), form(DW_FORM_string), None),
            ],
        ),
        abbrev(
            2,
            VARIABLE,
            false,
            &[
                (at(DW_AT_name), form(DW_FORM_string), None),
                (at(DW_AT_name), form(DW_FORM_string), None),
                (at(DW_AT_linkage_name), form(DW_FORM_string), None),
                (at(DW_AT_linkage_name), form(DW_FORM_string), None),
                (at(DW_AT_decl_line), form(DW_FORM_data1), None),
                (at(DW_AT_decl_line), form(DW_FORM_data2), None),
                (at(DW_AT_decl_file), form(DW_FORM_data1), None),
                (at(DW_AT_decl_file), form(DW_FORM_data1), None),
            ],
        ),
        // A non-string DW_AT_name leaves the dedicated column empty.
        abbrev(
            3,
            VARIABLE,
            false,
            &[(at(DW_AT_name), form(DW_FORM_data1), None)],
        ),
    ];
    let dies = [
        die(1, &[&le32(0), &cstr("unit")]),
        die(
            2,
            &[
                &cstr("first"),
                &cstr("second"),
                &cstr("l1"),
                &cstr("l2"),
                &[5],
                &le16(6),
                &[1],
                &[2],
            ],
        ),
        die(3, &[&[7]]),
        vec![0],
    ]
    .concat();
    let file = one_unit(
        4,
        8,
        &decls,
        &dies,
        vec![(".debug_line", line_v4(4, &[], &["a.c", "b.c"]))],
    );
    let d = rows_of(&file);
    assert_eq!(d.name.as_ref().unwrap()[1], b"first");
    assert_eq!(d.linkage_name.as_ref().unwrap()[1], b"l1");
    assert_eq!(d.decl_line.as_ref().unwrap()[1], 5);
    // DWARF 4 file numbers are one-based: 1 -> first file.
    assert_eq!(d.decl_file.as_ref().unwrap()[1], b"a.c");
    assert_eq!(
        d.attr_int.as_ref().unwrap()[1],
        vec![0, 0, 0, 0, 5, 6, 1, 2]
    );
    assert_eq!(
        d.attr_str.as_ref().unwrap()[1],
        strs(&["first", "second", "l1", "l2", "", "", "a.c", "b.c"])
    );
    assert_eq!(
        d.attr_form.as_ref().unwrap()[1],
        vec![
            form(DW_FORM_string),
            form(DW_FORM_string),
            form(DW_FORM_string),
            form(DW_FORM_string),
            form(DW_FORM_data1),
            form(DW_FORM_data2),
            form(DW_FORM_data1),
            form(DW_FORM_data1)
        ]
    );
    assert_eq!(d.name.as_ref().unwrap()[2], b"");
    assert_eq!(d.attr_int.as_ref().unwrap()[2], vec![7]);
    assert_eq!(d.decl_file.as_ref().unwrap()[2], b"");
    assert_eq!(d.decl_line.as_ref().unwrap()[2], 0);
}

#[test]
fn unit_name_is_the_last_compile_unit_name() {
    let decls = [abbrev(
        1,
        CU,
        true,
        &[(at(DW_AT_name), form(DW_FORM_string), None)],
    )];
    let dies = [
        die(1, &[&cstr("outer")]),
        die(1, &[&cstr("inner")]),
        vec![0],
        vec![0],
    ]
    .concat();
    let file = one_unit(4, 8, &decls, &dies, vec![]);
    let mut st = Collector::new();
    let (d, infos) = read_unit(&file, 0, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(d.name.as_ref().unwrap(), &strs(&["outer", "inner", "", ""]));
    assert_eq!(infos.last().unwrap().unit_name, b"inner");
}

#[test]
fn constant_address_block_string_and_reference_groups() {
    let b = at(DW_AT_byte_size);
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VARIABLE,
            false,
            &[
                (b, form(DW_FORM_data1), None),
                (b, form(DW_FORM_data2), None),
                (b, form(DW_FORM_data4), None),
                (b, form(DW_FORM_data8), None),
                (b, form(DW_FORM_sdata), None),
                (b, form(DW_FORM_udata), None),
                (b, form(DW_FORM_data16), None),
                (b, form(DW_FORM_flag), None),
                (b, form(DW_FORM_flag_present), None),
                (b, form(DW_FORM_sec_offset), None),
                (b, form(DW_FORM_block1), None),
                (b, form(DW_FORM_block2), None),
                (b, form(DW_FORM_block4), None),
                (b, form(DW_FORM_block), None),
                (b, form(DW_FORM_exprloc), None),
                (b, form(DW_FORM_string), None),
                (b, form(DW_FORM_ref4), None),
                (b, form(DW_FORM_ref_addr), None),
                (b, form(DW_FORM_ref_sig8), None),
                (b, form(DW_FORM_ref_sup4), None),
                (b, form(DW_FORM_ref_udata), None),
                (b, form(DW_FORM_ref1), None),
                (b, form(DW_FORM_addr), None),
                (b, form(DW_FORM_implicit_const), Some(-3)),
                (b, form(DW_FORM_indirect), None),
                (b, form(DW_FORM_loclistx), None),
                (b, form(DW_FORM_rnglistx), None),
            ],
        ),
    ];
    let sixteen: Vec<u8> = (1..=16).collect();
    let dies = [
        die(1, &[]),
        die(
            2,
            &[
                &[0xfe],
                &le16(0x1234),
                &le32(0x0102_0304),
                &le64(0x0102_0304_0506_0708),
                &sleb(-2),
                &uleb(300),
                &sixteen,
                &[1],
                &[],
                &le32(7),
                &[3, 1, 0, 2],
                &[le16(2), vec![9, 0]].concat(),
                &[le32(1), vec![0]].concat(),
                &[uleb(2), vec![0, 0]].concat(),
                &[uleb(1), vec![0x9c]].concat(),
                &cstr("hi"),
                &le32(0x10),
                &le32(0x40),
                &le64(0xdead),
                &le32(0xbeef),
                &uleb(5),
                &[0x20],
                &le64(0x1122_3344_5566_7788),
                &[],
                &[uleb(u64::from(form(DW_FORM_data1))), vec![0x2a]].concat(),
                &uleb(9),
                &uleb(10),
            ],
        ),
        vec![0],
    ]
    .concat();
    let file = one_unit(4, 8, &decls, &dies, vec![]);
    let d = rows_of(&file);
    let ints = &d.attr_int.as_ref().unwrap()[1];
    let s = &d.attr_str.as_ref().unwrap()[1];
    let expected_ints: Vec<u64> = vec![
        0xfe,
        0x1234,
        0x0102_0304,
        0x0102_0304_0506_0708,
        (-2i64) as u64,
        300,
        16, // data16: LLVM's numeric value is the block length
        1,
        1,
        7,
        0,
        0,
        0,
        0,
        0,
        0,
        0x10, // ref4 is unit relative (the unit starts at 0)
        0x40, // ref_addr is already .debug_info relative
        0,    // ref_sig8 is dropped
        0,    // ref_sup4 is dropped
        5,
        0x20,
        0x1122_3344_5566_7788,
        (-3i64) as u64, // implicit_const
        0x2a,           // indirect -> data1
        9,
        10,
    ];
    assert_eq!(ints, &expected_ints);
    let mut expected_str = vec![Vec::new(); expected_ints.len()];
    expected_str[10] = vec![1, 0, 2];
    expected_str[11] = vec![9, 0];
    expected_str[12] = vec![0];
    expected_str[13] = vec![0, 0];
    expected_str[14] = vec![0x9c];
    expected_str[15] = b"hi".to_vec();
    assert_eq!(s, &expected_str);
    // attr_form reports the declared form (indirect stays indirect).
    let forms = &d.attr_form.as_ref().unwrap()[1];
    assert_eq!(forms[23], form(DW_FORM_implicit_const));
    assert_eq!(forms[24], form(DW_FORM_indirect));
}

#[test]
fn language_and_encoding_names() {
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VARIABLE,
            false,
            &[
                (at(DW_AT_language), form(DW_FORM_data1), None),
                (at(DW_AT_encoding), form(DW_FORM_data1), None),
                (at(DW_AT_language), form(DW_FORM_data2), None),
                (at(DW_AT_encoding), form(DW_FORM_data1), None),
                (at(DW_AT_language), form(DW_FORM_string), None),
            ],
        ),
    ];
    let dies = [
        die(1, &[]),
        die(2, &[&[0x04], &[0x07], &le16(0xdead), &[0xee], &cstr("x")]),
        vec![0],
    ]
    .concat();
    let d = rows_of(&one_unit(4, 8, &decls, &dies, vec![]));
    assert_eq!(
        d.attr_str.as_ref().unwrap()[1],
        strs(&["C_plus_plus", "unsigned", "", "", "x"])
    );
}

fn addr_pool(entries: &[u64]) -> Vec<u8> {
    let mut v = le32(4 + 2 + 1 + 1 + entries.len() as u32 * 8);
    v.extend(le16(5));
    v.extend([8, 0]);
    for e in entries {
        v.extend(le64(*e));
    }
    v
}

#[test]
fn indexed_addresses_use_the_unit_address_pool() {
    let decls = [
        abbrev(
            1,
            CU,
            true,
            &[(at(DW_AT_addr_base), form(DW_FORM_sec_offset), None)],
        ),
        abbrev(
            2,
            VARIABLE,
            false,
            &[
                (at(DW_AT_byte_size), form(DW_FORM_addrx), None),
                (at(DW_AT_byte_size), form(DW_FORM_addrx1), None),
                (at(DW_AT_byte_size), form(DW_FORM_GNU_addr_index), None),
                (at(DW_AT_byte_size), form(DW_FORM_LLVM_addrx_offset), None),
                (at(DW_AT_byte_size), form(DW_FORM_addrx), None),
            ],
        ),
    ];
    let llvm_addrx_offset = [uleb(2), le32(5)].concat();
    let dies = [
        die(1, &[&le32(8)]),
        die(
            2,
            &[&uleb(1), &[2], &uleb(0), &llvm_addrx_offset, &uleb(77)],
        ),
        vec![0],
    ]
    .concat();
    let file = one_unit(
        5,
        8,
        &decls,
        &dies,
        vec![(".debug_addr", addr_pool(&[0x1000, 0x2000, 0x3000]))],
    );
    let d = rows_of(&file);
    // The last index is out of range: unresolved addresses read as zero.
    assert_eq!(
        d.attr_int.as_ref().unwrap()[1],
        vec![0x2000, 0x3000, 0x1000, 0x3005, 0]
    );
}

#[test]
fn addr_base_without_pool_or_base_reads_zero() {
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VARIABLE,
            false,
            &[(at(DW_AT_byte_size), form(DW_FORM_addrx), None)],
        ),
    ];
    let dies = [die(1, &[]), die(2, &[&uleb(0)]), vec![0]].concat();
    // No DW_AT_addr_base, no .debug_addr.
    let d = rows_of(&one_unit(5, 8, &decls, &dies, vec![]));
    assert_eq!(d.attr_int.as_ref().unwrap()[1], vec![0]);
}

#[test]
fn gnu_addr_base_is_only_used_without_the_standard_base() {
    let pool = addr_pool(&[0xaaa, 0xbbb]);
    let variable = abbrev(
        2,
        VARIABLE,
        false,
        &[(at(DW_AT_byte_size), form(DW_FORM_addrx), None)],
    );
    let uses = |cu: Vec<u8>, dies: Vec<u8>| {
        let decls = [cu, variable.clone()];
        rows_of(&one_unit(
            5,
            8,
            &decls,
            &dies,
            vec![(".debug_addr", pool.clone())],
        ))
        .attr_int
        .unwrap()[1]
            .clone()
    };
    // Only the GNU base: used.
    let gnu_only = abbrev(
        1,
        CU,
        true,
        &[(at(DW_AT_GNU_addr_base), form(DW_FORM_sec_offset), None)],
    );
    let body = |cu_values: &[&[u8]]| [die(1, cu_values), die(2, &[&uleb(1)]), vec![0]].concat();
    assert_eq!(uses(gnu_only, body(&[&le32(8)])), vec![0xbbb]);
    // Both: the standard base wins (here pointing one entry further).
    let both = abbrev(
        1,
        CU,
        true,
        &[
            (at(DW_AT_GNU_addr_base), form(DW_FORM_sec_offset), None),
            (at(DW_AT_addr_base), form(DW_FORM_sec_offset), None),
        ],
    );
    assert_eq!(uses(both, body(&[&le32(0), &le32(16)])), vec![0]);
    // The standard base with a non-section-offset form is ignored, so the GNU base applies.
    let weird = abbrev(
        1,
        CU,
        true,
        &[
            (at(DW_AT_addr_base), form(DW_FORM_data1), None),
            (at(DW_AT_GNU_addr_base), form(DW_FORM_sec_offset), None),
        ],
    );
    assert_eq!(uses(weird, body(&[&[0], &le32(8)])), vec![0xbbb]);
}

fn str_section(strings: &[&str]) -> (Vec<u8>, Vec<u32>) {
    let mut data = Vec::new();
    let mut offs = Vec::new();
    for s in strings {
        offs.push(data.len() as u32);
        data.extend(cstr(s));
    }
    (data, offs)
}

fn str_offsets_section(offsets: &[u32]) -> Vec<u8> {
    let mut v = le32(4 + offsets.len() as u32 * 4);
    v.extend(le16(5));
    v.extend(le16(0));
    for o in offsets {
        v.extend(le32(*o));
    }
    v
}

#[test]
fn strx_resolves_through_the_offsets_table_whatever_the_attribute_order() {
    let (debug_str, offs) = str_section(&["zero", "one", "two"]);
    let sections = || {
        vec![
            (".debug_str", debug_str.clone()),
            (".debug_str_offsets", str_offsets_section(&offs)),
        ]
    };
    // strx1 comes *before* DW_AT_str_offsets_base in the root DIE.
    let decls = [
        abbrev(
            1,
            CU,
            true,
            &[
                (at(DW_AT_name), form(DW_FORM_strx1), None),
                (at(DW_AT_str_offsets_base), form(DW_FORM_sec_offset), None),
            ],
        ),
        abbrev(
            2,
            VARIABLE,
            false,
            &[
                (at(DW_AT_name), form(DW_FORM_strx), None),
                (at(DW_AT_linkage_name), form(DW_FORM_strx2), None),
                (at(DW_AT_byte_size), form(DW_FORM_strx3), None),
                (at(DW_AT_byte_size), form(DW_FORM_strx4), None),
                (at(DW_AT_byte_size), form(DW_FORM_GNU_str_index), None),
            ],
        ),
    ];
    let dies = [
        die(1, &[&[1], &le32(8)]),
        die(2, &[&uleb(2), &le16(0), &[1, 0, 0], &le32(2), &uleb(0)]),
        vec![0],
    ]
    .concat();
    let file = one_unit(5, 8, &decls, &dies, sections());
    let mut st = Collector::new();
    let (d, infos) = read_unit(&file, 0, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(d.name.as_ref().unwrap()[0], b"one");
    assert_eq!(infos[0].unit_name, b"one");
    assert_eq!(d.name.as_ref().unwrap()[1], b"two");
    assert_eq!(d.linkage_name.as_ref().unwrap()[1], b"zero");
    assert_eq!(
        d.attr_str.as_ref().unwrap()[1],
        strs(&["two", "zero", "one", "two", "zero"])
    );

    // A DWARF 4 unit has no string offsets table: strx cannot resolve.
    let file4 = one_unit(4, 8, &decls, &dies, sections());
    assert_eq!(fail_of(&file4), ERR_DWARF);

    // A base that cannot be a valid contribution is rejected when the unit opens.
    let bad_base = [die(1, &[&[1], &le32(2)]), vec![0]].concat();
    let f = one_unit(5, 8, &decls, &bad_base, sections());
    assert_eq!(f.open_unit(0).err().unwrap().0, ERR_DWARF);

    // A contribution that claims more than the section holds.
    let mut oversized = str_offsets_section(&offs);
    oversized[..4].copy_from_slice(&le32(4 + 400));
    let f = one_unit(
        5,
        8,
        &decls,
        &dies,
        vec![
            (".debug_str", debug_str.clone()),
            (".debug_str_offsets", oversized),
        ],
    );
    assert_eq!(f.open_unit(0).err().unwrap().0, ERR_DWARF);

    // No DW_AT_str_offsets_base at all: strx fails when used.
    let no_base_decls = [abbrev(
        1,
        CU,
        true,
        &[(at(DW_AT_name), form(DW_FORM_strx1), None)],
    )];
    let f = one_unit(
        5,
        8,
        &no_base_decls,
        &[die(1, &[&[0]]), vec![0]].concat(),
        sections(),
    );
    assert_eq!(fail_of(&f), ERR_DWARF);

    // An index beyond the table.
    let far = [die(1, &[&[200], &le32(8)]), vec![0]].concat();
    let f = one_unit(5, 8, &decls, &far, sections());
    assert_eq!(fail_of(&f), ERR_DWARF);
}

#[test]
fn string_offset_forms() {
    let (debug_str, offs) = str_section(&["zero", "one"]);
    let line_str = b"lines\0".to_vec();
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VARIABLE,
            false,
            &[
                (at(DW_AT_name), form(DW_FORM_strp), None),
                (at(DW_AT_linkage_name), form(DW_FORM_line_strp), None),
                (at(DW_AT_byte_size), form(DW_FORM_strp_sup), None),
            ],
        ),
    ];
    let dies = [
        die(1, &[]),
        die(2, &[&le32(offs[1]), &le32(0), &le32(0)]),
        vec![0],
    ]
    .concat();
    let extra = || {
        vec![
            (".debug_str", debug_str.clone()),
            (".debug_line_str", line_str.clone()),
        ]
    };
    let d = rows_of(&one_unit(5, 8, &decls, &dies, extra()));
    // strp_sup resolves against the local .debug_str.
    assert_eq!(
        d.attr_str.as_ref().unwrap()[1],
        strs(&["one", "lines", "zero"])
    );

    // Offsets outside the section, and an unterminated string, are errors.
    for bad in [le32(debug_str.len() as u32), le32(4000)] {
        let dies = [die(1, &[]), die(2, &[&bad, &le32(0), &le32(0)]), vec![0]].concat();
        assert_eq!(fail_of(&one_unit(5, 8, &decls, &dies, extra())), ERR_DWARF);
    }
    let unterminated = b"ab".to_vec();
    let f = one_unit(
        5,
        8,
        &decls,
        &dies,
        vec![
            (".debug_str", unterminated),
            (".debug_line_str", line_str.clone()),
        ],
    );
    assert_eq!(fail_of(&f), ERR_DWARF);

    // DW_FORM_GNU_strp_alt is not supported.
    let alt = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VARIABLE,
            false,
            &[(at(DW_AT_name), form(DW_FORM_GNU_strp_alt), None)],
        ),
    ];
    let dies = [die(1, &[]), die(2, &[&le32(0)]), vec![0]].concat();
    assert_eq!(fail_of(&one_unit(5, 8, &alt, &dies, extra())), ERR_DWARF);
}

#[test]
fn references_and_offsets_in_other_dwarf_encodings() {
    // DWARF 2 reads ref_addr with the address size (2 here); DWARF 3 with the offset size.
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VARIABLE,
            false,
            &[(at(DW_AT_byte_size), form(DW_FORM_ref_addr), None)],
        ),
    ];
    let v2 = [die(1, &[]), die(2, &[&le16(0x1234)]), vec![0]].concat();
    let d = rows_of(&one_unit(2, 2, &decls, &v2, vec![]));
    assert_eq!(d.attr_int.as_ref().unwrap()[1], vec![0x1234]);
    let v3 = [die(1, &[]), die(2, &[&le32(0x1234_5678)]), vec![0]].concat();
    let d = rows_of(&one_unit(3, 8, &decls, &v3, vec![]));
    assert_eq!(d.attr_int.as_ref().unwrap()[1], vec![0x1234_5678]);

    // 64-bit DWARF: section offsets and ref_addr are 8 bytes; a unit-relative ref is rebased.
    let decls64 = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VARIABLE,
            false,
            &[
                (at(DW_AT_byte_size), form(DW_FORM_sec_offset), None),
                (at(DW_AT_byte_size), form(DW_FORM_ref_addr), None),
                (at(DW_AT_byte_size), form(DW_FORM_ref4), None),
                (at(DW_AT_byte_size), form(DW_FORM_addr), None),
            ],
        ),
    ];
    let dies = [
        die(1, &[]),
        die(
            2,
            &[&le64(0x0102_0304_0506), &le64(0x77), &le32(8), &le32(0x55)],
        ),
        vec![0],
    ]
    .concat();
    let elf = Elf::new(unit(4, Fmt::Dwarf64, 4, 0, &dies), abbrev_table(&decls64));
    let file = TestFile::open(elf.build()).unwrap();
    let d = rows_of(&file);
    // The 64-bit unit header is 23 bytes, so the unit-relative ref4 value 8 becomes 8 (unit at 0).
    assert_eq!(
        d.attr_int.as_ref().unwrap()[1],
        vec![0x0102_0304_0506, 0x77, 8, 0x55]
    );
    assert_eq!(d.offset.as_ref().unwrap()[0], 23);
}

#[test]
fn second_unit_references_are_rebased_to_debug_info() {
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VARIABLE,
            false,
            &[(at(DW_AT_byte_size), form(DW_FORM_ref4), None)],
        ),
    ];
    let body = [die(1, &[]), die(2, &[&le32(3)]), vec![0]].concat();
    let first = unit(4, Fmt::Dwarf32, 8, 0, &body);
    let second = unit(4, Fmt::Dwarf32, 8, 0, &body);
    let second_offset = first.len() as u64;
    let elf = Elf::new([first, second].concat(), abbrev_table(&decls));
    let file = TestFile::open(elf.build()).unwrap();
    assert_eq!(file.unit_count(), 2);
    let mut st = Collector::new();
    let (d, _) = read_unit(&file, 1, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(file.open_unit(1).unwrap().offset, second_offset);
    assert_eq!(d.attr_int.as_ref().unwrap()[1], vec![second_offset + 3]);
    assert_eq!(d.offset.as_ref().unwrap()[0], second_offset + 11);
}
