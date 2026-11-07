//! Address ranges, line-table file names, chunking and projection behavior.
#![allow(unsafe_code)]

mod common;

use _ch_rust_dwarf::outbuf::*;
use common::*;
use gimli::constants::*;

const CU: u16 = 0x11;
const SUB: u16 = 0x2e;
const VAR: u16 = 0x34;

fn at(a: gimli::DwAt) -> u16 {
    a.0
}
fn form(f: gimli::DwForm) -> u16 {
    f.0
}
fn pair(a: u64, b: u64) -> Vec<u8> {
    [le64(a), le64(b)].concat()
}
fn strs(rows: &[&str]) -> Vec<Vec<u8>> {
    rows.iter().map(|s| s.as_bytes().to_vec()).collect()
}

fn v4_ranges_section() -> Vec<u8> {
    [
        pair(0x10, 0x20),
        pair(0x30, 0x40),
        pair(0, 0), // offset 0: two pairs relative to the unit base
        pair(u64::MAX, 0x5000),
        pair(1, 2),
        pair(0, 0), // offset 48: base selection, then one pair
    ]
    .concat()
}

#[test]
fn dwarf4_contiguous_and_list_ranges() {
    let lo = at(DW_AT_low_pc);
    let hi = at(DW_AT_high_pc);
    let rg = at(DW_AT_ranges);
    let decls = [
        abbrev(1, CU, true, &[(lo, form(DW_FORM_addr), None)]),
        abbrev(2, SUB, false, &[(rg, form(DW_FORM_sec_offset), None)]),
        abbrev(
            3,
            SUB,
            false,
            &[
                (lo, form(DW_FORM_addr), None),
                (hi, form(DW_FORM_data8), None),
            ],
        ),
        abbrev(
            4,
            SUB,
            false,
            &[
                (lo, form(DW_FORM_addr), None),
                (hi, form(DW_FORM_addr), None),
            ],
        ),
        abbrev(5, SUB, false, &[(lo, form(DW_FORM_addr), None)]),
        abbrev(6, SUB, false, &[(hi, form(DW_FORM_data8), None)]),
        abbrev(
            7,
            SUB,
            false,
            &[
                (lo, form(DW_FORM_addr), None),
                (lo, form(DW_FORM_addr), None),
                (hi, form(DW_FORM_addr), None),
            ],
        ),
        abbrev(
            8,
            SUB,
            false,
            &[
                (hi, form(DW_FORM_addr), None),
                (lo, form(DW_FORM_addr), None),
            ],
        ),
        abbrev(
            9,
            SUB,
            false,
            &[
                (lo, form(DW_FORM_addr), None),
                (rg, form(DW_FORM_sec_offset), None),
            ],
        ),
    ];
    let dies = [
        die(1, &[&le64(0x1000)]),
        die(2, &[&le32(0)]),
        die(3, &[&le64(0x100), &le64(0x10)]),
        die(4, &[&le64(0x100), &le64(0x180)]),
        die(5, &[&le64(0x700)]),
        die(6, &[&le64(0x10)]),
        die(7, &[&le64(0x100), &le64(0x200), &le64(0x300)]),
        die(8, &[&le64(0x500), &le64(0x100)]),
        die(2, &[&le32(48)]),
        die(9, &[&le64(0x10), &le32(0)]),
        vec![0],
    ]
    .concat();
    let file = one_unit(
        4,
        8,
        &decls,
        &dies,
        vec![(".debug_ranges", v4_ranges_section())],
    );
    let d = rows_of(&file);
    let r = d.ranges.as_ref().unwrap();
    assert_eq!(r[0], vec![(0x1000, 0x1001)]); // lone low_pc: [low, low + 1)
    assert_eq!(r[1], vec![(0x1010, 0x1020), (0x1030, 0x1040)]);
    assert_eq!(r[2], vec![(0x100, 0x110)]); // relative high_pc
    assert_eq!(r[3], vec![(0x100, 0x180)]); // absolute high_pc
    assert_eq!(r[4], vec![(0x700, 0x701)]);
    assert_eq!(r[5], vec![]); // high_pc without low_pc
    assert_eq!(r[6], vec![(0x200, 0x300)]); // the last low_pc wins
    assert_eq!(r[7], vec![(0x100, 0x500)]);
    assert_eq!(r[8], vec![(0x5001, 0x5002)]); // base address selection entry
    assert_eq!(r[9], vec![(0x1010, 0x1020), (0x1030, 0x1040)]); // ranges beat low_pc
}

#[test]
fn nested_compile_unit_dies_reset_the_range_state() {
    let lo = at(DW_AT_low_pc);
    let decls = [
        abbrev(1, CU, true, &[(lo, form(DW_FORM_addr), None)]),
        abbrev(
            2,
            SUB,
            false,
            &[(at(DW_AT_ranges), form(DW_FORM_sec_offset), None)],
        ),
    ];
    let dies = [
        die(1, &[&le64(0x1000)]),
        die(2, &[&le32(0)]),
        die(1, &[&le64(0x2000)]),
        die(2, &[&le32(0)]),
        vec![0],
        vec![0],
    ]
    .concat();
    let file = one_unit(
        4,
        8,
        &decls,
        &dies,
        vec![(".debug_ranges", v4_ranges_section())],
    );
    let r = rows_of(&file).ranges.unwrap();
    assert_eq!(r[1], vec![(0x1010, 0x1020), (0x1030, 0x1040)]);
    assert_eq!(r[3], vec![(0x2010, 0x2020), (0x2030, 0x2040)]);
}

#[test]
fn dwarf5_range_lists_direct_and_indexed() {
    // Header (12 bytes) + two offsets, then two lists.
    let list0 = [vec![6u8], pair(0x10, 0x20), vec![0]].concat();
    let list1 = [
        vec![5u8],
        le64(0x400),
        vec![4, 4, 8],
        vec![7],
        le64(0x900),
        uleb(0x10),
        vec![0],
    ]
    .concat();
    let mut section = le32(0); // patched below
    section.extend(le16(5));
    section.extend([8, 0]);
    section.extend(le32(2));
    section.extend(le32(8));
    section.extend(le32(8 + list0.len() as u32));
    section.extend(&list0);
    section.extend(&list1);
    let len = section.len() as u32 - 4;
    section[..4].copy_from_slice(&le32(len));
    let direct_list0 = 12 + 8;
    let direct_list1 = direct_list0 + list0.len() as u32;

    let rg = at(DW_AT_ranges);
    let decls = [
        abbrev(
            1,
            CU,
            true,
            &[(at(DW_AT_rnglists_base), form(DW_FORM_sec_offset), None)],
        ),
        abbrev(2, SUB, false, &[(rg, form(DW_FORM_rnglistx), None)]),
        abbrev(3, SUB, false, &[(rg, form(DW_FORM_sec_offset), None)]),
    ];
    let dies = [
        die(1, &[&le32(12)]),
        die(2, &[&uleb(0)]),
        die(2, &[&uleb(1)]),
        die(3, &[&le32(direct_list0)]),
        die(3, &[&le32(direct_list1)]),
        vec![0],
    ]
    .concat();
    let file = one_unit(
        5,
        8,
        &decls,
        &dies,
        vec![(".debug_rnglists", section.clone())],
    );
    let r = rows_of(&file).ranges.unwrap();
    assert_eq!(r[1], vec![(0x10, 0x20)]);
    assert_eq!(r[2], vec![(0x404, 0x408), (0x900, 0x910)]);
    assert_eq!(r[3], r[1]);
    assert_eq!(r[4], r[2]);

    // rnglistx index past the table, and no .debug_rnglists_base.
    let past = [die(1, &[&le32(12)]), die(2, &[&uleb(2)]), vec![0]].concat();
    let f = one_unit(
        5,
        8,
        &decls,
        &past,
        vec![(".debug_rnglists", section.clone())],
    );
    assert_eq!(fail_of(&f), ERR_DWARF);
    let no_base = [
        abbrev(1, CU, true, &[]),
        abbrev(2, SUB, false, &[(rg, form(DW_FORM_rnglistx), None)]),
    ];
    let dies = [die(1, &[]), die(2, &[&uleb(0)]), vec![0]].concat();
    let f = one_unit(5, 8, &no_base, &dies, vec![(".debug_rnglists", section)]);
    assert_eq!(fail_of(&f), ERR_DWARF);
}

#[test]
fn dwarf5_indexed_range_entries_use_the_pool() {
    let mut pool = le32(4 + 2 + 2 + 16);
    pool.extend(le16(5));
    pool.extend([8, 0]);
    pool.extend(le64(0x1000));
    pool.extend(le64(0x2000));
    // startx_endx 0..1, then startx_length 1 + 0x10
    let list = [vec![2u8, 0, 1], vec![3, 1, 0x10], vec![0]].concat();
    let mut section = le32(0);
    section.extend(le16(5));
    section.extend([8, 0]);
    section.extend(le32(0));
    section.extend(&list);
    let len = section.len() as u32 - 4;
    section[..4].copy_from_slice(&le32(len));
    let decls = [
        abbrev(
            1,
            CU,
            true,
            &[(at(DW_AT_addr_base), form(DW_FORM_sec_offset), None)],
        ),
        abbrev(
            2,
            SUB,
            false,
            &[(at(DW_AT_ranges), form(DW_FORM_sec_offset), None)],
        ),
    ];
    let dies = [die(1, &[&le32(8)]), die(2, &[&le32(12)]), vec![0]].concat();
    let extra = vec![(".debug_addr", pool), (".debug_rnglists", section)];
    let r = rows_of(&one_unit(5, 8, &decls, &dies, extra))
        .ranges
        .unwrap();
    assert_eq!(r[1], vec![(0x1000, 0x2000), (0x2000, 0x2010)]);
}

#[test]
fn ranges_are_only_decoded_when_projected_but_the_pre_pass_always_runs() {
    let rg = at(DW_AT_ranges);
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(2, SUB, false, &[(rg, form(DW_FORM_sec_offset), None)]),
    ];
    // The range list offset is garbage and there is no .debug_ranges section at all.
    let dies = [die(1, &[]), die(2, &[&le32(0xffff)]), vec![0]].concat();
    let file = one_unit(4, 8, &decls, &dies, vec![]);
    let mut st = Collector::new();
    let without_ranges = ALL_COLS & !COL_RANGES;
    let (d, _) = read_unit(&file, 0, without_ranges, 65536, &mut st).unwrap();
    assert_eq!(d.rows, 3);
    assert_eq!(
        read_unit(&file, 0, COL_RANGES, 65536, &mut st)
            .err()
            .unwrap()
            .0,
        ERR_MISSING_AUX_SECTION
    );
    let with_section = one_unit(
        4,
        8,
        &decls,
        &dies,
        vec![(".debug_ranges", v4_ranges_section())],
    );
    assert_eq!(
        read_unit(&with_section, 0, COL_RANGES, 65536, &mut st)
            .err()
            .unwrap()
            .0,
        ERR_DWARF
    );

    // A compile_unit DIE whose low_pc form is not an address is rejected whatever is projected.
    let bad_cu = [abbrev(
        1,
        CU,
        true,
        &[(at(DW_AT_low_pc), form(DW_FORM_data4), None)],
    )];
    let dies = [die(1, &[&le32(5)]), vec![0]].concat();
    let file = one_unit(4, 8, &bad_cu, &dies, vec![]);
    assert_eq!(
        read_unit(&file, 0, COL_OFFSET, 65536, &mut st)
            .err()
            .unwrap()
            .0,
        ERR_DWARF
    );

    // A pooled low_pc without .debug_addr: fine until `ranges` is projected.
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            SUB,
            false,
            &[(at(DW_AT_low_pc), form(DW_FORM_addrx), None)],
        ),
    ];
    let dies = [die(1, &[]), die(2, &[&uleb(0)]), vec![0]].concat();
    let file = one_unit(5, 8, &decls, &dies, vec![]);
    assert!(read_unit(&file, 0, ALL_COLS & !COL_RANGES, 65536, &mut st).is_ok());
    assert_eq!(
        read_unit(&file, 0, COL_RANGES, 65536, &mut st)
            .err()
            .unwrap()
            .0,
        ERR_MISSING_AUX_SECTION
    );
}

fn cu_stmt_decls(extra_var: &[(u16, u16, Option<i64>)]) -> Vec<Vec<u8>> {
    vec![
        abbrev(
            1,
            CU,
            true,
            &[(at(DW_AT_stmt_list), form(DW_FORM_sec_offset), None)],
        ),
        abbrev(2, VAR, false, extra_var),
    ]
}

#[test]
fn line_table_names_dwarf5_and_numbering() {
    let line_str = b"a.c\0b.c\0".to_vec();
    let line = line_v5(
        form(DW_FORM_line_strp),
        &[le32(0)],
        &[le32(0), le32(4), le32(100)], // the last path points outside .debug_line_str
    );
    let decls = cu_stmt_decls(&[
        (at(DW_AT_decl_file), form(DW_FORM_data1), None),
        (at(DW_AT_call_file), form(DW_FORM_data1), None),
    ]);
    let var = |decl: u8, call: u8| die(2, &[&[decl], &[call]]);
    let dies = [
        die(1, &[&le32(0)]),
        var(0, 1),
        var(2, 0),
        var(3, 9),
        vec![0],
    ]
    .concat();
    let extra = vec![(".debug_line", line), (".debug_line_str", line_str)];
    let d = rows_of(&one_unit(5, 8, &decls, &dies, extra));
    // DWARF 5 file numbers are zero-based. A path that cannot be read is "<error>".
    assert_eq!(
        d.decl_file.as_ref().unwrap(),
        &strs(&["", "a.c", "<error>", "", ""])
    );
    assert_eq!(
        d.attr_str.as_ref().unwrap()[1],
        strs(&["a.c", "b.c"]),
        "decl_file and call_file both stringify"
    );
    assert_eq!(
        d.attr_str.as_ref().unwrap()[3],
        strs(&["", ""]),
        "out-of-range indexes stay empty"
    );
}

#[test]
fn line_table_names_dwarf2_to_4_are_one_based_with_an_invalid_placeholder() {
    for version in [2u16, 3, 4] {
        let decls = cu_stmt_decls(&[(at(DW_AT_decl_file), form(DW_FORM_data1), None)]);
        let dies = [
            die(1, &[&le32(0)]),
            die(2, &[&[0]]),
            die(2, &[&[1]]),
            die(2, &[&[2]]),
            die(2, &[&[3]]),
            vec![0],
        ]
        .concat();
        let line = line_v4(version, &["dir"], &["a.c", "b.c"]);
        let d = rows_of(&one_unit(
            version,
            8,
            &decls,
            &dies,
            vec![(".debug_line", line)],
        ));
        assert_eq!(
            d.decl_file.as_ref().unwrap(),
            &strs(&["", "<invalid>", "a.c", "b.c", "", ""]),
            "DWARF {version}"
        );
    }
}

#[test]
fn line_table_lookup_is_order_dependent_and_parsed_once() {
    // decl_file before DW_AT_stmt_list in the same DIE misses; after it hits.
    let decls = [abbrev(
        1,
        CU,
        true,
        &[
            (at(DW_AT_decl_file), form(DW_FORM_data1), None),
            (at(DW_AT_stmt_list), form(DW_FORM_sec_offset), None),
            (at(DW_AT_decl_file), form(DW_FORM_data1), None),
            (at(DW_AT_stmt_list), form(DW_FORM_sec_offset), None),
        ],
    )];
    let dies = [die(1, &[&[1], &le32(0), &[1], &le32(0xdead)]), vec![0]].concat();
    let line = line_v4(4, &[], &["a.c"]);
    let d = rows_of(&one_unit(4, 8, &decls, &dies, vec![(".debug_line", line)]));
    // The second stmt_list (bogus offset) is ignored: the table exists already.
    assert_eq!(d.attr_str.as_ref().unwrap()[0], strs(&["", "", "a.c", ""]));
    assert_eq!(
        d.decl_file.as_ref().unwrap()[0],
        b"a.c",
        "the first miss does not consume the column"
    );
}

#[test]
fn stmt_list_errors_and_ignored_forms() {
    let decls = [abbrev(
        1,
        CU,
        true,
        &[(at(DW_AT_stmt_list), form(DW_FORM_sec_offset), None)],
    )];
    let dies = [die(1, &[&le32(0)]), vec![0]].concat();
    // No .debug_line at all.
    assert_eq!(
        fail_of(&one_unit(4, 8, &decls, &dies, vec![])),
        ERR_MISSING_AUX_SECTION
    );
    // Offset outside .debug_line.
    let bad = [die(1, &[&le32(1 << 20)]), vec![0]].concat();
    let line = line_v4(4, &[], &["a.c"]);
    assert_eq!(
        fail_of(&one_unit(
            4,
            8,
            &decls,
            &bad,
            vec![(".debug_line", line.clone())]
        )),
        ERR_DWARF
    );
    // A garbage line table version.
    let mut broken = line.clone();
    broken[4] = 9;
    assert_eq!(
        fail_of(&one_unit(
            4,
            8,
            &decls,
            &dies,
            vec![(".debug_line", broken)]
        )),
        ERR_DWARF
    );
    // A DWARF 4 data4 stmt_list is a constant, not a section offset: never parsed, so no error
    // even though there is no .debug_line.
    let constant = [abbrev(
        1,
        CU,
        true,
        &[(at(DW_AT_stmt_list), form(DW_FORM_data4), None)],
    )];
    assert!(rows_of(&one_unit(4, 8, &constant, &dies, vec![])).rows == 2);
    // ... but in DWARF 3 it is.
    assert_eq!(
        fail_of(&one_unit(3, 8, &constant, &dies, vec![])),
        ERR_MISSING_AUX_SECTION
    );
}

#[test]
fn chunking_and_progress() {
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VAR,
            false,
            &[(at(DW_AT_decl_line), form(DW_FORM_data1), None)],
        ),
    ];
    let build = |rows: usize| {
        let mut dies = die(1, &[]);
        for i in 0..rows - 2 {
            dies.extend(die(2, &[&[(i % 251) as u8]]));
        }
        dies.push(0);
        one_unit(4, 8, &decls, &dies, vec![])
    };
    let mut st = Collector::new();
    let cases: &[(usize, usize, usize)] = if cfg!(miri) {
        &[
            (14, 7, 2),
            (15, 7, 3),
            (6, 7, 1),
            (7, 7, 1),
            (8, 7, 2),
            (2, 1, 2),
        ]
    } else {
        &[
            (98, 7, 14), // exact multiple: no empty trailing chunk
            (102, 7, 15),
            (7, 7, 1),
            (8, 7, 2),
            (2, 1, 2),
            (65_535, 65_536, 1),
            (65_536, 65_536, 1),
            (65_537, 65_536, 2),
        ]
    };
    for &(rows, max_rows, expected_chunks) in cases {
        let file = build(rows);
        let (d, infos) = read_unit(&file, 0, ALL_COLS, max_rows, &mut st).unwrap();
        assert_eq!(d.rows, rows, "rows for {rows}/{max_rows}");
        assert_eq!(infos.len(), expected_chunks, "chunks for {rows}/{max_rows}");
        assert_eq!(infos.last().unwrap().status, UNIT_DONE);
        assert!(infos[..infos.len() - 1]
            .iter()
            .all(|i| i.status == CHUNK_READY));
        // Offsets are contiguous and the byte counts add up to the unit payload.
        let offsets = d.offset.as_ref().unwrap();
        assert_eq!(offsets[0], 11);
        let sizes = d.size.as_ref().unwrap();
        for w in 0..rows - 1 {
            assert_eq!(offsets[w] + u64::from(sizes[w]), offsets[w + 1]);
        }
        let payload = offsets[rows - 1] + u64::from(sizes[rows - 1]) - 11;
        assert_eq!(infos.iter().map(|i| i.bytes_advanced).sum::<u64>(), payload);
        // Array offsets restart in every chunk.
        assert_eq!(d.decl_line.as_ref().unwrap().len(), rows);
    }
}

#[test]
fn chunks_restart_their_array_offsets_and_keep_unit_state() {
    let decls = [
        abbrev(1, CU, true, &[(at(DW_AT_name), form(DW_FORM_string), None)]),
        abbrev(2, SUB, true, &[]),
        abbrev(
            3,
            VAR,
            false,
            &[(at(DW_AT_byte_size), form(DW_FORM_data1), None)],
        ),
    ];
    let dies = [
        die(1, &[&cstr("unit")]),
        die(2, &[]),
        die(3, &[&[1]]),
        die(3, &[&[2]]),
        vec![0],
        vec![0],
    ]
    .concat();
    let file = one_unit(4, 8, &decls, &dies, vec![]);
    let mut st = Collector::new();
    let mut unit = file.open_unit(0).unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        let (rc, _, chunk) = parse_one(&file, &mut unit, ALL_COLS, 2, &mut st);
        let (d, info) = chunk.unwrap();
        // `decode` already proved the offsets of this chunk are relative to the chunk start.
        seen.push((
            rc,
            d.rows,
            d.ancestor_offsets.unwrap(),
            d.attr_int.unwrap(),
            info.unit_name,
        ));
        if rc == UNIT_DONE {
            break;
        }
    }
    assert_eq!(seen[0].2, vec![vec![], vec![11]]);
    assert_eq!(seen[1].2, vec![vec![17, 11], vec![17, 11]]);
    assert_eq!(seen[1].3, vec![vec![1], vec![2]]);
    assert_eq!(seen[2].2, vec![vec![17, 11], vec![11]]);
    assert!(
        seen.iter().all(|s| s.4 == b"unit"),
        "unit_name is reported by every chunk"
    );
}

/// A richer unit used to compare every projection with the full decode.
fn projection_unit() -> TestFile {
    let decls = [
        abbrev(
            1,
            CU,
            true,
            &[
                (at(DW_AT_name), form(DW_FORM_string), None),
                (at(DW_AT_stmt_list), form(DW_FORM_sec_offset), None),
                (at(DW_AT_low_pc), form(DW_FORM_addr), None),
            ],
        ),
        abbrev(
            2,
            SUB,
            true,
            &[
                (at(DW_AT_name), form(DW_FORM_string), None),
                (at(DW_AT_linkage_name), form(DW_FORM_string), None),
                (at(DW_AT_decl_file), form(DW_FORM_data1), None),
                (at(DW_AT_decl_line), form(DW_FORM_data2), None),
                (at(DW_AT_ranges), form(DW_FORM_sec_offset), None),
            ],
        ),
        abbrev(
            3,
            VAR,
            false,
            &[
                (at(DW_AT_byte_size), form(DW_FORM_block1), None),
                (at(DW_AT_language), form(DW_FORM_data1), None),
                (at(DW_AT_type), form(DW_FORM_ref4), None),
            ],
        ),
    ];
    let dies = [
        die(1, &[&cstr("proj.c"), &le32(0), &le64(0x1000)]),
        die(
            2,
            &[&cstr("main"), &cstr("_main"), &[1], &le16(42), &le32(0)],
        ),
        die(3, &[&[2, 0, 7], &[4], &le32(0x0b)]),
        die(3, &[&[0], &[1], &le32(0x0c)]),
        vec![0],
        die(2, &[&cstr(""), &cstr("x"), &[2], &le16(7), &le32(48)]),
        vec![0],
        vec![0],
    ]
    .concat();
    one_unit(
        4,
        8,
        &decls,
        &dies,
        vec![
            (".debug_line", line_v4(4, &[], &["one.c", "two.c"])),
            (".debug_ranges", v4_ranges_section()),
        ],
    )
}

fn column_view(d: &Decoded, mask: u32) -> Decoded {
    // The same decode restricted to the columns in `mask`.
    let keep = |bit: u32| mask & bit != 0;
    let mut v = Decoded {
        rows: d.rows,
        ..Decoded::default()
    };
    if keep(COL_OFFSET) {
        v.offset = d.offset.clone();
    }
    if keep(COL_SIZE) {
        v.size = d.size.clone();
    }
    if keep(COL_TAG) {
        v.tag = d.tag.clone();
    }
    if keep(COL_ANCESTOR_TAGS) {
        v.ancestor_tags = d.ancestor_tags.clone();
    }
    if keep(COL_ANCESTOR_OFFSETS) {
        v.ancestor_offsets = d.ancestor_offsets.clone();
    }
    if keep(COL_NAME) {
        v.name = d.name.clone();
    }
    if keep(COL_LINKAGE_NAME) {
        v.linkage_name = d.linkage_name.clone();
    }
    if keep(COL_DECL_FILE) {
        v.decl_file = d.decl_file.clone();
    }
    if keep(COL_DECL_LINE) {
        v.decl_line = d.decl_line.clone();
    }
    if keep(COL_RANGES) {
        v.ranges = d.ranges.clone();
    }
    if keep(COL_ATTR_NAME) {
        v.attr_name = d.attr_name.clone();
    }
    if keep(COL_ATTR_FORM) {
        v.attr_form = d.attr_form.clone();
    }
    if keep(COL_ATTR_INT) {
        v.attr_int = d.attr_int.clone();
    }
    if keep(COL_ATTR_STR) {
        v.attr_str = d.attr_str.clone();
    }
    v
}

#[test]
fn every_projection_matches_the_full_decode() {
    let file = projection_unit();
    let mut st = Collector::new();
    let full = rows_of(&file);
    assert_eq!(full.rows, 8);
    assert_eq!(full.decl_file.as_ref().unwrap()[1], b"one.c");
    // All 65,536 column subsets (including the replicated unit_name/unit_offset bits, which have
    // no sinks): equal row counts, consistent offset vectors (checked inside `decode`), and the
    // same values as the full decode.
    let masks: Box<dyn Iterator<Item = u32>> = if cfg!(miri) {
        Box::new(
            [0u32, ALL_COLUMNS_MASK]
                .into_iter()
                .chain((0..16).map(|b| 1u32 << b))
                .chain((0..16).map(|b| ALL_COLUMNS_MASK ^ (1u32 << b))),
        )
    } else {
        Box::new(0..=ALL_COLUMNS_MASK)
    };
    for mask in masks {
        let mut unit = file.open_unit(0).unwrap();
        let (rc, msg, chunk) = parse_one(&file, &mut unit, mask, 65536, &mut st);
        let (d, _) = chunk.unwrap_or_else(|| panic!("mask {mask:#x}: {rc} {msg}"));
        assert_eq!(rc, UNIT_DONE);
        assert_eq!(d, column_view(&full, mask), "mask {mask:#x}");
    }
}

#[test]
fn invalid_projections_are_rejected_without_touching_the_sinks() {
    let file = projection_unit();
    let mut st = Collector::new();
    let mut unit = file.open_unit(0).unwrap();
    let call = |proj: ChDwarfProjectionOwned, st: &mut Collector, unit: &mut TestUnit| {
        let plan = plan_of(COL_OFFSET);
        let mut sinks = fresh_sinks(plan);
        let mut progress = std::mem::MaybeUninit::<_ch_rust_dwarf::ffi::ChDwarfProgress>::zeroed();
        let mut err = [0 as std::ffi::c_char; 256];
        let rc = unsafe {
            _ch_rust_dwarf::ffi::ch_dwarf_unit_parse_chunk(
                file.file,
                unit.unit,
                &proj.0,
                &mut sinks,
                Some(reserve_cb),
                Some(cancel_cb),
                (st as *mut Collector).cast(),
                10,
                progress.as_mut_ptr(),
                err.as_mut_ptr(),
            )
        };
        (rc, sinks.sinks[SinkId::Offset as usize].len)
    };
    use _ch_rust_dwarf::ffi::ChDwarfProjection;
    struct ChDwarfProjectionOwned(ChDwarfProjection);
    let ok = projection(COL_OFFSET);
    for bad in [
        ChDwarfProjection {
            column_mask: 1 << 16,
            ..ok
        },
        ChDwarfProjection { reserved: 1, ..ok },
        ChDwarfProjection {
            need_attr_offsets_vec: 1,
            ..ok
        },
        ChDwarfProjection {
            need_ancestor_offsets_vec: 2,
            ..ok
        },
    ] {
        assert_eq!(
            call(ChDwarfProjectionOwned(bad), &mut st, &mut unit),
            (ERR_INVALID_ARG, 0)
        );
    }
    // A rejected call does not poison the unit.
    assert_eq!(
        call(ChDwarfProjectionOwned(ok), &mut st, &mut unit).0,
        UNIT_DONE
    );
}
