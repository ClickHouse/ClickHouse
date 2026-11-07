//! Valid parses of the checked-in fixtures, compared with rows recorded in
//! `tests/queries/0_stateless/02896_dwarf_format.reference` (produced by the old LLVM parser).

mod common;

use _ch_rust_dwarf::names::lookup_sparse;
use _ch_rust_dwarf::names_generated::{ATTR_NAMES, FORM_NAMES, TAG_NAMES};
use common::*;
use std::collections::HashMap;

const UNIT_NAME: &[u8] = b"tests/queries/0_stateless/data_dwarf/tiny.cpp";

fn load(name: &str) -> (Decoded, Vec<ChunkInfo>) {
    let file = TestFile::open(tiny_dwarf(name)).expect("fixture opens");
    assert_eq!(file.unit_count(), 1);
    let mut st = Collector::new();
    read_unit(&file, 0, ALL_COLS, 65536, &mut st).expect("unit parses")
}

fn name_of(table: &'static [(u16, &'static str)], id: u16, zero_empty: bool) -> String {
    if id == 0 && zero_empty {
        return String::new();
    }
    lookup_sparse(table, id).map_or_else(|| format!("unknown: {id}"), str::to_owned)
}

/// Elements of a ClickHouse text array: `[1,2]`, `['a','b, c']`, `[(1,2),(3,4)]`.
fn array_elements(text: &str) -> Vec<String> {
    let inner = text
        .strip_prefix('[')
        .and_then(|t| t.strip_suffix(']'))
        .expect("array text");
    let (mut out, mut cur, mut chars) = (Vec::new(), String::new(), inner.chars().peekable());
    let (mut in_quote, mut depth) = (false, 0);
    while let Some(c) = chars.next() {
        match c {
            '\\' if in_quote => cur.push(chars.next().expect("escape")),
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => {
                depth += 1;
                cur.push(c);
            }
            ')' if !in_quote => {
                depth -= 1;
                cur.push(c);
            }
            ',' if !in_quote && depth == 0 => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    if !inner.is_empty() {
        out.push(cur);
    }
    out
}

#[test]
fn rows_match_the_recorded_reference_output() {
    let reference = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../tests/queries/0_stateless/02896_dwarf_format.reference"),
    )
    .unwrap();
    let lines: Vec<&str> = reference.lines().collect();
    // Lines 1-10 are DWARF 4 rows, 28-37 DWARF 5 rows (`select * ... limit 10` per file).
    for (file, range) in [("tiny_dwarf4", 0..10), ("tiny_dwarf5", 27..37)] {
        let (d, infos) = load(file);
        assert_eq!(infos.last().unwrap().unit_name, UNIT_NAME);
        let by_offset: HashMap<u64, usize> = d
            .offset
            .as_ref()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, &o)| (o, i))
            .collect();
        for line in &lines[range] {
            let f: Vec<&str> = line.split('\t').collect();
            assert_eq!(f.len(), 16, "{line}");
            let offset: u64 = f[0].parse().unwrap();
            let r = *by_offset
                .get(&offset)
                .unwrap_or_else(|| panic!("{file}: no DIE at {offset}"));
            let ctx = format!("{file} @ {offset}");
            assert_eq!(
                u64::from(d.size.as_ref().unwrap()[r]),
                f[1].parse::<u64>().unwrap(),
                "{ctx} size"
            );
            assert_eq!(
                name_of(TAG_NAMES, d.tag.as_ref().unwrap()[r], true),
                f[2],
                "{ctx} tag"
            );
            assert_eq!(f[3].as_bytes(), UNIT_NAME, "{ctx} unit_name");
            assert_eq!(f[4], "0", "{ctx} unit_offset");
            let anc_tags: Vec<String> = d.ancestor_tags.as_ref().unwrap()[r]
                .iter()
                .map(|&t| name_of(TAG_NAMES, t, true))
                .collect();
            assert_eq!(anc_tags, array_elements(f[5]), "{ctx} ancestor_tags");
            let anc_offsets: Vec<String> = d.ancestor_offsets.as_ref().unwrap()[r]
                .iter()
                .map(u64::to_string)
                .collect();
            assert_eq!(anc_offsets, array_elements(f[6]), "{ctx} ancestor_offsets");
            assert_eq!(d.name.as_ref().unwrap()[r], f[7].as_bytes(), "{ctx} name");
            assert_eq!(
                d.linkage_name.as_ref().unwrap()[r],
                f[8].as_bytes(),
                "{ctx} linkage_name"
            );
            assert_eq!(
                d.decl_file.as_ref().unwrap()[r],
                f[9].as_bytes(),
                "{ctx} decl_file"
            );
            assert_eq!(
                d.decl_line.as_ref().unwrap()[r].to_string(),
                f[10],
                "{ctx} decl_line"
            );
            let ranges: Vec<String> = d.ranges.as_ref().unwrap()[r]
                .iter()
                .map(|(a, b)| format!("({a},{b})"))
                .collect();
            assert_eq!(ranges, array_elements(f[11]), "{ctx} ranges");
            let attr_names: Vec<String> = d.attr_name.as_ref().unwrap()[r]
                .iter()
                .map(|&a| name_of(ATTR_NAMES, a, false))
                .collect();
            assert_eq!(attr_names, array_elements(f[12]), "{ctx} attr_name");
            let attr_forms: Vec<String> = d.attr_form.as_ref().unwrap()[r]
                .iter()
                .map(|&a| name_of(FORM_NAMES, a, false))
                .collect();
            assert_eq!(attr_forms, array_elements(f[13]), "{ctx} attr_form");
            let attr_int: Vec<String> = d.attr_int.as_ref().unwrap()[r]
                .iter()
                .map(u64::to_string)
                .collect();
            assert_eq!(attr_int, array_elements(f[14]), "{ctx} attr_int");
            let attr_str: Vec<String> = d.attr_str.as_ref().unwrap()[r]
                .iter()
                .map(|s| String::from_utf8(s.clone()).unwrap())
                .collect();
            assert_eq!(attr_str, array_elements(f[15]), "{ctx} attr_str");
        }
    }
}

#[test]
fn row_counts_and_structure() {
    for name in ["tiny_dwarf4", "tiny_dwarf5"] {
        let (d, infos) = load(name);
        // Counted with the baseline binary: `select count() from file(..., DWARF)`.
        assert_eq!(d.rows, 2475, "{name}");
        assert_eq!(infos.len(), 1);
        let tags = d.tag.as_ref().unwrap();
        assert_eq!(name_of(TAG_NAMES, tags[0], true), "compile_unit");
        assert_eq!(d.ancestor_tags.as_ref().unwrap()[0], Vec::<u16>::new());
        // Every null entry closes an ancestor; the tree closes exactly at the last row.
        let nulls = tags.iter().filter(|&&t| t == 0).count();
        let with_children = d.ancestor_tags.as_ref().unwrap().iter().flatten().count();
        assert!(nulls > 100 && with_children > 1000);
        assert_eq!(
            *tags.last().unwrap(),
            0,
            "the last row is the root's terminator"
        );
        assert_eq!(
            d.ancestor_offsets.as_ref().unwrap().last().unwrap().len(),
            1
        );
        // Rows are in encoded order and contiguous: offset + size is the next offset.
        let (offs, sizes) = (d.offset.as_ref().unwrap(), d.size.as_ref().unwrap());
        for i in 0..d.rows - 1 {
            assert_eq!(offs[i] + u64::from(sizes[i]), offs[i + 1]);
        }
        assert_eq!(
            infos[0].bytes_advanced,
            offs[d.rows - 1] + u64::from(sizes[d.rows - 1]) - offs[0]
        );
    }
}

#[test]
fn chunk_size_does_not_change_the_rows() {
    for name in ["tiny_dwarf4", "tiny_dwarf5"] {
        let file = TestFile::open(tiny_dwarf(name)).unwrap();
        let mut st = Collector::new();
        let (whole, _) = read_unit(&file, 0, ALL_COLS, 65536, &mut st).unwrap();
        for max_rows in [1usize, 7, 64, 1000, 2474, 2475, 2476] {
            let (parts, infos) = read_unit(&file, 0, ALL_COLS, max_rows, &mut st).unwrap();
            assert_eq!(parts, whole, "{name} with {max_rows} rows per chunk");
            assert_eq!(infos.len(), whole.rows.div_ceil(max_rows));
        }
    }
}
