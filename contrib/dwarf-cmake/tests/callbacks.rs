//! Callback contract (reserve, cancel, growth and pointer refresh), argument validation, handle
//! lifecycle, string column layout and the name tables, all through the C ABI.
#![allow(unsafe_code)]

mod common;

use _ch_rust_dwarf::ffi::*;
use _ch_rust_dwarf::outbuf::*;
use common::*;
use gimli::constants::*;
use std::collections::HashSet;
use std::ffi::{c_char, c_void};

const CU: u16 = 0x11;
const VAR: u16 = 0x34;

fn at(a: gimli::DwAt) -> u16 {
    a.0
}
fn form(f: gimli::DwForm) -> u16 {
    f.0
}

/// `rows` rows: a CU, `rows - 2` one-attribute variables, and the closing null entry.
fn big_unit(rows: usize) -> TestFile {
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VAR,
            false,
            &[(at(DW_AT_decl_line), form(DW_FORM_data1), None)],
        ),
    ];
    let mut dies = die(1, &[]);
    for i in 0..rows - 2 {
        dies.extend(die(2, &[&[(i % 250) as u8]]));
    }
    dies.push(0);
    one_unit(4, 8, &decls, &dies, vec![])
}

#[test]
fn reserve_failure_is_reported_and_poisons_the_unit() {
    let file = big_unit(50);
    let mut st = Collector::new();
    st.fail_at_reserve = Some(1);
    let mut unit = file.open_unit(0).unwrap();
    let (rc, msg, chunk) = parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st);
    assert_eq!(rc, ERR_CALLBACK);
    assert!(chunk.is_none());
    assert!(msg.contains("reserve"), "{msg}");
    // The unit is unusable after a failed call, but the failure does not crash anything.
    let mut st = Collector::new();
    let (rc, _, _) = parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st);
    assert_eq!(rc, ERR_INVALID_ARG);
    // A fresh unit of the same file works.
    let (d, _) = read_unit(&file, 0, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(d.rows, 50);
}

#[test]
fn a_late_reserve_failure_discards_the_whole_call() {
    let file = big_unit(if cfg!(miri) { 40 } else { 5000 });
    let mut st = Collector::new();
    st.min_growth = 1;
    st.fail_at_reserve = Some(5);
    let mut unit = file.open_unit(0).unwrap();
    let (rc, _, chunk) = parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st);
    assert_eq!(rc, ERR_CALLBACK);
    assert!(chunk.is_none());
    assert!(st.reserve_calls >= 5);
}

#[test]
fn misbehaving_reserve_callbacks_are_detected() {
    let file = big_unit(10);
    let mut unit = file.open_unit(0).unwrap();
    let mut st = Collector::new();
    st.ignore_reserve = true;
    let (rc, msg, _) = parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st);
    assert_eq!(rc, ERR_CALLBACK, "{msg}");

    let mut unit = file.open_unit(0).unwrap();
    let mut st = Collector::new();
    st.corrupt_elem_size = true;
    let (rc, msg, _) = parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st);
    assert_eq!(rc, ERR_INVALID_ARG, "{msg}");
}

#[test]
fn cancel_stops_the_walk_without_a_message() {
    let file = big_unit(if cfg!(miri) { 20 } else { 20_000 });
    let mut st = Collector::new();
    st.stop_at_poll = Some(1);
    let mut unit = file.open_unit(0).unwrap();
    let (rc, msg, chunk) = parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st);
    assert_eq!(rc, CANCELLED);
    assert!(chunk.is_none());
    assert_eq!(msg, "", "CANCELLED carries no error text");

    if !cfg!(miri) {
        // Cancel in the middle of a chunk, after two full quanta of work.
        let mut st = Collector::new();
        st.stop_at_poll = Some(3);
        let mut unit = file.open_unit(0).unwrap();
        let (rc, _, _) = parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st);
        assert_eq!(rc, CANCELLED);
        assert_eq!(st.polls, 3);
    }
    // A cancelled unit is poisoned.
    let mut st = Collector::new();
    assert_eq!(
        parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st).0,
        ERR_INVALID_ARG
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn cancel_is_polled_at_least_every_4096_work_items() {
    let rows = 20_000usize;
    let file = big_unit(rows);
    let mut st = Collector::new();
    let (d, _) = read_unit(&file, 0, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(d.rows, rows);
    // One poll at the start of the call, then one per 4096 work items. Work items are DIEs plus
    // attributes: the CU and the null entry count 1, each variable 2.
    let work = 2 * rows - 2;
    assert_eq!(st.polls, 1 + work / 4096);
}

#[test]
fn a_failing_cancel_callback_is_an_output_callback_failure() {
    let file = big_unit(if cfg!(miri) { 20 } else { 20_000 });
    let mut st = Collector::new();
    st.fail_at_poll = Some(if cfg!(miri) { 1 } else { 2 });
    let mut unit = file.open_unit(0).unwrap();
    let (rc, msg, _) = parse_one(&file, &mut unit, ALL_COLS, 65536, &mut st);
    assert_eq!(rc, ERR_CALLBACK);
    assert!(msg.contains("cancel"), "{msg}");
}

#[test]
fn reserve_growth_is_geometric_and_pointers_are_refreshed() {
    let rows = if cfg!(miri) { 60 } else { 5000 };
    let file = big_unit(rows);
    let mut reference = Collector::new();
    reference.min_growth = if cfg!(miri) { 128 } else { 1 << 16 };
    let (expected, _) = read_unit(&file, 0, ALL_COLS, 65536, &mut reference).unwrap();
    assert!(
        reference.pointer_changes <= SINK_COUNT,
        "one allocation per sink is enough"
    );

    let mut st = Collector::new();
    st.min_growth = 1;
    let (d, _) = read_unit(&file, 0, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(
        d, expected,
        "tiny initial capacities change nothing but the reservation pattern"
    );
    let min_moves = if cfg!(miri) { 2 } else { 20 };
    assert!(
        st.pointer_changes > min_moves,
        "buffers moved while parsing ({})",
        st.pointer_changes
    );
    // Geometric growth: far fewer callbacks than rows, and never one per value.
    assert!(
        st.reserve_calls < 120,
        "{} reserve calls for {rows} rows",
        st.reserve_calls
    );
    assert!(st.reserve_calls > 3);
    // Every request names only active sinks, each at most once, with a positive minimum.
    let plan = plan_of(ALL_COLS);
    for call in &st.reserve_requests {
        let mut seen = HashSet::new();
        for &(id, min_cap) in call {
            assert!(plan.sink_active(ALL_SINK_IDS[id]));
            assert!(seen.insert(id), "duplicate sink in one request list");
            assert!(min_cap > 0);
        }
    }
}

#[test]
fn only_projected_sinks_are_requested() {
    let rows = if cfg!(miri) { 20 } else { 200 };
    let file = big_unit(rows);
    let mut st = Collector::new();
    let mask = COL_OFFSET | COL_ATTR_INT;
    let (d, _) = read_unit(&file, 0, mask, 65536, &mut st).unwrap();
    assert_eq!(d.rows, rows);
    let plan = plan_of(mask);
    for call in &st.reserve_requests {
        for &(id, _) in call {
            assert!(
                plan.sink_active(ALL_SINK_IDS[id]),
                "{:?} requested",
                ALL_SINK_IDS[id]
            );
        }
    }
}

#[test]
fn strings_keep_their_exact_bytes_without_terminators() {
    let decls = [
        abbrev(1, CU, true, &[]),
        abbrev(
            2,
            VAR,
            false,
            &[
                (at(DW_AT_name), form(DW_FORM_string), None),
                (at(DW_AT_linkage_name), form(DW_FORM_string), None),
                (at(DW_AT_byte_size), form(DW_FORM_block1), None),
                (at(DW_AT_byte_size), form(DW_FORM_block4), None),
                (at(DW_AT_byte_size), form(DW_FORM_string), None),
            ],
        ),
    ];
    let big_len = if cfg!(miri) { 512usize } else { 65_536usize };
    let big_name = "n".repeat(big_len);
    let big_block: Vec<u8> = (0..big_len as u32).map(|i| (i % 3) as u8).collect(); // embedded zeros
    let dies = [
        die(1, &[]),
        die(
            2,
            &[
                &cstr(""),
                &cstr("l"),
                &[0],
                &[le32(0), vec![]].concat(),
                &cstr(""),
            ],
        ),
        die(
            2,
            &[
                &cstr(&big_name),
                &cstr(""),
                &[3, 0, 0, 0],
                &[le32(big_block.len() as u32), big_block.clone()].concat(),
                &cstr(&big_name),
            ],
        ),
        die(
            2,
            &[
                &cstr("tail"),
                &cstr("x"),
                &[2, 0, 9],
                &[le32(1), vec![0]].concat(),
                &cstr("z"),
            ],
        ),
        vec![0],
    ]
    .concat();
    let file = one_unit(4, 8, &decls, &dies, vec![]);
    let d = rows_of(&file);
    let name = d.name.as_ref().unwrap();
    assert_eq!(name[1], b"");
    assert_eq!(name[2], big_name.as_bytes());
    assert_eq!(name[3], b"tail");
    assert_eq!(d.linkage_name.as_ref().unwrap()[1], b"l");
    assert_eq!(d.linkage_name.as_ref().unwrap()[2], b"");
    let attr_str = d.attr_str.as_ref().unwrap();
    assert_eq!(
        attr_str[1],
        vec![b"".to_vec(), b"l".to_vec(), vec![], vec![], vec![]]
    );
    assert_eq!(attr_str[2][2], vec![0, 0, 0]);
    assert_eq!(attr_str[2][3], big_block);
    assert_eq!(attr_str[2][4], big_name.as_bytes());
    assert_eq!(attr_str[3][2], vec![0, 9]);
    assert_eq!(attr_str[3][3], vec![0]);
    // The raw sink holds exactly the payload bytes: no terminators anywhere.
    let mut st = Collector::new();
    let mut unit = file.open_unit(0).unwrap();
    let plan = plan_of(COL_NAME);
    let mut sinks = fresh_sinks(plan);
    let mut progress = unsafe { std::mem::zeroed::<ChDwarfProgress>() };
    let mut err = [0 as c_char; 256];
    let proj = projection(COL_NAME);
    let rc = unsafe {
        ch_dwarf_unit_parse_chunk(
            file.file,
            unit.unit,
            &proj,
            &mut sinks,
            Some(reserve_cb),
            Some(cancel_cb),
            (&mut st as *mut Collector).cast(),
            65536,
            &mut progress,
            err.as_mut_ptr(),
        )
    };
    assert_eq!(rc, UNIT_DONE);
    assert_eq!(
        sinks.sinks[SinkId::NameChars as usize].len,
        big_name.len() + 4
    );
    assert_eq!(sinks.sinks[SinkId::NameOffsets as usize].len, 5);
    let _ = &mut unit;
}

#[test]
fn null_and_inconsistent_arguments_are_rejected() {
    let file = big_unit(10);
    let mut unit = file.open_unit(0).unwrap();
    let plan = plan_of(ALL_COLS);
    let proj = projection(ALL_COLS);
    let mut st = Collector::new();
    let opaque: *mut c_void = (&mut st as *mut Collector).cast();
    let mut err = [0 as c_char; 256];
    let mut progress = unsafe { std::mem::zeroed::<ChDwarfProgress>() };

    let mut call = |file: *const ChDwarfFile,
                    unit: *mut ChDwarfUnit,
                    proj: *const ChDwarfProjection,
                    sinks: *mut ChDwarfBatchSinks,
                    reserve: ChDwarfReserveFn,
                    cancel: ChDwarfCancelFn,
                    max_rows: usize,
                    progress: *mut ChDwarfProgress| unsafe {
        ch_dwarf_unit_parse_chunk(
            file,
            unit,
            proj,
            sinks,
            reserve,
            cancel,
            opaque,
            max_rows,
            progress,
            err.as_mut_ptr(),
        )
    };

    let mut sinks = fresh_sinks(plan);
    let s: *mut ChDwarfBatchSinks = &mut sinks;
    let p: *mut ChDwarfProgress = &mut progress;
    let (f, u) = (file.file as *const ChDwarfFile, unit.unit);
    let (r, c) = (Some(reserve_cb as _), Some(cancel_cb as _));
    use std::ptr::{null, null_mut};
    assert_eq!(call(null(), u, &proj, s, r, c, 10, p), ERR_INVALID_ARG);
    assert_eq!(call(f, null_mut(), &proj, s, r, c, 10, p), ERR_INVALID_ARG);
    assert_eq!(call(f, u, null(), s, r, c, 10, p), ERR_INVALID_ARG);
    assert_eq!(call(f, u, &proj, null_mut(), r, c, 10, p), ERR_INVALID_ARG);
    assert_eq!(call(f, u, &proj, s, None, c, 10, p), ERR_INVALID_ARG);
    assert_eq!(call(f, u, &proj, s, r, None, 10, p), ERR_INVALID_ARG);
    assert_eq!(call(f, u, &proj, s, r, c, 10, null_mut()), ERR_INVALID_ARG);
    assert_eq!(call(f, u, &proj, s, r, c, 0, p), ERR_INVALID_ARG);

    // Descriptor problems: wrong element size, len > cap, null data with capacity, non-empty sink.
    for corrupt in [
        |s: &mut ChDwarfBatchSinks| s.sinks[SinkId::Offset as usize].elem_size = 4,
        |s: &mut ChDwarfBatchSinks| s.sinks[SinkId::Size as usize].len = 1,
        |s: &mut ChDwarfBatchSinks| {
            s.sinks[SinkId::Tag as usize].cap = 8;
        },
        |s: &mut ChDwarfBatchSinks| {
            let sink = &mut s.sinks[SinkId::DeclLine as usize];
            sink.len = 2;
            sink.cap = 1;
        },
        |s: &mut ChDwarfBatchSinks| s.sinks[SinkId::AttrStrChars as usize].elem_size = 0,
    ] {
        let mut bad = fresh_sinks(plan);
        corrupt(&mut bad);
        assert_eq!(call(f, u, &proj, &mut bad, r, c, 10, p), ERR_INVALID_ARG);
        assert_eq!(
            bad.sinks[SinkId::Offset as usize].len,
            0,
            "nothing was written"
        );
    }

    // A unit from another file (another generation) is refused.
    let other = big_unit(10);
    assert_eq!(call(other.file, u, &proj, s, r, c, 10, p), ERR_INVALID_ARG);
    // Rejected calls leave the unit usable.
    assert_eq!(call(f, u, &proj, s, r, c, 10, p), UNIT_DONE);
    // ... and a finished unit refuses further work.
    let mut again = fresh_sinks(plan);
    assert_eq!(call(f, u, &proj, &mut again, r, c, 10, p), ERR_INVALID_ARG);
    unit.offset = 0;
}

#[test]
fn open_and_free_argument_checks() {
    let bytes = vec![0u8; 16];
    let mut file: *mut ChDwarfFile = std::ptr::null_mut();
    let mut err = [0 as c_char; 256];
    let opts = ChDwarfOptions {
        flags: 0,
        reserved: 0,
    };
    unsafe {
        // Null out pointer, null options, null bytes with a length, unsupported flags.
        assert_eq!(
            ch_dwarf_file_open(
                bytes.as_ptr(),
                16,
                &opts,
                std::ptr::null_mut(),
                err.as_mut_ptr()
            ),
            ERR_INVALID_ARG
        );
        assert_eq!(
            ch_dwarf_file_open(
                bytes.as_ptr(),
                16,
                std::ptr::null(),
                &mut file,
                err.as_mut_ptr()
            ),
            ERR_INVALID_ARG
        );
        assert_eq!(
            ch_dwarf_file_open(std::ptr::null(), 16, &opts, &mut file, err.as_mut_ptr()),
            ERR_INVALID_ARG
        );
        let flagged = ChDwarfOptions {
            flags: 1,
            reserved: 0,
        };
        assert_eq!(
            ch_dwarf_file_open(bytes.as_ptr(), 16, &flagged, &mut file, err.as_mut_ptr()),
            ERR_INVALID_ARG
        );
        // Zero length input is an ELF error, not an argument error; a null error buffer is fine.
        assert_eq!(
            ch_dwarf_file_open(std::ptr::null(), 0, &opts, &mut file, std::ptr::null_mut()),
            ERR_ELF
        );
        assert!(file.is_null());
        assert_eq!(
            ch_dwarf_file_unit_count(std::ptr::null(), std::ptr::null_mut()),
            ERR_INVALID_ARG
        );
        // Freeing null is a no-op.
        ch_dwarf_unit_free(std::ptr::null_mut());
        ch_dwarf_file_free(std::ptr::null_mut());
    }
    // The error text is always NUL terminated and cleared on success.
    let (rc, msg, _) = open_raw(&bytes);
    assert_eq!(rc, ERR_ELF);
    assert!(!msg.is_empty());
}

#[test]
fn units_move_between_threads_and_files_can_be_reopened() {
    struct SendUnit(*mut ChDwarfUnit);
    unsafe impl Send for SendUnit {}
    struct SendFile(*mut ChDwarfFile);
    unsafe impl Send for SendFile {}

    let file = big_unit(30);
    let mut st = Collector::new();
    let mut unit = file.open_unit(0).unwrap();
    let (rc, _, first) = parse_one(&file, &mut unit, ALL_COLS, 10, &mut st);
    assert_eq!(rc, CHUNK_READY);
    let first = first.unwrap().0;
    // Continue the same unit on another thread: ownership moves between calls.
    let (su, sf) = (SendUnit(unit.unit), SendFile(file.file));
    std::mem::forget(unit);
    let handle = std::thread::spawn(move || {
        let (su, sf) = (su, sf);
        let mut tmp = TestUnit {
            unit: su.0,
            offset: 0,
        };
        let f = TestFile {
            bytes: Vec::new(),
            file: sf.0,
        };
        let mut st = Collector::new();
        let (rc, _, chunk) = parse_one(&f, &mut tmp, ALL_COLS, 10, &mut st);
        std::mem::forget(f);
        let out = (rc, chunk.unwrap().0, SendUnit(tmp.unit));
        std::mem::forget(tmp);
        out
    });
    let (rc, second, su) = handle.join().unwrap();
    assert_eq!(rc, CHUNK_READY);
    assert_eq!(first.offset.as_ref().unwrap().len(), 10);
    assert!(second.offset.as_ref().unwrap()[0] > *first.offset.as_ref().unwrap().last().unwrap());
    unsafe { ch_dwarf_unit_free(su.0) };

    // Reset: free everything, reopen the same bytes, read again from scratch.
    let bytes = file.bytes.clone();
    drop(file);
    let again = TestFile::open(bytes).unwrap();
    let (d, _) = read_unit(&again, 0, ALL_COLS, 65536, &mut st).unwrap();
    assert_eq!(d.rows, 30);
}

fn dictionary(kind: i32) -> Vec<Vec<u8>> {
    struct Names {
        chars: Vec<u8>,
        offsets: Vec<u8>,
        calls: usize,
        fail: bool,
    }
    unsafe extern "C" fn reserve(
        opaque: *mut c_void,
        sink: *mut ChDwarfStringSink,
        min_chars: usize,
        min_offsets: usize,
    ) -> i32 {
        let st = &mut *opaque.cast::<Names>();
        st.calls += 1;
        if st.fail {
            return CH_DWARF_RESERVE_FAILED;
        }
        let sink = &mut *sink;
        st.chars.resize(min_chars.max(sink.chars.cap), 0xAA);
        st.offsets
            .resize(min_offsets.max(sink.offsets.cap) * 8, 0xAA);
        sink.chars.data = st.chars.as_mut_ptr();
        sink.chars.cap = st.chars.len();
        sink.offsets.data = st.offsets.as_mut_ptr();
        sink.offsets.cap = st.offsets.len() / 8;
        CH_DWARF_RESERVE_OK
    }
    let mut st = Names {
        chars: vec![],
        offsets: vec![],
        calls: 0,
        fail: false,
    };
    let empty = ChDwarfSink {
        data: std::ptr::null_mut(),
        len: 0,
        cap: 0,
        elem_size: 1,
        reserved: 0,
    };
    let mut sink = ChDwarfStringSink {
        chars: empty,
        offsets: ChDwarfSink {
            elem_size: 8,
            ..empty
        },
    };
    let mut err = [0 as c_char; 256];
    let rc = unsafe {
        ch_dwarf_write_name_table(
            kind,
            &mut sink,
            Some(reserve),
            (&mut st as *mut Names).cast(),
            err.as_mut_ptr(),
        )
    };
    assert_eq!(rc, OK, "{}", err_string(&err));
    assert_eq!(st.calls, 1, "one reservation for the whole table");
    assert_eq!(sink.offsets.len, 65_536);
    let ends: Vec<u64> = st.offsets[..65_536 * 8]
        .chunks_exact(8)
        .map(|c| u64::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(*ends.last().unwrap() as usize, sink.chars.len);
    let mut prev = 0;
    ends.iter()
        .map(|&e| {
            let s = st.chars[prev as usize..e as usize].to_vec();
            prev = e;
            s
        })
        .collect()
}

#[test]
#[cfg_attr(miri, ignore)]
fn name_tables_are_complete_and_unique() {
    let tags = dictionary(0);
    assert_eq!(tags.len(), 65_536);
    assert_eq!(tags[0], b"");
    assert_eq!(tags[0x11], b"compile_unit");
    assert_eq!(tags[0x2e], b"subprogram");
    assert_eq!(tags[0x9999], b"unknown: 39321");
    let attrs = dictionary(1);
    assert_eq!(attrs[3], b"name");
    assert_eq!(attrs[0x3a], b"decl_file");
    assert_eq!(attrs[0xffff], b"unknown: 65535");
    let forms = dictionary(2);
    assert_eq!(forms[0x08], b"string");
    assert_eq!(forms[0x21], b"implicit_const");
    assert_eq!(forms[0x2001], b"LLVM_addrx_offset");
    // ColumnUnique needs distinct entries, used or not.
    for (name, table) in [("tags", &tags), ("attrs", &attrs), ("forms", &forms)] {
        let distinct: HashSet<&Vec<u8>> = table.iter().collect();
        assert_eq!(
            distinct.len(),
            65_536,
            "{name} dictionary entries must be unique"
        );
    }
}

#[test]
fn name_table_argument_and_callback_errors() {
    let empty = ChDwarfSink {
        data: std::ptr::null_mut(),
        len: 0,
        cap: 0,
        elem_size: 1,
        reserved: 0,
    };
    let mut sink = ChDwarfStringSink {
        chars: empty,
        offsets: ChDwarfSink {
            elem_size: 8,
            ..empty
        },
    };
    let mut err = [0 as c_char; 256];
    unsafe extern "C" fn failing(
        _: *mut c_void,
        _: *mut ChDwarfStringSink,
        _: usize,
        _: usize,
    ) -> i32 {
        CH_DWARF_RESERVE_FAILED
    }
    unsafe extern "C" fn lying(
        _: *mut c_void,
        _: *mut ChDwarfStringSink,
        _: usize,
        _: usize,
    ) -> i32 {
        CH_DWARF_RESERVE_OK
    }
    let call = |kind: i32,
                sink: *mut ChDwarfStringSink,
                cb: ChDwarfStringReserveFn,
                err: &mut [c_char; 256]| unsafe {
        ch_dwarf_write_name_table(kind, sink, cb, std::ptr::null_mut(), err.as_mut_ptr())
    };
    assert_eq!(call(7, &mut sink, Some(failing), &mut err), ERR_INVALID_ARG);
    assert_eq!(
        call(0, std::ptr::null_mut(), Some(failing), &mut err),
        ERR_INVALID_ARG
    );
    assert_eq!(call(0, &mut sink, None, &mut err), ERR_INVALID_ARG);
    assert_eq!(call(0, &mut sink, Some(failing), &mut err), ERR_CALLBACK);
    assert_eq!(call(0, &mut sink, Some(lying), &mut err), ERR_CALLBACK);
    let mut wrong_width = sink;
    wrong_width.chars.elem_size = 2;
    assert_eq!(
        call(0, &mut wrong_width, Some(failing), &mut err),
        ERR_INVALID_ARG
    );
    let mut dirty = sink;
    dirty.offsets.len = 1;
    dirty.offsets.cap = 1;
    dirty.offsets.data = std::ptr::NonNull::<u64>::dangling().as_ptr().cast();
    assert_eq!(
        call(0, &mut dirty, Some(failing), &mut err),
        ERR_INVALID_ARG
    );
}
