//! The work for items that occupy no bytes. A row may spend `MAX_ZERO_SIZE_ITEMS` of them, and every row has a
//! budget of its own, so the items of a stream of rows are bounded by the bytes of the stream times that
//! budget. For the loops whose items nothing reads that is no work at all: they are counted, not iterated.

mod common;

use std::time::{Duration, Instant};

use _ch_rust_avro::exec::{decode, State};
use _ch_rust_avro::program::Program;
use _ch_rust_avro::MAX_ZERO_SIZE_ITEMS;
use common::*;

fn program(json: &str) -> (Program, Vec<_ch_rust_avro::program::SinkDesc>) {
    let s = schema(json);
    let u = skip_plan(&s);
    (Program::compile(&s, &u.sinks, &u.plan).unwrap(), u.sinks)
}

/// 256 rows that each hold the whole budget: four billion items of 0 bytes in 5 * 256 bytes of input. Iterating
/// them takes seconds (about 2 s per billion in a release build); counting them is a few instructions per block
/// header.
#[test]
fn rows_of_unread_null_items_cost_their_headers_not_their_items() {
    let (skip, _) = program(r#"{"type":"array","items":"null"}"#);
    let rows = 256u64;
    let mut row = zz(MAX_ZERO_SIZE_ITEMS as i64);
    row.extend(zz(0));
    let input: Vec<u8> = (0..rows).flat_map(|_| row.clone()).collect();
    assert!(input.len() < 2000);
    let mut state = State::new(0);
    let t = Instant::now();
    let p = decode(&skip, &mut state, &input, rows, &mut []).unwrap();
    assert!(
        t.elapsed() < Duration::from_secs(1),
        "took {:?}",
        t.elapsed()
    );
    assert_eq!((p.rows_done, p.consumed), (rows, input.len()));
    // All the items of each block but its last, which goes through the read of the next header.
    assert_eq!(
        state.stats().empty_loop_items,
        rows * (MAX_ZERO_SIZE_ITEMS - 1)
    );
    assert_eq!(state.stats().bulk_items, 0);
}

/// The items are counted for the offsets that the program writes, too: the loop has no instruction in its body
/// either way, and the sink gets the total of every block.
#[test]
fn the_offsets_of_an_array_of_null_items_are_their_total() {
    let s = schema(r#"{"type":"array","items":"null"}"#);
    let u = universal_plan(&s);
    let program = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let mut input = Vec::new();
    // Row 0: three blocks (one of them with a size, negative count); row 1: empty; row 2: one item.
    input.extend(zz(7));
    input.extend(zz(-5));
    input.extend(zz(0));
    input.extend(zz(1));
    input.extend(zz(0));
    input.extend(zz(0));
    input.extend(zz(1));
    input.extend(zz(0));
    let mut bufs = Buffers::new(&u.sinks, 4);
    let mut state = State::new(0);
    let p = bufs
        .with_sinks(|sinks| decode(&program, &mut state, &input, 3, sinks))
        .unwrap();
    assert_eq!((p.rows_done, p.consumed), (3, input.len()));
    assert_eq!(bufs.dump(), "0:13 13 14\n");
    // 6 + 4 + 0 of the blocks of row 0 and nothing of the others: a block's last item is not counted here.
    assert_eq!(state.stats().empty_loop_items, 6 + 4);
}

/// Loops whose items have instructions are not collapsed: a map has a key per item, and an array of numbers
/// or of anything with bytes is decoded (in a run, for a number).
#[test]
fn loops_with_instructions_in_their_body_still_iterate() {
    for (json, row) in [
        (r#"{"type":"map","values":"null"}"#, {
            let mut v = zz(3);
            for key in [&b"a"[..], b"b", b"c"] {
                v.extend(avro_bytes(key));
            }
            v.extend(zz(0));
            v
        }),
        (r#"{"type":"array","items":"long"}"#, {
            let mut v = zz(3);
            v.extend([2, 4, 6]);
            v.extend(zz(0));
            v
        }),
        (r#"{"type":"array","items":"string"}"#, {
            let mut v = zz(3);
            for s in [&b"x"[..], b"yy", b""] {
                v.extend(avro_bytes(s));
            }
            v.extend(zz(0));
            v
        }),
    ] {
        let (skip, _) = program(json);
        let mut state = State::new(0);
        let p = decode(&skip, &mut state, &row, 1, &mut []).unwrap();
        assert_eq!((p.rows_done, p.consumed), (1, row.len()), "{json}");
        assert_eq!(state.stats().empty_loop_items, 0, "{json}");
    }
}

/// The inner loops of nested arrays are collapsed one outer item at a time, each with the row's budget.
#[test]
fn nested_unread_null_arrays_are_counted_per_outer_item() {
    let (skip, _) = program(r#"{"type":"array","items":{"type":"array","items":"null"}}"#);
    let (outer, inner) = (4096u64, 4096u64);
    let mut row = zz(outer as i64);
    for _ in 0..outer {
        row.extend(zz(inner as i64));
        row.extend(zz(0));
    }
    row.extend(zz(0));
    assert_eq!(outer * inner, MAX_ZERO_SIZE_ITEMS);
    let mut state = State::new(0);
    let p = decode(&skip, &mut state, &row, 1, &mut []).unwrap();
    assert_eq!((p.rows_done, p.consumed), (1, row.len()));
    assert_eq!(state.stats().empty_loop_items, outer * (inner - 1));
}

/// A record that always encodes to no bytes has nothing to skip whether it is written once or shared (a shared
/// record is otherwise a routine that is called, which would be an instruction per item), and however deep its
/// fields go: loops over it are empty loops too. The program is the two loops and the end of the row.
#[test]
fn unread_zero_width_records_shared_or_nested_are_counted_too() {
    let (skip, _) = program(
        r#"{"type":"record","name":"R","fields":[
            {"name":"a","type":{"type":"array","items":{"type":"record","name":"E","fields":[
                {"name":"n","type":"null"},
                {"name":"inner","type":{"type":"record","name":"I","fields":[{"name":"m","type":"null"}]}}]}}},
            {"name":"b","type":{"type":"array","items":"E"}},
            {"name":"c","type":{"type":"array","items":"I"}}]}"#,
    );
    assert_eq!(skip.op_count(), 7, "{skip:?}");
    let (a, b, c) = (
        MAX_ZERO_SIZE_ITEMS / 4,
        MAX_ZERO_SIZE_ITEMS / 2,
        MAX_ZERO_SIZE_ITEMS / 4,
    );
    let mut row = Vec::new();
    for items in [a, b, c] {
        row.extend(zz(items as i64));
        row.extend(zz(0));
    }
    let mut state = State::new(0);
    let p = decode(&skip, &mut state, &row, 1, &mut []).unwrap();
    assert_eq!((p.rows_done, p.consumed), (1, row.len()));
    assert_eq!(state.stats().empty_loop_items, (a - 1) + (b - 1) + (c - 1));
}
