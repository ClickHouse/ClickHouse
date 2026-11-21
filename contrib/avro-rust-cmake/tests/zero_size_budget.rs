//! The budget of items that occupy no bytes (`MAX_ZERO_SIZE_ITEMS`) belongs to one row, not to the block that the
//! row happens to be in: valid data must read the same whatever the container framing, while one row (however
//! its decode is split into calls, pauses and replays) must never get more than one budget.
//!
//! The data is the one that a reviewer found unreadable: `{id: long, ignored: array<null>, tail: string}`,
//! 1000 rows of 20000 null elements each in one OCF block, which is 12 kB of file and 20 million items in
//! total, read with the query that projects `id` and `tail`.

mod common;

use _ch_rust_avro::exec::{State, Status};
use _ch_rust_avro::ocf::{self, BlockCtx, BlockHeader, Parsed};
use _ch_rust_avro::program::{Action, PlanNode, Program, SinkDesc, SinkKind};
use _ch_rust_avro::schema::Schema;
use _ch_rust_avro::{ErrorCode, Result, MAX_ZERO_SIZE_ITEMS};
use common::*;

// ---------------------------------------------------------------------------
// The file of the review: one projection, many framings
// ---------------------------------------------------------------------------

const FILE_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[
    {"name":"id","type":"long"},
    {"name":"ignored","type":{"type":"array","items":"null"}},
    {"name":"tail","type":"string"}]}"#;
const ROWS: u64 = 1000;
const ITEMS_PER_ROW: u64 = 20_000;
/// The file holds more zero-size items than one budget allows, so a budget shared by the block rejects it.
const _: () = assert!(ROWS * ITEMS_PER_ROW > MAX_ZERO_SIZE_ITEMS);

/// How the null items of a row are written. All of them are the same array.
#[derive(Clone, Copy, Debug)]
enum Encoding {
    /// One block with a positive count.
    Positive,
    /// One block with a negative count: the byte size of its items follows the count (here 0).
    Negative,
    /// Positive and negative in turn, by row.
    Alternating,
    /// Two blocks of half the items each.
    TwoBlocks,
}

fn null_array(items: u64, negative: bool) -> Vec<u8> {
    let mut v = if negative {
        let mut v = zz(-(items as i64));
        v.extend(zz(0));
        v
    } else {
        zz(items as i64)
    };
    v.extend(zz(0));
    v
}

fn file_row(id: u64, enc: Encoding) -> Vec<u8> {
    let mut v = zz(id as i64);
    match enc {
        Encoding::Positive => v.extend(null_array(ITEMS_PER_ROW, false)),
        Encoding::Negative => v.extend(null_array(ITEMS_PER_ROW, true)),
        Encoding::Alternating => v.extend(null_array(ITEMS_PER_ROW, id % 2 == 1)),
        Encoding::TwoBlocks => {
            v.extend(zz((ITEMS_PER_ROW / 2) as i64));
            v.extend(zz((ITEMS_PER_ROW / 2) as i64));
            v.extend(zz(0));
        }
    }
    v.extend(avro_bytes(b"after"));
    v
}

/// The container file with `rows_per_block` rows in each of its blocks.
fn ocf_file(rows_per_block: u64, enc: Encoding) -> Vec<u8> {
    let sync = [0x5au8; 16];
    let mut f = b"Obj\x01".to_vec();
    f.extend(zz(2));
    f.extend(avro_bytes(b"avro.schema"));
    f.extend(avro_bytes(FILE_SCHEMA.as_bytes()));
    f.extend(avro_bytes(b"avro.codec"));
    f.extend(avro_bytes(b"null"));
    f.extend(zz(0));
    f.extend(sync);
    let mut id = 0;
    while id < ROWS {
        let rows = rows_per_block.min(ROWS - id);
        let payload: Vec<u8> = (id..id + rows).flat_map(|i| file_row(i, enc)).collect();
        f.extend(zz(rows as i64));
        f.extend(zz(payload.len() as i64));
        f.extend(payload);
        f.extend(sync);
        id += rows;
    }
    f
}

/// `SELECT id, tail`: `ignored` is skipped.
fn projection() -> (Program, Vec<SinkDesc>) {
    let sch: Schema = schema(FILE_SCHEMA);
    let root = sch.root();
    let plan = [
        PlanNode::new(Action::Record, root, 3),
        PlanNode::value(sch.child(root, 0), 0),
        PlanNode::skip(sch.child(root, 1)),
        PlanNode::value(sch.child(root, 2), 1),
    ];
    let sinks = vec![
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::Bytes, 0),
    ];
    (Program::compile(&sch, &sinks, &plan).unwrap(), sinks)
}

fn expected_projection() -> String {
    let ids: Vec<String> = (0..ROWS).map(|i| i.to_string()).collect();
    let tails = vec![hex(b"after"); ROWS as usize];
    format!("0:{}\n1:{}\n", ids.join(" "), tails.join(" "))
}

/// Reads a whole container file the way the ClickHouse reader does: one decoder state for the file, reset
/// before the first decode of every block (`Plan::beginBlock`), and the block decoded in batches of
/// `rows_per_call` rows into sinks that start with `initial` elements and grow by `growth` when the executor asks.
fn read_projected(
    file: &[u8],
    program: &Program,
    sinks: &[SinkDesc],
    rows_per_call: u64,
    initial: usize,
    growth: usize,
) -> Result<String> {
    let limits = ocf::OcfLimits::default();
    let (header, mut pos) = match ocf::parse_header(file, true, &limits)? {
        Parsed::Done { value, consumed } => (value, consumed),
        Parsed::NeedMore { .. } => unreachable!("eof was set"),
    };
    let ctx = BlockCtx {
        codec: header.codec,
        min_record_size: program.min_row_size(),
        max_block_bytes: 0,
    };
    let mut state = State::new(0);
    let mut bufs = Buffers::new(sinks, initial);
    let mut rows = 0;
    loop {
        let (objects, bytes) = match ocf::parse_block_header(&file[pos..], true, &ctx)? {
            Parsed::Done {
                value: BlockHeader::End,
                ..
            } => break,
            Parsed::Done {
                value:
                    BlockHeader::Block {
                        object_count,
                        byte_count,
                    },
                consumed,
            } => {
                pos += consumed;
                (object_count, byte_count as usize)
            }
            Parsed::NeedMore { .. } => unreachable!("eof was set"),
        };
        state.reset();
        let (stats, used) = drive_state(
            &mut state,
            program,
            &file[pos..pos + bytes],
            objects,
            rows_per_call,
            growth,
            &mut bufs,
        )?;
        assert_eq!((stats.rows, used), (objects, bytes));
        rows += objects;
        pos += bytes;
        match ocf::check_sync(&file[pos..], &header.sync, true)? {
            Parsed::Done { consumed, .. } => pos += consumed,
            Parsed::NeedMore { .. } => unreachable!("eof was set"),
        }
    }
    assert_eq!(rows, ROWS);
    Ok(bufs.dump())
}

/// The same rows, the same query, and the same answer whatever the OCF block boundaries, the sign of the
/// array counts, the size of the batches of the reader, and the stops for room of the sinks. One block of 1000
/// rows is the file that is rejected when the budget is the block's: it holds 20 million zero-size items,
/// 16777216 are allowed.
#[test]
fn projection_reads_the_same_rows_whatever_the_ocf_block_boundaries() {
    let (program, sinks) = projection();
    let expected = expected_projection();
    let mut failures = Vec::new();
    for enc in [
        Encoding::Positive,
        Encoding::Negative,
        Encoding::Alternating,
        Encoding::TwoBlocks,
    ] {
        for rows_per_block in [ROWS, 50, 1] {
            let file = ocf_file(rows_per_block, enc);
            if rows_per_block == ROWS && matches!(enc, Encoding::Positive) {
                assert!(file.len() < 16 * 1024, "{} bytes", file.len());
            }
            // Sinks with room for the whole file decode a block of 1000 rows in one call that never stops.
            for (rows_per_call, initial, growth) in
                [(ROWS, 8192, 1), (ROWS, 1, 3), (7, 1, 1), (1, 1, 16)]
            {
                let got = read_projected(&file, &program, &sinks, rows_per_call, initial, growth);
                let layout = format!(
                    "{enc:?}, {rows_per_block} rows per block, {rows_per_call} rows per call, {initial} initial"
                );
                match got {
                    Ok(dump) if dump == expected => {}
                    Ok(_) => failures.push(format!("{layout}: other rows")),
                    Err(e) => failures.push(format!("{layout}: {:?}", e.code)),
                }
            }
        }
    }
    // Every layout is listed, not the first that fails.
    assert!(failures.is_empty(), "{:#?}", failures);
}

// ---------------------------------------------------------------------------
// One row, however it is split
// ---------------------------------------------------------------------------

type Outcome = std::result::Result<String, ErrorCode>;

/// `{a: array<null>, b: array<null>}` with the offsets of both arrays in sinks, so that a row can stop for room
/// between `a` and `b` and again after both.
fn two_arrays() -> (Program, Vec<SinkDesc>) {
    let s = schema(
        r#"{"type":"record","name":"R","fields":[
            {"name":"a","type":{"type":"array","items":"null"}},
            {"name":"b","type":{"type":"array","items":"null"}}]}"#,
    );
    let u = universal_plan(&s);
    (Program::compile(&s, &u.sinks, &u.plan).unwrap(), u.sinks)
}

/// One array of null items made of the blocks of `counts` (positive counts, or negative ones with a size).
fn blocks(counts: &[u64], negative: bool) -> Vec<u8> {
    let mut v = Vec::new();
    for &c in counts.iter().filter(|c| **c > 0) {
        if negative {
            v.extend(zz(-(c as i64)));
            v.extend(zz(0));
        } else {
            v.extend(zz(c as i64));
        }
    }
    v.extend(zz(0));
    v
}

fn row(a: &[u64], b: &[u64], negative: bool) -> Vec<u8> {
    let mut v = blocks(a, negative);
    v.extend(blocks(b, negative));
    v
}

/// The same input through every way a caller has of cutting a decode into calls: all the rows in one call or a
/// row per call, sinks that have room or that stop every row (completed rows are replayed after a stop that is
/// not in the first row of a call), windows that cut rows (an error, and the row is decoded again from its
/// start), and streaming windows that pause rows for input. Windows of one byte cut every header.
fn every_way(
    program: &Program,
    descs: &[SinkDesc],
    input: &[u8],
    rows: u64,
) -> Vec<(String, Outcome)> {
    let mut out = Vec::new();
    let finish =
        |r: Result<()>, bufs: &Buffers| -> Outcome { r.map(|()| bufs.dump()).map_err(|e| e.code) };
    for (name, rows_per_call, initial) in [
        ("one call", rows, 64),
        ("row per call", 1, 64),
        ("tight sinks", rows, 0),
        ("tight sinks, row per call", 1, 0),
    ] {
        let mut bufs = Buffers::new(descs, initial);
        let r = drive(program, input, rows, rows_per_call, initial, 1, &mut bufs).map(|_| ());
        out.push((name.to_string(), finish(r, &bufs)));
    }
    for (first, step) in [(1usize, 1usize), (2, 3), (7, 5)] {
        let mut bufs = Buffers::new(descs, 0);
        let r = drive_windows(program, input, rows, first, step, &mut bufs).map(|_| ());
        out.push((format!("windows {first}+{step}"), finish(r, &bufs)));
        for rows_per_call in [rows, 1] {
            let mut bufs = Buffers::new(descs, 0);
            let r = drive_stream(program, input, rows, rows_per_call, first, step, &mut bufs)
                .map(|_| ());
            out.push((
                format!("streaming {first}+{step}, {rows_per_call} rows per call"),
                finish(r, &bufs),
            ));
        }
    }
    out
}

fn offsets(per_row: u64, rows: u64) -> String {
    let v: Vec<String> = (1..=rows).map(|r| (r * per_row).to_string()).collect();
    v.join(" ")
}

/// Rows of exactly the budget each, in one block, are all valid: each row spends its own budget. The budget
/// is the same whether the row has one array, two, or an array in several blocks, and whether the counts are
/// positive or negative.
#[test]
fn every_row_has_a_budget_of_its_own_in_every_way_of_decoding_it() {
    let (program, descs) = two_arrays();
    let half = MAX_ZERO_SIZE_ITEMS / 2;
    let rows = 3u64;
    for negative in [false, true] {
        for (a, b) in [
            (vec![half], vec![half]),
            (vec![MAX_ZERO_SIZE_ITEMS], vec![]),
            (vec![], vec![MAX_ZERO_SIZE_ITEMS]),
            (vec![half / 2, half / 2], vec![half / 2, half / 2]),
        ] {
            let input: Vec<u8> = (0..rows).flat_map(|_| row(&a, &b, negative)).collect();
            let (sum_a, sum_b) = (a.iter().sum::<u64>(), b.iter().sum::<u64>());
            let expected = format!("0:{}\n1:{}\n", offsets(sum_a, rows), offsets(sum_b, rows));
            for (way, outcome) in every_way(&program, &descs, &input, rows) {
                assert_eq!(
                    outcome,
                    Ok(expected.clone()),
                    "{way}: a={a:?} b={b:?} negative={negative}"
                );
            }
        }
    }
}

/// What is over the budget of one row is over it however the row is decoded, also when the row follows rows
/// that spent their whole budgets: a stop for room, a cut by a window, a pause for input, a replay of the rows
/// before it, none of them is a new budget for the row that is being decoded.
#[test]
fn one_row_over_the_budget_fails_in_every_way_of_decoding_it() {
    let (program, descs) = two_arrays();
    let half = MAX_ZERO_SIZE_ITEMS / 2;
    let quarter = half / 2;
    let full = row(&[half], &[half], false);
    for negative in [false, true] {
        for (a, b) in [
            // One block over, in `a`, in `b`, and spread over `a` and `b`.
            (vec![MAX_ZERO_SIZE_ITEMS + 1], vec![]),
            (vec![], vec![MAX_ZERO_SIZE_ITEMS + 1]),
            (vec![half], vec![half + 1]),
            (vec![half + 1], vec![half]),
            // Over by one item in the last of several blocks, in the first array and across the arrays.
            (vec![quarter, quarter, quarter, quarter + 1], vec![]),
            (vec![quarter, quarter], vec![quarter, quarter + 1]),
            // Blocks that are each within the budget and together over it.
            (vec![MAX_ZERO_SIZE_ITEMS, 1], vec![]),
        ] {
            let bad = row(&a, &b, negative);
            // Alone, after a row that spent all its budget, and after one that spent none of it.
            let after_full: Vec<u8> = [full.clone(), full.clone(), bad.clone()].concat();
            let after_empty: Vec<u8> = [row(&[], &[], false), bad.clone()].concat();
            for (input, rows) in [(bad.clone(), 1), (after_full, 3), (after_empty, 2)] {
                for (way, outcome) in every_way(&program, &descs, &input, rows) {
                    assert_eq!(
                        outcome,
                        Err(ErrorCode::TooManyItems),
                        "{way}: a={a:?} b={b:?} negative={negative} rows={rows}"
                    );
                }
            }
        }
    }
}

/// The calls of the executor itself, with what each reports: rows that complete are committed whole, and the
/// row that stops for room is rolled back, becomes the first row of the next call, is paused there and resumed,
/// with the budget of one row at every step: not the one that the rows before it used, and not a new one at a
/// pause or a replay.
#[test]
fn a_row_that_stops_is_decoded_again_with_the_budget_of_one_row() {
    use _ch_rust_avro::exec::decode_partial;
    let (program, descs) = two_arrays();
    let half = MAX_ZERO_SIZE_ITEMS / 2;
    let full = row(&[half], &[half], false);
    let rows = 3u64;
    let input: Vec<u8> = (0..rows).flat_map(|_| full.clone()).collect();

    // Room for one row in each sink: the second row stops for room in the middle of a call that has already
    // completed the first, which is replayed and reported, and the second is decoded again by the next call.
    let mut bufs = Buffers::new(&descs, 1);
    let mut state = State::new(0);
    let (mut done, mut pos, mut stops) = (0u64, 0usize, 0u32);
    while done < rows {
        let p = bufs
            .with_sinks(|s| decode_partial(&program, &mut state, &input[pos..], rows - done, s))
            .unwrap();
        if stops == 0 {
            assert!(matches!(p.status, Status::NeedSpace { .. }));
            assert_eq!((p.rows_done, p.consumed), (1, full.len()));
            assert_eq!(state.stats().restarts, 1);
            assert!(state.at_row_boundary());
        }
        done += p.rows_done;
        pos += p.consumed;
        match p.status {
            Status::NeedSpace { .. } => {
                stops += 1;
                assert!(stops < 100, "no progress");
                for need in state.needs() {
                    bufs.grow(need.sink as usize, need.min_cap, need.min_cap2);
                }
            }
            Status::RowLimit | Status::BlockDone => {}
            Status::NeedInput => unreachable!("streaming mode is off"),
        }
    }
    assert_eq!((done, pos), (rows, input.len()));
    // Both ways of stopping happened: a row rolled back after completed rows, and a row paused in its first.
    let stats = state.stats();
    assert!(
        stats.restarts >= 1 && stats.need_space > stats.restarts,
        "{stats:?}"
    );
    assert_eq!(
        bufs.dump(),
        format!("0:{}\n1:{}\n", offsets(half, rows), offsets(half, rows))
    );
}

/// A row that the end of the window cuts is decoded again from its start with the budget of a row: what the
/// cut attempt counted is not charged a second time, and what the completed rows before it spent is not
/// charged at all.
#[test]
fn a_row_cut_by_the_window_is_charged_once_when_it_is_decoded_again() {
    let s = schema(r#"{"type":"array","items":"null"}"#);
    let u = skip_plan(&s);
    let skip = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let half = MAX_ZERO_SIZE_ITEMS / 2;
    let one_array = |counts: &[u64]| blocks(counts, false);
    let decode_rows = |state: &mut State, input: &[u8], rows: u64| {
        _ch_rust_avro::exec::decode_partial(&skip, state, input, rows, &mut [])
    };
    let mut state = State::new(0);
    // Rows that spent the budget, then a row cut after a block, again and again.
    let full = one_array(&[MAX_ZERO_SIZE_ITEMS]);
    let mut two = full.clone();
    two.extend(full.clone());
    assert_eq!(decode_rows(&mut state, &two, 2).unwrap().rows_done, 2);
    let mut cut = one_array(&[half]);
    cut.pop();
    for _ in 0..4 {
        let e = decode_rows(&mut state, &cut, 1).unwrap_err();
        assert_eq!(
            (e.error.code, e.rows_done, e.consumed),
            (ErrorCode::Eof, 0, 0)
        );
    }
    // The row, complete, takes the budget of a row: all of it.
    assert_eq!(
        decode_rows(&mut state, &one_array(&[half, half]), 1)
            .unwrap()
            .rows_done,
        1
    );
    // And not a single item more, though the attempts before it counted `half` each.
    let mut cut = one_array(&[half, half]);
    cut.pop();
    let e = decode_rows(&mut state, &cut, 1).unwrap_err();
    assert_eq!(e.error.code, ErrorCode::Eof);
    let e = decode_rows(&mut state, &one_array(&[half, half + 1]), 1).unwrap_err();
    assert_eq!(e.error.code, ErrorCode::TooManyItems);
    // The failure left the state clean for the rows after it.
    assert_eq!(decode_rows(&mut state, &full, 1).unwrap().rows_done, 1);
}
