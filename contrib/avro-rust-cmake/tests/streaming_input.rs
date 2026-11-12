//! Streaming mode: a caller that has more bytes of a block than it passes to the executor does not have to
//! hold a whole row. The input ending inside a row is `NeedInput`, the row is paused, and the bytes that the
//! executor is done with are dropped, so a `bytes`, `string` or `fixed` value that the query does not read
//! costs no more memory than the window, however large it is.

mod common;

use _ch_rust_avro::exec::{decode_partial, decode_resumed, State, Status};
use _ch_rust_avro::program::{Action, PlanNode, Program, SinkDesc, SinkKind};
use _ch_rust_avro::{ErrorCode, MAX_ZERO_SIZE_ITEMS};
use common::*;

fn compile(s: &str, sinks: &[SinkDesc], plan: &[PlanNode]) -> Program {
    Program::compile(&schema(s), sinks, plan).unwrap()
}

/// The result of decoding all of `input` at once, without streaming.
fn one_shot(program: &Program, input: &[u8], rows: u64) -> _ch_rust_avro::Result<String> {
    let mut bufs = Buffers::new(program.sinks(), 1);
    let (stats, used) = drive(program, input, rows, rows, 1, 1, &mut bufs)?;
    if stats.rows < rows {
        // The input ended at a row boundary, before the rows that were asked for.
        return Err(_ch_rust_avro::Error::new(ErrorCode::Eof, "EOF reached"));
    }
    assert_eq!((stats.rows, used), (rows, input.len()));
    Ok(bufs.dump())
}

fn streamed(
    program: &Program,
    input: &[u8],
    rows: u64,
    rows_per_call: u64,
    first: usize,
    step: usize,
) -> _ch_rust_avro::Result<(String, StreamStats)> {
    let mut bufs = Buffers::new(program.sinks(), 1);
    let stats = drive_stream(program, input, rows, rows_per_call, first, step, &mut bufs)?;
    Ok((bufs.dump(), stats))
}

/// `{id: long, s: bytes, t: string, f: fixed(fixed_len), n: long}` that reads `id`, `t` and `n` only.
fn project_around_large_values(fixed_len: u32) -> Program {
    let json = format!(
        r#"{{"type":"record","name":"R","fields":[
            {{"name":"id","type":"long"}},
            {{"name":"s","type":"bytes"}},
            {{"name":"t","type":"string"}},
            {{"name":"f","type":{{"type":"fixed","name":"F","size":{fixed_len}}}}},
            {{"name":"n","type":"long"}}]}}"#
    );
    let sch = schema(&json);
    let root = sch.root();
    let plan = [
        PlanNode::new(Action::Record, root, 5),
        PlanNode::value(sch.child(root, 0), 0),
        PlanNode::skip(sch.child(root, 1)),
        PlanNode::value(sch.child(root, 2), 1),
        PlanNode::skip(sch.child(root, 3)),
        PlanNode::value(sch.child(root, 4), 2),
    ];
    let sinks = [
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::Bytes, 0),
        SinkDesc::new(SinkKind::Int64, 0),
    ];
    Program::compile(&sch, &sinks, &plan).unwrap()
}

fn large_value_rows(rows: usize, bytes_len: usize, fixed_len: usize) -> Vec<u8> {
    let mut input = Vec::new();
    for r in 0..rows {
        input.extend(zz(40 + r as i64));
        input.extend(avro_bytes(&vec![0xab; bytes_len + r]));
        input.extend(avro_bytes(format!("t{r}").as_bytes()));
        input.extend(vec![0xcd; fixed_len]);
        input.extend(zz(-(r as i64)));
    }
    input
}

/// The point of the mode: skipped values of 200 kB and 100 kB are never in the window, which is what the
/// caller passes plus the few bytes of the value that the input was cut in.
#[test]
fn a_skipped_value_larger_than_the_window_is_never_held() {
    let (bytes_len, fixed_len, rows) = (200_000, 100_000, 3);
    let program = project_around_large_values(fixed_len as u32);
    let input = large_value_rows(rows, bytes_len, fixed_len);
    let expected = one_shot(&program, &input, rows as u64).unwrap();
    for step in [7usize, 64, 4096, 1 << 16] {
        for first in [1, step] {
            let (dump, stats) = streamed(&program, &input, rows as u64, 100, first, step).unwrap();
            assert_eq!(dump, expected, "step={step} first={first}");
            assert_eq!(stats.consumed, input.len());
            // A window is what is left of the op that was cut (at most a header and a 4 byte value) plus a
            // step; it is not a row, nor a value.
            assert!(
                stats.max_window <= first.max(step) + 32,
                "step={step} first={first}: window of {} bytes",
                stats.max_window
            );
            assert!(stats.need_input > 0);
        }
    }
}

/// Values that are read have to be in the window whole: that costs their size, and no more than that.
#[test]
fn a_value_that_is_read_costs_its_own_size() {
    let sch = r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#;
    let s = schema(sch);
    let plan = [
        PlanNode::new(Action::Record, s.root(), 1),
        PlanNode::value(s.child(s.root(), 0), 0),
    ];
    let program = compile(sch, &[SinkDesc::new(SinkKind::Bytes, 0)], &plan);
    let value = vec![b'x'; 50_000];
    let mut input = avro_bytes(&value);
    input.extend(avro_bytes(b"after"));
    let expected = one_shot(&program, &input, 2).unwrap();
    let (dump, stats) = streamed(&program, &input, 2, 2, 100, 1000).unwrap();
    assert_eq!(dump, expected);
    assert!(stats.max_window >= value.len());
    assert!(stats.max_window <= value.len() + 1000 + 16, "{stats:?}");
}

/// Whatever the cut and the calls: the same columns as one call over everything, for schemas that put every
/// kind of op, and every way of skipping, at the cut.
#[test]
fn every_cut_gives_the_columns_of_one_call() {
    let json = r#"{"type":"record","name":"R","fields":[
        {"name":"i","type":"int"},
        {"name":"l","type":"long"},
        {"name":"f","type":"float"},
        {"name":"d","type":"double"},
        {"name":"b","type":"boolean"},
        {"name":"s","type":"string"},
        {"name":"fx","type":{"type":"fixed","name":"F5","size":5}},
        {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]}},
        {"name":"u","type":["null","long"]},
        {"name":"arr","type":{"type":"array","items":"long"}},
        {"name":"strs","type":{"type":"array","items":"string"}},
        {"name":"m","type":{"type":"map","values":"int"}},
        {"name":"n","type":{"type":"record","name":"N","fields":[
            {"name":"x","type":"int"},{"name":"y","type":"string"}]}}
    ]}"#;
    let sch = schema(json);
    let u = universal_plan(&sch);
    let everything = Program::compile(&sch, &u.sinks, &u.plan).unwrap();
    // Reads `i` and the last field and skips the rest: every skip op is in the way.
    let root = sch.root();
    let mut plan = vec![PlanNode::new(Action::Record, root, 13)];
    let mut sinks = vec![];
    for k in 0..13 {
        let child = sch.child(root, k);
        if k == 0 || k == 5 {
            sinks.push(SinkDesc::new(
                if k == 0 {
                    SinkKind::Int32
                } else {
                    SinkKind::Bytes
                },
                0,
            ));
            plan.push(PlanNode::value(child, sinks.len() as u32 - 1));
        } else {
            plan.push(PlanNode::skip(child));
        }
    }
    let projected = Program::compile(&sch, &sinks, &plan).unwrap();
    let skipped = Program::compile(&sch, &[], &[PlanNode::skip(root)]).unwrap();

    let mut input = Vec::new();
    let rows = 4u64;
    for r in 0..rows as i64 {
        input.extend(zz(r - 1));
        input.extend(zz(1 << (20 + r)));
        input.extend(1.5f32.to_le_bytes());
        input.extend((-2.25f64 * r as f64).to_le_bytes());
        input.push((r & 1) as u8);
        input.extend(avro_bytes(&b"abc".repeat(r as usize * 7)));
        input.extend([1, 2, 3, 4, r as u8]);
        input.extend(zz(r % 3));
        input.extend(zz(r % 2));
        if r % 2 == 1 {
            input.extend(zz(77));
        }
        input.extend(zz(30 + r));
        for k in 0..30 + r {
            input.extend(zz(k * 1000 - 5));
        }
        input.extend(zz(0));
        input.extend(zz(2));
        input.extend(avro_bytes(b"x"));
        input.extend(avro_bytes(&b"y".repeat(40)));
        input.extend(zz(-1)); // a block with its byte size
        input.extend(zz(5));
        input.extend(avro_bytes(b"last"));
        input.extend(zz(0));
        input.extend(zz(2));
        input.extend(avro_bytes(b"k1"));
        input.extend(zz(10));
        input.extend(avro_bytes(b"key2"));
        input.extend(zz(-20));
        input.extend(zz(0));
        input.extend(zz(r));
        input.extend(avro_bytes(b"why"));
    }
    for (name, program) in [
        ("everything", &everything),
        ("projected", &projected),
        ("skipped", &skipped),
    ] {
        let expected = one_shot(program, &input, rows).unwrap();
        for rows_per_call in [1, 2, rows] {
            for step in [1usize, 2, 3, 5, 8, 13, 100, 1000] {
                for first in [0, 1, step] {
                    let (dump, stats) =
                        streamed(program, &input, rows, rows_per_call, first, step).unwrap();
                    assert_eq!(
                        dump, expected,
                        "{name} rows_per_call={rows_per_call} step={step} first={first}"
                    );
                    assert_eq!(stats.consumed, input.len());
                }
            }
        }
    }
}

/// Input that ends where the block ends is the error it is without streaming, for every cut of a valid block.
#[test]
fn the_end_of_the_input_is_the_same_error_with_and_without_streaming() {
    let json = r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"s","type":"bytes"},
        {"name":"arr","type":{"type":"array","items":"string"}},
        {"name":"t","type":"string"}]}"#;
    let sch = schema(json);
    let root = sch.root();
    let plan = [
        PlanNode::new(Action::Record, root, 4),
        PlanNode::value(sch.child(root, 0), 0),
        PlanNode::skip(sch.child(root, 1)),
        PlanNode::skip(sch.child(root, 2)),
        PlanNode::value(sch.child(root, 3), 1),
    ];
    let sinks = [
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::Bytes, 0),
    ];
    let program = Program::compile(&sch, &sinks, &plan).unwrap();
    let mut input = Vec::new();
    for r in 0..3i64 {
        input.extend(zz(r));
        input.extend(avro_bytes(&vec![9; 300 + r as usize]));
        input.extend(zz(3));
        for _ in 0..3 {
            input.extend(avro_bytes(&[5; 10]));
        }
        input.extend(zz(0));
        input.extend(avro_bytes(b"tail"));
    }
    for cut in 0..=input.len() {
        let prefix = &input[..cut];
        let rows = 3u64;
        let plain = one_shot(&program, prefix, rows);
        for rows_per_call in [1, rows] {
            for (first, step) in [(1, 1), (7, 3), (64, 64)] {
                let streamed =
                    streamed(&program, prefix, rows, rows_per_call, first, step).map(|(d, _)| d);
                // A block whose count the input cannot hold is `CountExceedsPayload` in one call and, when the
                // block was entered while more input could come, the `Eof` of its items: both are an input that
                // ends inside a row.
                let class = |e: &_ch_rust_avro::Error| {
                    matches!(e.code, ErrorCode::Eof | ErrorCode::CountExceedsPayload)
                        .then_some(ErrorCode::Eof)
                        .unwrap_or(e.code)
                };
                assert_eq!(
                    streamed.as_ref().map_err(class),
                    plain.as_ref().map_err(class),
                    "cut={cut} rows_per_call={rows_per_call} first={first} step={step}"
                );
                if let (Ok(a), Ok(b)) = (&streamed, &plain) {
                    assert_eq!(a, b);
                }
            }
        }
    }
}

/// Pausing does not count the items that occupy no bytes of a row twice or forget them, however the window cuts
/// the block headers: the blocks of one row share its budget, and the next row starts with a whole one.
#[test]
fn the_zero_size_item_budget_of_a_row_is_exact_across_pauses() {
    let sch = r#"{"type":"array","items":"null"}"#;
    let s = schema(sch);
    let skip = Program::compile(&s, &[], &[PlanNode::skip(s.root())]).unwrap();
    let half = MAX_ZERO_SIZE_ITEMS / 2;
    // One row: an array of two blocks.
    let row = |a: u64, b: u64| {
        let mut v = zz(a as i64);
        v.extend(zz(b as i64));
        v.extend(zz(0));
        v
    };
    for step in [1usize, 2, 3] {
        // `half + half` is the budget of a row, and each of the two rows has all of it.
        let input = [row(half, half), row(half, half)].concat();
        let (_, stats) = streamed(&skip, &input, 2, 2, 1, step).unwrap();
        assert_eq!(stats.rows, 2);
        assert!(stats.need_input > 0);
        // One more item than the row may have is rejected, by the block that exceeds it, in either row.
        let first_over = [row(half, half + 1), row(1, 1)].concat();
        let second_over = [row(half, half), row(half, half + 1)].concat();
        for input in [first_over, second_over] {
            for rows_per_call in [2, 1] {
                let e = streamed(&skip, &input, 2, rows_per_call, 1, step).unwrap_err();
                assert_eq!(
                    e.code,
                    ErrorCode::TooManyItems,
                    "step={step} rows_per_call={rows_per_call}"
                );
            }
        }
    }
}

/// A row paused for input holds what it wrote so far in the sinks; an error in the rest of it takes all of
/// that back, and the next row can start.
#[test]
fn an_error_after_a_pause_rolls_the_row_back_to_its_start() {
    let json = r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"s","type":"bytes"},
        {"name":"b","type":"boolean"}]}"#;
    let sch = schema(json);
    let root = sch.root();
    let plan = [
        PlanNode::new(Action::Record, root, 3),
        PlanNode::value(sch.child(root, 0), 0),
        PlanNode::skip(sch.child(root, 1)),
        PlanNode::value(sch.child(root, 2), 1),
    ];
    let sinks = [
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::Bool, 0),
    ];
    let program = Program::compile(&sch, &sinks, &plan).unwrap();
    // A good row, then a row with a bad boolean after a skipped value that is cut by the window.
    let mut input = zz(1);
    input.extend(avro_bytes(&[7; 20]));
    input.push(1);
    let bad_row = input.len();
    input.extend(zz(2));
    input.extend(avro_bytes(&[7; 100]));
    input.push(2);

    let mut bufs = Buffers::new(&sinks, 8);
    let mut state = State::new(0);
    state.set_streaming(true);
    // The first call sees the good row and part of the skipped value of the next one.
    let window = &input[..bad_row + 50];
    let p = bufs
        .with_sinks(|s| decode_partial(&program, &mut state, window, 10, s))
        .unwrap();
    assert_eq!(p.status, Status::NeedInput);
    assert_eq!(p.rows_done, 1);
    // The row that was cut is not kept: it is decoded again from its start, which is the next call's.
    assert_eq!(p.consumed, bad_row);
    assert_eq!(bufs.dump(), "0:1\n1:1\n");
    // Now the call starts that row: the window ends inside the skipped value, the row is paused. Everything
    // the window holds is consumed, the id of the row is in the sink.
    let window = &input[bad_row..bad_row + 50];
    let p = bufs
        .with_sinks(|s| decode_partial(&program, &mut state, window, 10, s))
        .unwrap();
    assert_eq!(
        (p.rows_done, p.consumed, p.status),
        (0, 50, Status::NeedInput)
    );
    assert!(!state.at_row_boundary());
    assert_eq!(bufs.dump(), "0:1 2\n1:1\n");
    // The rest of the value and the bad boolean.
    let rest = &input[bad_row + 50..];
    let e = bufs
        .with_sinks(|s| decode_resumed(&program, &mut state, rest, 10, s))
        .unwrap_err();
    assert_eq!(e.error.code, ErrorCode::InvalidBool);
    assert_eq!((e.rows_done, e.consumed), (0, 0));
    assert!(state.at_row_boundary());
    // The levels of the good row are back.
    assert_eq!(bufs.dump(), "0:1\n1:1\n");
}

/// The call that continues a row paused for input completes it and stops, so that a later row that needs room
/// is never rolled back into input that was dropped.
#[test]
fn the_call_that_completes_a_paused_row_stops_after_it() {
    let program = project_around_large_values(8);
    let input = large_value_rows(3, 100, 8);
    let row = large_value_rows(1, 100, 8).len();
    let mut bufs = Buffers::new(program.sinks(), 16);
    let mut state = State::new(0);
    state.set_streaming(true);
    // The window ends inside the skipped bytes of the first row.
    let p = bufs
        .with_sinks(|s| decode_partial(&program, &mut state, &input[..30], 3, s))
        .unwrap();
    assert_eq!(
        (p.rows_done, p.consumed, p.status),
        (0, 30, Status::NeedInput)
    );
    // Everything that is left of the input: the paused row is completed, and that is all the call does.
    let p = bufs
        .with_sinks(|s| decode_resumed(&program, &mut state, &input[30..], 3, s))
        .unwrap();
    assert_eq!(
        (p.rows_done, p.consumed, p.status),
        (1, row - 30, Status::RowLimit)
    );
    assert!(state.at_row_boundary());
    // The next call is an ordinary one.
    let p = bufs
        .with_sinks(|s| decode_partial(&program, &mut state, &input[row..], 3, s))
        .unwrap();
    assert_eq!((p.rows_done, p.consumed), (2, input.len() - row));
    assert_eq!(bufs.dump(), one_shot(&program, &input, 3).unwrap());
    assert_eq!(state.stats().need_input, 1);
}

/// Without streaming nothing changes: an input that ends inside a row is `Eof`, and the row is rolled back.
#[test]
fn without_streaming_the_end_of_the_input_is_an_error() {
    let program = project_around_large_values(8);
    let input = large_value_rows(2, 100, 8);
    let mut bufs = Buffers::new(program.sinks(), 16);
    let mut state = State::new(0);
    let e = bufs
        .with_sinks(|s| decode_partial(&program, &mut state, &input[..30], 2, s))
        .unwrap_err();
    assert_eq!(e.error.code, ErrorCode::Eof);
    assert_eq!((e.rows_done, e.consumed), (0, 0));
    assert_eq!(state.stats().need_input, 0);
    // A row paused for input and then given less than it needs, with streaming switched off: the same error.
    state.set_streaming(true);
    let p = bufs
        .with_sinks(|s| decode_partial(&program, &mut state, &input[..30], 2, s))
        .unwrap();
    assert_eq!(p.status, Status::NeedInput);
    state.set_streaming(false);
    let e = bufs
        .with_sinks(|s| decode_resumed(&program, &mut state, &input[30..40], 2, s))
        .unwrap_err();
    assert_eq!(e.error.code, ErrorCode::Eof);
    assert!(state.at_row_boundary());
    assert_eq!(bufs.dump(), "0:\n1:\n2:\n");
}

/// A skip-only program for `json`.
fn skip_program(json: &str) -> Program {
    let s = schema(json);
    Program::compile(&s, &[], &[PlanNode::skip(s.root())]).unwrap()
}

/// Collections do not make the window hold their payload: the count of a block is not checked against the
/// window while more input may come, because every item consumes input. The bot's three shapes: an array with
/// one 64 MiB `fixed`, a map with one 64 MiB `fixed` value, and an array of 16 MiB of one byte items.
#[test]
fn skipped_collections_are_consumed_as_they_arrive() {
    let fixed = 64usize << 20;
    let items = 16usize << 20;
    let cases: Vec<(&str, String, Vec<u8>)> = vec![
        (
            "array of fixed",
            format!(r#"{{"type":"array","items":{{"type":"fixed","name":"F","size":{fixed}}}}}"#),
            [zz(1), vec![7; fixed], zz(0)].concat(),
        ),
        (
            "map of fixed",
            format!(r#"{{"type":"map","values":{{"type":"fixed","name":"F","size":{fixed}}}}}"#),
            [zz(1), avro_bytes(b"k"), vec![7; fixed], zz(0)].concat(),
        ),
        (
            "array of longs",
            r#"{"type":"array","items":"long"}"#.to_string(),
            [zz(items as i64), vec![0; items], zz(0)].concat(),
        ),
    ];
    for (name, json, input) in cases {
        let program = skip_program(&json);
        for step in [4096usize, 1 << 16, 1 << 20] {
            let (_, stats) = streamed(&program, &input, 1, 1, step, step).unwrap();
            assert_eq!(stats.consumed, input.len(), "{name}");
            assert!(
                stats.max_window <= step + 64,
                "{name} step={step}: window of {} bytes",
                stats.max_window
            );
        }
    }
}

/// A count that the block cannot back is still an error, found quickly and without reserving anything for it:
/// the work is bounded by the bytes that there are, and the sinks by what the window holds.
#[test]
fn a_hostile_count_in_a_short_block_fails_without_reserving_for_it() {
    let sch = r#"{"type":"array","items":"long"}"#;
    let s = schema(sch);
    let u = universal_plan(&s);
    let read = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let skip = skip_program(sch);
    for count in [1i64 << 20, 1 << 40, 1 << 62] {
        let mut input = zz(count);
        input.extend(std::iter::repeat_n(2u8, 100));
        for step in [1usize, 7, 64, 1000] {
            for program in [&read, &skip] {
                let mut bufs = Buffers::new(program.sinks(), 1);
                let e = drive_stream(program, &input, 1, 1, step, step, &mut bufs).unwrap_err();
                assert!(
                    matches!(e.code, ErrorCode::Eof | ErrorCode::CountExceedsPayload),
                    "count={count} step={step}: {e:?}"
                );
                for sink in 0..program.sinks().len() {
                    let (cap, cap2) = bufs.capacity(sink);
                    assert!(
                        cap <= 2 * input.len() && cap2 <= 2 * input.len(),
                        "count={count} step={step}: sink {sink} has room for {cap}/{cap2}"
                    );
                }
            }
        }
    }
}
