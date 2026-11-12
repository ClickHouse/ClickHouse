//! The work that `NeedSpace` costs, counted instead of timed.
//!
//! A caller that stages every field of an array of records in its own sink grows each sink separately, so
//! the executor stops once per sink and per capacity doubling. Stopping must not cost a pass over the part of
//! the row that was decoded before: the total work has to stay `O(payload + output)` however many sinks there are.

mod common;

use _ch_rust_avro::exec::{decode, decode_partial, decode_resumed, State, Status};
use _ch_rust_avro::program::Program;
use common::{universal_plan, zz, Buffers};

/// What the C++ driver starts every staging sink with.
const INITIAL_ELEMENTS: usize = 16;

fn compile(schema_json: &str) -> (Program, Vec<_ch_rust_avro::program::SinkDesc>) {
    let s = common::schema(schema_json);
    let u = universal_plan(&s);
    (Program::compile(&s, &u.sinks, &u.plan).unwrap(), u.sinks)
}

/// `{x: array<record{f0..f<fields>: long}>}`: the shape of `Array(Tuple(...))` and flattened `Nested`.
fn array_of_records(fields: usize) -> String {
    let fs: Vec<String> = (0..fields)
        .map(|i| format!(r#"{{"name":"f{i}","type":"long"}}"#))
        .collect();
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"x","type":{{"type":"array","items":
        {{"type":"record","name":"T","fields":[{}]}}}}}}]}}"#,
        fs.join(",")
    )
}

/// `{f0..f<fields>: array<long>}`: one array per field, each staged separately.
fn parallel_arrays(fields: usize) -> String {
    let fs: Vec<String> = (0..fields)
        .map(|i| format!(r#"{{"name":"f{i}","type":{{"type":"array","items":"long"}}}}"#))
        .collect();
    format!(
        r#"{{"type":"record","name":"R","fields":[{}]}}"#,
        fs.join(",")
    )
}

/// One row of `array_of_records`: `items` records of `fields` zero longs, in one block.
fn wide_row(items: usize, fields: usize) -> Vec<u8> {
    let mut v = zz(items as i64);
    v.resize(v.len() + items * fields, 0);
    v.push(0);
    v
}

/// One row of `parallel_arrays`: every array holds `items` zero longs.
fn parallel_row(items: usize, fields: usize) -> Vec<u8> {
    let mut v = Vec::new();
    for _ in 0..fields {
        v.extend(zz(items as i64));
        v.resize(v.len() + items, 0);
        v.push(0);
    }
    v
}

struct Run {
    dump: String,
    rows: u64,
    consumed: usize,
    state: State,
    /// Sink growths, from the executor's point of view.
    need_space: u64,
}

/// Decodes like the C++ driver: every sink starts with `INITIAL_ELEMENTS` elements and doubles whenever the
/// executor asks, `max_rows` rows in one go, growing every sink that the stop names. A row that is paused
/// continues with `decode_resumed`, which is what the C ABI does with the sinks it kept.
fn decode_like_cpp(
    program: &Program,
    descs: &[_ch_rust_avro::program::SinkDesc],
    input: &[u8],
    max_rows: u64,
) -> Run {
    let mut bufs = Buffers::new(descs, INITIAL_ELEMENTS);
    let mut state = State::new(0);
    let (mut rows, mut pos, mut need_space) = (0u64, 0usize, 0u64);
    let mut paused = false;
    while rows < max_rows {
        let p = bufs
            .with_sinks(|sinks| {
                if std::mem::take(&mut paused) {
                    decode_resumed(program, &mut state, &input[pos..], max_rows - rows, sinks)
                } else {
                    decode_partial(program, &mut state, &input[pos..], max_rows - rows, sinks)
                }
            })
            .map_err(|e| e.error)
            .unwrap();
        rows += p.rows_done;
        pos += p.consumed;
        if let Status::NeedSpace { .. } = p.status {
            need_space += 1;
            paused = p.rows_done == 0;
            let needs = state.needs().to_vec();
            assert!(!needs.is_empty());
            for need in needs {
                let (cap, cap2) = bufs.capacity(need.sink as usize);
                let grow = |cap: usize, min: u64| {
                    if min as usize > cap {
                        min.max(2 * cap as u64)
                    } else {
                        min
                    }
                };
                bufs.grow(
                    need.sink as usize,
                    grow(cap, need.min_cap),
                    grow(cap2, need.min_cap2),
                );
            }
        }
    }
    Run {
        dump: bufs.dump(),
        rows,
        consumed: pos,
        state,
        need_space,
    }
}

/// The same rows decoded in one call into buffers that are large enough: the reference output.
fn decode_one_shot(
    program: &Program,
    descs: &[_ch_rust_avro::program::SinkDesc],
    input: &[u8],
    rows: u64,
    elements: usize,
) -> String {
    let mut bufs = Buffers::new(descs, elements);
    let mut state = State::new(0);
    let p = bufs
        .with_sinks(|sinks| decode(program, &mut state, input, rows, sinks))
        .unwrap();
    assert_eq!((p.rows_done, p.status), (rows, Status::RowLimit));
    assert_eq!(p.consumed, input.len());
    assert_eq!(state.stats().need_space, 0);
    bufs.dump()
}

/// The bot's case: one array of 1000 records with `fields` long fields each. Every field sink lacks room for
/// the block, and the stop names all of them: one round for the block, whatever the number of fields, and none
/// of them decodes any byte twice.
#[test]
fn wide_array_of_records_does_not_replay_the_row() {
    let items = 1000;
    for fields in [1usize, 4, 16, 64, 256, 1000] {
        let (program, descs) = compile(&array_of_records(fields));
        let input = wide_row(items, fields);
        let run = decode_like_cpp(&program, &descs, &input, 1);
        let stats = run.state.stats();
        eprintln!(
            "array_of_records fields={fields}: payload={} need_space={} calls={} restarts={} replayed_bytes={}",
            input.len(),
            stats.need_space,
            stats.calls,
            stats.restarts,
            stats.replayed_bytes
        );
        assert_eq!((run.rows, run.consumed), (1, input.len()));
        assert_eq!(run.need_space, stats.need_space);
        // The block of 1000 records is asked for at once, in every sink that writes the fields (the offsets
        // sink has room for the one element of the row): one stop, not one per field or per doubling.
        assert_eq!(stats.need_space, 1, "fields={fields}");
        // The first call checks every sink; the call that resumes the row checks the sinks that grew.
        assert_eq!(
            stats.sinks_checked,
            descs.len() as u64 + fields as u64,
            "work per pause must not depend on the number of sinks it does not name"
        );
        assert_eq!(stats.calls, 2);
        assert_eq!(stats.restarts, 0, "a pause must not discard the row");
        assert_eq!(
            stats.replayed_bytes, 0,
            "a pause must not decode a byte twice"
        );
        assert_eq!(
            run.dump,
            decode_one_shot(&program, &descs, &input, 1, items),
            "fields={fields}"
        );
    }
}

/// Asking for the room of a block walks the reservation list of the loop: once to find what is lacking, and
/// once more to find that the caller has made room. A block that needs room in `n` sinks must cost about `2n`
/// visits of that list in all, not `n * (n + 1) / 2`, however the caller grows them.
/// The bot's case: 17 records of `fields` numeric fields, every staging sink starting with 16 elements.
#[test]
fn reservation_list_is_walked_twice_per_block() {
    for fields in [1usize, 16, 1000, 10000] {
        let (program, descs) = compile(&array_of_records(fields));
        let input = wide_row(17, fields);
        let run = decode_like_cpp(&program, &descs, &input, 1);
        let stats = run.state.stats();
        assert_eq!((run.rows, run.consumed), (1, input.len()));
        assert_eq!(stats.need_space, 1);
        assert!(
            stats.reserve_visits <= 2 * fields as u64 + 4,
            "fields={fields}: {} visits of the reservation list",
            stats.reserve_visits
        );
        assert_eq!(run.dump, decode_one_shot(&program, &descs, &input, 1, 17));
    }
}

/// A caller that grows only the first sink that a stop names (what a caller written before the stop named all
/// of them does) decodes the same bytes: it is stopped again for each of the others, and the walk of the list
/// stays proportional to the sinks that it still lacks.
#[test]
fn growing_only_the_first_need_is_correct() {
    let fields = 50;
    let (program, descs) = compile(&array_of_records(fields));
    let input = wide_row(100, fields);
    let mut bufs = Buffers::new(&descs, INITIAL_ELEMENTS);
    let mut state = State::new(0);
    let mut paused = false;
    let mut stops = 0u64;
    loop {
        let p = bufs
            .with_sinks(|sinks| {
                if std::mem::take(&mut paused) {
                    decode_resumed(&program, &mut state, &input, 1, sinks)
                } else {
                    decode_partial(&program, &mut state, &input, 1, sinks)
                }
            })
            .map_err(|e| e.error)
            .unwrap();
        match p.status {
            Status::NeedSpace {
                sink,
                min_cap,
                min_cap2,
            } => {
                stops += 1;
                paused = true;
                assert_eq!((sink, min_cap, min_cap2), {
                    let first = state.needs()[0];
                    (first.sink, first.min_cap, first.min_cap2)
                });
                bufs.grow(sink as usize, min_cap, min_cap2);
            }
            Status::RowLimit => break,
            s => panic!("{s:?}"),
        }
    }
    // One stop for each of the fields: the first stop named all of them, and every later one named what was
    // still lacking.
    assert_eq!(stops, fields as u64);
    assert!(
        state.stats().reserve_visits <= (fields as u64 + 1) * (fields as u64 + 2),
        "{} visits",
        state.stats().reserve_visits
    );
    assert_eq!(
        bufs.dump(),
        decode_one_shot(&program, &descs, &input, 1, 100)
    );
}

/// One array per field instead of one array of records: the shape of `Nested` columns and of the `wide_arrays`
/// perf test (250 parallel arrays of 1000 numbers, one row).
///
/// The count of an array is known only when its header is reached, after every array before it was decoded, so
/// each array asks for its own room: one stop per array, whatever its length (the block is asked for at once,
/// not once per doubling) and however many arrays there are. What a stop costs does not grow with the number of
/// sinks: one call, one sink checked and converted again, no byte decoded twice. The numbers of each array but
/// the first of its block are taken in one run each.
#[test]
fn parallel_arrays_cost_one_stop_each() {
    for (fields, items) in [
        (1usize, 1000usize),
        (4, 1000),
        (16, 1000),
        (64, 1000),
        (250, 1000),
        (250, 20),
        (250, 20000),
    ] {
        let (program, descs) = compile(&parallel_arrays(fields));
        let input = parallel_row(items, fields);
        let run = decode_like_cpp(&program, &descs, &input, 1);
        let stats = run.state.stats();
        eprintln!(
            "parallel_arrays fields={fields} items={items}: payload={} need_space={} calls={} restarts={} replayed_bytes={} bulk_items={}",
            input.len(),
            stats.need_space,
            stats.calls,
            stats.restarts,
            stats.replayed_bytes,
            stats.bulk_items
        );
        assert_eq!((run.rows, run.consumed), (1, input.len()));
        assert_eq!(stats.need_space, fields as u64, "one stop per array");
        assert_eq!(stats.calls, fields as u64 + 1);
        assert_eq!(
            stats.sinks_checked,
            descs.len() as u64 + stats.need_space,
            "a resumed row checks the sink that grew, not all of them"
        );
        assert_eq!((stats.restarts, stats.replayed_bytes), (0, 0));
        assert_eq!(
            stats.bulk_items,
            (fields * (items - 1)) as u64,
            "all the numbers of an array but the first are taken in one run"
        );
        assert_eq!(
            run.dump,
            decode_one_shot(&program, &descs, &input, 1, items)
        );
    }
}

/// Rows that complete before the row that needs space are replayed once at most, and a row that is
/// discarded is discarded once at most: the extra work is bounded by twice the payload, whatever the
/// number of sinks.
#[test]
fn replay_is_bounded_by_the_payload_for_many_rows() {
    for fields in [1usize, 8, 64] {
        let (program, descs) = compile(&array_of_records(fields));
        // Row sizes grow, so that growth is needed in rows that are not the first of a call.
        let mut input = Vec::new();
        let mut rows = 0u64;
        for items in [1usize, 3, 20, 5, 70, 2, 300, 1, 600, 40] {
            input.extend(wide_row(items, fields));
            rows += 1;
        }
        let run = decode_like_cpp(&program, &descs, &input, rows);
        let stats = run.state.stats();
        eprintln!(
            "many_rows fields={fields}: payload={} need_space={} calls={} restarts={} replayed_bytes={}",
            input.len(),
            stats.need_space,
            stats.calls,
            stats.restarts,
            stats.replayed_bytes
        );
        assert_eq!((run.rows, run.consumed), (rows, input.len()));
        assert!(
            stats.replayed_bytes <= 2 * input.len() as u64,
            "{} bytes replayed of a payload of {}",
            stats.replayed_bytes,
            input.len()
        );
        assert!(stats.restarts <= rows);
        assert_eq!(
            run.dump,
            decode_one_shot(&program, &descs, &input, rows, 4000)
        );
    }
}

/// A row paused for space and resumed gives the bytes of a one-shot decode whatever the capacity it started
/// with and however much the caller grows the reported sink by.
#[test]
fn pause_resume_matches_one_shot_at_every_growth_point() {
    let items = 40;
    let fields = 3;
    let (program, descs) = compile(&array_of_records(fields));
    let input = wide_row(items, fields);
    let expected = decode_one_shot(&program, &descs, &input, 1, items);
    // Decode with a different cap for the first stop each time: `first_cap` elements for every sink.
    for first_cap in 0..=items {
        for step in [1usize, 2, 7, 1000] {
            let mut bufs = Buffers::new(&descs, first_cap);
            let mut state = State::new(0);
            let mut pauses = 0;
            loop {
                let p = bufs
                    .with_sinks(|sinks| decode_partial(&program, &mut state, &input, 1, sinks))
                    .unwrap();
                match p.status {
                    Status::NeedSpace {
                        sink,
                        min_cap,
                        min_cap2,
                    } => {
                        assert_eq!((p.rows_done, p.consumed), (0, 0));
                        pauses += 1;
                        let (cap, cap2) = bufs.capacity(sink as usize);
                        assert!(min_cap as usize > cap || min_cap2 as usize > cap2);
                        bufs.grow(sink as usize, min_cap + step as u64 - 1, min_cap2);
                    }
                    Status::RowLimit => {
                        assert_eq!((p.rows_done, p.consumed), (1, input.len()));
                        break;
                    }
                    Status::BlockDone => panic!("a row of this program takes bytes"),
                    Status::NeedInput => unreachable!("streaming mode is off"),
                }
            }
            assert_eq!(bufs.dump(), expected, "first_cap={first_cap} step={step}");
            let stats = state.stats();
            assert_eq!((stats.restarts, stats.replayed_bytes), (0, 0));
            assert_eq!(stats.need_space, pauses);
        }
    }
}

/// The row's budget of zero-size items is neither counted twice nor lost when the row is paused between
/// two arrays: the first array is counted once, and the second one is checked against what is left of the row's.
#[test]
fn pausing_keeps_the_zero_size_item_budget_exact() {
    use _ch_rust_avro::{ErrorCode, MAX_ZERO_SIZE_ITEMS};
    let (program, descs) = compile(
        r#"{"type":"record","name":"R","fields":[
            {"name":"a","type":{"type":"array","items":"null"}},
            {"name":"b","type":{"type":"array","items":"null"}}]}"#,
    );
    let row = |a: u64, b: u64| {
        let mut v = zz(a as i64);
        v.push(0);
        v.extend(zz(b as i64));
        v.push(0);
        v
    };
    // `b`'s offsets sink has no room: the row pauses after `a` was counted.
    let run = |a: u64, b: u64| {
        let input = row(a, b);
        let mut bufs = Buffers::new(&descs, 0);
        bufs.grow(0, 1, 0);
        let mut state = State::new(0);
        let p = bufs
            .with_sinks(|sinks| decode(&program, &mut state, &input, 1, sinks))
            .unwrap();
        assert_eq!((p.rows_done, p.consumed), (0, 0));
        assert!(matches!(p.status, Status::NeedSpace { sink: 1, .. }));
        bufs.grow(1, 1, 0);
        let result = bufs.with_sinks(|sinks| decode(&program, &mut state, &input, 1, sinks));
        (result, bufs.dump())
    };
    let half = MAX_ZERO_SIZE_ITEMS / 2;
    let (ok, dump) = run(half, half);
    assert_eq!(ok.unwrap().status, Status::RowLimit);
    assert_eq!(dump, format!("0:{half}\n1:{half}\n"));
    // One item over the budget in total: it is `b`, on resume, that exceeds it.
    let (err, _) = run(half, half + 1);
    assert_eq!(err.unwrap_err().code, ErrorCode::TooManyItems);
}

/// An input that ends inside a row that was paused rolls the row back to its start, as for a row that was
/// never paused: the fill levels are the ones of the call that started the row.
#[test]
fn eof_after_a_pause_rolls_back_to_the_start_of_the_row() {
    use _ch_rust_avro::ErrorCode;
    let (program, descs) = compile(r#"{"type":"array","items":"long"}"#);
    // Five items and no end marker.
    let mut input = zz(5);
    input.extend([2, 4, 6, 8, 10]);
    let mut bufs = Buffers::new(&descs, 0);
    bufs.grow(0, 1, 0);
    bufs.grow(1, 2, 0);
    let mut state = State::new(0);
    let p = bufs
        .with_sinks(|sinks| decode_partial(&program, &mut state, &input, 1, sinks))
        .unwrap();
    assert!(matches!(
        p.status,
        Status::NeedSpace {
            sink: 1,
            min_cap: 3,
            ..
        }
    ));
    assert!(!state.at_row_boundary());
    bufs.grow(1, 8, 0);
    let e = bufs
        .with_sinks(|sinks| decode_partial(&program, &mut state, &input, 1, sinks))
        .unwrap_err();
    assert_eq!(
        (e.error.code, e.rows_done, e.consumed),
        (ErrorCode::Eof, 0, 0)
    );
    assert!(state.at_row_boundary());
    // Nothing of the row is left in the sinks.
    assert_eq!(bufs.dump(), "0:\n1:\n");
    // With the end marker the same state decodes the row.
    input.push(0);
    let p = bufs
        .with_sinks(|sinks| decode_partial(&program, &mut state, &input, 1, sinks))
        .unwrap();
    assert_eq!(
        (p.rows_done, p.consumed, p.status),
        (1, input.len(), Status::RowLimit)
    );
    assert_eq!(bufs.dump(), "0:5\n1:1 2 3 4 5\n");
}

/// A block asks for the room of all its items at once, from the fill level of each sink, and only if its
/// bytes can hold that many items.
#[test]
fn a_block_that_the_payload_cannot_hold_is_an_error_not_a_request() {
    use _ch_rust_avro::ErrorCode;
    let (program, descs) = compile(r#"{"type":"array","items":"long"}"#);
    // A block of 2^40 items followed by three bytes.
    let mut input = zz(1 << 40);
    input.extend([2, 4, 6]);
    let mut bufs = Buffers::new(&descs, 4);
    let mut state = State::new(0);
    let e = bufs
        .with_sinks(|sinks| decode(&program, &mut state, &input, 1, sinks))
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::CountExceedsPayload);
    assert_eq!(state.stats().need_space, 0);
}

/// The second block of an array asks for its own room when its header is read, counted from where the
/// first block left the sink.
#[test]
fn each_block_of_an_array_asks_once() {
    let (program, descs) = compile(r#"{"type":"array","items":"long"}"#);
    let mut input = Vec::new();
    for block in [20usize, 30] {
        input.extend(zz(block as i64));
        input.resize(input.len() + block, 0);
    }
    input.push(0);
    let run = decode_like_cpp(&program, &descs, &input, 1);
    let stats = run.state.stats();
    assert_eq!((run.rows, run.consumed), (1, input.len()));
    // Values: 16 -> 32 for the first block of 20, then 32 -> 64 for the 50 that the second block reaches.
    assert_eq!(stats.need_space, 2);
    assert_eq!((stats.restarts, stats.replayed_bytes), (0, 0));
    assert_eq!(run.dump, decode_one_shot(&program, &descs, &input, 1, 50));
}

/// The first request of a block of strings is for their offsets: one for each item. The bytes are not known
/// before the strings are read and grow with the items, as before.
#[test]
fn strings_ask_for_their_offsets() {
    let (program, descs) = compile(r#"{"type":"array","items":"string"}"#);
    let mut input = zz(40);
    for _ in 0..40 {
        input.extend([6, b'a', b'b', b'c']);
    }
    input.push(0);
    let mut bufs = Buffers::new(&descs, INITIAL_ELEMENTS);
    let mut state = State::new(0);
    let p = bufs
        .with_sinks(|sinks| decode(&program, &mut state, &input, 1, sinks))
        .unwrap();
    // Sink 0 is the offsets sink of the array, sink 1 the strings.
    assert_eq!(
        (p.rows_done, p.consumed, p.status),
        (
            0,
            0,
            Status::NeedSpace {
                sink: 1,
                min_cap: 0,
                min_cap2: 40
            }
        )
    );
    let run = decode_like_cpp(&program, &descs, &input, 1);
    assert_eq!(run.dump, decode_one_shot(&program, &descs, &input, 1, 128));
}

/// Arrays of arrays ask for the offsets of the inner arrays, one for each outer item, and nothing for what
/// the inner arrays hold: a loop that is stepped over is not looked into.
#[test]
fn an_outer_loop_asks_for_the_offsets_of_the_inner_ones() {
    let (program, descs) = compile(r#"{"type":"array","items":{"type":"array","items":"long"}}"#);
    let mut input = zz(30);
    for _ in 0..30 {
        input.extend(zz(2));
        input.extend([2, 4, 0]);
    }
    input.push(0);
    let mut bufs = Buffers::new(&descs, INITIAL_ELEMENTS);
    let mut state = State::new(0);
    let p = bufs
        .with_sinks(|sinks| decode(&program, &mut state, &input, 1, sinks))
        .unwrap();
    // Sink 0: the outer offsets, 1: the inner ones, 2: the values.
    assert_eq!(
        (p.rows_done, p.consumed, p.status),
        (
            0,
            0,
            Status::NeedSpace {
                sink: 1,
                min_cap: 30,
                min_cap2: 0
            }
        )
    );
    let run = decode_like_cpp(&program, &descs, &input, 1);
    assert_eq!(run.dump, decode_one_shot(&program, &descs, &input, 1, 64));
}

/// What follows a union in the body of a loop is not certain to be written, so it is not asked for: it grows
/// with the items. The result is the same.
#[test]
fn the_request_of_a_loop_stops_at_its_first_union() {
    let (program, descs) = compile(
        r#"{"type":"array","items":{"type":"record","name":"T","fields":[
            {"name":"a","type":"long"},
            {"name":"u","type":["null","long"]},
            {"name":"b","type":"long"}]}}"#,
    );
    let mut input = zz(40);
    for i in 0..40 {
        input.extend(zz(i));
        input.extend(if i % 3 == 0 {
            vec![0]
        } else {
            [vec![2], zz(i)].concat()
        });
        input.extend(zz(-i));
    }
    input.push(0);
    let mut bufs = Buffers::new(&descs, INITIAL_ELEMENTS);
    let mut state = State::new(0);
    let p = bufs
        .with_sinks(|sinks| decode(&program, &mut state, &input, 1, sinks))
        .unwrap();
    let Status::NeedSpace { sink, min_cap, .. } = p.status else {
        panic!("{:?}", p.status)
    };
    // The first request is for `a`, which comes before the union, all of the block.
    assert_eq!(min_cap, 40, "sink {sink}");
    let run = decode_like_cpp(&program, &descs, &input, 1);
    assert_eq!(run.dump, decode_one_shot(&program, &descs, &input, 1, 64));
    assert_eq!(
        (run.state.stats().restarts, run.state.stats().replayed_bytes),
        (0, 0)
    );
}

/// A shape of the perf tests: its schema and row for a number of fields, and the widths to report.
struct Shape {
    name: &'static str,
    schema: fn(usize) -> String,
    row: fn(usize, usize) -> Vec<u8>,
    widths: &'static [usize],
}

/// Prints the counters of the perf test shapes (`wide_tuple_*`, `wide_arrays_*`) at several widths:
/// `cargo test --release --test need_space_work counter_report -- --ignored --nocapture`.
#[test]
#[ignore]
fn counter_report() {
    let items = 1000;
    let shapes = [
        Shape {
            name: "wide_tuple (array of records)",
            schema: array_of_records,
            row: wide_row,
            widths: &[1, 10, 100, 250, 1000],
        },
        Shape {
            name: "wide_arrays (parallel arrays)",
            schema: parallel_arrays,
            row: parallel_row,
            widths: &[1, 10, 100, 250],
        },
    ];
    for shape in shapes {
        for &fields in shape.widths {
            let (program, descs) = compile(&(shape.schema)(fields));
            let input = (shape.row)(items, fields);
            let run = decode_like_cpp(&program, &descs, &input, 1);
            let stats = run.state.stats();
            eprintln!(
                "{} {items} items x {fields} fields: sinks={} payload={} need_space={} calls={} restarts={} replayed_bytes={} sinks_checked={} reserve_visits={} bulk_items={}",
                shape.name,
                descs.len(),
                input.len(),
                stats.need_space,
                stats.calls,
                stats.restarts,
                stats.replayed_bytes,
                stats.sinks_checked,
                stats.reserve_visits,
                stats.bulk_items
            );
        }
    }
}
