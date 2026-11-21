//! Attacker-shaped inputs: truncation, lying counts, zero-byte items, deep
//! recursion, schema bombs. Every test asserts an error or a bounded result,
//! a time bound, and where it matters a bound on allocated memory (measured by a
//! counting global allocator). Time bounds are generous: they catch
//! amplification (seconds), not slowness.

mod common;

use std::time::{Duration, Instant};

use _ch_rust_avro::exec::{decode, State, Status};
use _ch_rust_avro::json::JsonLimits;
use _ch_rust_avro::program::Program;
use _ch_rust_avro::schema::Schema;
use _ch_rust_avro::{ErrorCode, MAX_ZERO_SIZE_ITEMS};
use common::*;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn timed<T>(limit: Duration, f: impl FnOnce() -> T) -> T {
    let t = Instant::now();
    let r = f();
    assert!(t.elapsed() < limit, "took {:?}", t.elapsed());
    r
}

/// Skip-all program for a schema.
fn skip_program(s: &Schema) -> Program {
    let u = skip_plan(s);
    Program::compile(s, &u.sinks, &u.plan).unwrap()
}

fn skip_rows(program: &Program, input: &[u8], rows: u64) -> _ch_rust_avro::Result<u64> {
    let mut state = State::new(0);
    let p = decode(program, &mut state, input, rows, &mut [])?;
    Ok(p.rows_done)
}

// ---------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------

#[test]
fn every_truncation_of_every_shape_is_an_error_or_a_row_boundary() {
    let sch = r#"{"type":"record","name":"R","fields":[
        {"name":"a","type":"long"},
        {"name":"f","type":"float"},
        {"name":"s","type":"string"},
        {"name":"e","type":{"type":"enum","name":"E","symbols":["X","Y"]}},
        {"name":"x","type":{"type":"fixed","name":"F","size":3}},
        {"name":"u","type":["null","double","string"]},
        {"name":"arr","type":{"type":"array","items":"int"}},
        {"name":"m","type":{"type":"map","values":{"type":"array","items":"string"}}},
        {"name":"b","type":"boolean"}]}"#;
    let s = schema(sch);
    let mut one_row = Vec::new();
    one_row.extend(zz(-1234567));
    one_row.extend_from_slice(&2.5f32.to_le_bytes());
    one_row.extend(avro_bytes(b"hello"));
    one_row.extend(zz(1));
    one_row.extend_from_slice(&[1, 2, 3]);
    one_row.extend(zz(2));
    one_row.extend(avro_bytes(b"uu"));
    one_row.extend(zz(-2)); // negative block count: two items, then the byte size
    one_row.extend(zz(2));
    one_row.extend(zz(5));
    one_row.extend(zz(-6));
    one_row.extend(zz(0));
    one_row.extend(zz(1)); // one map entry
    one_row.extend(avro_bytes(b"key"));
    one_row.extend(zz(1));
    one_row.extend(avro_bytes(b"v"));
    one_row.extend(zz(0));
    one_row.extend(zz(0));
    one_row.push(1);
    let two_rows = [one_row.clone(), one_row.clone()].concat();

    let skip = skip_program(&s);
    let u = universal_plan(&s);
    let full = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    for program in [&skip, &full] {
        assert_eq!(skip_rows_or_dump(program, &two_rows, 2).unwrap(), 2);
        for cut in 0..two_rows.len() {
            let r = skip_rows_or_dump(program, &two_rows[..cut], 2);
            match r {
                Ok(rows) => {
                    // Only cuts at row boundaries succeed, with the rows before the cut.
                    assert!(
                        cut == 0 || cut == one_row.len(),
                        "cut {cut} was accepted with {rows} rows"
                    );
                    assert_eq!(rows, (cut / one_row.len()) as u64);
                }
                Err(e) => assert!(
                    matches!(
                        e.code,
                        ErrorCode::Eof | ErrorCode::CountExceedsPayload | ErrorCode::NegativeLength
                    ),
                    "cut {cut}: {e:?}"
                ),
            }
        }
    }
}

fn skip_rows_or_dump(program: &Program, input: &[u8], rows: u64) -> _ch_rust_avro::Result<u64> {
    let mut bufs = Buffers::new(program.sinks(), 1);
    let mut state = State::new(0);
    let mut pos = 0;
    let mut done = 0;
    loop {
        let p = bufs.with_sinks(|s| decode(program, &mut state, &input[pos..], rows - done, s))?;
        pos += p.consumed;
        done += p.rows_done;
        match p.status {
            Status::NeedSpace {
                sink,
                min_cap,
                min_cap2,
            } => bufs.grow(sink as usize, min_cap.max(2), min_cap2.max(2)),
            Status::RowLimit | Status::BlockDone => return Ok(done),
            Status::NeedInput => unreachable!("streaming mode is off"),
        }
    }
}

// ---------------------------------------------------------------------------
// Lying counts and zero-byte items
// ---------------------------------------------------------------------------

#[test]
fn giant_block_counts_are_rejected_before_any_work() {
    for (sch, name) in [
        (r#"{"type":"array","items":"long"}"#, "array<long>"),
        (r#"{"type":"array","items":"string"}"#, "array<string>"),
        (r#"{"type":"map","values":"null"}"#, "map<null>"),
        (
            r#"{"type":"array","items":{"type":"array","items":"int"}}"#,
            "array<array<int>>",
        ),
        (r#"{"type":"array","items":["null","int"]}"#, "array<union>"),
    ] {
        let s = schema(sch);
        let skip = skip_program(&s);
        let u = universal_plan(&s);
        let full = Program::compile(&s, &u.sinks, &u.plan).unwrap();
        for count in [i64::MAX, 1 << 62, 1 << 40, 1 << 31, 1 << 20, 100] {
            let mut input = zz(count);
            input.extend_from_slice(&[0, 1, 2, 3]);
            for (label, program) in [("skip", &skip), ("full", &full)] {
                let ((), peak) = peak_alloc(|| {
                    let e = timed(Duration::from_secs(2), || {
                        skip_rows_or_dump(program, &input, 1)
                    })
                    .unwrap_err();
                    assert_eq!(
                        e.code,
                        ErrorCode::CountExceedsPayload,
                        "{name} {label} {count}"
                    );
                });
                assert!(
                    peak < 64 << 10,
                    "{name} {label} {count}: peak allocation {peak}"
                );
            }
        }
        // i64::MIN has no positive counterpart.
        let mut input = zz(i64::MIN);
        input.extend_from_slice(&[0, 0]);
        assert_eq!(
            skip_rows_or_dump(&skip, &input, 1).unwrap_err().code,
            ErrorCode::InvalidCount
        );
        // A negative count that claims too many items for the bytes that remain.
        let mut input = zz(-(1 << 40));
        input.extend(zz(8));
        input.extend_from_slice(&[0; 8]);
        assert_eq!(
            skip_rows_or_dump(&skip, &input, 1).unwrap_err().code,
            ErrorCode::CountExceedsPayload
        );
    }
}

#[test]
fn count_boundary_matches_remaining_bytes() {
    // array<long>: each item is at least one byte, so a count equal to the bytes left is fine
    // and one more is not.
    let s = schema(r#"{"type":"array","items":"long"}"#);
    let skip = skip_program(&s);
    let mut ok = zz(5);
    ok.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    assert_eq!(skip_rows(&skip, &ok, 1).unwrap(), 1);
    let mut bad = zz(7);
    bad.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    assert_eq!(
        skip_rows(&skip, &bad, 1).unwrap_err().code,
        ErrorCode::CountExceedsPayload
    );
    // Five items in exactly five bytes, and then the end marker is missing.
    let mut tight = zz(5);
    tight.extend_from_slice(&[0, 0, 0, 0, 0]);
    assert_eq!(
        skip_rows(&skip, &tight, 1).unwrap_err().code,
        ErrorCode::Eof
    );
}

#[test]
fn zero_byte_items_are_capped_per_array_and_per_row() {
    let s = schema(r#"{"type":"array","items":"null"}"#);
    let skip = skip_program(&s);
    // At the cap: accepted, and fast.
    let mut input = zz(MAX_ZERO_SIZE_ITEMS as i64);
    input.extend(zz(0));
    let rows = timed(Duration::from_secs(20), || skip_rows(&skip, &input, 1)).unwrap();
    assert_eq!(rows, 1);
    // One above: rejected without iterating.
    let mut input = zz(MAX_ZERO_SIZE_ITEMS as i64 + 1);
    input.extend(zz(0));
    let e = timed(Duration::from_secs(1), || skip_rows(&skip, &input, 1)).unwrap_err();
    assert_eq!(e.code, ErrorCode::TooManyItems);
    // Enormous.
    let mut input = zz(1 << 62);
    input.extend(zz(0));
    assert_eq!(
        timed(Duration::from_secs(1), || skip_rows(&skip, &input, 1))
            .unwrap_err()
            .code,
        ErrorCode::TooManyItems
    );
    // Two blocks that together exceed the cap in one array.
    let half = (MAX_ZERO_SIZE_ITEMS / 2 + 1) as i64;
    let mut input = zz(half);
    input.extend(zz(half));
    input.extend(zz(0));
    assert_eq!(
        timed(Duration::from_secs(20), || skip_rows(&skip, &input, 1))
            .unwrap_err()
            .code,
        ErrorCode::TooManyItems
    );
    // Nested zero-byte arrays share the budget of their row: 4096 x 4096 = 2^24 fits, one more inner item does not.
    let nested = schema(r#"{"type":"array","items":{"type":"array","items":"null"}}"#);
    let skip = skip_program(&nested);
    let build = |outer: i64, inner: i64| {
        let mut v = zz(outer);
        for _ in 0..outer {
            v.extend(zz(inner));
            v.extend(zz(0));
        }
        v.extend(zz(0));
        v
    };
    // The outer items are arrays and take at least one byte each, so only the inner ones are zero-size.
    assert_eq!(
        timed(Duration::from_secs(20), || skip_rows(
            &skip,
            &build(4096, 4096),
            1
        ))
        .unwrap(),
        1
    );
    let e = timed(Duration::from_secs(20), || {
        skip_rows(&skip, &build(4096, 4097), 1)
    })
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::TooManyItems);
    // The budget is the row's: neither the rows nor the calls that decode them share it. Rows of half the
    // budget, of one item, and of the whole budget follow one another in calls of one row.
    let half_row = build(2048, 4096);
    let one_item_row = build(1, 1);
    let full_row = build(4096, 4096);
    let over_row = build(4096, 4097);
    let mut state = State::new(0);
    timed(Duration::from_secs(20), || {
        for row in [&half_row, &half_row, &one_item_row, &full_row, &half_row] {
            assert_eq!(
                decode(&skip, &mut state, row, 1, &mut [])
                    .unwrap()
                    .rows_done,
                1
            );
        }
    });
    // One item over the budget of one row is an error wherever the row is, also after rows that used all of
    // theirs.
    assert_eq!(
        decode(&skip, &mut state, &over_row, 1, &mut [])
            .unwrap_err()
            .code,
        ErrorCode::TooManyItems
    );
    // An error resets the state for the next row.
    assert_eq!(
        decode(&skip, &mut state, &one_item_row, 1, &mut [])
            .unwrap()
            .rows_done,
        1
    );
    // Empty records are zero-size too, and so are maps of them... keys cost a byte, values nothing.
    let empty = schema(r#"{"type":"array","items":{"type":"record","name":"E","fields":[]}}"#);
    let skip = skip_program(&empty);
    let mut input = zz(1 << 40);
    input.extend(zz(0));
    assert_eq!(
        skip_rows(&skip, &input, 1).unwrap_err().code,
        ErrorCode::TooManyItems
    );
    let m = schema(r#"{"type":"map","values":"null"}"#);
    let skip = skip_program(&m);
    let mut input = zz(1 << 40);
    input.extend(zz(0));
    assert_eq!(
        skip_rows(&skip, &input, 1).unwrap_err().code,
        ErrorCode::CountExceedsPayload,
        "map keys cost bytes"
    );
}

#[test]
fn zero_size_rows_follow_caller_bounds_and_not_block_framing() {
    // Rows that occupy no bytes: the executor stops at max_rows, not at the end of the input.
    let s = schema("\"null\"");
    let skip = skip_program(&s);
    assert_eq!(skip.min_row_size(), 0);
    assert_eq!(
        timed(Duration::from_secs(5), || skip_rows(&skip, &[], 1_000_000)).unwrap(),
        1_000_000
    );
    let s = schema(r#"{"type":"record","name":"E","fields":[]}"#);
    assert_eq!(skip_rows(&skip_program(&s), &[], 10).unwrap(), 10);
}

// ---------------------------------------------------------------------------
// Recursion
// ---------------------------------------------------------------------------

#[test]
fn deep_recursive_data_hits_the_depth_limit_with_bounded_memory() {
    let s = schema(
        r#"{"type":"record","name":"L","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","L"]}]}"#,
    );
    let skip = skip_program(&s);
    // Each level is v (0x00) and the union index 1 (0x02): 2 bytes. Five million levels.
    let mut input = Vec::with_capacity(10_000_000 + 2);
    for _ in 0..5_000_000 {
        input.extend_from_slice(&[0x00, 0x02]);
    }
    input.extend_from_slice(&[0x00, 0x00]);
    let ((), peak) = peak_alloc(|| {
        let mut state = State::new(100_000);
        let e = timed(Duration::from_secs(5), || {
            decode(&skip, &mut state, &input, 1, &mut [])
        })
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::DataTooDeep);
        assert_eq!(e.message, "Avro data nesting depth exceeds the limit");
    });
    assert!(
        peak < 8 << 20,
        "frame stack is bounded by the depth limit: {peak}"
    );
    // The configured limit is exact. Every record on the recursive path is one call frame, so a chain
    // of `levels` records inside the root (levels + 1 records in all) needs levels + 1 frames.
    let chain = |levels: usize| {
        let mut v = Vec::new();
        for _ in 0..levels {
            v.extend_from_slice(&[0x00, 0x02]);
        }
        v.extend_from_slice(&[0x00, 0x00]);
        v
    };
    let mut state = State::new(10);
    assert_eq!(
        decode(&skip, &mut state, &chain(9), 1, &mut [])
            .unwrap()
            .rows_done,
        1
    );
    let mut state = State::new(10);
    assert_eq!(
        decode(&skip, &mut state, &chain(10), 1, &mut [])
            .unwrap_err()
            .code,
        ErrorCode::DataTooDeep
    );
    // An error leaves the state at a row boundary, so the same state decodes the next block.
    let mut state = State::new(10);
    let _ = decode(&skip, &mut state, &chain(11), 1, &mut []);
    assert!(state.at_row_boundary());
    assert_eq!(
        decode(&skip, &mut state, &chain(3), 1, &mut [])
            .unwrap()
            .rows_done,
        1
    );
}

#[test]
fn recursion_through_arrays_and_maps() {
    // A tree: node { children: array<node> }.
    let s = schema(
        r#"{"type":"record","name":"N","fields":[{"name":"c","type":{"type":"array","items":"N"}}]}"#,
    );
    let skip = skip_program(&s);
    // depth d: each level is count 1 then the child, ending with count 0 and the end markers.
    let build = |d: usize| {
        let mut v = Vec::new();
        for _ in 0..d {
            v.extend(zz(1));
        }
        v.extend(zz(0));
        for _ in 0..d {
            v.extend(zz(0));
        }
        v
    };
    let mut state = State::new(0);
    assert_eq!(
        decode(&skip, &mut state, &build(1000), 1, &mut [])
            .unwrap()
            .rows_done,
        1
    );
    let mut state = State::new(64);
    assert_eq!(
        decode(&skip, &mut state, &build(1000), 1, &mut [])
            .unwrap_err()
            .code,
        ErrorCode::DataTooDeep
    );
    // map<string, N>
    let s = schema(
        r#"{"type":"record","name":"N","fields":[{"name":"c","type":{"type":"map","values":"N"}}]}"#,
    );
    let skip = skip_program(&s);
    let mut v = Vec::new();
    for _ in 0..50 {
        v.extend(zz(1));
        v.extend(avro_bytes(b"k"));
    }
    v.extend(zz(0));
    for _ in 0..50 {
        v.extend(zz(0));
    }
    let mut state = State::new(0);
    assert_eq!(
        decode(&skip, &mut state, &v, 1, &mut []).unwrap().rows_done,
        1
    );
}

#[test]
fn deep_schemas_compile_and_decode_without_native_recursion() {
    // 40 000 nested arrays: JSON, schema, canonical form, min size, program, execution.
    let n = 40_000;
    let mut sch = String::from("\"int\"");
    for _ in 0..n {
        sch = format!("{{\"type\":\"array\",\"items\":{sch}}}");
    }
    let t = Instant::now();
    let s = Schema::parse(sch.as_bytes(), 0).unwrap();
    assert_eq!(s.canonical_form().len(), sch.len());
    assert_eq!(s.min_encoded_size(s.root()), 1);
    assert_eq!(s.max_encoded_size(s.root()), None);
    let skip = skip_program(&s);
    let u = universal_plan(&s);
    let full = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    // One element at every level, then the int 7: n * (count 1) + 7 + n * end marker.
    let mut data = Vec::new();
    for _ in 0..n {
        data.extend(zz(1));
    }
    data.extend(zz(7));
    for _ in 0..n {
        data.extend(zz(0));
    }
    assert_eq!(skip_rows(&skip, &data, 1).unwrap(), 1);
    let mut bufs = Buffers::new(&u.sinks, 1);
    let (st, used) = drive(&full, &data, 1, 1, 1, 1, &mut bufs).unwrap();
    assert_eq!((st.rows, used), (1, data.len()));
    assert!(t.elapsed() < Duration::from_secs(30), "{:?}", t.elapsed());
}

// ---------------------------------------------------------------------------
// Schema bombs
// ---------------------------------------------------------------------------

#[test]
fn duplicate_names_bad_references_and_huge_values_are_errors_not_crashes() {
    let bad = [
        r#"{"type":"record","name":"A","fields":[{"name":"x","type":{"type":"record","name":"A","fields":[]}}]}"#,
        r#"{"type":"record","name":"A","fields":[{"name":"x","type":"B"}]}"#,
        r#"[{"type":"fixed","name":"A","size":1},{"type":"fixed","name":"A","size":1}]"#,
        r#"{"type":"fixed","name":"A","size":9223372036854775807}"#,
        r#"{"type":"fixed","name":"A","size":18446744073709551616}"#,
        r#"{"type":"enum","name":"E","symbols":["a","a"]}"#,
        r#"{"type":"map","values":"A"}"#,
        r#"{"type":"array","items":{"type":"array"}}"#,
        "[[[[[[[[[[]]]]]]]]]]",
        "{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"x\"}]}",
        "\u{0}",
        "[\"int\"",
    ];
    for b in bad {
        assert!(
            timed(Duration::from_secs(1), || Schema::parse(b.as_bytes(), 256)).is_err(),
            "{b}"
        );
    }
    // A large fixed is fine as a schema and rejected by short data without allocating.
    let s = schema(r#"{"type":"fixed","name":"F","size":2147483647}"#);
    assert_eq!(s.min_encoded_size(s.root()), 2147483647);
    let skip = skip_program(&s);
    let ((), peak) = peak_alloc(|| {
        assert_eq!(
            skip_rows(&skip, &[1, 2, 3], 1).unwrap_err().code,
            ErrorCode::Eof
        );
    });
    assert!(peak < 4096);
    // A string whose length claims 2 GiB.
    let s = schema("\"string\"");
    let u = universal_plan(&s);
    let full = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let input = [0xfe, 0xff, 0xff, 0xff, 0x0f, b'x'];
    let (r, peak) = peak_alloc(|| {
        let mut bufs = Buffers::new(&u.sinks, 1);
        drive(&full, &input, 1, 1, 1, 1, &mut bufs)
    });
    assert_eq!(r.err().map(|e| e.code), Some(ErrorCode::Eof));
    assert!(
        peak < 4096,
        "no allocation follows the claimed length: {peak}"
    );
}

#[test]
fn exponential_schemas_stay_linear() {
    // R_k = { a: R_{k-1}, b: R_{k-1} }: the tree has 2^40 leaves, the graph 41 nodes.
    let mut sch = String::from(r#"{"type":"fixed","name":"R0","size":1}"#);
    for k in 1..=40 {
        sch = format!(
            r#"{{"type":"record","name":"R{k}","fields":[{{"name":"a","type":{sch}}},{{"name":"b","type":"R{}"}}]}}"#,
            k - 1
        );
    }
    let (s, peak) =
        peak_alloc(|| timed(Duration::from_secs(5), || Schema::parse(sch.as_bytes(), 0)).unwrap());
    assert!(peak < 4 << 20);
    assert_eq!(s.min_encoded_size(s.root()), 1 << 40);
    assert_eq!(s.max_encoded_size(s.root()), Some(1 << 40));
    assert!(
        s.canonical_form().len() < 4000,
        "canonical form prints each named type once"
    );
    let skip = timed(Duration::from_secs(5), || skip_program(&s));
    assert!(
        skip.op_count() < 1000,
        "shared records are subroutines: {}",
        skip.op_count()
    );
    // And it decodes: one row of 2^40 bytes is not something to build, but a short input fails cleanly.
    assert_eq!(
        skip_rows(&skip, &[0; 100], 1).unwrap_err().code,
        ErrorCode::Eof
    );
}

#[test]
fn wide_schemas_are_linear() {
    // A union of 20 000 distinct fixed types and a record of 20 000 fields.
    let mut branches = Vec::new();
    for i in 0..20_000 {
        branches.push(format!(r#"{{"type":"fixed","name":"F{i}","size":1}}"#));
    }
    let union = format!("[{}]", branches.join(","));
    let s = timed(Duration::from_secs(5), || {
        Schema::parse(union.as_bytes(), 0)
    })
    .unwrap();
    assert_eq!(s.branches(s.root()).len(), 20_000);
    assert_eq!(s.min_encoded_size(s.root()), 2);
    let u = universal_plan(&s);
    let p = timed(Duration::from_secs(5), || {
        Program::compile(&s, &u.sinks, &u.plan)
    })
    .unwrap();
    let mut input = zz(19_999);
    input.push(7);
    let mut bufs = Buffers::new(&u.sinks, 1);
    let (st, _) = drive(&p, &input, 1, 1, 1, 1, &mut bufs).unwrap();
    assert_eq!(st.rows, 1);

    let fields: Vec<String> = (0..20_000)
        .map(|i| format!(r#"{{"name":"f{i}","type":"long"}}"#))
        .collect();
    let rec = format!(
        r#"{{"type":"record","name":"W","fields":[{}]}}"#,
        fields.join(",")
    );
    let s = timed(Duration::from_secs(5), || Schema::parse(rec.as_bytes(), 0)).unwrap();
    assert_eq!(s.min_encoded_size(s.root()), 20_000);
    assert_eq!(s.max_encoded_size(s.root()), Some(200_000));
}

#[test]
fn schema_memory_is_bounded_by_the_text() {
    let fields: Vec<String> = (0..20_000)
        .map(|i| format!(r#"{{"name":"field{i}","type":["null","string"],"doc":"d"}}"#))
        .collect();
    let rec = format!(
        r#"{{"type":"record","name":"W","fields":[{}]}}"#,
        fields.join(",")
    );
    let (s, peak) = peak_alloc(|| Schema::parse(rec.as_bytes(), 0).unwrap());
    assert_eq!(s.fields(s.root()).len(), 20_000);
    assert!(
        peak < rec.len() * 40,
        "peak {peak} for {} bytes of text",
        rec.len()
    );
    let limits = JsonLimits {
        max_nodes: 1000,
        ..JsonLimits::default()
    };
    assert_eq!(
        Schema::parse_with(rec.as_bytes(), 0, limits)
            .unwrap_err()
            .code,
        ErrorCode::SchemaTooLarge
    );
}

/// A record in a namespace of `namespace_len` characters whose `fields` fields all refer to one named `fixed`
/// type by its short name. The text is about `fields * 28` bytes; the Parsing Canonical Form prints the full
/// name of the `fixed` type for every reference, so it is about `fields * namespace_len` bytes.
fn shared_named_type_schema(namespace_char: char, namespace_len: usize, fields: usize) -> String {
    let namespace = namespace_char.to_string().repeat(namespace_len);
    let mut text = format!(
        r#"{{"type":"record","name":"R","namespace":"{namespace}","fields":[{{"name":"f0","type":{{"type":"fixed","name":"F","size":1}}}}"#
    );
    for i in 1..fields {
        text.push_str(&format!(r#",{{"name":"f{i}","type":"F"}}"#));
    }
    text.push_str("]}");
    text
}

#[test]
fn parsing_does_not_materialize_the_canonical_form() {
    // 283 KB of text with a canonical form of 40 MB.
    let text = shared_named_type_schema('n', 4000, 10_000);
    let text = text.as_bytes();
    assert!(text.len() < 300_000);

    let (s, parse_peak) = peak_alloc(|| Schema::parse(text, 0).unwrap());
    assert!(
        parse_peak < text.len() * 16,
        "parsing {} bytes of text allocated {parse_peak}",
        text.len()
    );
    assert_eq!(s.source(), text);

    // The program that a reader compiles from the schema is linear in the text too.
    let (program, program_peak) = peak_alloc(|| skip_program(&s));
    assert!(
        program_peak < text.len() * 16 && program.memory_size() < text.len() * 16,
        "compiling allocated {program_peak}, the program holds {}",
        program.memory_size()
    );

    // A prefix for a message is as large as its limit, and its work area is the stack of the walk, which is
    // linear in the schema text, never in the 40 MB of the form.
    let ((prefix, cut), prefix_peak) = peak_alloc(|| s.canonical_prefix(4096));
    assert!(cut);
    assert_eq!(prefix.len(), 4096);
    assert!(
        prefix_peak < text.len() * 16,
        "a prefix of 4096 bytes allocated {prefix_peak} for {} bytes of text",
        text.len()
    );

    // The complete form is built only on request.
    let (canonical_len, canonical_peak) = peak_alloc(|| s.canonical_form().len());
    assert!(canonical_len > 100 * text.len());
    assert!(canonical_peak >= canonical_len);
    assert!(s.canonical_form().starts_with(&prefix));
}

#[test]
fn distinct_schemas_retain_memory_linear_in_their_text() {
    // What a cache that keeps the schema text (or a program) of each of them holds: four schemas that
    // differ only in the namespace, each 283 KB of text with a canonical form of 40 MB.
    let texts: Vec<String> = ['a', 'b', 'c', 'd']
        .into_iter()
        .map(|c| shared_named_type_schema(c, 4000, 10_000))
        .collect();
    let total: usize = texts.iter().map(String::len).sum();
    let (kept, peak) = peak_alloc(|| {
        texts
            .iter()
            .map(|t| {
                let s = Schema::parse(t.as_bytes(), 0).unwrap();
                let program = skip_program(&s);
                (s.source().to_vec(), program)
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(kept.len(), 4);
    assert!(
        peak < total * 16,
        "{total} bytes of text across four schemas allocated {peak} while their programs were kept"
    );
    assert!(kept.iter().all(|(source, _)| source.len() < 300_000));
}

#[test]
fn executing_over_ten_megabytes_allocates_nothing_but_the_frame_stack() {
    let s = schema(
        r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"s","type":"string"},{"name":"u","type":["null","double"]},{"name":"arr","type":{"type":"array","items":"int"}}]}"#,
    );
    let skip = skip_program(&s);
    let mut row = Vec::new();
    row.extend(zz(123456789));
    row.extend(avro_bytes(b"0123456789"));
    row.extend(zz(1));
    row.extend_from_slice(&1.5f64.to_le_bytes());
    row.extend(zz(3));
    row.extend(zz(1));
    row.extend(zz(2));
    row.extend(zz(3));
    row.extend(zz(0));
    let n = 10_000_000 / row.len();
    let input: Vec<u8> = row.iter().copied().cycle().take(row.len() * n).collect();
    let (rows, peak) = peak_alloc(|| {
        timed(Duration::from_secs(60), || {
            skip_rows(&skip, &input, n as u64)
        })
        .unwrap()
    });
    assert_eq!(rows, n as u64);
    assert!(peak < 1024, "executor allocated {peak} bytes");
}
