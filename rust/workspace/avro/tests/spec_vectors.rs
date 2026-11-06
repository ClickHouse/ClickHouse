//! Expected values in this file come from the text of the Apache Avro 1.12
//! specification ("Binary Encoding", "Data Serialization and Deserialization"),
//! not from this crate's encoder.

mod common;

use _ch_rust_avro::exec::{decode, decode_partial, Sink, State, Status};
use _ch_rust_avro::outbuf::OutBuf;
use _ch_rust_avro::program::{PlanNode, Program, SinkDesc, SinkKind};
use _ch_rust_avro::schema::Schema;
use _ch_rust_avro::ErrorCode;
use common::*;

/// Decodes `input` as consecutive rows of `schema_json` into one sink per leaf and returns the dump.
fn dump(schema_json: &str, input: &[u8], rows: u64) -> String {
    let s = schema(schema_json);
    let u = universal_plan(&s);
    let p = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let mut bufs = Buffers::new(&u.sinks, 1);
    let (stats, used) = drive(&p, input, rows, 1000, 1, 1, &mut bufs).unwrap();
    assert_eq!(stats.rows, rows);
    assert_eq!(used, input.len(), "all input bytes are consumed");
    bufs.dump()
}

fn error_of(schema_json: &str, input: &[u8], rows: u64) -> _ch_rust_avro::Error {
    let s = schema(schema_json);
    let u = universal_plan(&s);
    let p = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let mut bufs = Buffers::new(&u.sinks, 1);
    match drive(&p, input, rows, 1000, 1, 1, &mut bufs) {
        Err(e) => e,
        Ok(_) => panic!("expected an error"),
    }
}

#[test]
fn spec_long_examples() {
    // value -> bytes, from the table in the specification (zigzag + varint).
    let input: &[u8] = &[0x00, 0x01, 0x02, 0x03, 0x04, 0x7f, 0x80, 0x01];
    assert_eq!(dump("\"long\"", input, 7), "0:0 -1 1 -2 2 -64 64\n");
    // `int` uses the same encoding.
    assert_eq!(dump("\"int\"", input, 7), "0:0 -1 1 -2 2 -64 64\n");
}

#[test]
fn spec_string_example() {
    // "foo" is 06 66 6f 6f.
    assert_eq!(
        dump("\"string\"", &[0x06, 0x66, 0x6f, 0x6f], 1),
        "0:666f6f\n"
    );
    assert_eq!(
        dump("\"bytes\"", &[0x06, 0x66, 0x6f, 0x6f, 0x00], 2),
        "0:666f6f -\n"
    );
}

#[test]
fn spec_record_example() {
    // {"a": 27, "b": "foo"} is 36 06 66 6f 6f.
    let d = dump(
        r#"{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}"#,
        &[0x36, 0x06, 0x66, 0x6f, 0x6f],
        1,
    );
    assert_eq!(d, "0:27\n1:666f6f\n");
}

#[test]
fn spec_union_examples() {
    // ["null","string"]: null is 00, "a" is 02 02 61.
    let d = dump(r#"["null","string"]"#, &[0x00, 0x02, 0x02, 0x61], 2);
    assert_eq!(d, "0:0 1\n1:61\n");
}

#[test]
fn spec_array_examples() {
    let sch = r#"{"type":"array","items":"long"}"#;
    // [3, 27]: one block of two items then the end marker: 04 06 36 00.
    assert_eq!(dump(sch, &[0x04, 0x06, 0x36, 0x00], 1), "0:2\n1:3 27\n");
    // The same array as a block with a negative count (-2) and a byte size (2): 03 04 06 36 00.
    assert_eq!(
        dump(sch, &[0x03, 0x04, 0x06, 0x36, 0x00], 1),
        "0:2\n1:3 27\n"
    );
    // Two blocks of one item each.
    assert_eq!(
        dump(sch, &[0x02, 0x06, 0x02, 0x36, 0x00], 1),
        "0:2\n1:3 27\n"
    );
    // Mixed block kinds.
    assert_eq!(
        dump(sch, &[0x02, 0x06, 0x01, 0x02, 0x36, 0x00], 1),
        "0:2\n1:3 27\n"
    );
    // The empty array is a single 00; two rows share one offsets sink.
    assert_eq!(
        dump(sch, &[0x00, 0x04, 0x06, 0x36, 0x00, 0x00], 3),
        "0:0 2 2\n1:3 27\n"
    );
}

#[test]
fn spec_map_examples() {
    // {"a": 1}: one block of one entry (key "a" = 02 61, value 1 = 02), end marker 00.
    let sch = r#"{"type":"map","values":"long"}"#;
    assert_eq!(
        dump(sch, &[0x02, 0x02, 0x61, 0x02, 0x00], 1),
        "0:1\n1:61\n2:1\n"
    );
    // With a negative count and byte size.
    assert_eq!(
        dump(sch, &[0x01, 0x06, 0x02, 0x61, 0x02, 0x00], 1),
        "0:1\n1:61\n2:1\n"
    );
    assert_eq!(dump(sch, &[0x00], 1), "0:0\n1:\n2:\n");
}

#[test]
fn spec_scalar_examples() {
    // boolean: 00 / 01.
    assert_eq!(dump("\"boolean\"", &[0x01, 0x00], 2), "0:1 0\n");
    // float 1.0 and double 1.0 are little-endian IEEE 754.
    assert_eq!(
        dump("\"float\"", &[0x00, 0x00, 0x80, 0x3f], 1),
        "0:3f800000\n"
    );
    assert_eq!(
        dump("\"double\"", &[0, 0, 0, 0, 0, 0, 0xf0, 0x3f], 1),
        "0:3ff0000000000000\n"
    );
    // null takes no bytes at all.
    assert_eq!(dump("\"null\"", &[], 3), "");
    // enum is encoded as an int: symbol index 1 of [A,B,C] is 02.
    assert_eq!(
        dump(
            r#"{"type":"enum","name":"E","symbols":["A","B","C"]}"#,
            &[0x02, 0x00],
            2
        ),
        "0:1 0\n"
    );
    // fixed is its raw bytes.
    assert_eq!(
        dump(
            r#"{"type":"fixed","name":"F","size":3}"#,
            &[1, 2, 3, 4, 5, 6],
            2
        ),
        "0:010203 040506\n"
    );
}

#[test]
fn spec_error_cases() {
    // A boolean other than 0 or 1.
    let e = error_of("\"boolean\"", &[0x02], 1);
    assert_eq!(
        (e.code, e.message.as_str()),
        (ErrorCode::InvalidBool, "Invalid value for bool")
    );
    // int outside the 32 bit range: 2^31 zigzagged is 80 80 80 80 10.
    let e = error_of("\"int\"", &[0x80, 0x80, 0x80, 0x80, 0x10], 1);
    assert_eq!(
        (e.code, e.message.as_str()),
        (
            ErrorCode::IntOutOfRange,
            "Value out of range for Avro int: 2147483648"
        )
    );
    // Negative string length.
    let e = error_of("\"string\"", &[0x03, 0x61], 1);
    assert_eq!(
        (e.code, e.message.as_str()),
        (ErrorCode::NegativeLength, "Cannot have negative length: -2")
    );
    // Length beyond the data.
    let e = error_of("\"string\"", &[0x0a, 0x61], 1);
    assert_eq!(
        (e.code, e.message.as_str()),
        (ErrorCode::Eof, "EOF reached")
    );
    // Enum and union indexes out of range.
    let e = error_of(
        r#"{"type":"enum","name":"E","symbols":["A","B"]}"#,
        &[0x04],
        1,
    );
    assert_eq!(
        (e.code, e.message.as_str()),
        (
            ErrorCode::EnumIndexOutOfRange,
            "Avro enum index 2 is out of range, the schema has 2 symbols"
        )
    );
    let e = error_of(
        r#"{"type":"enum","name":"E","symbols":["A","B"]}"#,
        &[0x01],
        1,
    );
    assert_eq!(e.code, ErrorCode::EnumIndexOutOfRange);
    let e = error_of(r#"["null","int"]"#, &[0x04], 1);
    assert_eq!(e.code, ErrorCode::UnionIndexOutOfRange);
    let e = error_of(r#"["null","int"]"#, &[0x01], 1);
    assert_eq!(e.code, ErrorCode::UnionIndexOutOfRange);
    // Varint longer than ten bytes.
    let e = error_of("\"long\"", &[0x80; 11], 1);
    assert_eq!(
        (e.code, e.message.as_str()),
        (ErrorCode::InvalidVarint, "Invalid Avro varint")
    );
}

#[test]
fn resumable_decode_matches_one_shot() {
    let sch = r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"tags","type":{"type":"array","items":"string"}},
        {"name":"m","type":{"type":"map","values":["null","double"]}},
        {"name":"s","type":"string"}]}"#;
    let s = schema(sch);
    // Two rows built by hand.
    let mut input = Vec::new();
    for (id, tags, name) in [(1i64, vec!["a", "bcd"], "first"), (-2, vec![], "")] {
        input.extend(zz(id));
        if tags.is_empty() {
            input.extend(zz(0));
        } else {
            input.extend(zz(tags.len() as i64));
            for t in &tags {
                input.extend(avro_bytes(t.as_bytes()));
            }
            input.extend(zz(0));
        }
        // map with one entry whose value is the double branch, then the end marker
        input.extend(zz(1));
        input.extend(avro_bytes(b"k"));
        input.extend(zz(1));
        input.extend_from_slice(&1.5f64.to_le_bytes());
        input.extend(zz(0));
        input.extend(avro_bytes(name.as_bytes()));
    }
    let u = universal_plan(&s);
    let p = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let mut one = Buffers::new(&u.sinks, 1000);
    let (st, used) = drive(&p, &input, 2, 2, 1000, 1000, &mut one).unwrap();
    assert_eq!((st.rows, used), (2, input.len()));
    assert_eq!(st.need_space, 0);
    // Tiny buffers, growing one element at a time, one row per call.
    let mut two = Buffers::new(&u.sinks, 0);
    let (st2, used2) = drive(&p, &input, 2, 1, 0, 1, &mut two).unwrap();
    assert_eq!((st2.rows, used2), (2, input.len()));
    assert!(
        st2.need_space > 5,
        "growth was actually exercised: {}",
        st2.need_space
    );
    assert_eq!(one.dump(), two.dump());
    assert!(one.dump().contains("first") || one.dump().contains(&hex(b"first")));
}

/// The rows of the schema of `resumable_decode_matches_one_shot`, with every kind of cut inside them: a varint, a
/// string, an array count, an end marker, the entries of a map and its null branch.
fn window_rows() -> (Schema, Vec<u8>, u64) {
    let s = schema(
        r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"tags","type":{"type":"array","items":"string"}},
        {"name":"m","type":{"type":"map","values":["null","double"]}},
        {"name":"s","type":"string"}]}"#,
    );
    let mut input = Vec::new();
    let rows: [(i64, Vec<&str>, Option<f64>, &str); 4] = [
        (1, vec!["a", "bcd"], Some(1.5), "first"),
        (-2, vec![], None, ""),
        (
            1 << 40,
            vec!["", "x", "yz0123456789"],
            Some(-0.0),
            "a longer string than the others",
        ),
        (3, vec!["only"], None, "z"),
    ];
    for (id, tags, value, name) in &rows {
        input.extend(zz(*id));
        if !tags.is_empty() {
            input.extend(zz(tags.len() as i64));
            for t in tags {
                input.extend(avro_bytes(t.as_bytes()));
            }
        }
        input.extend(zz(0));
        input.extend(zz(1));
        input.extend(avro_bytes(b"k"));
        match value {
            Some(v) => {
                input.extend(zz(1));
                input.extend_from_slice(&v.to_le_bytes());
            }
            None => input.extend(zz(0)),
        }
        input.extend(zz(0));
        input.extend(avro_bytes(name.as_bytes()));
    }
    (s, input, rows.len() as u64)
}

/// A window that ends at any byte, however the rest of the input arrives, must give the rows of a one-shot decode:
/// the rows that completed stay, the row that the window cut is rolled back and decoded again from its start.
#[test]
fn every_window_boundary_matches_one_shot() {
    let (s, input, rows) = window_rows();
    let u = universal_plan(&s);
    let p = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let mut one = Buffers::new(&u.sinks, 1);
    let (stats, used) = drive(&p, &input, rows, rows, 1, 1, &mut one).unwrap();
    assert_eq!((stats.rows, used), (rows, input.len()));
    let expected = one.dump();

    let mut cuts_inside_a_row = 0;
    for first in 0..=input.len() {
        for step in [1, 2, 7, input.len()] {
            let mut bufs = Buffers::new(&u.sinks, 1);
            let (done, consumed, incomplete) =
                drive_windows(&p, &input, rows, first, step, &mut bufs).unwrap();
            assert_eq!(
                (done, consumed),
                (rows, input.len()),
                "first {first}, step {step}"
            );
            assert_eq!(bufs.dump(), expected, "first {first}, step {step}");
            cuts_inside_a_row += incomplete;
        }
    }
    // Both `Eof` and a block count that cannot fit the visible bytes happen, many times over.
    assert!(
        cuts_inside_a_row > input.len() as u32,
        "{cuts_inside_a_row}"
    );
}

/// The same input, but the window never grows past what the input holds: a cut that cannot be completed is the
/// error of the one-shot decode, with the rows before it committed.
#[test]
fn truncated_input_is_an_error_after_the_committed_rows() {
    let (s, input, rows) = window_rows();
    let u = universal_plan(&s);
    let p = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    for cut in 1..input.len() {
        let mut bufs = Buffers::new(&u.sinks, 1);
        let e = drive_windows(&p, &input[..cut], rows, 1, 3, &mut bufs).unwrap_err();
        assert!(
            matches!(e.code, ErrorCode::Eof | ErrorCode::CountExceedsPayload),
            "cut {cut}: {e:?}"
        );
    }
}

/// A row that occupies no bytes is not a window boundary: all rows decode from an empty window.
#[test]
fn zero_byte_rows_need_no_window() {
    let s = schema(r#""null""#);
    let u = universal_plan(&s);
    let p = Program::compile(&s, &u.sinks, &u.plan).unwrap();
    let mut bufs = Buffers::new(&u.sinks, 1);
    let (done, consumed, incomplete) = drive_windows(&p, &[], 5, 0, 1, &mut bufs).unwrap();
    assert_eq!((done, consumed, incomplete), (5, 0, 0));
}

#[test]
fn block_done_and_row_limit_statuses() {
    let s = schema("\"long\"");
    let sinks = vec![SinkDesc::new(SinkKind::Int64, 0)];
    let plan = vec![PlanNode::value(s.root(), 0)];
    let p = Program::compile(&s, &sinks, &plan).unwrap();
    let mut state = State::new(0);
    let mut data = vec![0u8; 8 * 4];
    let input = [0x02, 0x04, 0x06];
    let mut sink = [Sink::new(&mut data)];
    let pr = decode(&p, &mut state, &input, 2, &mut sink).unwrap();
    assert_eq!(
        (pr.rows_done, pr.consumed, pr.status),
        (2, 2, Status::RowLimit)
    );
    let pr = decode(&p, &mut state, &input[2..], 10, &mut sink).unwrap();
    assert_eq!(
        (pr.rows_done, pr.consumed, pr.status),
        (1, 1, Status::BlockDone)
    );
    assert_eq!(sink[0].len, 3);
    // max_rows 0 does nothing.
    let pr = decode(&p, &mut state, &input, 0, &mut sink).unwrap();
    assert_eq!(
        (pr.rows_done, pr.consumed, pr.status),
        (0, 0, Status::RowLimit)
    );
}

#[test]
fn need_space_pauses_the_row_and_resumes_it_without_decoding_again() {
    let s = schema(
        r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}"#,
    );
    let sinks = vec![
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::Bytes, 0),
    ];
    let mut plan = vec![PlanNode::new(
        _ch_rust_avro::program::Action::Record,
        s.root(),
        2,
    )];
    plan.push(PlanNode::value(s.child(s.root(), 0), 0));
    plan.push(PlanNode::value(s.child(s.root(), 1), 1));
    let p = Program::compile(&s, &sinks, &plan).unwrap();
    let mut state = State::new(0);
    let input = [0x02, 0x06, b'x', b'y', b'z'];
    let (mut a, mut chars, mut offs) = (vec![0u8; 8], vec![0u8; 2], vec![0u8; 8]);
    let mut v = [Sink::new(&mut a), Sink::with_offsets(&mut chars, &mut offs)];
    let pr = decode(&p, &mut state, &input, 1, &mut v).unwrap();
    // The long was decoded, the three byte string does not fit in two bytes: the row is paused. Nothing
    // is committed or consumed, and the long stays in its sink where the row stopped.
    assert_eq!(
        pr.status,
        Status::NeedSpace {
            sink: 1,
            min_cap: 3,
            min_cap2: 1
        }
    );
    assert_eq!((pr.rows_done, pr.consumed), (0, 0));
    assert_eq!((v[0].len, v[1].len, v[1].len2), (1, 0, 0));
    assert!(!state.at_row_boundary());
    let a_len = v[0].len;
    // The caller grows the string sink, keeping what the row wrote, and calls again with the same input.
    let (mut chars2, mut offs2) = (vec![0u8; 3], vec![0u8; 8]);
    let mut v2 = [
        Sink::new(&mut a),
        Sink::with_offsets(&mut chars2, &mut offs2),
    ];
    v2[0].len = a_len;
    let pr = decode(&p, &mut state, &input[pr.consumed..], 1, &mut v2).unwrap();
    assert_eq!(
        (pr.rows_done, pr.consumed, pr.status),
        (1, 5, Status::RowLimit)
    );
    assert_eq!(&chars2, b"xyz");
    assert_eq!(i64::from_ne_bytes(a[..8].try_into().unwrap()), 1);
    assert!(state.at_row_boundary());
    // The long was decoded once: the string was the only op that was repeated.
    assert_eq!(
        (
            state.stats().calls,
            state.stats().restarts,
            state.stats().replayed_bytes
        ),
        (2, 0, 0)
    );
}

/// Builds the schema `{a: long, s: string, t: array<int>}` with one sink per leaf plus offsets.
fn tx_program() -> Program {
    let s = schema(
        r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"s","type":"string"},{"name":"t","type":{"type":"array","items":"int"}}]}"#,
    );
    let sinks = vec![
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::Bytes, 0),
        SinkDesc::new(SinkKind::ArrayOffsets, 0),
        SinkDesc::new(SinkKind::Int32, 0),
    ];
    let root = s.root();
    let mut arr = PlanNode::new(_ch_rust_avro::program::Action::Array, s.child(root, 2), 1);
    arr.sink = 2;
    let plan = vec![
        PlanNode::new(_ch_rust_avro::program::Action::Record, root, 3),
        PlanNode::value(s.child(root, 0), 0),
        PlanNode::value(s.child(root, 1), 1),
        arr,
        PlanNode::value(s.child(s.child(root, 2), 0), 3),
    ];
    Program::compile(&s, &sinks, &plan).unwrap()
}

fn tx_row(a: i64, s: &str, items: &[i64]) -> Vec<u8> {
    let mut v = zz(a);
    v.extend(avro_bytes(s.as_bytes()));
    if !items.is_empty() {
        v.extend(zz(items.len() as i64));
        for i in items {
            v.extend(zz(*i));
        }
    }
    v.extend(zz(0));
    v
}

#[test]
fn an_error_reports_the_committed_rows_and_rolls_the_failing_row_back() {
    let p = tx_program();
    let rows = [
        tx_row(1, "ab", &[5, 6]),
        tx_row(-2, "", &[]),
        tx_row(3, "xyz", &[7]),
    ];
    let good: Vec<u8> = rows[..2].concat();
    let mut third = rows[2].clone();
    // Corrupt the third row inside its array: an int that is out of the 32-bit range.
    let cut = third.len() - 2;
    third.truncate(cut);
    third.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x10, 0x00]);
    let input = [good.clone(), third].concat();

    let (mut ids, mut chars, mut offs, mut aoffs, mut vals) =
        ([0u8; 64], [0u8; 64], [0u8; 64], [0u8; 64], [0u8; 64]);
    let mut state = State::new(0);
    let mut v = [
        Sink::new(&mut ids),
        Sink::with_offsets(&mut chars, &mut offs),
        Sink::new(&mut aoffs),
        Sink::new(&mut vals),
    ];
    let e = decode_partial(&p, &mut state, &input, 3, &mut v).unwrap_err();
    assert_eq!(e.error.code, ErrorCode::IntOutOfRange);
    assert_eq!((e.rows_done, e.consumed), (2, good.len()));
    // Exactly the two completed rows are in the sinks: the third row's `a`, "xyz" and offset are gone.
    let fills: Vec<_> = v.iter().map(|s| (s.len, s.len2, s.offset)).collect();
    assert_eq!(fills, vec![(2, 0, 0), (2, 2, 2), (2, 0, 2), (2, 0, 0)]);
    assert_eq!(
        &ids[..16],
        &[1i64, -2]
            .iter()
            .flat_map(|x| x.to_ne_bytes())
            .collect::<Vec<u8>>()[..]
    );
    assert_eq!(&chars[..2], b"ab");
    assert_eq!(
        &vals[..8],
        &[5i32, 6]
            .iter()
            .flat_map(|x| x.to_ne_bytes())
            .collect::<Vec<u8>>()[..]
    );
    assert!(state.at_row_boundary());
}

#[test]
fn three_rows_where_the_second_fails_commit_exactly_the_first() {
    let p = tx_program();
    let row1 = tx_row(1, "ab", &[5, 6]);
    // Row 2 writes `a` and starts its string before it fails on a negative length.
    let mut row2 = zz(2);
    row2.extend(zz(-1));
    let row3 = tx_row(3, "xyz", &[7]);
    let input = [row1.clone(), row2, row3].concat();
    let (mut ids, mut chars, mut offs, mut aoffs, mut vals) =
        ([0u8; 64], [0u8; 64], [0u8; 64], [0u8; 64], [0u8; 64]);
    let mut state = State::new(0);
    let mut v = [
        Sink::new(&mut ids),
        Sink::with_offsets(&mut chars, &mut offs),
        Sink::new(&mut aoffs),
        Sink::new(&mut vals),
    ];
    let e = decode_partial(&p, &mut state, &input, 3, &mut v).unwrap_err();
    assert_eq!(e.error.code, ErrorCode::NegativeLength);
    // Exactly row 1 is committed and the cursor is at the start of row 2.
    assert_eq!((e.rows_done, e.consumed), (1, row1.len()));
    let fills: Vec<_> = v.iter().map(|s| (s.len, s.len2, s.offset)).collect();
    assert_eq!(fills, vec![(1, 0, 0), (2, 1, 2), (1, 0, 2), (2, 0, 0)]);
    assert!(state.at_row_boundary());
    // The plain `decode` still reports just the error.
    let mut state = State::new(0);
    assert_eq!(
        decode(&p, &mut state, &input, 3, &mut v).unwrap_err().code,
        ErrorCode::NegativeLength
    );
}
#[test]
fn an_error_in_the_first_row_keeps_the_fill_levels_the_caller_passed_in() {
    let p = tx_program();
    let mut bad = zz(9);
    bad.extend(zz(-1)); // negative string length
    let (mut ids, mut chars, mut offs, mut aoffs, mut vals) =
        ([0u8; 32], [0u8; 32], [0u8; 32], [0u8; 32], [0u8; 32]);
    let mut state = State::new(0);
    // Pretend earlier blocks left data in the columns.
    let mut v = [
        Sink {
            data: (&mut ids[..]).into(),
            data2: OutBuf::empty(),
            len: 3,
            len2: 0,
            offset: 0,
        },
        Sink {
            data: (&mut chars[..]).into(),
            data2: (&mut offs[..]).into(),
            len: 5,
            len2: 2,
            offset: 5,
        },
        Sink {
            data: (&mut aoffs[..]).into(),
            data2: OutBuf::empty(),
            len: 2,
            len2: 0,
            offset: 7,
        },
        Sink {
            data: (&mut vals[..]).into(),
            data2: OutBuf::empty(),
            len: 7,
            len2: 0,
            offset: 0,
        },
    ];
    let e = decode_partial(&p, &mut state, &bad, 1, &mut v).unwrap_err();
    assert_eq!(e.error.code, ErrorCode::NegativeLength);
    assert_eq!((e.rows_done, e.consumed), (0, 0));
    let fills: Vec<_> = v.iter().map(|s| (s.len, s.len2, s.offset)).collect();
    assert_eq!(fills, vec![(3, 0, 0), (5, 2, 5), (2, 0, 7), (7, 0, 0)]);
}

#[test]
fn chunked_decoding_with_row_rollback_matches_one_shot_for_every_buffer_size() {
    let p = tx_program();
    let rows: Vec<Vec<u8>> = (0..40)
        .map(|i| {
            tx_row(
                i,
                &"q".repeat((i % 7) as usize),
                &(0..(i % 5)).collect::<Vec<_>>(),
            )
        })
        .collect();
    let input = rows.concat();
    let mut one = Buffers::new(p.sinks(), 1000);
    drive(&p, &input, 40, 1000, 1000, 1000, &mut one).unwrap();
    for (rows_per_call, growth) in [(1u64, 1usize), (3, 1), (40, 1), (7, 2), (1000, 3)] {
        let mut b = Buffers::new(p.sinks(), 0);
        let (st, used) = drive(&p, &input, 40, rows_per_call, 0, growth, &mut b).unwrap();
        assert_eq!((st.rows, used), (40, input.len()));
        assert_eq!(
            b.dump(),
            one.dump(),
            "rows/call {rows_per_call}, growth {growth}"
        );
    }
}
