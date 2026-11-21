#![allow(unsafe_code)]
//! The caller's output buffers are not initialized: a ClickHouse `PODArray::resize` exposes bytes that
//! nobody wrote. The executor must only write to them, and what it reports as committed must have been
//! written. These tests decode into buffers that are really uninitialized, directly and through the C ABI,
//! and read back the committed bytes (which is where an unwritten byte would be an error).
//!
//! They are meant for Miri, which tracks initialization byte by byte and, with
//! `-Zmiri-recursive-validation`, also rejects a `&mut [u8]` that is made over uninitialized bytes:
//!
//! ```sh
//! MIRIFLAGS=-Zmiri-recursive-validation cargo +nightly miri test --test uninit_output
//! ```
//!
//! They run as plain tests too, where they check that the result does not depend on the content that the
//! buffers had before.

mod common;

use std::mem::MaybeUninit;

use _ch_rust_avro::exec::{decode_partial, Sink, State, Status};
use _ch_rust_avro::ffi::{
    avro_decode, avro_state_free, avro_state_new, avro_state_set_streaming, AvroProgram,
    AvroProgress, AvroSink, AVRO_STATUS_BLOCK_DONE, AVRO_STATUS_NEED_INPUT, AVRO_STATUS_NEED_SPACE,
    AVRO_STATUS_ROW_LIMIT,
};
use _ch_rust_avro::program::{Action, PlanNode, Program, SinkDesc, SinkKind, SINK_TERMINATED};
use common::*;

/// The committed bytes of every sink: primary buffer, then secondary buffer.
type Columns = Vec<(Vec<u8>, Vec<u8>)>;

/// A buffer of `n` bytes: uninitialized, or filled with a pattern that the executor must not rely on.
fn buffer(n: usize, init: bool) -> Vec<MaybeUninit<u8>> {
    if init {
        vec![MaybeUninit::new(0xa5); n]
    } else {
        vec![MaybeUninit::uninit(); n]
    }
}

/// The first `n` bytes of `buf`, which must have been written.
fn committed(buf: &[MaybeUninit<u8>], n: usize) -> Vec<u8> {
    assert!(n <= buf.len());
    // SAFETY: the first `n` bytes are inside the buffer. That they are initialized is what is tested: Miri
    // reports an unwritten byte at the copy.
    unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, n) }.to_vec()
}

struct Column {
    data: Vec<MaybeUninit<u8>>,
    data2: Vec<MaybeUninit<u8>>,
    len: usize,
    len2: usize,
    offset: u64,
}

fn columns(program: &Program, start_elems: usize, init: bool) -> Vec<Column> {
    program
        .sinks()
        .iter()
        .map(|d| Column {
            data: buffer(d.width() * start_elems, init),
            data2: buffer(d.width2() * start_elems, init),
            len: 0,
            len2: 0,
            offset: 0,
        })
        .collect()
}

fn read_back(program: &Program, cols: &[Column]) -> Columns {
    program
        .sinks()
        .iter()
        .zip(cols)
        .map(|(d, c)| {
            (
                committed(&c.data, c.len * d.width()),
                committed(&c.data2, c.len2 * d.width2()),
            )
        })
        .collect()
}

/// Grows what `NEED_SPACE` asked for: resizing keeps the bytes below the fill level and leaves the new
/// ones as they are, the way `PODArray::resize` does.
fn grow(
    program: &Program,
    cols: &mut [Column],
    sink: usize,
    min_cap: u64,
    min_cap2: u64,
    init: bool,
) {
    let d = program.sinks()[sink];
    let fill = if init {
        MaybeUninit::new(0xa5)
    } else {
        MaybeUninit::uninit()
    };
    let want = d.width() * min_cap as usize;
    if want > cols[sink].data.len() {
        cols[sink].data.resize(want, fill);
    }
    let want2 = d.width2() * min_cap2 as usize;
    if want2 > cols[sink].data2.len() {
        cols[sink].data2.resize(want2, fill);
    }
}

/// Decodes all of `input` with the executor, `rows_per_call` rows at a time, into buffers of `start_elems`
/// elements that grow on demand.
fn run_exec(
    program: &Program,
    input: &[u8],
    rows_per_call: u64,
    start_elems: usize,
    init: bool,
) -> Columns {
    let mut cols = columns(program, start_elems, init);
    let mut state = State::new(0);
    let mut pos = 0;
    loop {
        let mut sinks: Vec<Sink<'_>> = cols
            .iter_mut()
            .map(|c| {
                let mut s = Sink::with_offsets_uninit(&mut c.data, &mut c.data2);
                (s.len, s.len2, s.offset) = (c.len, c.len2, c.offset);
                s
            })
            .collect();
        let r = decode_partial(
            program,
            &mut state,
            &input[pos..],
            rows_per_call,
            &mut sinks,
        );
        let fills: Vec<_> = sinks.iter().map(|s| (s.len, s.len2, s.offset)).collect();
        drop(sinks);
        for (c, f) in cols.iter_mut().zip(fills) {
            (c.len, c.len2, c.offset) = f;
        }
        let p = r.unwrap_or_else(|e| panic!("decode failed: {e:?}"));
        pos += p.consumed;
        match p.status {
            Status::NeedSpace {
                sink,
                min_cap,
                min_cap2,
            } => grow(program, &mut cols, sink as usize, min_cap, min_cap2, init),
            Status::RowLimit => {}
            Status::BlockDone => break,
            Status::NeedInput => unreachable!("streaming mode is off"),
        }
    }
    assert_eq!(pos, input.len());
    read_back(program, &cols)
}

/// The same through `avro_decode`, whose sinks are raw pointers to the caller's memory. With a `window`, the
/// caller passes that many bytes at first and as many more whenever the executor needs input (streaming mode),
/// dropping the bytes that are consumed: the C++ driver of a block.
fn run_ffi(
    program: &Program,
    input: &[u8],
    rows_per_call: u64,
    start_elems: usize,
    init: bool,
    window: Option<usize>,
) -> Columns {
    let mut cols = columns(program, start_elems, init);
    let mut c_sinks: Vec<AvroSink> = Vec::new();
    let sync = |cols: &mut [Column], c_sinks: &mut Vec<AvroSink>, only: Option<usize>| {
        if c_sinks.is_empty() {
            c_sinks.extend(cols.iter_mut().map(|c| AvroSink {
                data: c.data.as_mut_ptr() as *mut u8,
                data_size: c.data.len() as u64,
                data2: c.data2.as_mut_ptr() as *mut u8,
                data2_size: c.data2.len() as u64,
                len: 0,
                len2: 0,
                offset: 0,
            }));
        } else if let Some(i) = only {
            c_sinks[i].data = cols[i].data.as_mut_ptr() as *mut u8;
            c_sinks[i].data_size = cols[i].data.len() as u64;
            c_sinks[i].data2 = cols[i].data2.as_mut_ptr() as *mut u8;
            c_sinks[i].data2_size = cols[i].data2.len() as u64;
        }
    };
    sync(&mut cols, &mut c_sinks, None);
    let state = avro_state_new(0);
    // The window is `input[pos..visible]`.
    let mut pos = 0;
    let mut visible = window.map_or(input.len(), |w| w.min(input.len()));
    loop {
        let mut err = [0 as std::ffi::c_char; 256];
        let mut p = MaybeUninit::<AvroProgress>::zeroed();
        // SAFETY: the pointers are those of live buffers and sinks; the program is a `Program`, which is what
        // an `AvroProgram` handle is. The buffers need not be initialized. The state is ours.
        let rc = unsafe {
            avro_state_set_streaming(state, i32::from(visible < input.len()));
            avro_decode(
                state,
                program as *const Program as *const AvroProgram,
                input[pos..visible].as_ptr(),
                visible - pos,
                rows_per_call,
                c_sinks.as_mut_ptr(),
                c_sinks.len(),
                p.as_mut_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert_eq!(rc, 0);
        // SAFETY: `avro_decode` always sets its out parameter.
        let p = unsafe { p.assume_init() };
        pos += p.consumed as usize;
        let more = |visible: usize| (visible + window.unwrap_or(0)).min(input.len());
        match p.status {
            AVRO_STATUS_NEED_SPACE => {
                let k = p.need_sink as usize;
                grow(program, &mut cols, k, p.need_cap, p.need_cap2, init);
                sync(&mut cols, &mut c_sinks, Some(k));
            }
            AVRO_STATUS_NEED_INPUT => {
                assert!(visible < input.len());
                visible = more(visible);
            }
            AVRO_STATUS_ROW_LIMIT => {}
            AVRO_STATUS_BLOCK_DONE => {
                if visible == input.len() {
                    break;
                }
                visible = more(visible);
            }
            s => panic!("status {s}"),
        }
    }
    // SAFETY: the state came from `avro_state_new` and is not used again.
    unsafe { avro_state_free(state) };
    assert_eq!(pos, input.len());
    for (c, s) in cols.iter_mut().zip(&c_sinks) {
        (c.len, c.len2, c.offset) = (s.len as usize, s.len2 as usize, s.offset);
    }
    read_back(program, &cols)
}

/// Every way of calling that the tests know, on uninitialized and on pattern-filled buffers: the committed
/// bytes are always the same. Returns them.
fn decode_every_way(program: &Program, input: &[u8], rows: u64) -> Columns {
    let reference = run_exec(program, input, rows, 128, true);
    for rows_per_call in [1, 2, rows] {
        // `0` starts every sink without room: the first row pauses for each of its sinks.
        for start_elems in [0, 1, 3, 128] {
            for init in [false, true] {
                assert_eq!(
                    run_exec(program, input, rows_per_call, start_elems, init),
                    reference,
                    "exec rows_per_call={rows_per_call} start_elems={start_elems} init={init}"
                );
                assert_eq!(
                    run_ffi(program, input, rows_per_call, start_elems, init, None),
                    reference,
                    "ffi rows_per_call={rows_per_call} start_elems={start_elems} init={init}"
                );
            }
        }
    }
    // Streaming: the row is paused for input, with whatever it wrote so far in the uninitialized buffers.
    for rows_per_call in [1, rows] {
        for start_elems in [0, 3] {
            for window in [5, 64] {
                assert_eq!(
                    run_ffi(
                        program,
                        input,
                        rows_per_call,
                        start_elems,
                        false,
                        Some(window)
                    ),
                    reference,
                    "ffi window={window} rows_per_call={rows_per_call} start_elems={start_elems}"
                );
            }
        }
    }
    reference
}

fn f32le(v: f32) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

fn f64le(v: f64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

/// A long array of `n` items: one block, then the end marker.
fn longs(n: usize) -> Vec<u8> {
    let mut v = zz(n as i64);
    for i in 0..n {
        v.extend(zz(i as i64 * 1000 - 5));
    }
    v.extend(zz(0));
    v
}

#[test]
fn every_leaf_kind_of_the_universal_plan_commits_only_written_bytes() {
    let json = r#"{"type":"record","name":"R","fields":[
        {"name":"i","type":"int"},
        {"name":"l","type":"long"},
        {"name":"f","type":"float"},
        {"name":"d","type":"double"},
        {"name":"b","type":"boolean"},
        {"name":"s","type":"string"},
        {"name":"by","type":"bytes"},
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
    let program = Program::compile(&sch, &u.sinks, &u.plan).unwrap();
    let mut input = Vec::new();
    let rows = 3u64;
    for r in 0..rows as i64 {
        input.extend(zz(r - 1)); // i
        input.extend(zz(1 << (20 + r))); // l
        input.extend(f32le(1.5 + r as f32)); // f
        input.extend(f64le(-2.25 * r as f64)); // d
        input.push((r & 1) as u8); // b
        input.extend(avro_bytes(&b"abc".repeat(r as usize))); // s
        input.extend(avro_bytes(&[0xff, 0, r as u8])); // by
        input.extend_from_slice(&[1, 2, 3, 4, r as u8]); // fx
        input.extend(zz(r % 3)); // e
        input.extend(zz(r % 2)); // u: null or long
        if r % 2 == 1 {
            input.extend(zz(77));
        }
        input.extend(longs(20 + r as usize * 20)); // arr: a block big enough to ask for room up front
        input.extend(zz(2)); // strs: two blocks
        input.extend(avro_bytes(b"x"));
        input.extend(avro_bytes(b""));
        input.extend(zz(1));
        input.extend(avro_bytes(b"last"));
        input.extend(zz(0));
        input.extend(zz(2)); // m
        input.extend(avro_bytes(b"k1"));
        input.extend(zz(10));
        input.extend(avro_bytes(b"key2"));
        input.extend(zz(-20));
        input.extend(zz(0));
        input.extend(zz(r)); // n.x
        input.extend(avro_bytes(b"why")); // n.y
    }
    let out = decode_every_way(&program, &input, rows);
    // Sink 0 is `i`: three int32.
    let ints: Vec<i32> = out[0]
        .0
        .chunks_exact(4)
        .map(|c| i32::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(ints, [-1, 0, 1]);
    // Sink 5 is `s` (the sixth leaf): the bytes of the three values and one end offset for each.
    assert_eq!(out[5].0, b"abcabcabc");
    let offsets: Vec<u64> = out[5]
        .1
        .chunks_exact(8)
        .map(|c| u64::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(offsets, [0, 3, 9]);
}

#[test]
fn widening_decimals_terminated_strings_and_null_maps_commit_only_written_bytes() {
    let json = r#"{"type":"record","name":"R","fields":[
        {"name":"a","type":"int"},
        {"name":"b","type":"float"},
        {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
        {"name":"decf","type":{"type":"fixed","name":"D3","size":3,"logicalType":"decimal","precision":5,"scale":2}},
        {"name":"s","type":"string"},
        {"name":"n","type":["null","long"]},
        {"name":"t","type":["int","string","null"]}
    ]}"#;
    let sch = schema(json);
    let root = sch.root();
    let field = |i| sch.child(root, i);
    let mut terminated = SinkDesc::new(SinkKind::Bytes, 0);
    terminated.flags = SINK_TERMINATED;
    let sinks = [
        SinkDesc::new(SinkKind::Int64, 0),   // a, widened
        SinkDesc::new(SinkKind::Float64, 0), // b, widened
        SinkDesc::new(SinkKind::Decimal, 16),
        SinkDesc::new(SinkKind::Decimal, 4),
        terminated,
        SinkDesc::new(SinkKind::NullMap, 0),
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::UnionTag, 2),
        SinkDesc::new(SinkKind::Int32, 0),
        SinkDesc::new(SinkKind::Bytes, 0),
    ];
    let mut n = PlanNode::new(Action::Union, field(5), 2);
    n.sink = 5;
    n.arg = 0;
    let mut t = PlanNode::new(Action::Union, field(6), 3);
    t.sink = 7;
    let plan = [
        PlanNode::new(Action::Record, root, 7),
        PlanNode::value(field(0), 0),
        PlanNode::value(field(1), 1),
        PlanNode::value(field(2), 2),
        PlanNode::value(field(3), 3),
        PlanNode::value(field(4), 4),
        n,
        PlanNode::skip(sch.child(field(5), 0)),
        PlanNode::value(sch.child(field(5), 1), 6),
        t,
        PlanNode::value(sch.child(field(6), 0), 8),
        PlanNode::value(sch.child(field(6), 1), 9),
        PlanNode::skip(sch.child(field(6), 2)),
    ];
    let program = Program::compile(&sch, &sinks, &plan).unwrap();
    let mut input = Vec::new();
    let rows = 4u64;
    for r in 0..rows as i64 {
        input.extend(zz(-70000 * (r + 1))); // a
        input.extend(f32le(r as f32 + 0.5)); // b
        input.extend(avro_bytes(&[0xff, 0xfe, r as u8])); // dec: a negative number
        input.extend_from_slice(&[0x00, 0x01, r as u8]); // decf
        input.extend(avro_bytes(&b"term".repeat(r as usize))); // s
        input.extend(zz(r % 2)); // n
        if r % 2 == 1 {
            input.extend(zz(5 * r));
        }
        input.extend(zz(r % 3)); // t
        match r % 3 {
            0 => input.extend(zz(r)),
            1 => input.extend(avro_bytes(b"str")),
            _ => {}
        }
    }
    let out = decode_every_way(&program, &input, rows);
    // The terminated string sink: every value gets a zero byte, and the offsets count it.
    assert_eq!(out[4].0, b"\0term\0termterm\0termtermterm\0");
}
