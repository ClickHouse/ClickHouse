//! The run that takes the numbers of an array or map block without going back to the dispatch of the executor
//! for each of them (`Stats::bulk_items`) must be invisible: the same sinks, the same bytes consumed and the
//! same errors as decoding the items one by one, whatever the sink has room for, wherever the input ends, and
//! whatever the bytes are.
//!
//! The expected values are built here, from the values that the input was encoded from.

mod common;

use _ch_rust_avro::exec::{decode_partial, State, Status};
use _ch_rust_avro::program::Program;
use _ch_rust_avro::ErrorCode;
use common::{drive, drive_stream, schema, universal_plan, zz, Buffers};

fn compile(items: &str) -> (Program, Vec<_ch_rust_avro::program::SinkDesc>) {
    let s = schema(&format!(r#"{{"type":"array","items":"{items}"}}"#));
    let u = universal_plan(&s);
    (Program::compile(&s, &u.sinks, &u.plan).unwrap(), u.sinks)
}

/// A small deterministic generator, so that a failure can be reproduced.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Long,
    Int,
    Float,
    Double,
    Boolean,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Long => "long",
            Kind::Int => "int",
            Kind::Float => "float",
            Kind::Double => "double",
            Kind::Boolean => "boolean",
        }
    }

    /// An item: its encoding and the text that the dump of the sink has for it.
    fn item(self, rng: &mut Rng) -> (Vec<u8>, String) {
        let r = rng.next();
        // Short values mostly, some of every length of varint.
        let bits = [0u32, 3, 6, 13, 20, 27, 34, 41, 50, 62][(r % 10) as usize];
        let v = (r >> 8) as i64 >> (63 - bits.min(63));
        match self {
            Kind::Long => (zz(v), v.to_string()),
            Kind::Int => {
                let v = (v as i32) >> (r as u32 % 3);
                (zz(i64::from(v)), v.to_string())
            }
            Kind::Float => {
                let f = f32::from_bits((r >> 16) as u32);
                (f.to_le_bytes().to_vec(), format!("{:08x}", f.to_bits()))
            }
            Kind::Double => {
                let f = f64::from_bits(r.rotate_left(17));
                (f.to_le_bytes().to_vec(), format!("{:016x}", f.to_bits()))
            }
            Kind::Boolean => (vec![(r >> 20) as u8 & 1], ((r >> 20) & 1).to_string()),
        }
    }
}

/// `rows` arrays, each in one to three blocks, some of them with the byte size that a negative count carries.
/// Returns the input and the dump that a correct decode has: the offsets sink, then the items.
fn build(kind: Kind, rows: usize, max_items: u64, seed: u64) -> (Vec<u8>, String) {
    let mut rng = Rng(seed | 1);
    let mut input = Vec::new();
    let (mut ends, mut values) = (Vec::new(), Vec::new());
    let mut total = 0u64;
    for _ in 0..rows {
        let n = rng.next() % (max_items + 1);
        let mut left = n;
        while left > 0 {
            let block = if rng.next().is_multiple_of(2) {
                left
            } else {
                1 + rng.next() % left
            };
            let mut payload = Vec::new();
            for _ in 0..block {
                let (bytes, text) = kind.item(&mut rng);
                payload.extend(bytes);
                values.push(text);
            }
            if rng.next().is_multiple_of(3) {
                input.extend(zz(-(block as i64)));
                input.extend(zz(payload.len() as i64));
            } else {
                input.extend(zz(block as i64));
            }
            input.extend(payload);
            left -= block;
        }
        input.push(0);
        total += n;
        ends.push(total.to_string());
    }
    (
        input,
        format!("0:{}\n1:{}\n", ends.join(" "), values.join(" ")),
    )
}

const KINDS: [Kind; 5] = [
    Kind::Long,
    Kind::Int,
    Kind::Float,
    Kind::Double,
    Kind::Boolean,
];

/// However the sinks start and grow, and however many rows a call decodes, the sinks hold the items.
#[test]
fn every_room_gives_the_same_items() {
    for kind in KINDS {
        let (program, descs) = compile(kind.name());
        for seed in 1..=6u64 {
            let rows = 8;
            let (input, expected) = build(kind, rows, 70, seed * 7919);
            for initial in [0usize, 1, 3, 16, 100] {
                for growth in [1usize, 2, 1000] {
                    for rows_per_call in [1u64, 3, rows as u64] {
                        let mut bufs = Buffers::new(&descs, initial);
                        let (stats, used) = drive(
                            &program,
                            &input,
                            rows as u64,
                            rows_per_call,
                            initial,
                            growth,
                            &mut bufs,
                        )
                        .unwrap_or_else(|e| panic!("{kind:?} seed={seed} initial={initial}: {e}"));
                        assert_eq!((stats.rows, used), (rows as u64, input.len()));
                        assert_eq!(
                            bufs.dump(),
                            expected,
                            "{kind:?} seed={seed} initial={initial} growth={growth} rows_per_call={rows_per_call}"
                        );
                    }
                }
            }
        }
    }
}

/// An input that is cut anywhere, and handed over in pieces, gives the same items.
#[test]
fn every_window_gives_the_same_items() {
    for kind in KINDS {
        let (program, descs) = compile(kind.name());
        let rows = 5;
        let (input, expected) = build(kind, rows, 40, 12345);
        for first in [1usize, 2, 5, 17, 64] {
            for step in [1usize, 3, 9, 100] {
                let mut bufs = Buffers::new(&descs, 4);
                let stats = drive_stream(&program, &input, rows as u64, 2, first, step, &mut bufs)
                    .unwrap_or_else(|e| panic!("{kind:?} first={first} step={step}: {e}"));
                assert_eq!(stats.consumed, input.len());
                assert_eq!(bufs.dump(), expected, "{kind:?} first={first} step={step}");
            }
        }
    }
}

/// An input that stops anywhere in the row is an end of the input, and what the row wrote is not left in the
/// sinks.
#[test]
fn a_cut_input_is_an_eof_and_leaves_no_trace() {
    for kind in KINDS {
        let (program, descs) = compile(kind.name());
        let (input, _) = build(kind, 1, 50, 99);
        for cut in 1..input.len() {
            let mut bufs = Buffers::new(&descs, 100);
            let mut state = State::new(0);
            let e = bufs
                .with_sinks(|sinks| decode_partial(&program, &mut state, &input[..cut], 1, sinks))
                .unwrap_err();
            assert!(
                matches!(
                    e.error.code,
                    ErrorCode::Eof | ErrorCode::CountExceedsPayload
                ),
                "{kind:?} cut={cut}: {:?}",
                e.error
            );
            assert_eq!((e.rows_done, e.consumed), (0, 0), "{kind:?} cut={cut}");
            assert_eq!(bufs.dump(), "0:\n1:\n", "{kind:?} cut={cut}");
        }
    }
}

/// The error of the item that the run stops at is the error of decoding it alone, at the same place of the
/// same row, with the rows before it committed and nothing of its own row in the sinks.
#[test]
fn the_item_that_stops_the_run_reports_its_own_error() {
    let cases: [(Kind, Vec<u8>, ErrorCode); 3] = [
        // A varint of eleven bytes.
        (Kind::Long, vec![0xff; 11], ErrorCode::InvalidVarint),
        // 2^31, which is not an `int`.
        (Kind::Int, zz(1 << 31), ErrorCode::IntOutOfRange),
        // Not 0 or 1.
        (Kind::Boolean, vec![2], ErrorCode::InvalidBool),
    ];
    for (kind, bad, code) in cases {
        let (program, descs) = compile(kind.name());
        // The first row is good; the second has `bad` as its item number `at`.
        for at in [0usize, 1, 2, 5, 19, 20] {
            let mut rng = Rng(77);
            let (first, _) = build(kind, 1, 30, 4242);
            let mut second = zz(21);
            for i in 0..21 {
                if i == at {
                    second.extend(&bad);
                } else {
                    second.extend(kind.item(&mut rng).0);
                }
            }
            second.push(0);
            let mut input = first.clone();
            input.extend(&second);
            let mut bufs = Buffers::new(&descs, 200);
            let mut state = State::new(0);
            let e = bufs
                .with_sinks(|sinks| decode_partial(&program, &mut state, &input, 2, sinks))
                .unwrap_err();
            assert_eq!(e.error.code, code, "{kind:?} at={at}");
            assert_eq!(
                (e.rows_done, e.consumed),
                (1, first.len()),
                "{kind:?} at={at}"
            );
            // The first row is what the sinks have.
            let (_, dump_first) = build(kind, 1, 30, 4242);
            assert_eq!(bufs.dump(), dump_first, "{kind:?} at={at}");
            assert!(state.at_row_boundary());
        }
    }
}

/// The numbers of a block are taken in runs, which the counter shows, and a sink that is full stops a run
/// where the dispatch would have stopped.
#[test]
fn a_run_ends_where_the_sink_is_full() {
    let (program, descs) = compile("long");
    let mut input = zz(10);
    input.extend([2, 4, 6, 8, 10, 12, 14, 16, 18, 20]);
    input.push(0);
    // Room for 4 of the 10 numbers: the first is read by the dispatch, the next three by a run, and the
    // sixth finds the sink full.
    let mut bufs = Buffers::new(&descs, 0);
    bufs.grow(0, 1, 0);
    bufs.grow(1, 4, 0);
    let mut state = State::new(0);
    let p = bufs
        .with_sinks(|sinks| decode_partial(&program, &mut state, &input, 1, sinks))
        .unwrap();
    assert_eq!((p.rows_done, p.consumed), (0, 0));
    assert!(matches!(
        p.status,
        Status::NeedSpace {
            sink: 1,
            min_cap: 5,
            ..
        }
    ));
    assert_eq!(state.stats().bulk_items, 3);
    assert!(!state.at_row_boundary());
    bufs.grow(1, 10, 0);
    let p = bufs
        .with_sinks(|sinks| {
            _ch_rust_avro::exec::decode_resumed(&program, &mut state, &input, 1, sinks)
        })
        .unwrap();
    assert_eq!(
        (p.rows_done, p.consumed, p.status),
        (1, input.len(), Status::RowLimit)
    );
    assert_eq!(state.stats().bulk_items, 3 + 5);
    assert_eq!(bufs.dump(), "0:10\n1:1 2 3 4 5 6 7 8 9 10\n");
}
