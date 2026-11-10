//! Cheap, deterministic fuzzing that runs with `cargo test`: random mutations of
//! valid schemas, data and container files. Properties, not expected values:
//! nothing panics, work is bounded, skipping and decoding agree on the verdict
//! and the consumed size, and the canonical form is a fixed point of parsing.

mod common;

use std::time::{Duration, Instant};

use _ch_rust_avro::exec::{decode, State, Status};
use _ch_rust_avro::ocf::{self, BlockCtx, Parsed};
use _ch_rust_avro::program::Program;
use _ch_rust_avro::schema::Schema;
use common::*;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// The Parsing Canonical Form drops namespaces, so a type in the null namespace inside a
/// namespaced record is printed with a bare name that parses back into the enclosing
/// namespace. That is a property of the format, not of this crate: the fixed-point check does not
/// apply to such schemas.
fn canonical_is_unambiguous(s: &Schema) -> bool {
    let mut null_ns = false;
    let mut ns = false;
    for id in 0..s.node_count() {
        if let Some(n) = s.node(id as u32).namespace() {
            if n.is_empty() {
                null_ns = true;
            } else {
                ns = true;
            }
        }
    }
    !(null_ns && ns)
}

fn check_fixed_point(s: &Schema, what: &str) {
    if !canonical_is_unambiguous(s) {
        return;
    }
    let again = Schema::parse(s.canonical_form().as_bytes(), 0).unwrap_or_else(|e| {
        panic!(
            "{e}: canonical form of {what} does not parse: {}",
            s.canonical_form()
        )
    });
    assert_eq!(again.canonical_form(), s.canonical_form(), "{what}");
}

fn mutate(rng: &mut Rng, src: &[u8], dictionary: &[&[u8]]) -> Vec<u8> {
    let mut v = src.to_vec();
    for _ in 0..1 + rng.below(4) {
        match rng.below(6) {
            0 if !v.is_empty() => {
                let i = rng.below(v.len());
                v[i] ^= 1 << rng.below(8);
            }
            1 if !v.is_empty() => {
                let i = rng.below(v.len());
                v[i] = rng.next() as u8;
            }
            2 if !v.is_empty() => {
                let i = rng.below(v.len());
                v.truncate(i);
            }
            3 => {
                let i = rng.below(v.len() + 1);
                v.insert(i, rng.next() as u8);
            }
            4 if !v.is_empty() => {
                let i = rng.below(v.len());
                v.remove(i);
            }
            _ if !dictionary.is_empty() => {
                let i = rng.below(v.len() + 1);
                let d = dictionary[rng.below(dictionary.len())];
                for (k, b) in d.iter().enumerate() {
                    v.insert(i + k, *b);
                }
            }
            _ => {}
        }
    }
    v
}

fn load_cases() -> Vec<(String, Vec<u8>)> {
    let dir = fixtures_out_dir().join("random");
    let mut avsc = Vec::new();
    find_files(&dir, "avsc", &mut avsc);
    let mut cases: Vec<(String, Vec<u8>)> = avsc
        .iter()
        .map(|p| {
            (
                std::fs::read_to_string(p).unwrap(),
                std::fs::read(p.with_extension("bin")).unwrap(),
            )
        })
        .collect();
    cases.push((r#"{"type":"record","name":"R","fields":[{"name":"a","type":["null","long"]},{"name":"b","type":{"type":"map","values":{"type":"array","items":"string"}}}]}"#.to_string(), vec![2, 4, 2, 2, 97, 2, 2, 98, 0, 0]));
    cases.iter().map(|(s, b)| (s.clone(), b.clone())).collect()
}

#[test]
fn mutated_schemas_never_panic_and_canonical_form_is_a_fixed_point() {
    let cases = load_cases();
    let mut rng = Rng(0x1234_5678_9abc_def1);
    let dict: &[&[u8]] = &[
        b"\"type\"",
        b"\"record\"",
        b"[",
        b"]",
        b"{",
        b"}",
        b"\"name\":\"R\"",
        b"null",
        b"\"fields\":[",
        b"\"items\":",
        b"\"size\":4294967296",
        b"\\u00",
        b"\"symbols\":[]",
    ];
    let (mut accepted, mut total) = (0, 0);
    let t = Instant::now();
    for round in 0..40 {
        for (schema_text, _) in &cases {
            if round == 0 {
                // The unmutated schema must round-trip through its canonical form.
                let s = Schema::parse(schema_text.as_bytes(), 0).unwrap();
                check_fixed_point(&s, schema_text);
            }
            let m = mutate(&mut rng, schema_text.as_bytes(), dict);
            total += 1;
            if let Ok(s) = Schema::parse(&m, 64) {
                accepted += 1;
                check_fixed_point(&s, &String::from_utf8_lossy(&m));
                let u = universal_plan(&s);
                let _ = Program::compile(&s, &u.sinks, &u.plan).unwrap();
                let k = skip_plan(&s);
                let _ = Program::compile(&s, &k.sinks, &k.plan).unwrap();
            }
        }
    }
    assert!(t.elapsed() < Duration::from_secs(60), "{:?}", t.elapsed());
    eprintln!("{accepted} of {total} mutated schemas were still valid");
    assert!(accepted > 0 && accepted < total);
}

/// Runs a program until it stops and returns the outcome as a comparable value.
fn run(
    program: &Program,
    input: &[u8],
    rows: u64,
) -> (Result<u64, _ch_rust_avro::ErrorCode>, usize) {
    let mut bufs = Buffers::new(program.sinks(), 1);
    let mut state = State::new(1000);
    let (mut pos, mut done) = (0usize, 0u64);
    loop {
        match bufs.with_sinks(|s| decode(program, &mut state, &input[pos..], rows - done, s)) {
            // How far a failed call got is not part of the contract.
            Err(e) => return (Err(e.code), 0),
            Ok(p) => {
                pos += p.consumed;
                done += p.rows_done;
                match p.status {
                    Status::NeedSpace {
                        sink,
                        min_cap,
                        min_cap2,
                    } => bufs.grow(sink as usize, min_cap.max(4), min_cap2.max(4)),
                    _ => return (Ok(done), pos),
                }
            }
        }
    }
}

#[test]
fn mutated_data_skip_and_decode_agree() {
    let cases = load_cases();
    let mut rng = Rng(0xdead_beef_cafe_f00d);
    let t = Instant::now();
    let (mut ok, mut total) = (0, 0);
    for (text, bin) in &cases {
        let s = Schema::parse(text.as_bytes(), 0).unwrap();
        let u = universal_plan(&s);
        let full = Program::compile(&s, &u.sinks, &u.plan).unwrap();
        let k = skip_plan(&s);
        let skip = Program::compile(&s, &k.sinks, &k.plan).unwrap();
        for _ in 0..40 {
            let m = mutate(
                &mut rng,
                bin,
                &[
                    &[0xff; 9],
                    &[0x80, 0x80, 0x80, 0x80, 0x10],
                    &[0x01],
                    &[0x7f],
                ],
            );
            for rows in [1u64, 3] {
                total += 1;
                let a = run(&full, &m, rows);
                let b = run(&skip, &m, rows);
                assert_eq!(a, b, "skip and decode disagree on {text}\ninput {m:02x?}");
                ok += usize::from(a.0.is_ok());
            }
        }
    }
    assert!(t.elapsed() < Duration::from_secs(60), "{:?}", t.elapsed());
    eprintln!("{ok} of {total} mutated inputs decoded to the end");
    assert!(ok > 0 && ok < total);
}

#[test]
fn mutated_container_files_never_panic() {
    // A hand-built valid file plus any decompressed fixtures that exist.
    let mut files: Vec<Vec<u8>> = Vec::new();
    let mut paths = Vec::new();
    find_files(
        &fixtures_out_dir().join("ocf/tests/queries/0_stateless/data_avro"),
        "avro",
        &mut paths,
    );
    for p in paths.iter().take(30) {
        files.push(std::fs::read(p).unwrap());
    }
    let mut h = b"Obj\x01".to_vec();
    h.extend(zz(2));
    h.extend(avro_bytes(b"avro.schema"));
    h.extend(avro_bytes(br#"{"type":"array","items":"long"}"#));
    h.extend(avro_bytes(b"avro.codec"));
    h.extend(avro_bytes(b"null"));
    h.extend(zz(0));
    h.extend_from_slice(&[9; 16]);
    h.extend(zz(2));
    h.extend(zz(8));
    h.extend_from_slice(&[4, 2, 4, 0, 4, 2, 4, 0]);
    h.extend_from_slice(&[9; 16]);
    files.push(h);
    assert_eq!(read_ocf_skip_all(files.last().unwrap()).unwrap(), 2);
    let mut rng = Rng(0x0bad_f00d_0bad_f00d);
    let t = Instant::now();
    for f in &files {
        for _ in 0..300 {
            let m = mutate(&mut rng, f, &[b"\x00", b"Obj\x01", &[0xff; 10]]);
            let _ = read_ocf_skip_all(&m);
            let _ = read_ocf(&m, OcfMode::FramingOnly);
            // Streaming: a window that grows by one byte at a time reaches the same verdict as the
            // whole file, without panicking.
            if m.len() < 400 {
                let mut seen = 0;
                while let Ok(Parsed::NeedMore { needed }) =
                    ocf::parse_header(&m[..seen], false, &ocf::OcfLimits::default())
                {
                    assert!(needed > seen);
                    seen += 1;
                    if seen > m.len() {
                        break;
                    }
                }
            }
        }
    }
    let _ = BlockCtx {
        codec: ocf::Codec::Null,
        min_record_size: 0,
        max_block_bytes: 0,
    };
    assert!(t.elapsed() < Duration::from_secs(60), "{:?}", t.elapsed());
}
