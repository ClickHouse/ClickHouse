//! Differential test against `fastavro`: schemas and data are generated and
//! encoded by `gen_random.py` (which also computes the columns the executor must
//! produce, the Parsing Canonical Form and the fingerprint). The Rust side decodes
//! the same bytes in several chunkings and must agree, and must also consume
//! exactly as many bytes when it only skips.

mod common;

use _ch_rust_avro::canonical::fingerprint64;
use _ch_rust_avro::program::Program;
use _ch_rust_avro::schema::Schema;
use common::*;

#[test]
fn random_schemas_and_data_agree_with_fastavro() {
    let dir = fixtures_out_dir().join("random");
    let mut cases: Vec<_> = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "avsc"))
                .collect()
        })
        .unwrap_or_default();
    cases.sort();
    if cases.is_empty() {
        assert!(
            std::env::var_os("AVRO_REQUIRE_FIXTURES").is_none(),
            "random cases are missing: run tests/gen_random.py {}",
            fixtures_out_dir().display()
        );
        eprintln!(
            "SKIPPED: random cases are missing; run tests/gen_random.py {}",
            fixtures_out_dir().display()
        );
        return;
    }
    let (mut total_rows, mut need_space_total, mut unions, mut maps, mut recursive) =
        (0u64, 0u32, 0, 0, 0);
    for avsc in &cases {
        let name = avsc.file_stem().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(avsc).unwrap();
        let input = std::fs::read(avsc.with_extension("bin")).unwrap();
        let expected_txt = std::fs::read_to_string(avsc.with_extension("txt")).unwrap();
        let (first, expected_dump) = expected_txt.split_once('\n').unwrap();
        let rows: u64 = first.parse().unwrap();
        let s = Schema::parse(text.as_bytes(), 0).unwrap_or_else(|e| panic!("{name}: {e}\n{text}"));

        // Canonical form and fingerprint: fastavro is the oracle.
        let canon = std::fs::read_to_string(avsc.with_extension("canon")).unwrap();
        assert_eq!(s.canonical_form(), canon, "{name}: canonical form\n{text}");
        // Both Python implementations (fastavro, avro) print the fingerprint as the hex of its
        // little-endian bytes.
        let fp = std::fs::read_to_string(avsc.with_extension("fp")).unwrap();
        assert_eq!(
            hex(&fingerprint64(canon.as_bytes()).to_le_bytes()),
            fp.trim(),
            "{name}: fingerprint"
        );

        let u = universal_plan(&s);
        let p = Program::compile(&s, &u.sinks, &u.plan)
            .unwrap_or_else(|e| panic!("{name}: {e}\n{text}"));
        for (rows_per_call, initial, growth) in
            [(1000u64, 4096usize, 4096usize), (1, 0, 1), (3, 1, 2)]
        {
            let mut bufs = Buffers::new(&u.sinks, initial);
            let (st, used) = drive(&p, &input, rows, rows_per_call, initial, growth, &mut bufs)
                .unwrap_or_else(|e| panic!("{name}: {e}\n{text}"));
            assert_eq!((st.rows, used), (rows, input.len()), "{name} rows/consumed");
            assert_eq!(
                bufs.dump(),
                expected_dump,
                "{name} chunking ({rows_per_call},{initial},{growth})\n{text}"
            );
            need_space_total += st.need_space;
        }
        // Skipping consumes the same bytes as decoding.
        let skip = skip_plan(&s);
        let sp = Program::compile(&s, &skip.sinks, &skip.plan).unwrap();
        let mut none = Buffers::new(&skip.sinks, 0);
        let (st, used) = drive(&sp, &input, rows, 1000, 0, 1, &mut none)
            .unwrap_or_else(|e| panic!("{name} skip: {e}\n{text}"));
        assert_eq!(
            (st.rows, used),
            (rows, input.len()),
            "{name} skip consumption"
        );
        total_rows += rows;
        unions += text.matches("[\"").count() + text.matches(",\"null\"").count();
        maps += text.matches("\"map\"").count();
        recursive += usize::from(s.in_degree(s.root()) > 1);
    }
    eprintln!("{} cases, {total_rows} rows, {need_space_total} NeedSpace round trips, {unions} unions, {maps} maps, {recursive} recursive roots", cases.len());
    assert!(cases.len() >= 200);
    assert!(need_space_total > 1000, "buffer growth was exercised");
    assert!(maps > 20 && unions > 100);
}
