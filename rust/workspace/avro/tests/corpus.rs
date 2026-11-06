//! Runs every `.avro` file of the repository through the crate's container
//! reader and executor, and compares the verdict and the row count with
//! `fastavro`, an independent implementation (see `gen_fixtures.py`, which
//! also produces decompressed copies: decompression is the C++ caller's job).

mod common;

use std::collections::BTreeSet;

use _ch_rust_avro::ErrorCode;
use common::*;

/// The files that the ClickHouse stateless tests expect to be rejected
/// (02373, 04036, 02101) plus the Iceberg manifest whose magic is wrong.
const KNOWN_BAD: &[&str] = &[
    "tests/queries/0_stateless/data_avro/corrupted.avro",
    "tests/queries/0_stateless/data_avro/enum_index_out_of_range.avro",
    "tests/queries/0_stateless/data_avro/nested_complex_incorrect_data.avro",
    "tests/queries/0_stateless/data_minio/corrupted_avro_files_test/metadata/83521e92-07a7-450d-ba44-3a179b730c85-m0.avro",
];

fn manifest() -> Option<Vec<(String, String, bool, u64)>> {
    let dir = fixtures_out_dir().join("ocf");
    let Ok(text) = std::fs::read_to_string(dir.join("manifest.tsv")) else {
        assert!(
            std::env::var_os("AVRO_REQUIRE_FIXTURES").is_none(),
            "fixtures are missing: run tests/gen_fixtures.py <repo root> {}",
            fixtures_out_dir().display()
        );
        eprintln!(
            "SKIPPED: fixtures are missing; run tests/gen_fixtures.py <repo root> {}",
            fixtures_out_dir().display()
        );
        return None;
    };
    Some(
        text.lines()
            .map(|l| {
                let f: Vec<&str> = l.split('\t').collect();
                (
                    f[0].to_string(),
                    f[1].to_string(),
                    f[2] == "ok",
                    f[3].parse().unwrap(),
                )
            })
            .collect(),
    )
}

#[test]
fn every_repository_avro_file_agrees_with_fastavro() {
    let Some(rows) = manifest() else { return };
    assert!(rows.len() >= 200, "found only {} files", rows.len());
    let out = fixtures_out_dir().join("ocf");
    let mut rejected: BTreeSet<String> = BTreeSet::new();
    let mut total_rows = 0u64;
    let mut compressed_decoded = 0;
    for (rel, codec, oracle_ok, oracle_rows) in &rows {
        let decompressed = out.join(rel);
        let result = if decompressed.exists() {
            let bytes = std::fs::read(&decompressed).unwrap();
            if codec != "null" {
                compressed_decoded += 1;
            }
            read_ocf_skip_all(&bytes)
        } else {
            // No decompressed copy: the oracle could not read the file at all. Run the
            // framing checks on the original; they are what rejects it here.
            let bytes = std::fs::read(repo_root().join(rel)).unwrap();
            read_ocf(&bytes, OcfMode::FramingOnly).map(|(n, _)| n)
        };
        if rel.ends_with("deeply_nested_schema.avro") {
            // Tests 04093: error or success, never a crash. Here the depth limit of 256 applies.
            match result {
                Ok(n) => assert_eq!(n, *oracle_rows),
                Err(e) => assert!(
                    e.code == ErrorCode::JsonDepth || e.code == ErrorCode::SchemaDepth,
                    "{rel}: unexpected error {e:?}"
                ),
            }
            continue;
        }
        match (&result, oracle_ok) {
            (Ok(n), true) => {
                assert_eq!(n, oracle_rows, "{rel}: row count differs from fastavro");
                total_rows += n;
            }
            (Err(e), false) => {
                rejected.insert(rel.clone());
                eprintln!("rejected as expected: {rel}: {e}");
            }
            (Ok(n), false) => panic!("{rel}: accepted ({n} rows) but fastavro rejects it"),
            (Err(e), true) => {
                panic!("{rel}: rejected ({e:?}) but fastavro reads {oracle_rows} rows")
            }
        }
    }
    let expected: BTreeSet<String> = KNOWN_BAD.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        rejected, expected,
        "the set of rejected files is exactly the known-bad set"
    );
    assert!(
        compressed_decoded > 150,
        "decompressed fixtures were used: {compressed_decoded}"
    );
    eprintln!(
        "{} files, {total_rows} rows decoded, {} rejected",
        rows.len(),
        rejected.len()
    );
}

#[test]
fn known_bad_files_fail_for_the_right_reason() {
    let Some(_) = manifest() else { return };
    let out = fixtures_out_dir().join("ocf");
    let root = repo_root();
    let run = |rel: &str| {
        let d = out.join(rel);
        if d.exists() {
            read_ocf_skip_all(&std::fs::read(d).unwrap())
        } else {
            read_ocf(
                &std::fs::read(root.join(rel)).unwrap(),
                OcfMode::FramingOnly,
            )
            .map(|(n, _)| n)
        }
    };
    let e = run(KNOWN_BAD[1]).unwrap_err();
    assert_eq!(e.code, ErrorCode::EnumIndexOutOfRange, "{e:?}");
    let e = run(KNOWN_BAD[2]).unwrap_err();
    assert_eq!(e.code, ErrorCode::UnionIndexOutOfRange, "{e:?}");
    let e = run(KNOWN_BAD[3]).unwrap_err();
    assert_eq!(
        (e.code, e.message.as_str()),
        (
            ErrorCode::BadMagic,
            "Invalid data file. Magic does not match: "
        )
    );
    // corrupted.avro is a snappy file whose second block header lies about its size.
    let e = run(KNOWN_BAD[0]).unwrap_err();
    assert!(
        matches!(
            e.code,
            ErrorCode::Eof | ErrorCode::SyncMismatch | ErrorCode::ObjectCountExceedsPayload
        ),
        "{e:?}"
    );
}

/// Hand-checked values for fixtures whose expected content is in the repository
/// (`*.json`, written by the Avro tools that generated the `.avro`).
#[test]
fn primitive_avro_values() {
    let out = fixtures_out_dir().join("ocf");
    let Ok(bytes) = std::fs::read(out.join("tests/queries/0_stateless/data_avro/primitive.avro"))
    else {
        eprintln!("SKIPPED: fixtures are missing");
        return;
    };
    let (rows, dump) = read_ocf_dump(&bytes).unwrap();
    assert_eq!(rows, 2);
    // primitive.json: {"a_bool":true,"b_int":1,"c_long":2,"d_float":3.4,"e_double":5.6,"f_bytes":"b1","g_string":"s1","h_null":null}
    //                 {"a_bool":false,"b_int":-1,"c_long":9223372036854775807,"d_float":3.00004,"e_double":0.00001,"f_bytes":"","g_string":"","h_null":null}
    let expected = format!(
        "0:1 0\n1:1 -1\n2:2 9223372036854775807\n3:{:08x} {:08x}\n4:{:016x} {:016x}\n5:{} -\n6:{} -\n",
        3.4f32.to_bits(),
        3.00004f32.to_bits(),
        5.6f64.to_bits(),
        0.00001f64.to_bits(),
        hex(b"b1"),
        hex(b"s1"),
    );
    assert_eq!(dump, expected);
}

/// Every real schema in the repository (Iceberg manifests, Spark and Java generated files, the
/// stateless tests' files) must produce the Parsing Canonical Form and fingerprint that fastavro
/// computes. Schemas that only differ in how they spell a type must not differ here, and
/// the reverse.
#[test]
fn canonical_forms_of_real_schemas_agree_with_fastavro() {
    use _ch_rust_avro::canonical::fingerprint64;
    use _ch_rust_avro::ocf::{self, Parsed};
    use _ch_rust_avro::schema::Schema;
    let Some(rows) = manifest() else { return };
    let out = fixtures_out_dir().join("ocf");
    let mut checked = 0;
    let mut distinct = BTreeSet::new();
    for (rel, _codec, ok, _n) in &rows {
        if !ok || rel.ends_with("deeply_nested_schema.avro") {
            continue;
        }
        let bytes = std::fs::read(out.join(rel)).unwrap();
        let canon_file = std::fs::read_to_string(out.join(format!("{rel}.canon"))).unwrap();
        let (want, want_fp) = canon_file.split_once('\n').unwrap();
        let Parsed::Done { value: header, .. } =
            ocf::parse_header(&bytes, true, &Default::default()).unwrap()
        else {
            panic!()
        };
        let s = Schema::parse(header.schema_json(), 256).unwrap_or_else(|e| panic!("{rel}: {e}"));
        assert_eq!(s.canonical_form(), want, "{rel}");
        assert_eq!(
            hex(&fingerprint64(want.as_bytes()).to_le_bytes()),
            want_fp.trim(),
            "{rel}"
        );
        distinct.insert(want.to_string());
        checked += 1;
    }
    eprintln!(
        "{checked} schemas, {} distinct canonical forms",
        distinct.len()
    );
    assert!(checked >= 200 && distinct.len() >= 20);
}

/// Iceberg stores `field-id`, `element-id`, `key-id` and `value-id` as extra attributes. Every one
/// of them that appears in a real manifest's schema text must be readable through the attribute API,
/// on the field or node it was written on.
#[test]
fn iceberg_ids_are_readable_as_attributes() {
    use _ch_rust_avro::ocf::{self, Parsed};
    use _ch_rust_avro::schema::{AttrValue, Kind, Schema};
    let Some(rows) = manifest() else { return };
    let out = fixtures_out_dir().join("ocf");
    let (mut files_with_ids, mut total_ids) = (0, 0);
    for (rel, _codec, ok, _n) in &rows {
        if !ok || rel.ends_with("deeply_nested_schema.avro") {
            continue;
        }
        let bytes = std::fs::read(out.join(rel)).unwrap();
        let Parsed::Done { value: header, .. } =
            ocf::parse_header(&bytes, true, &Default::default()).unwrap()
        else {
            panic!()
        };
        let text = String::from_utf8_lossy(header.schema_json()).into_owned();
        let s = Schema::parse(header.schema_json(), 256).unwrap();
        for key in ["field-id", "element-id", "key-id", "value-id"] {
            // `"field-id"` followed by a colon and an integer.
            let in_text = text.matches(&format!("\"{key}\":")).count();
            let mut found = 0;
            for node in 0..s.node_count() as u32 {
                if s.kind(node) == Kind::Record {
                    for i in 0..s.fields(node).len() {
                        if let Some(AttrValue::Int(_)) = s.field_attr(node, i, key) {
                            found += 1;
                        }
                    }
                }
                if let Some(AttrValue::Int(_)) = s.node_attr(node, key) {
                    found += 1;
                }
            }
            assert_eq!(found, in_text, "{rel}: {key}");
            total_ids += found;
        }
        files_with_ids += usize::from(text.contains("field-id"));
    }
    eprintln!("{files_with_ids} files with field ids, {total_ids} ids read back");
    assert!(files_with_ids > 50 && total_ids > 500);
}
