//! The decoder must not depend on the native stack for attacker-controlled depth. These tests run the shapes of the
//! stateless test `05345_avro_skip_deeply_nested_field` (which runs under `ulimit -s 1024`, so ClickHouse threads get a
//! 1 MiB stack) on a 192 KiB thread. `cargo test` builds the crate in the dev profile, the profile of the Debug
//! ClickHouse build (opt-level 0 with overflow checks): its frames are several times the size of a release frame, so
//! any recursion on the nesting depth would overflow here long before the 50 000 levels of the inputs.

#![allow(dead_code)]

mod common;

use _ch_rust_avro::exec::{decode, State};
use _ch_rust_avro::program::Program;
use _ch_rust_avro::schema::Schema;
use _ch_rust_avro::ErrorCode;
use common::*;

const STACK: usize = 192 * 1024;

/// The depth that ClickHouse passes to `avro_schema_parse`.
const CLICKHOUSE_SCHEMA_DEPTH: usize = 256;
/// The depth that ClickHouse passes to `avro_state_new`: none, so the hard cap of the library applies.
const CLICKHOUSE_DATA_DEPTH: usize = 0;

fn on_small_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(STACK)
        .spawn(f)
        .unwrap()
        .join()
        .expect("the decoder overflowed a small stack or panicked")
}

/// `chain`: a chain of named records `A_k { x: A_(k-1) }`, each defined inside its own array field, and a trailing field
/// of type `A_n`. Only `id` is read, so the rest is skipped.
fn chain_schema(n: usize) -> String {
    let mut fields = String::from(
        r#"{"name":"id","type":"long"},{"name":"f1","type":{"type":"array","items":{"type":"record","name":"A1","fields":[{"name":"v","type":"long"}]}}}"#,
    );
    for k in 2..=n {
        fields.push_str(&format!(
            r#",{{"name":"f{k}","type":{{"type":"array","items":{{"type":"record","name":"A{k}","fields":[{{"name":"x","type":"A{}"}}]}}}}}}"#,
            k - 1
        ));
    }
    fields.push_str(&format!(r#",{{"name":"last","type":"A{n}"}}"#));
    format!(r#"{{"type":"record","name":"root","fields":[{fields}]}}"#)
}

fn skip_program(s: &Schema) -> Program {
    let u = skip_plan(s);
    Program::compile(s, &u.sinks, &u.plan).unwrap()
}

#[test]
fn a_chain_of_fifty_thousand_named_records_decodes_on_a_small_stack() {
    let n = 50_000;
    // `last` is a chain of `n` nested records: the data nests `n` levels although the schema JSON is shallow.
    let (limited, decoded) = on_small_stack(move || {
        let json = chain_schema(n);
        let s = Schema::parse(json.as_bytes(), CLICKHOUSE_SCHEMA_DEPTH).unwrap();
        // Same as the plan the stateless test builds: every field but `id` is skipped.
        let skip = skip_program(&s);
        let mut payload = zz(42);
        payload.extend(std::iter::repeat_n(0u8, n));
        payload.extend(zz(7));
        assert!(s.canonical_form().len() > n);
        // The limit that a caller sets is exact and leaves the state at a row boundary.
        let mut state = State::new(256);
        let limited = decode(&skip, &mut state, &payload, 1, &mut []).unwrap_err();
        assert!(state.at_row_boundary());
        // The depth that ClickHouse passes decodes the same 50 000 levels: they are on the heap.
        let mut state = State::new(CLICKHOUSE_DATA_DEPTH);
        let decoded = decode(&skip, &mut state, &payload, 1, &mut [])
            .unwrap()
            .rows_done;
        (limited, decoded)
    });
    // `TOO_DEEP_RECURSION` is what ClickHouse maps `DataTooDeep` to.
    assert_eq!(limited.code, ErrorCode::DataTooDeep);
    assert_eq!(decoded, 1);
}

fn list_schema() -> Schema {
    Schema::parse(
        br#"{"type":"record","name":"root","fields":[{"name":"id","type":"long"},{"name":"list","type":{"type":"record","name":"L","fields":[{"name":"next","type":["null","L"]}]}}]}"#,
        CLICKHOUSE_SCHEMA_DEPTH,
    )
    .unwrap()
}

/// `id` and a list of `links` records `L { next: [null, L] }`.
fn list_payload(links: usize) -> Vec<u8> {
    let mut payload = zz(42);
    for _ in 0..links {
        payload.extend(zz(1));
    }
    payload.extend(zz(0));
    payload
}

/// The schema is a few levels deep, but the data of a recursive record is as deep as it has links. A list of more than
/// 256 links (the depth of a schema) is read, as it is by the decoder over `contrib/avro`, which recurses on the native
/// stack; the stateless test `05576_avro_recursive_data_deeper_than_schema` runs the same shapes.
#[test]
fn a_recursive_list_of_more_than_the_schema_depth_decodes_on_a_small_stack() {
    let decoded = on_small_stack(|| {
        let skip = skip_program(&list_schema());
        [300usize, 1_000, 90_000].map(|links| {
            let mut state = State::new(CLICKHOUSE_DATA_DEPTH);
            let p = decode(&skip, &mut state, &list_payload(links), 1, &mut []).unwrap();
            assert!(state.at_row_boundary());
            (p.rows_done, p.consumed)
        })
    });
    assert_eq!(
        decoded,
        [300usize, 1_000, 90_000].map(|links| (1, list_payload(links).len()))
    );
}

#[test]
fn a_recursive_list_beyond_the_hard_cap_is_rejected_by_the_depth_limit() {
    let err = on_small_stack(|| {
        let skip = skip_program(&list_schema());
        let mut state = State::new(CLICKHOUSE_DATA_DEPTH);
        let e = decode(&skip, &mut state, &list_payload(200_000), 1, &mut []).unwrap_err();
        assert!(state.at_row_boundary());
        e
    });
    // `TOO_DEEP_RECURSION` is what ClickHouse maps `DataTooDeep` to.
    assert_eq!(err.code, ErrorCode::DataTooDeep);
}

#[test]
fn deeply_nested_json_and_schemas_are_rejected_without_native_recursion() {
    on_small_stack(|| {
        for n in [1_000usize, 100_000] {
            let json = "[".repeat(n) + &"]".repeat(n);
            let e = Schema::parse(json.as_bytes(), CLICKHOUSE_SCHEMA_DEPTH).unwrap_err();
            assert_eq!(e.code, ErrorCode::JsonDepth);
            let mut sch = String::from("\"int\"");
            for _ in 0..n.min(40_000) {
                sch = format!(r#"{{"type":"array","items":{sch}}}"#);
            }
            let e = Schema::parse(sch.as_bytes(), CLICKHOUSE_SCHEMA_DEPTH).unwrap_err();
            assert!(
                matches!(e.code, ErrorCode::JsonDepth | ErrorCode::SchemaDepth),
                "{e}"
            );
        }
        // With no configured limit the internal cap applies and a 40 000-level schema compiles iteratively.
        let mut sch = String::from("\"int\"");
        for _ in 0..40_000 {
            sch = format!(r#"{{"type":"array","items":{sch}}}"#);
        }
        let s = Schema::parse(sch.as_bytes(), 0).unwrap();
        assert_eq!(s.canonical_form().len(), sch.len());
        let skip = skip_program(&s);
        let mut data = Vec::new();
        for _ in 0..40_000 {
            data.extend(zz(1));
        }
        data.extend(zz(7));
        for _ in 0..40_000 {
            data.extend(zz(0));
        }
        let mut state = State::new(0);
        assert_eq!(
            decode(&skip, &mut state, &data, 1, &mut [])
                .unwrap()
                .rows_done,
            1
        );
    });
}
