//! Plan validation and the sink kinds that the universal plan of the other tests
//! does not use: decimals, range checks, null maps, tags of several widths,
//! terminated strings, projection (skipping parts of a record).

mod common;

use _ch_rust_avro::exec::{decode, first_out_of_range_i32, Sink, State, Status};
use _ch_rust_avro::outbuf::OutBuf;
use _ch_rust_avro::program::{
    Action, PlanNode, Program, SinkDesc, SinkKind, NO_SINK, SINK_TERMINATED,
};
use _ch_rust_avro::ErrorCode;
use common::*;

fn compile(s: &str, sinks: &[SinkDesc], plan: &[PlanNode]) -> _ch_rust_avro::Result<Program> {
    Program::compile(&schema(s), sinks, plan)
}

fn decode_all(p: &Program, input: &[u8], rows: u64) -> _ch_rust_avro::Result<Buffers> {
    let mut bufs = Buffers::new(p.sinks(), 1);
    let (st, used) = drive(p, input, rows, 1000, 1, 1, &mut bufs)?;
    assert_eq!((st.rows, used), (rows, input.len()));
    Ok(bufs)
}

#[test]
fn decimals_sign_extend_big_endian_twos_complement() {
    // Avro: the unscaled value as big-endian two's complement. Expected integers are computed by hand.
    let cases: [(&[u8], i128); 10] = [
        (&[], 0),
        (&[0x00], 0),
        (&[0x05], 5),
        (&[0xff], -1),
        (&[0x00, 0xff], 255),
        (&[0xff, 0x00], -256),
        (&[0x80], -128),
        (&[0x00, 0x00, 0x00, 0x00, 0x05], 5), // redundant sign extension beyond the width is fine
        (&[0xff, 0xff, 0xff, 0xff, 0xfe], -2),
        (&[0x7f, 0xff, 0xff, 0xff], 0x7fff_ffff),
    ];
    let s = r#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"#;
    for width in [4u32, 8, 16, 32] {
        let sinks = [SinkDesc::new(SinkKind::Decimal, width)];
        let root = schema(s).root();
        let plan = [PlanNode::value(root, 0)];
        let p = compile(s, &sinks, &plan).unwrap();
        for (bytes, expected) in cases {
            let bufs = decode_all(&p, &avro_bytes(bytes), 1).unwrap();
            let got = bufs.dump();
            // Build the expected bytes: two's complement of `expected` in `width` bytes, host order.
            let le = expected.to_le_bytes();
            let mut want: Vec<u8> = if expected < 0 {
                vec![0xff; width as usize]
            } else {
                vec![0; width as usize]
            };
            want[..16.min(width as usize)].copy_from_slice(&le[..16.min(width as usize)]);
            if cfg!(target_endian = "big") {
                want.reverse();
            }
            assert_eq!(
                got,
                format!("0:{}\n", hex(&want)),
                "{bytes:02x?} in {width} bytes"
            );
        }
    }
    // Too wide: 5 significant bytes in 4.
    let sinks = [SinkDesc::new(SinkKind::Decimal, 4)];
    let p = compile(s, &sinks, &[PlanNode::value(schema(s).root(), 0)]).unwrap();
    for bad in [
        &[0x01u8, 0x00, 0x00, 0x00, 0x00][..],
        &[0x00, 0x80, 0x00, 0x00, 0x00],
        &[0xff, 0x7f, 0xff, 0xff, 0xff],
        &[0x00, 0xff, 0xff, 0xff, 0xff, 0xff],
    ] {
        let e = decode_all(&p, &avro_bytes(bad), 1)
            .err()
            .unwrap_or_else(|| panic!("{bad:02x?} must not fit"));
        assert_eq!(e.code, ErrorCode::DecimalTooWide, "{bad:02x?}");
    }
    // Fixed-backed decimal.
    let s =
        r#"{"type":"fixed","name":"D","size":3,"logicalType":"decimal","precision":7,"scale":0}"#;
    let sinks = [SinkDesc::new(SinkKind::Decimal, 8)];
    let p = compile(s, &sinks, &[PlanNode::value(schema(s).root(), 0)]).unwrap();
    let bufs = decode_all(&p, &[0xff, 0xff, 0xfe, 0x00, 0x01, 0x00], 2).unwrap();
    let want = |v: i64| hex(&v.to_ne_bytes());
    assert_eq!(bufs.dump(), format!("0:{} {}\n", want(-2), want(256)));
    // A fixed wider than the decimal is rejected when the program is compiled.
    let s = r#"{"type":"fixed","name":"D","size":9,"logicalType":"decimal","precision":7}"#;
    let e = compile(
        s,
        &[SinkDesc::new(SinkKind::Decimal, 8)],
        &[PlanNode::value(schema(s).root(), 0)],
    )
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::DecimalTooWide);
}

#[test]
fn range_checks_apply_to_int32_and_int64_sinks() {
    let root = schema("\"int\"").root();
    let mut e = PlanNode::value(root, 0);
    e.check = true;
    e.check_min = 0;
    e.check_max = 100;
    for kind in [SinkKind::Int32, SinkKind::Int64] {
        let p = compile("\"int\"", &[SinkDesc::new(kind, 0)], &[e]).unwrap();
        assert_eq!(
            decode_all(&p, &[0x00, 0xc8, 0x01], 2).unwrap().dump(),
            "0:0 100\n"
        );
        let err = decode_all(&p, &[0xca, 0x01], 1).err().unwrap();
        assert_eq!(
            (err.code, err.message.as_str()),
            (
                ErrorCode::ValueOutOfRange,
                "Value 101 is out of range [0, 100]"
            )
        );
        let err = decode_all(&p, &[0x01], 1).err().unwrap();
        assert_eq!(err.code, ErrorCode::ValueOutOfRange);
    }
    // long into Int64 with a check; checks on other kinds, or min > max, are plan errors.
    let s = "\"long\"";
    let root = schema(s).root();
    let mut e = PlanNode::value(root, 0);
    e.check = true;
    e.check_min = -5;
    e.check_max = 5;
    let p = compile(s, &[SinkDesc::new(SinkKind::Int64, 0)], &[e]).unwrap();
    assert_eq!(decode_all(&p, &[0x09, 0x0a], 2).unwrap().dump(), "0:-5 5\n");
    assert!(decode_all(&p, &[0x0c], 1).is_err());
    e.check_min = 6;
    assert_eq!(
        compile(s, &[SinkDesc::new(SinkKind::Int64, 0)], &[e])
            .unwrap_err()
            .code,
        ErrorCode::PlanInvalid
    );
    let s = "\"float\"";
    let mut e = PlanNode::value(schema(s).root(), 0);
    e.check = true;
    assert_eq!(
        compile(s, &[SinkDesc::new(SinkKind::Float32, 0)], &[e])
            .unwrap_err()
            .code,
        ErrorCode::PlanInvalid
    );
    // The post-pass helper for date and datetime columns.
    let data: Vec<u8> = [5i32, 10, -1, 7]
        .iter()
        .flat_map(|v| v.to_ne_bytes())
        .collect();
    assert_eq!(first_out_of_range_i32(&data, 0, 10), Some(2));
    assert_eq!(first_out_of_range_i32(&data, -1, 10), None);
    assert_eq!(first_out_of_range_i32(&data, 6, 10), Some(0));
    assert_eq!(first_out_of_range_i32(&[], 0, 0), None);
}

#[test]
fn widening_conversions() {
    // int into Int64, float into Float64.
    let s = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"float"}]}"#;
    let sch = schema(s);
    let plan = [
        PlanNode::new(Action::Record, sch.root(), 2),
        PlanNode::value(sch.child(sch.root(), 0), 0),
        PlanNode::value(sch.child(sch.root(), 1), 1),
    ];
    let p = compile(
        s,
        &[
            SinkDesc::new(SinkKind::Int64, 0),
            SinkDesc::new(SinkKind::Float64, 0),
        ],
        &plan,
    )
    .unwrap();
    let mut input = zz(-70000);
    input.extend_from_slice(&1.5f32.to_le_bytes());
    let bufs = decode_all(&p, &input, 1).unwrap();
    assert_eq!(
        bufs.dump(),
        format!("0:-70000\n1:{:016x}\n", 1.5f64.to_bits())
    );
}

#[test]
fn terminated_strings_append_a_zero_byte_and_count_it_in_the_offsets() {
    let s = "\"string\"";
    let mut d = SinkDesc::new(SinkKind::Bytes, 0);
    d.flags = SINK_TERMINATED;
    let p = compile(s, &[d], &[PlanNode::value(schema(s).root(), 0)]).unwrap();
    let mut chars = [0xaa; 16];
    let mut offs = [0u8; 8 * 3];
    let mut v = [Sink {
        data: (&mut chars[..]).into(),
        data2: (&mut offs[..]).into(),
        len: 0,
        len2: 0,
        offset: 100,
    }];
    let mut st = State::new(0);
    let input = [0x04, b'a', b'b', 0x00, 0x02, b'z'];
    let pr = decode(&p, &mut st, &input, 3, &mut v).unwrap();
    assert_eq!((pr.rows_done, pr.consumed), (3, 6));
    assert_eq!((v[0].len, v[0].len2, v[0].offset), (3 + 1 + 2, 3, 106));
    assert_eq!(&chars[..7], &[b'a', b'b', 0, 0, b'z', 0, 0xaa]);
    let offsets: Vec<u64> = offs
        .chunks(8)
        .map(|c| u64::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(
        offsets,
        vec![103, 104, 106],
        "offsets continue from `offset` and count the terminators"
    );
}

#[test]
fn null_map_and_union_tags() {
    // [null, long]: null map plus the non-null values only.
    let s = r#"["null","long"]"#;
    let sch = schema(s);
    let mut u = PlanNode::new(Action::Union, sch.root(), 2);
    u.sink = 0;
    u.arg = 0;
    let plan = [
        u,
        PlanNode::skip(sch.child(sch.root(), 0)),
        PlanNode::value(sch.child(sch.root(), 1), 1),
    ];
    let sinks = [
        SinkDesc::new(SinkKind::NullMap, 0),
        SinkDesc::new(SinkKind::Int64, 0),
    ];
    let p = compile(s, &sinks, &plan).unwrap();
    // null, 3, null, -1
    let bufs = decode_all(&p, &[0x00, 0x02, 0x06, 0x00, 0x02, 0x01], 4).unwrap();
    assert_eq!(bufs.dump(), "0:1 0 1 0\n1:3 -1\n");
    // [long, null]: the null branch is the second one.
    let s = r#"["long","null"]"#;
    let sch = schema(s);
    let mut u = PlanNode::new(Action::Union, sch.root(), 2);
    u.sink = 0;
    u.arg = 1;
    let plan = [
        u,
        PlanNode::value(sch.child(sch.root(), 0), 1),
        PlanNode::skip(sch.child(sch.root(), 1)),
    ];
    let p = compile(s, &sinks, &plan).unwrap();
    // branch 0 with the long 2, then the null branch
    assert_eq!(
        decode_all(&p, &[0x00, 0x04, 0x02], 2).unwrap().dump(),
        "0:0 1\n1:2\n"
    );
    // The null branch argument must point at a null branch.
    let mut bad = u;
    bad.arg = 0;
    assert_eq!(
        compile(s, &sinks, &[bad, plan[1], plan[2]])
            .unwrap_err()
            .code,
        ErrorCode::PlanInvalid
    );
    // Union tags of 1, 2 and 4 bytes.
    for width in [1u32, 2, 4] {
        let s = r#"["null","int","string"]"#;
        let sch = schema(s);
        let mut u = PlanNode::new(Action::Union, sch.root(), 3);
        u.sink = 0;
        let plan = [
            u,
            PlanNode::skip(sch.child(sch.root(), 0)),
            PlanNode::skip(sch.child(sch.root(), 1)),
            PlanNode::skip(sch.child(sch.root(), 2)),
        ];
        let p = compile(s, &[SinkDesc::new(SinkKind::UnionTag, width)], &plan).unwrap();
        // Rows: branch 2 (the string "x"), branch 1 (the int 4), branch 0 (null).
        let bufs = decode_all(&p, &[0x04, 0x02, b'x', 0x02, 0x08, 0x00], 3).unwrap();
        assert_eq!(bufs.dump(), "0:2 1 0\n", "tag width {width}");
    }
    // Too many branches for the tag width.
    let branches: Vec<String> = (0..300)
        .map(|i| format!(r#"{{"type":"fixed","name":"F{i}","size":1}}"#))
        .collect();
    let s = format!("[{}]", branches.join(","));
    let sch = schema(&s);
    let mut u = PlanNode::new(Action::Union, sch.root(), 300);
    u.sink = 0;
    let mut plan = vec![u];
    plan.extend((0..300).map(|i| PlanNode::skip(sch.child(sch.root(), i))));
    assert_eq!(
        Program::compile(&sch, &[SinkDesc::new(SinkKind::UnionTag, 1)], &plan)
            .unwrap_err()
            .code,
        ErrorCode::PlanInvalid
    );
    assert!(Program::compile(&sch, &[SinkDesc::new(SinkKind::UnionTag, 2)], &plan).is_ok());
}

#[test]
fn enum_indexes_in_every_width() {
    let s = r#"{"type":"enum","name":"E","symbols":["A","B","C"]}"#;
    let root = schema(s).root();
    for width in [1u32, 2, 4] {
        let p = compile(
            s,
            &[SinkDesc::new(SinkKind::EnumIndex, width)],
            &[PlanNode::value(root, 0)],
        )
        .unwrap();
        let bufs = decode_all(&p, &[0x04, 0x00, 0x02], 3).unwrap();
        assert_eq!(bufs.dump(), "0:2 0 1\n", "enum index width {width}");
    }
    // 257 symbols do not fit in one byte.
    let symbols: Vec<String> = (0..257).map(|i| format!("\"S{i}\"")).collect();
    let s = format!(
        r#"{{"type":"enum","name":"E","symbols":[{}]}}"#,
        symbols.join(",")
    );
    let root = schema(&s).root();
    assert_eq!(
        Program::compile(
            &schema(&s),
            &[SinkDesc::new(SinkKind::EnumIndex, 1)],
            &[PlanNode::value(root, 0)]
        )
        .unwrap_err()
        .code,
        ErrorCode::PlanInvalid
    );
    assert!(Program::compile(
        &schema(&s),
        &[SinkDesc::new(SinkKind::EnumIndex, 2)],
        &[PlanNode::value(root, 0)]
    )
    .is_ok());
}

#[test]
fn projection_skips_parts_of_a_record_and_flattens_arrays() {
    let s = r#"{"type":"record","name":"R","fields":[
        {"name":"skipme","type":{"type":"map","values":{"type":"array","items":"string"}}},
        {"name":"id","type":"long"},
        {"name":"tags","type":{"type":"array","items":"string"}},
        {"name":"m","type":{"type":"map","values":"long"}},
        {"name":"last","type":"boolean"}]}"#;
    let sch = schema(s);
    let root = sch.root();
    let f = |i| sch.child(root, i);
    let tags_items = sch.child(f(2), 0);
    let m_values = sch.child(f(3), 0);
    // sinks: 0 id, 1 tags (no offsets: flattened), 2 map values; map offsets and keys are skipped.
    let mut m = PlanNode::new(Action::Map, f(3), 1);
    m.sink = NO_SINK;
    let a = PlanNode::new(Action::Array, f(2), 1);
    let plan = [
        PlanNode::new(Action::Record, root, 5),
        PlanNode::skip(f(0)),
        PlanNode::value(f(1), 0),
        a,
        PlanNode::value(tags_items, 1),
        m,
        PlanNode::value(m_values, 2),
        PlanNode::skip(f(4)),
    ];
    let sinks = [
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::Bytes, 0),
        SinkDesc::new(SinkKind::Int64, 0),
    ];
    let p = compile(s, &sinks, &plan).unwrap();
    let mut row = Vec::new();
    row.extend(zz(1)); // skipped map: one entry "k" -> ["x","y"]
    row.extend(avro_bytes(b"k"));
    row.extend(zz(2));
    row.extend(avro_bytes(b"x"));
    row.extend(avro_bytes(b"y"));
    row.extend(zz(0));
    row.extend(zz(0));
    row.extend(zz(42)); // id
    row.extend(zz(2)); // tags a, bc
    row.extend(avro_bytes(b"a"));
    row.extend(avro_bytes(b"bc"));
    row.extend(zz(0));
    row.extend(zz(2)); // m: {"p": 7, "q": 8}
    row.extend(avro_bytes(b"p"));
    row.extend(zz(7));
    row.extend(avro_bytes(b"q"));
    row.extend(zz(8));
    row.extend(zz(0));
    row.push(1); // last
    let two = [row.clone(), row.clone()].concat();
    let bufs = decode_all(&p, &two, 2).unwrap();
    assert_eq!(
        bufs.dump(),
        format!(
            "0:42 42\n1:{} {} {} {}\n2:7 8 7 8\n",
            hex(b"a"),
            hex(b"bc"),
            hex(b"a"),
            hex(b"bc")
        )
    );
    // A truncated skipped part is still an error.
    assert!(decode_all(&p, &row[..5], 1).is_err());
}

#[test]
fn shared_records_are_subroutines_and_skip_correctly() {
    // `Inner` is used twice, so skipping it goes through a Call.
    let s = r#"{"type":"record","name":"R","fields":[
        {"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"long"},{"name":"y","type":["null","Inner"]}]}},
        {"name":"b","type":"Inner"},
        {"name":"c","type":"long"}]}"#;
    let sch = schema(s);
    assert!(sch.in_degree(sch.child(sch.root(), 0)) > 1);
    let root = sch.root();
    let plan = [
        PlanNode::new(Action::Record, root, 3),
        PlanNode::skip(sch.child(root, 0)),
        PlanNode::skip(sch.child(root, 1)),
        PlanNode::value(sch.child(root, 2), 0),
    ];
    let p = compile(s, &[SinkDesc::new(SinkKind::Int64, 0)], &plan).unwrap();
    // a = {x:1, y: {x:2, y:null}}, b = {x:3, y:null}, c = 99
    let mut input = Vec::new();
    input.extend([2, 2, 4, 0]); // a
    input.extend([6, 0]); // b
    input.extend(zz(99));
    assert_eq!(decode_all(&p, &input, 1).unwrap().dump(), "0:99\n");
    // Many rows, tiny output buffer: calls and returns survive NeedSpace.
    let rows = 50;
    let many: Vec<u8> = (0..rows).flat_map(|_| input.clone()).collect();
    assert_eq!(
        decode_all(&p, &many, rows as u64)
            .unwrap()
            .dump()
            .split(' ')
            .count(),
        rows
    );
}

#[test]
fn plan_validation() {
    let s = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}"#;
    let sch = schema(s);
    let root = sch.root();
    let (a, b) = (sch.child(root, 0), sch.child(root, 1));
    let sinks = [
        SinkDesc::new(SinkKind::Int64, 0),
        SinkDesc::new(SinkKind::Bytes, 0),
    ];
    let good = [
        PlanNode::new(Action::Record, root, 2),
        PlanNode::value(a, 0),
        PlanNode::value(b, 1),
    ];
    assert!(Program::compile(&sch, &sinks, &good).is_ok());
    let expect = |plan: &[PlanNode], sinks: &[SinkDesc]| {
        let e = Program::compile(&sch, sinks, plan).unwrap_err();
        assert_eq!(e.code, ErrorCode::PlanInvalid, "{e}");
        assert!(e.message.starts_with("Invalid decode plan: "));
    };
    expect(&good[..2], &sinks); // shorter than the schema walk
    expect(&[good[0], good[1], good[2], PlanNode::skip(a)], &sinks); // longer
    expect(&[good[0], good[2], good[1]], &sinks); // wrong order: entries name the wrong schema nodes
    expect(
        &[PlanNode::new(Action::Record, root, 3), good[1], good[2]],
        &sinks,
    ); // wrong child count
    expect(&[good[0], PlanNode::value(a, 1), good[2]], &sinks); // Int64 value into a Bytes sink
    expect(
        &[good[0], PlanNode::value(a, 0), PlanNode::value(b, 0)],
        &sinks,
    ); // string into an Int64 sink
    expect(
        &[good[0], PlanNode::value(a, 0), PlanNode::value(b, 7)],
        &sinks,
    ); // no such sink
    expect(
        &[good[0], PlanNode::value(a, 0), PlanNode::value(b, NO_SINK)],
        &sinks,
    ); // missing sink
    expect(&[PlanNode::new(Action::Array, root, 1), good[1]], &sinks); // array entry for a record
    expect(&[PlanNode::value(root, 0)], &sinks); // a record is not a value
    let mut sk = PlanNode::skip(root);
    sk.n_children = 1;
    expect(&[sk], &sinks);
    let mut sk = PlanNode::skip(root);
    sk.sink = 0;
    expect(&[sk], &sinks);
    // Reusing a sink for two values.
    let two_longs = schema(
        r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}"#,
    );
    let r = two_longs.root();
    let plan = [
        PlanNode::new(Action::Record, r, 2),
        PlanNode::value(two_longs.child(r, 0), 0),
        PlanNode::value(two_longs.child(r, 1), 0),
    ];
    assert_eq!(
        Program::compile(&two_longs, &sinks[..1], &plan)
            .unwrap_err()
            .code,
        ErrorCode::PlanInvalid
    );
    // Bad sink descriptions.
    let long = schema("\"long\"");
    for d in [
        SinkDesc::new(SinkKind::Decimal, 5),
        SinkDesc::new(SinkKind::EnumIndex, 3),
        SinkDesc::new(SinkKind::FixedBytes, 0),
        SinkDesc {
            kind: SinkKind::Int64,
            arg: 0,
            flags: 1,
        },
    ] {
        assert_eq!(
            Program::compile(&long, &[d], &[PlanNode::skip(long.root())])
                .unwrap_err()
                .code,
            ErrorCode::PlanInvalid,
            "{d:?}"
        );
    }
    // Fixed sink size must equal the fixed size.
    let fx = schema(r#"{"type":"fixed","name":"F","size":4}"#);
    assert_eq!(
        Program::compile(
            &fx,
            &[SinkDesc::new(SinkKind::FixedBytes, 3)],
            &[PlanNode::value(fx.root(), 0)]
        )
        .unwrap_err()
        .code,
        ErrorCode::PlanInvalid
    );
    assert!(Program::compile(
        &fx,
        &[SinkDesc::new(SinkKind::FixedBytes, 4)],
        &[PlanNode::value(fx.root(), 0)]
    )
    .is_ok());
    // Offsets sinks only where they belong.
    let arr = schema(r#"{"type":"array","items":"long"}"#);
    let mut a = PlanNode::new(Action::Array, arr.root(), 1);
    a.sink = 0;
    assert_eq!(
        Program::compile(
            &arr,
            &[
                SinkDesc::new(SinkKind::MapOffsets, 0),
                SinkDesc::new(SinkKind::Int64, 0)
            ],
            &[a, PlanNode::skip(arr.child(arr.root(), 0))]
        )
        .unwrap_err()
        .code,
        ErrorCode::PlanInvalid
    );
}

#[test]
fn wrong_sink_arguments_at_decode_time_are_rejected() {
    let long = schema("\"long\"");
    let p = Program::compile(
        &long,
        &[SinkDesc::new(SinkKind::Int64, 0)],
        &[PlanNode::value(long.root(), 0)],
    )
    .unwrap();
    let mut st = State::new(0);
    // No sinks at all.
    assert_eq!(
        decode(&p, &mut st, &[0], 1, &mut []).unwrap_err().code,
        ErrorCode::InvalidArgument
    );
    // A fill level beyond the buffer.
    let mut d = [0u8; 8];
    let mut v = [Sink {
        data: (&mut d[..]).into(),
        data2: OutBuf::empty(),
        len: 2,
        len2: 0,
        offset: 0,
    }];
    assert_eq!(
        decode(&p, &mut st, &[0], 1, &mut v).unwrap_err().code,
        ErrorCode::InvalidArgument
    );
    // Unaligned, odd-sized buffers work: the executor never needs alignment.
    let mut raw = [0u8; 17];
    let mut v = [Sink::new(&mut raw[1..17])];
    assert_eq!(
        decode(&p, &mut st, &[0x02, 0x04, 0x06], 3, &mut v)
            .unwrap()
            .status,
        Status::NeedSpace {
            sink: 0,
            min_cap: 3,
            min_cap2: 0
        }
    );
}

#[test]
fn truncated_fixed_is_eof_before_it_asks_for_space() {
    // A fixed of 64 MiB with 4 bytes present: the executor must not make the caller allocate the
    // 64 MiB for a value that is not in the input.
    let fx = schema(r#"{"type":"fixed","name":"F","size":67108864}"#);
    let p = Program::compile(
        &fx,
        &[SinkDesc::new(SinkKind::FixedBytes, 67108864)],
        &[PlanNode::value(fx.root(), 0)],
    )
    .unwrap();
    let mut st = State::new(0);
    let mut v = [Sink::new(&mut [])];
    assert_eq!(
        decode(&p, &mut st, b"abcd", 1, &mut v).unwrap_err().code,
        ErrorCode::Eof
    );
}

#[test]
fn decimals_fixture_matches_fastavro() {
    let dir = fixtures_out_dir();
    let (Ok(bytes), Ok(expected)) = (
        std::fs::read(dir.join("ocf/tests/queries/0_stateless/data_avro/decimals.avro")),
        std::fs::read_to_string(dir.join("values/decimals.txt")),
    ) else {
        eprintln!("SKIPPED: fixtures are missing");
        return;
    };
    // The file is bytes-backed decimals: decode into 16 byte sinks and compare with the integers fastavro computed.
    use _ch_rust_avro::ocf::{self, BlockHeader, Parsed};
    let Parsed::Done { value: h, consumed } =
        ocf::parse_header(&bytes, true, &Default::default()).unwrap()
    else {
        panic!()
    };
    let sch = _ch_rust_avro::schema::Schema::parse(h.schema_json(), 256).unwrap();
    let field = sch.child(sch.root(), 0);
    assert_eq!(sch.node(field).precision, 14);
    assert_eq!(sch.node(field).scale, 4);
    let plan = [
        PlanNode::new(Action::Record, sch.root(), 1),
        PlanNode::value(field, 0),
    ];
    let p = Program::compile(&sch, &[SinkDesc::new(SinkKind::Decimal, 16)], &plan).unwrap();
    let Parsed::Done {
        value: BlockHeader::Block {
            object_count,
            byte_count,
        },
        consumed: c2,
    } = ocf::parse_block_header(
        &bytes[consumed..],
        true,
        &ocf::BlockCtx {
            codec: h.codec,
            min_record_size: 1,
            max_block_bytes: 0,
        },
    )
    .unwrap()
    else {
        panic!()
    };
    let payload = &bytes[consumed + c2..consumed + c2 + byte_count as usize];
    let mut bufs = Buffers::new(p.sinks(), 8);
    let (st, _) = drive(&p, payload, object_count, 7, 8, 8, &mut bufs).unwrap();
    assert_eq!(st.rows, 24);
    let got: Vec<i128> = bufs
        .dump()
        .trim_end()
        .split_once(':')
        .unwrap()
        .1
        .split(' ')
        .map(|h| {
            let b: Vec<u8> = (0..16)
                .map(|i| u8::from_str_radix(&h[2 * i..2 * i + 2], 16).unwrap())
                .collect();
            i128::from_ne_bytes(b.try_into().unwrap())
        })
        .collect();
    let want: Vec<i128> = expected.lines().map(|l| l.parse().unwrap()).collect();
    assert_eq!(got, want);
}
