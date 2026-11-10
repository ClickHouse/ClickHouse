//! Executable binary-datum specification: Apache Avro 1.12.0, Binary Encoding.
//! This recursive, allocating test oracle does not call production decoding,
//! schema inspection, minimum-size analysis, or program ops to interpret bytes.
//! Schema JSON generation and plan compilation are adapter code, not the oracle.
//! Logical annotations retain their underlying binary value, as the spec requires.
#![forbid(unsafe_code)]

use _ch_rust_avro::exec::{decode, decode_partial, Sink, State, Status};
use _ch_rust_avro::program::{Action, PlanNode, Program, SinkDesc, SinkKind, NO_SINK};
use _ch_rust_avro::schema::{NodeId, Schema};
use _ch_rust_avro::varint::{decode_int, decode_long};

#[derive(Clone, Debug)]
enum Ty {
    Null,
    Bool,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Fixed(usize),
    Enum(usize),
    Record(Vec<Ty>),
    Array(Box<Ty>),
    Map(Box<Ty>),
    Union(Vec<Ty>),
    NamedRecord(String, Vec<Ty>),
    Ref(String),
}

#[derive(Clone, Debug, PartialEq)]
enum Value {
    Null,
    Bool(u8),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    Bytes(Vec<u8>),
    Enum(usize),
    Record(Vec<Value>),
    Array(Vec<Value>),
    Map(Vec<(Vec<u8>, Value)>),
    Union(usize, Box<Value>),
}

#[derive(Clone, Debug, PartialEq)]
enum RefError {
    Eof,
    Varint,
    IntRange,
    Bool,
    Length,
    Index,
    BlockSize,
    Budget,
}

struct Reference<'a> {
    input: &'a [u8],
    pos: usize,
    fuel: usize,
    definitions: std::collections::BTreeMap<String, Ty>,
}
impl<'a> Reference<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            pos: 0,
            fuel: 100_000,
            definitions: std::collections::BTreeMap::new(),
        }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], RefError> {
        let end = self.pos.checked_add(n).ok_or(RefError::Length)?;
        let out = self.input.get(self.pos..end).ok_or(RefError::Eof)?;
        self.pos = end;
        Ok(out)
    }
    // Spec: little-endian base-128 payload followed by zigzag inversion.
    fn integer(&mut self, int: bool) -> Result<i64, RefError> {
        let mut magnitude = 0u128;
        for digit in 0..if int { 5 } else { 10 } {
            let byte = self.take(1)?[0];
            magnitude += u128::from(byte % 128) * (1u128 << (digit * 7));
            if byte < 128 {
                if magnitude > u128::from(u64::MAX) {
                    return Err(RefError::Varint);
                }
                let signed = if magnitude.is_multiple_of(2) {
                    (magnitude / 2) as i128
                } else {
                    -((magnitude / 2) as i128) - 1
                };
                if int && (signed < i128::from(i32::MIN) || signed > i128::from(i32::MAX)) {
                    return Err(RefError::IntRange);
                }
                return Ok(signed as i64);
            }
        }
        Err(RefError::Varint)
    }
    fn bytes(&mut self) -> Result<Vec<u8>, RefError> {
        // Length is a long, NOT an int (spec: bytes/string binary encoding).
        let n = self.integer(false)?;
        let n = usize::try_from(n).map_err(|_| RefError::Length)?;
        Ok(self.take(n)?.to_vec())
    }
    fn datum(&mut self, ty: &Ty) -> Result<Value, RefError> {
        self.fuel = self.fuel.checked_sub(1).ok_or(RefError::Budget)?;
        Ok(match ty {
            Ty::Null => Value::Null,
            Ty::Bool => {
                let b = self.take(1)?[0];
                if b > 1 {
                    return Err(RefError::Bool);
                }
                Value::Bool(b)
            }
            Ty::Int => Value::Int(self.integer(true)? as i32),
            Ty::Long => Value::Long(self.integer(false)?),
            Ty::Float => Value::Float(u32::from_le_bytes(self.take(4)?.try_into().unwrap())),
            Ty::Double => Value::Double(u64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            Ty::Bytes | Ty::String => Value::Bytes(self.bytes()?),
            Ty::Fixed(n) => Value::Bytes(self.take(*n)?.to_vec()),
            Ty::Enum(n) => {
                let i = self.integer(true)?;
                if i < 0 || i as usize >= *n {
                    return Err(RefError::Index);
                }
                Value::Enum(i as usize)
            }
            Ty::Record(fields) => Value::Record(
                fields
                    .iter()
                    .map(|t| self.datum(t))
                    .collect::<Result<_, _>>()?,
            ),
            Ty::NamedRecord(name, fields) => {
                self.definitions.insert(name.clone(), ty.clone());
                Value::Record(
                    fields
                        .iter()
                        .map(|t| self.datum(t))
                        .collect::<Result<_, _>>()?,
                )
            }
            Ty::Ref(name) => {
                let definition = self
                    .definitions
                    .get(name)
                    .expect("reference schema names must resolve")
                    .clone();
                self.datum(&definition)?
            }
            Ty::Union(branches) => {
                let i = self.integer(false)?;
                let branch = branches
                    .get(usize::try_from(i).map_err(|_| RefError::Index)?)
                    .ok_or(RefError::Index)?;
                Value::Union(i as usize, Box::new(self.datum(branch)?))
            }
            Ty::Array(item) | Ty::Map(item) => {
                let map = matches!(ty, Ty::Map(_));
                let mut values = Vec::new();
                let mut entries = Vec::new();
                loop {
                    let count = self.integer(false)?;
                    if count == 0 {
                        break;
                    }
                    let count_abs = count.checked_abs().ok_or(RefError::Length)? as u64;
                    let block_end = if count < 0 {
                        let bytes = usize::try_from(self.integer(false)?)
                            .map_err(|_| RefError::BlockSize)?;
                        Some(self.pos.checked_add(bytes).ok_or(RefError::BlockSize)?)
                    } else {
                        None
                    };
                    if count_abs > self.fuel as u64 {
                        return Err(RefError::Budget);
                    }
                    for _ in 0..count_abs {
                        if map {
                            let key = self.bytes()?;
                            entries.push((key, self.datum(item)?));
                        } else {
                            values.push(self.datum(item)?);
                        }
                    }
                    if block_end.is_some_and(|end| end != self.pos) {
                        return Err(RefError::BlockSize);
                    }
                }
                if map {
                    Value::Map(entries)
                } else {
                    Value::Array(values)
                }
            }
        })
    }
}

#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn encode_integer(v: i64, out: &mut Vec<u8>) {
    let mut n = if v < 0 {
        ((-(i128::from(v)) * 2) - 1) as u64
    } else {
        v as u64 * 2
    };
    while n >= 128 {
        out.push((n % 128) as u8 + 128);
        n /= 128;
    }
    out.push(n as u8);
}
fn encode_bytes(v: &[u8], out: &mut Vec<u8>) {
    encode_integer(v.len() as i64, out);
    out.extend(v);
}
fn encode(ty: &Ty, v: &Value, out: &mut Vec<u8>, rng: &mut Rng) {
    match (ty, v) {
        (Ty::Null, Value::Null) => {}
        (Ty::Bool, Value::Bool(v)) => out.push(*v),
        (Ty::Int, Value::Int(v)) => encode_integer(i64::from(*v), out),
        (Ty::Long, Value::Long(v)) => encode_integer(*v, out),
        (Ty::Float, Value::Float(v)) => out.extend(v.to_le_bytes()),
        (Ty::Double, Value::Double(v)) => out.extend(v.to_le_bytes()),
        (Ty::Bytes | Ty::String, Value::Bytes(v)) => encode_bytes(v, out),
        (Ty::Fixed(_), Value::Bytes(v)) => out.extend(v),
        (Ty::Enum(_), Value::Enum(v)) => encode_integer(*v as i64, out),
        (Ty::Record(ts), Value::Record(vs)) => {
            for (t, v) in ts.iter().zip(vs) {
                encode(t, v, out, rng);
            }
        }
        (Ty::Union(ts), Value::Union(i, v)) => {
            encode_integer(*i as i64, out);
            encode(&ts[*i], v, out, rng);
        }
        (Ty::Array(t), Value::Array(vs)) => {
            let mut pos = 0;
            while pos < vs.len() {
                let n = 1 + rng.below(vs.len() - pos);
                let mut payload = Vec::new();
                for v in &vs[pos..pos + n] {
                    encode(t, v, &mut payload, rng);
                }
                encode_block(n, &payload, out, rng);
                pos += n;
            }
            encode_integer(0, out);
        }
        (Ty::Map(t), Value::Map(vs)) => {
            let mut pos = 0;
            while pos < vs.len() {
                let n = 1 + rng.below(vs.len() - pos);
                let mut payload = Vec::new();
                for (k, v) in &vs[pos..pos + n] {
                    encode_bytes(k, &mut payload);
                    encode(t, v, &mut payload, rng);
                }
                encode_block(n, &payload, out, rng);
                pos += n;
            }
            encode_integer(0, out);
        }
        _ => panic!("generator type mismatch"),
    }
}
fn encode_block(n: usize, payload: &[u8], out: &mut Vec<u8>, rng: &mut Rng) {
    if rng.below(2) == 0 {
        encode_integer(n as i64, out);
    } else {
        encode_integer(-(n as i64), out);
        encode_integer(payload.len() as i64, out);
    }
    out.extend(payload);
}
fn gen_ty(rng: &mut Rng, depth: usize) -> Ty {
    match rng.below(if depth == 0 { 10 } else { 14 }) {
        0 => Ty::Null,
        1 => Ty::Bool,
        2 => Ty::Int,
        3 => Ty::Long,
        4 => Ty::Float,
        5 => Ty::Double,
        6 => Ty::Bytes,
        7 => Ty::String,
        8 => Ty::Fixed(1 + rng.below(8)),
        9 => Ty::Enum(1 + rng.below(8)),
        10 => Ty::Record((0..rng.below(5)).map(|_| gen_ty(rng, depth - 1)).collect()),
        11 => Ty::Array(Box::new(gen_ty(rng, depth - 1))),
        12 => Ty::Map(Box::new(gen_ty(rng, depth - 1))),
        // No duplicate unnamed branches and no nested unions.
        _ => Ty::Union(vec![Ty::Null, Ty::Record(vec![gen_ty(rng, depth - 1)])]),
    }
}
fn gen_value(ty: &Ty, rng: &mut Rng) -> Value {
    let mut bytes = |n| (0..n).map(|_| rng.next() as u8).collect::<Vec<_>>();
    match ty {
        Ty::Null => Value::Null,
        Ty::Bool => Value::Bool(rng.below(2) as u8),
        Ty::Int => Value::Int(rng.next() as i32),
        Ty::Long => Value::Long(rng.next() as i64),
        Ty::Float => Value::Float(rng.next() as u32),
        Ty::Double => Value::Double(rng.next()),
        Ty::Bytes | Ty::String => {
            let n = rng.below(17);
            Value::Bytes((0..n).map(|_| rng.next() as u8).collect())
        }
        Ty::Fixed(n) => Value::Bytes(bytes(*n)),
        Ty::Enum(n) => Value::Enum(rng.below(*n)),
        Ty::Record(ts) => Value::Record(ts.iter().map(|t| gen_value(t, rng)).collect()),
        Ty::Union(ts) => {
            let i = rng.below(ts.len());
            Value::Union(i, Box::new(gen_value(&ts[i], rng)))
        }
        Ty::Array(t) => Value::Array((0..rng.below(5)).map(|_| gen_value(t, rng)).collect()),
        Ty::Map(t) => Value::Map(
            (0..rng.below(5))
                .map(|i| (format!("key{i}").into_bytes(), gen_value(t, rng)))
                .collect(),
        ),
        Ty::NamedRecord(_, ts) => Value::Record(ts.iter().map(|t| gen_value(t, rng)).collect()),
        Ty::Ref(_) => panic!("recursive data is generated by the directed recursion control"),
    }
}
fn json(ty: &Ty, names: &mut usize) -> String {
    *names += 1;
    let name = *names;
    match ty {
        Ty::Null => "\"null\"".into(),
        Ty::Bool => "\"boolean\"".into(),
        Ty::Int => "\"int\"".into(),
        Ty::Long => "\"long\"".into(),
        Ty::Float => "\"float\"".into(),
        Ty::Double => "\"double\"".into(),
        Ty::Bytes => "\"bytes\"".into(),
        Ty::String => "\"string\"".into(),
        Ty::Fixed(n) => format!("{{\"type\":\"fixed\",\"name\":\"F{name}\",\"size\":{n}}}"),
        Ty::Enum(n) => format!(
            "{{\"type\":\"enum\",\"name\":\"E{name}\",\"symbols\":[{}]}}",
            (0..*n)
                .map(|i| format!("\"S{i}\""))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Ty::Record(ts) => format!(
            "{{\"type\":\"record\",\"name\":\"R{name}\",\"fields\":[{}]}}",
            ts.iter()
                .enumerate()
                .map(|(i, t)| format!("{{\"name\":\"f{i}\",\"type\":{}}}", json(t, names)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Ty::Array(t) => format!("{{\"type\":\"array\",\"items\":{}}}", json(t, names)),
        Ty::Map(t) => format!("{{\"type\":\"map\",\"values\":{}}}", json(t, names)),
        Ty::Union(ts) => format!(
            "[{}]",
            ts.iter()
                .map(|t| json(t, names))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Ty::NamedRecord(name, ts) => format!(
            "{{\"type\":\"record\",\"name\":\"{name}\",\"fields\":[{}]}}",
            ts.iter()
                .enumerate()
                .map(|(i, t)| format!("{{\"name\":\"f{i}\",\"type\":{}}}", json(t, names)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Ty::Ref(name) => format!("\"{name}\""),
    }
}

#[derive(Clone)]
struct Projection {
    sink: u32,
    key: u32,
    children: Vec<Projection>,
}
fn plan(
    ty: &Ty,
    schema: &Schema,
    id: NodeId,
    rng: &mut Rng,
    desc: &mut Vec<SinkDesc>,
    ops: &mut Vec<PlanNode>,
) -> Projection {
    if matches!(ty, Ty::Null | Ty::Ref(_)) || rng.below(7) == 0 {
        ops.push(PlanNode::skip(id));
        return Projection {
            sink: NO_SINK,
            key: NO_SINK,
            children: vec![],
        };
    }
    let container = matches!(
        ty,
        Ty::Record(_) | Ty::NamedRecord(_, _) | Ty::Array(_) | Ty::Map(_) | Ty::Union(_)
    );
    let kind = match ty {
        Ty::Bool => SinkKind::Bool,
        Ty::Int => {
            if rng.below(2) == 0 {
                SinkKind::Int32
            } else {
                SinkKind::Int64
            }
        }
        Ty::Long => SinkKind::Int64,
        Ty::Float => {
            if rng.below(2) == 0 {
                SinkKind::Float32
            } else {
                SinkKind::Float64
            }
        }
        Ty::Double => SinkKind::Float64,
        Ty::Bytes | Ty::String => SinkKind::Bytes,
        Ty::Fixed(_) => SinkKind::FixedBytes,
        Ty::Enum(_) => SinkKind::EnumIndex,
        Ty::Array(_) => SinkKind::ArrayOffsets,
        Ty::Map(_) => SinkKind::MapOffsets,
        Ty::Union(_) => {
            if rng.below(2) == 0 {
                SinkKind::UnionTag
            } else {
                SinkKind::NullMap
            }
        }
        _ => SinkKind::Bool,
    };
    let sink = if matches!(ty, Ty::Record(_) | Ty::NamedRecord(_, _)) {
        NO_SINK
    } else {
        let arg = match ty {
            Ty::Fixed(n) => *n as u32,
            Ty::Enum(_) | Ty::Union(_) => 1 << rng.below(3),
            _ => 0,
        };
        let mut d = SinkDesc::new(kind, arg);
        if kind == SinkKind::Bytes {
            d.flags = rng.below(2) as u32;
        }
        desc.push(d);
        (desc.len() - 1) as u32
    };
    let action = match ty {
        Ty::Record(_) | Ty::NamedRecord(_, _) => Action::Record,
        Ty::Array(_) => Action::Array,
        Ty::Map(_) => Action::Map,
        Ty::Union(_) => Action::Union,
        _ => Action::Value,
    };
    let mut op = PlanNode::new(
        action,
        id,
        if container {
            schema.child_count(id) as u32
        } else {
            0
        },
    );
    op.sink = sink;
    let key = if matches!(ty, Ty::Map(_)) {
        let mut d = SinkDesc::new(SinkKind::Bytes, 0);
        d.flags = rng.below(2) as u32;
        desc.push(d);
        op.sink2 = (desc.len() - 1) as u32;
        op.sink2
    } else {
        NO_SINK
    };
    ops.push(op);
    let children = match ty {
        Ty::Record(ts) | Ty::NamedRecord(_, ts) | Ty::Union(ts) => ts
            .iter()
            .enumerate()
            .map(|(i, t)| plan(t, schema, schema.child(id, i), rng, desc, ops))
            .collect(),
        Ty::Array(t) | Ty::Map(t) => vec![plan(t, schema, schema.child(id, 0), rng, desc, ops)],
        _ => vec![],
    };
    Projection {
        sink,
        key,
        children,
    }
}
#[derive(Clone, Debug, Default, PartialEq)]
struct Column {
    data: Vec<u8>,
    offsets: Vec<u8>,
    len: usize,
    len2: usize,
    offset: u64,
}
fn append(col: &mut Column, d: SinkDesc, v: &Value) {
    col.len += 1;
    match v {
        Value::Bool(v) => col.data.push(*v),
        Value::Int(v) if d.kind == SinkKind::Int64 => col.data.extend(i64::from(*v).to_ne_bytes()),
        Value::Int(v) => col.data.extend(v.to_ne_bytes()),
        Value::Long(v) => col.data.extend(v.to_ne_bytes()),
        Value::Float(v) if d.kind == SinkKind::Float64 => col
            .data
            .extend(f64::from(f32::from_bits(*v)).to_bits().to_ne_bytes()),
        Value::Float(v) => col.data.extend(v.to_ne_bytes()),
        Value::Double(v) => col.data.extend(v.to_ne_bytes()),
        Value::Bytes(v) => {
            col.data.extend(v);
            if d.kind == SinkKind::Bytes {
                if d.flags != 0 {
                    col.data.push(0);
                }
                col.len = col.data.len();
                col.len2 += 1;
                col.offset = col.data.len() as u64;
                col.offsets.extend(col.offset.to_ne_bytes());
            }
        }
        Value::Enum(v) => match d.arg {
            1 => col.data.push(*v as u8),
            2 => col.data.extend((*v as u16).to_ne_bytes()),
            _ => col.data.extend((*v as u32).to_ne_bytes()),
        },
        _ => panic!("projection type mismatch"),
    }
}
fn project(v: &Value, p: &Projection, desc: &[SinkDesc], cols: &mut [Column]) {
    if p.sink == NO_SINK && p.children.is_empty() {
        return;
    }
    match v {
        Value::Record(vs) => {
            for (v, p) in vs.iter().zip(&p.children) {
                project(v, p, desc, cols);
            }
        }
        Value::Union(i, v) => {
            let d = desc[p.sink as usize];
            if d.kind == SinkKind::NullMap {
                append(
                    &mut cols[p.sink as usize],
                    d,
                    &Value::Bool(u8::from(*i == 0)),
                );
            } else {
                append(&mut cols[p.sink as usize], d, &Value::Enum(*i));
            }
            project(v, &p.children[*i], desc, cols);
        }
        Value::Array(vs) => {
            for v in vs {
                project(v, &p.children[0], desc, cols);
            }
            let c = &mut cols[p.sink as usize];
            c.offset += vs.len() as u64;
            c.len += 1;
            c.data.extend(c.offset.to_ne_bytes());
        }
        Value::Map(vs) => {
            for (k, v) in vs {
                append(
                    &mut cols[p.key as usize],
                    desc[p.key as usize],
                    &Value::Bytes(k.clone()),
                );
                project(v, &p.children[0], desc, cols);
            }
            let c = &mut cols[p.sink as usize];
            c.offset += vs.len() as u64;
            c.len += 1;
            c.data.extend(c.offset.to_ne_bytes());
        }
        _ => append(&mut cols[p.sink as usize], desc[p.sink as usize], v),
    }
}

// Each capacity window has a 16-byte canary on both sides. No unchecked access.
struct Storage {
    bytes: Vec<u8>,
    aux: Vec<u8>,
    cap: usize,
    cap2: usize,
    len: usize,
    len2: usize,
    offset: u64,
}
impl Storage {
    fn new() -> Self {
        Self {
            bytes: vec![0xa5; 32],
            aux: vec![0xa5; 32],
            cap: 0,
            cap2: 0,
            len: 0,
            len2: 0,
            offset: 0,
        }
    }
    fn grow(&mut self, cap: usize, cap2: usize) {
        for (buf, old, new) in [
            (&mut self.bytes, self.cap, cap),
            (&mut self.aux, self.cap2, cap2),
        ] {
            if new > old {
                buf.splice(16 + old..16 + old, std::iter::repeat_n(0xa5, new - old));
            }
        }
        self.cap = self.cap.max(cap);
        self.cap2 = self.cap2.max(cap2);
    }
    fn check(&self, d: SinkDesc) {
        assert!(self.len * d.width() <= self.cap, "P3 primary capacity");
        assert!(self.len2 * d.width2() <= self.cap2, "P3 secondary capacity");
        assert!(
            self.bytes[..16]
                .iter()
                .chain(&self.bytes[16 + self.cap..])
                .all(|b| *b == 0xa5),
            "P3 primary canary"
        );
        assert!(
            self.aux[..16]
                .iter()
                .chain(&self.aux[16 + self.cap2..])
                .all(|b| *b == 0xa5),
            "P3 secondary canary"
        );
    }
    fn observed(&self, d: SinkDesc) -> Column {
        Column {
            data: self.bytes[16..16 + self.len * d.width()].to_vec(),
            offsets: self.aux[16..16 + self.len2 * d.width2()].to_vec(),
            len: self.len,
            len2: self.len2,
            offset: self.offset,
        }
    }
}
fn run(ty: &Ty, input: &[u8], rows: usize, seed: u64) {
    let schema_json = json(ty, &mut 0);
    let schema = Schema::parse(schema_json.as_bytes(), 256).unwrap();
    let mut rng = Rng(seed | 1);
    let mut desc = vec![];
    let mut ops = vec![];
    let projection = plan(ty, &schema, schema.root(), &mut rng, &mut desc, &mut ops);
    let program = Program::compile(&schema, &desc, &ops).unwrap();
    let mut oracle = Reference::new(input);
    let expected_values: Vec<_> = (0..rows).map(|_| oracle.datum(ty).unwrap()).collect();
    assert_eq!(oracle.pos, input.len());
    let mut storage: Vec<_> = desc.iter().map(|_| Storage::new()).collect();
    let mut state = State::new(256);
    let mut pos = 0;
    let mut committed = 0;
    // Driver fuel is NOT executor instruction fuel or a termination proof.
    for _ in 0..100_000 {
        let want = (1 + rng.below(3)).min(rows - committed);
        let mut sinks: Vec<_> = storage
            .iter_mut()
            .map(|s| Sink {
                data: (&mut s.bytes[16..16 + s.cap]).into(),
                data2: (&mut s.aux[16..16 + s.cap2]).into(),
                len: s.len,
                len2: s.len2,
                offset: s.offset,
            })
            .collect();
        let progress = decode(&program, &mut state, &input[pos..], want as u64, &mut sinks)
            .unwrap_or_else(|e| panic!("{schema_json} input={input:?}: {e}"));
        let metadata: Vec<_> = sinks.iter().map(|s| (s.len, s.len2, s.offset)).collect();
        drop(sinks);
        for ((len, len2, offset), b) in metadata.into_iter().zip(&mut storage) {
            b.len = len;
            b.len2 = len2;
            b.offset = offset;
        }
        assert!(progress.consumed <= input.len() - pos, "P2 consumed bound");
        let old_pos = pos;
        pos += progress.consumed;
        assert!(pos >= old_pos, "P2 monotonic cursor");
        committed += progress.rows_done as usize;
        assert!(committed <= rows, "P3 committed rows bound");
        // A row that needs space in the first row of a call is paused, not rolled back: the sinks hold
        // what it wrote so far and the state is not at a row boundary.
        let paused = matches!(progress.status, Status::NeedSpace { .. }) && progress.rows_done == 0;
        assert_eq!(
            state.at_row_boundary(),
            !paused,
            "P3 transactional state boundary"
        );
        if paused {
            assert_eq!(progress.consumed, 0, "a paused row consumes nothing");
        }
        let mut prefix = vec![Column::default(); desc.len()];
        for v in &expected_values[..committed] {
            project(v, &projection, &desc, &mut prefix);
        }
        for (i, s) in storage.iter().enumerate() {
            s.check(desc[i]);
            if !paused {
                let actual = s.observed(desc[i]);
                assert_eq!(actual, prefix[i], "P3 exact committed projection");
            }
        }
        match progress.status {
            Status::NeedSpace {
                sink,
                min_cap,
                min_cap2,
            } => {
                let i = sink as usize;
                let d = desc[i];
                let s = &mut storage[i];
                assert!(
                    min_cap as usize * d.width() > s.cap || min_cap2 as usize * d.width2() > s.cap2,
                    "P3 capacity request must grow"
                );
                s.grow(
                    (min_cap as usize + rng.below(3)) * d.width(),
                    (min_cap2 as usize + rng.below(3)) * d.width2(),
                );
            }
            Status::NeedInput => unreachable!("streaming mode is off"),
            Status::RowLimit | Status::BlockDone => {
                assert!(state.at_row_boundary());
                assert_eq!(
                    storage
                        .iter()
                        .enumerate()
                        .map(|(i, s)| s.observed(desc[i]))
                        .collect::<Vec<_>>(),
                    prefix,
                    "P3 exact committed rows"
                );
                if committed == rows {
                    assert_eq!(pos, input.len());
                    return;
                }
            }
        }
    }
    panic!("P2 driver fuel exhausted");
}

#[test]
fn differential_random_schemas_data_and_chunkings() {
    let mut rng = Rng(0x4156_524f_1200_0001);
    let mut types = vec![
        Ty::Null,
        Ty::Bool,
        Ty::Int,
        Ty::Long,
        Ty::Float,
        Ty::Double,
        Ty::Bytes,
        Ty::String,
        Ty::Fixed(3),
        Ty::Enum(3),
        Ty::Record(vec![]),
        Ty::Array(Box::new(Ty::Null)),
        Ty::Map(Box::new(Ty::Null)),
    ];
    types.extend((0..1000).map(|_| gen_ty(&mut rng, 4)));
    for ty in types {
        let rows = 1 + rng.below(8);
        let mut input = vec![];
        for _ in 0..rows {
            let value = gen_value(&ty, &mut rng);
            encode(&ty, &value, &mut input, &mut rng);
        }
        for _ in 0..4 {
            run(&ty, &input, rows, rng.next());
        }
    }
}

#[test]
fn reference_boundary_and_malformed_controls() {
    for value in [
        i64::MIN,
        i64::MAX,
        i32::MIN as i64,
        i32::MAX as i64,
        -1,
        0,
        1,
        63,
        64,
        -64,
        -65,
    ] {
        let mut bytes = vec![];
        encode_integer(value, &mut bytes);
        assert_eq!(Reference::new(&bytes).integer(false), Ok(value));
        assert_eq!(decode_long(&bytes), Ok((value, bytes.len())));
        for n in 0..bytes.len() {
            assert!(Reference::new(&bytes[..n]).integer(false).is_err());
            assert!(decode_long(&bytes[..n]).is_err());
        }
    }
    assert_eq!(Reference::new(&[2]).datum(&Ty::Bool), Err(RefError::Bool));
    assert_eq!(
        Reference::new(&[2]).datum(&Ty::Enum(1)),
        Err(RefError::Index)
    );
    assert_eq!(
        Reference::new(&[2]).datum(&Ty::Union(vec![Ty::Null])),
        Err(RefError::Index)
    );
    assert_eq!(
        Reference::new(&[1]).datum(&Ty::Bytes),
        Err(RefError::Length)
    );
    assert_eq!(
        Reference::new(&[0x80; 10]).integer(false),
        Err(RefError::Varint)
    );
    let mut overflow = [0xff; 10];
    overflow[9] = 2;
    assert_eq!(
        Reference::new(&overflow).integer(false),
        Err(RefError::Varint)
    );
    assert!(decode_long(&overflow).is_err());
}

#[test]
fn structured_i64_fast_path_and_arbitrary_bytes() {
    let mut rng = Rng(0x1234_5678_9abc_def1);
    let mut values = vec![i64::MIN, i64::MAX, -1, 0, 1];
    for bit in 0..64 {
        let x = 1u64 << bit;
        for delta in [0u64, 1, 127, 128, 16383] {
            values.push(x.wrapping_add(delta) as i64);
            values.push(x.wrapping_sub(delta) as i64);
        }
    }
    values.extend((0..100_000).map(|_| rng.next() as i64));
    for value in values {
        let mut b = vec![];
        encode_integer(value, &mut b);
        let n = b.len();
        b.extend((0..16).map(|_| rng.next() as u8));
        for len in 0..=b.len() {
            let expected = Reference::new(&b[..len]).integer(false);
            match expected {
                Ok(v) => assert_eq!(decode_long(&b[..len]), Ok((v, n))),
                Err(_) => assert!(decode_long(&b[..len]).is_err()),
            }
        }
    }
    for _ in 0..100_000 {
        let mut b = [0u8; 16];
        for x in &mut b {
            *x = rng.next() as u8;
        }
        for n in 0..=16 {
            let mut reference = Reference::new(&b[..n]);
            let expected = reference.integer(false);
            match expected {
                Ok(v) => assert_eq!(decode_long(&b[..n]), Ok((v, reference.pos))),
                Err(_) => assert!(decode_long(&b[..n]).is_err()),
            }
        }
    }
}

// Explicit release-only correctness sweep; deliberately excluded from routine
// debug runs. Every bit-pattern of an int32 is exercised, not a random sample.
#[test]
#[ignore = "run explicitly with --release --ignored exhaustive_i32_domain"]
fn exhaustive_i32_domain() {
    for bits in 0..=u32::MAX {
        let value = bits as i32;
        let mut raw = ((value as u32) << 1) ^ ((value >> 31) as u32);
        let mut b = [0xa5u8; 16];
        let mut len = 0;
        while raw >= 128 {
            b[len] = (raw as u8 & 127) | 128;
            len += 1;
            raw >>= 7;
        }
        b[len] = raw as u8;
        len += 1;
        assert_eq!(decode_int(&b), Ok((value, len)), "P1 fast int");
        assert_eq!(decode_int(&b[..len]), Ok((value, len)), "P1 short int");
        assert_eq!(decode_long(&b), Ok((i64::from(value), len)), "P1 fast long");
    }
}

// Compatibility-policy witnesses, not demonstrated decoder defects:
// Avro defines valid block sizes and their skipping use, but does not mandate
// rejection of every malformed size by an item-by-item reader. This reference
// deliberately checks consistency; production preserves the fork's ignore policy.
#[test]
fn witness_negative_block_size_is_accepted_by_executor() {
    let bytes = [1, 1, 0]; // count=-1, byte-size=-1, one null item, terminator=0.
    let ty = Ty::Array(Box::new(Ty::Null));
    assert_eq!(Reference::new(&bytes).datum(&ty), Err(RefError::BlockSize));
    let schema = Schema::parse(br#"{"type":"array","items":"null"}"#, 256).unwrap();
    let program = Program::compile(&schema, &[], &[PlanNode::skip(schema.root())]).unwrap();
    let p = decode(&program, &mut State::new(256), &bytes, 1, &mut []).unwrap();
    assert_eq!((p.rows_done, p.consumed), (1, 3));
}

#[test]
fn witness_mismatched_block_size_is_accepted_by_executor() {
    let bytes = [1, 4, 0]; // count=-1, byte-size=2 but null payload occupies 0 bytes.
    assert_eq!(
        Reference::new(&bytes).datum(&Ty::Array(Box::new(Ty::Null))),
        Err(RefError::BlockSize)
    );
    let schema = Schema::parse(br#"{"type":"array","items":"null"}"#, 256).unwrap();
    let program = Program::compile(&schema, &[], &[PlanNode::skip(schema.root())]).unwrap();
    assert_eq!(
        decode(&program, &mut State::new(256), &bytes, 1, &mut [])
            .unwrap()
            .rows_done,
        1
    );
}

#[test]
fn recursive_named_reference_control() {
    let ty = Ty::NamedRecord(
        "List".into(),
        vec![Ty::Long, Ty::Union(vec![Ty::Null, Ty::Ref("List".into())])],
    );
    for depth in [0, 1, 2, 16, 64] {
        let mut bytes = vec![];
        for i in 0..=depth {
            encode_integer(i, &mut bytes);
            encode_integer(if i == depth { 0 } else { 1 }, &mut bytes);
        }
        for seed in 1..=8 {
            run(&ty, &bytes, 1, seed);
        }
    }
}

#[test]
fn differential_corrupt_data_and_input_prefixes() {
    let mut rng = Rng(0xfeed_1200_0000_0001);
    let mut checked = 0;
    for _ in 0..3000 {
        let ty = gen_ty(&mut rng, 3);
        let value = gen_value(&ty, &mut rng);
        let mut original = vec![];
        encode(&ty, &value, &mut original, &mut rng);
        for variant in 0..3 {
            let mut bytes = original.clone();
            if variant == 1 && !bytes.is_empty() {
                let i = rng.below(bytes.len());
                bytes[i] ^= 1 << rng.below(8);
            }
            if variant == 2 && !bytes.is_empty() {
                bytes.truncate(rng.below(bytes.len()));
            }
            let mut reference = Reference::new(&bytes);
            reference.fuel = 2048;
            let expected = reference.datum(&ty);
            // Named exclusions, not swallowed parser errors: resource policies
            // differ and block-size acceptance has a separate FINDING witness.
            if matches!(expected, Err(RefError::Budget | RefError::BlockSize)) {
                continue;
            }
            let schema = Schema::parse(json(&ty, &mut 0).as_bytes(), 256).unwrap();
            let mut desc = vec![];
            let mut ops = vec![];
            let projection = plan(&ty, &schema, schema.root(), &mut rng, &mut desc, &mut ops);
            let program = Program::compile(&schema, &desc, &ops).unwrap();
            let mut data = vec![vec![0u8; 65536]; desc.len()];
            let mut aux = vec![vec![0u8; 65536]; desc.len()];
            let mut sinks: Vec<_> = data
                .iter_mut()
                .zip(&mut aux)
                .map(|(a, b)| Sink::with_offsets(a, b))
                .collect();
            let actual = decode_partial(&program, &mut State::new(256), &bytes, 1, &mut sinks);
            let accepted = actual.as_ref().is_ok_and(|p| p.rows_done == 1);
            assert_eq!(
                accepted,
                expected.is_ok(),
                "P4 acceptance ty={ty:?} bytes={bytes:?} ref={expected:?} exec={actual:?}"
            );
            if let Ok(v) = expected {
                let mut columns = vec![Column::default(); desc.len()];
                project(&v, &projection, &desc, &mut columns);
                let p = actual.unwrap();
                assert_eq!(p.consumed, reference.pos);
                let fills: Vec<_> = sinks.iter().map(|s| (s.len, s.len2)).collect();
                drop(sinks);
                for (i, (len, len2)) in fills.into_iter().enumerate() {
                    assert_eq!(&data[i][..len * desc[i].width()], columns[i].data);
                    assert_eq!(&aux[i][..len2 * desc[i].width2()], columns[i].offsets);
                }
            } else if let Err(error) = actual {
                assert_eq!(
                    (error.rows_done, error.consumed),
                    (0, 0),
                    "P3 failed first row commits nothing"
                );
                assert!(sinks
                    .iter()
                    .all(|s| s.len == 0 && s.len2 == 0 && s.offset == 0));
            }
            checked += 1;
        }
    }
    assert!(
        checked > 6000,
        "P4 generator must exercise malformed and prefix cases"
    );
}

#[test]
fn decimal_bytes_and_fixed_projection_controls() {
    // Logical decimal has the binary encoding of its underlying bytes/fixed.
    // Independently compute sign extension, then compare every destination width.
    for fixed in [false, true] {
        for width in [4usize, 8, 16, 32] {
            for payload in [
                vec![],
                vec![0],
                vec![0x7f],
                vec![0x80],
                vec![0xff],
                vec![0xff, 0x7f],
                vec![0, 0x80],
            ] {
                if fixed && payload.is_empty() {
                    continue;
                }
                let text = if fixed {
                    format!("{{\"type\":\"fixed\",\"name\":\"D\",\"size\":{},\"logicalType\":\"decimal\",\"precision\":2}}",payload.len())
                } else {
                    "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":2}".into()
                };
                let schema = Schema::parse(text.as_bytes(), 256).unwrap();
                let program = Program::compile(
                    &schema,
                    &[SinkDesc::new(SinkKind::Decimal, width as u32)],
                    &[PlanNode::value(schema.root(), 0)],
                )
                .unwrap();
                let mut encoded = vec![];
                if fixed {
                    encoded.extend(&payload);
                } else {
                    encode_bytes(&payload, &mut encoded);
                }
                let ref_ty = if fixed {
                    Ty::Fixed(payload.len())
                } else {
                    Ty::Bytes
                };
                assert_eq!(
                    Reference::new(&encoded).datum(&ref_ty),
                    Ok(Value::Bytes(payload.clone()))
                );
                let mut expected = vec![
                    if payload.first().is_some_and(|b| b & 128 != 0) {
                        0xff
                    } else {
                        0
                    };
                    width
                ];
                expected[width - payload.len()..].copy_from_slice(&payload);
                if cfg!(target_endian = "little") {
                    expected.reverse();
                }
                let mut output = vec![0xa5; width + 2];
                let p = decode(
                    &program,
                    &mut State::new(256),
                    &encoded,
                    1,
                    &mut [Sink::new(&mut output[1..width + 1])],
                )
                .unwrap();
                assert_eq!(p.rows_done, 1);
                assert_eq!(&output[1..width + 1], expected);
                assert_eq!((output[0], output[width + 1]), (0xa5, 0xa5));
            }
        }
    }
}

#[test]
fn transaction_error_progress_matches_reference() {
    let ty = Ty::Record(vec![Ty::Bytes, Ty::Array(Box::new(Ty::Long)), Ty::Bool]);
    let mut rng = Rng(0x1200_ffff_0000_0001);
    let mut bytes = vec![];
    for _ in 0..3 {
        let value = gen_value(&ty, &mut rng);
        encode(&ty, &value, &mut bytes, &mut rng);
    }
    *bytes.last_mut().unwrap() = 2;
    let mut reference = Reference::new(&bytes);
    let values = vec![reference.datum(&ty).unwrap(), reference.datum(&ty).unwrap()];
    let boundary = reference.pos;
    assert_eq!(reference.datum(&ty), Err(RefError::Bool));
    for seed in 1..=16 {
        let schema = Schema::parse(json(&ty, &mut 0).as_bytes(), 256).unwrap();
        let mut desc = vec![];
        let mut ops = vec![];
        let projection = plan(
            &ty,
            &schema,
            schema.root(),
            &mut Rng(seed),
            &mut desc,
            &mut ops,
        );
        let program = Program::compile(&schema, &desc, &ops).unwrap();
        let mut data = vec![vec![0xa5; 4096]; desc.len()];
        let mut aux = vec![vec![0xa5; 4096]; desc.len()];
        let mut sinks: Vec<_> = data
            .iter_mut()
            .zip(&mut aux)
            .map(|(a, b)| Sink::with_offsets(a, b))
            .collect();
        let mut state = State::new(256);
        let error = decode_partial(&program, &mut state, &bytes, 3, &mut sinks).unwrap_err();
        assert_eq!(
            (error.rows_done, error.consumed),
            (2, boundary),
            "P3 failed-row committed progress"
        );
        assert!(state.at_row_boundary());
        let mut expected = vec![Column::default(); desc.len()];
        for v in &values {
            project(v, &projection, &desc, &mut expected);
        }
        let fills: Vec<_> = sinks.iter().map(|s| (s.len, s.len2)).collect();
        drop(sinks);
        for (i, (len, len2)) in fills.into_iter().enumerate() {
            assert_eq!(len, expected[i].len);
            assert_eq!(len2, expected[i].len2);
            assert_eq!(&data[i][..len * desc[i].width()], expected[i].data);
            assert_eq!(&aux[i][..len2 * desc[i].width2()], expected[i].offsets);
        }
    }
}
