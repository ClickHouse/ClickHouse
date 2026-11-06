#![allow(dead_code, unsafe_code)]
//! Helpers shared by the integration tests: a plan builder that decodes every
//! leaf of a schema, a driver that feeds the executor through arbitrary chunking
//! and buffer growth, a dump format for decoded columns, a container file
//! reader built only from the crate's `ocf` module, and an allocation counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::{Path, PathBuf};

use _ch_rust_avro::exec::{decode, decode_partial, Sink, State, Status};
use _ch_rust_avro::ocf::{self, BlockCtx, BlockHeader, Codec, Parsed};
use _ch_rust_avro::program::{Action, PlanNode, Program, SinkDesc, SinkKind};
use _ch_rust_avro::schema::{Kind, NodeId, Schema};
use _ch_rust_avro::{Error, Result};

// ---------------------------------------------------------------------------
// Allocation counting. One counter per thread so that parallel tests do not
// see each other.
// ---------------------------------------------------------------------------

pub struct CountingAlloc;

thread_local! {
    static CURRENT: Cell<usize> = const { Cell::new(0) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            let _ = CURRENT.try_with(|c| {
                let now = c.get() + layout.size();
                c.set(now);
                let _ = PEAK.try_with(|p| p.set(p.get().max(now)));
            });
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        let _ = CURRENT.try_with(|c| c.set(c.get().saturating_sub(layout.size())));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            let _ = CURRENT.try_with(|c| {
                let now = c.get().saturating_sub(layout.size()) + new_size;
                c.set(now);
                let _ = PEAK.try_with(|p| p.set(p.get().max(now)));
            });
        }
        p
    }
}

/// Runs `f` and returns its result with the peak number of bytes this thread had
/// allocated above the starting level.
pub fn peak_alloc<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let start = CURRENT.with(|c| c.get());
    PEAK.with(|p| p.set(start));
    let r = f();
    let peak = PEAK.with(|p| p.get());
    (r, peak.saturating_sub(start))
}

// ---------------------------------------------------------------------------
// Encoding helpers: the spec text, bit by bit. Used only to build inputs by
// hand; expected values always come from the spec or from other implementations.
// ---------------------------------------------------------------------------

pub fn zz(v: i64) -> Vec<u8> {
    let mut n = ((v << 1) ^ (v >> 63)) as u64;
    let mut out = Vec::new();
    while n >= 0x80 {
        out.push((n as u8 & 0x7f) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
    out
}

pub fn avro_bytes(b: &[u8]) -> Vec<u8> {
    let mut v = zz(b.len() as i64);
    v.extend_from_slice(b);
    v
}

pub fn schema(json: &str) -> Schema {
    Schema::parse(json.as_bytes(), 0).unwrap_or_else(|e| panic!("{json}: {e}"))
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Universal plan: decode every leaf into its own sink.
// ---------------------------------------------------------------------------

pub struct Universal {
    pub sinks: Vec<SinkDesc>,
    pub plan: Vec<PlanNode>,
}

/// A plan that decodes everything except the contents of recursive references,
/// which are skipped. Sinks are numbered in pre-order; a map has its offsets
/// sink before its keys sink.
pub fn universal_plan(schema: &Schema) -> Universal {
    let mut sinks = Vec::new();
    let mut plan = Vec::new();
    // Explicit stack: (node, next child to visit, plan index to patch, on_path guard).
    let mut path: Vec<NodeId> = Vec::new();
    enum Work {
        Enter(NodeId),
        Leave,
    }
    let mut work = vec![Work::Enter(schema.root())];
    while let Some(w) = work.pop() {
        let n = match w {
            Work::Leave => {
                path.pop();
                continue;
            }
            Work::Enter(n) => n,
        };
        let mut new_sink = |kind: SinkKind, arg: u32| -> u32 {
            sinks.push(SinkDesc::new(kind, arg));
            (sinks.len() - 1) as u32
        };
        let node = schema.node(n);
        let kind = node.kind;
        if kind == Kind::Record && path.contains(&n) {
            plan.push(PlanNode::skip(n));
            continue;
        }
        match kind {
            Kind::Null => plan.push(PlanNode::skip(n)),
            Kind::Boolean => plan.push(PlanNode::value(n, new_sink(SinkKind::Bool, 0))),
            Kind::Int => plan.push(PlanNode::value(n, new_sink(SinkKind::Int32, 0))),
            Kind::Long => plan.push(PlanNode::value(n, new_sink(SinkKind::Int64, 0))),
            Kind::Float => plan.push(PlanNode::value(n, new_sink(SinkKind::Float32, 0))),
            Kind::Double => plan.push(PlanNode::value(n, new_sink(SinkKind::Float64, 0))),
            Kind::Bytes | Kind::String => {
                plan.push(PlanNode::value(n, new_sink(SinkKind::Bytes, 0)))
            }
            Kind::Fixed => plan.push(PlanNode::value(
                n,
                new_sink(SinkKind::FixedBytes, node.fixed_size),
            )),
            Kind::Enum => plan.push(PlanNode::value(n, new_sink(SinkKind::EnumIndex, 4))),
            Kind::Record | Kind::Array | Kind::Map | Kind::Union => {
                let count = schema.child_count(n);
                let mut e = PlanNode::new(
                    match kind {
                        Kind::Record => Action::Record,
                        Kind::Array => Action::Array,
                        Kind::Map => Action::Map,
                        _ => Action::Union,
                    },
                    n,
                    count as u32,
                );
                match kind {
                    Kind::Array => e.sink = new_sink(SinkKind::ArrayOffsets, 0),
                    Kind::Map => {
                        e.sink = new_sink(SinkKind::MapOffsets, 0);
                        e.sink2 = new_sink(SinkKind::Bytes, 0);
                    }
                    Kind::Union => e.sink = new_sink(SinkKind::UnionTag, 4),
                    _ => {}
                }
                plan.push(e);
                path.push(n);
                work.push(Work::Leave);
                for i in (0..count).rev() {
                    work.push(Work::Enter(schema.child(n, i)));
                }
                continue;
            }
        }
    }
    Universal { sinks, plan }
}

pub fn skip_plan(schema: &Schema) -> Universal {
    Universal {
        sinks: Vec::new(),
        plan: vec![PlanNode::skip(schema.root())],
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Owned buffers for the sinks of a program, with the growth policy of a real caller.
pub struct Buffers {
    descs: Vec<SinkDesc>,
    data: Vec<Vec<u8>>,
    data2: Vec<Vec<u8>>,
    len: Vec<usize>,
    len2: Vec<usize>,
    offset: Vec<u64>,
}

impl Buffers {
    pub fn new(descs: &[SinkDesc], initial_elems: usize) -> Buffers {
        let mut b = Buffers {
            descs: descs.to_vec(),
            data: vec![],
            data2: vec![],
            len: vec![0; descs.len()],
            len2: vec![0; descs.len()],
            offset: vec![0; descs.len()],
        };
        for d in descs {
            b.data.push(vec![0; d.width() * initial_elems]);
            b.data2.push(vec![0; d.width2() * initial_elems]);
        }
        b
    }

    pub fn grow(&mut self, sink: usize, min_cap: u64, min_cap2: u64) {
        let d = self.descs[sink];
        let want = d.width() * min_cap as usize;
        if want > self.data[sink].len() {
            self.data[sink].resize(want, 0);
        }
        let want2 = d.width2() * min_cap2 as usize;
        if want2 > self.data2[sink].len() {
            self.data2[sink].resize(want2, 0);
        }
    }

    /// Runs `f` with `Sink`s borrowing these buffers, then stores the fill levels back.
    pub fn with_sinks<T>(&mut self, f: impl FnOnce(&mut [Sink<'_>]) -> T) -> T {
        let mut sinks: Vec<Sink<'_>> = self
            .data
            .iter_mut()
            .zip(self.data2.iter_mut())
            .enumerate()
            .map(|(i, (d, d2))| Sink {
                data: (&mut d[..]).into(),
                data2: (&mut d2[..]).into(),
                len: self.len[i],
                len2: self.len2[i],
                offset: self.offset[i],
            })
            .collect();
        let r = f(&mut sinks);
        for (i, s) in sinks.iter().enumerate() {
            self.len[i] = s.len;
            self.len2[i] = s.len2;
            self.offset[i] = s.offset;
        }
        r
    }

    /// One line per sink with the decoded values, in a format that the Python
    /// generator reproduces from the data it wrote.
    pub fn dump(&self) -> String {
        let mut out = String::new();
        for (i, d) in self.descs.iter().enumerate() {
            let w = d.width();
            let data = &self.data[i];
            let n = self.len[i];
            let vals: Vec<String> = match d.kind {
                SinkKind::Int32 => (0..n)
                    .map(|k| {
                        i32::from_ne_bytes(data[k * 4..k * 4 + 4].try_into().unwrap()).to_string()
                    })
                    .collect(),
                SinkKind::Int64 => (0..n)
                    .map(|k| {
                        i64::from_ne_bytes(data[k * 8..k * 8 + 8].try_into().unwrap()).to_string()
                    })
                    .collect(),
                SinkKind::Float32 => (0..n)
                    .map(|k| {
                        format!(
                            "{:08x}",
                            f32::from_ne_bytes(data[k * 4..k * 4 + 4].try_into().unwrap())
                                .to_bits()
                        )
                    })
                    .collect(),
                SinkKind::Float64 => (0..n)
                    .map(|k| {
                        format!(
                            "{:016x}",
                            f64::from_ne_bytes(data[k * 8..k * 8 + 8].try_into().unwrap())
                                .to_bits()
                        )
                    })
                    .collect(),
                SinkKind::Bool | SinkKind::NullMap => (0..n).map(|k| data[k].to_string()).collect(),
                SinkKind::Bytes => {
                    let mut start = 0usize;
                    (0..self.len2[i])
                        .map(|k| {
                            let end = u64::from_ne_bytes(
                                self.data2[i][k * 8..k * 8 + 8].try_into().unwrap(),
                            ) as usize;
                            let s = hex(&data[start..end]);
                            start = end;
                            if s.is_empty() {
                                "-".to_string()
                            } else {
                                s
                            }
                        })
                        .collect()
                }
                SinkKind::FixedBytes => (0..n).map(|k| hex(&data[k * w..(k + 1) * w])).collect(),
                SinkKind::Decimal => (0..n).map(|k| hex(&data[k * w..(k + 1) * w])).collect(),
                SinkKind::EnumIndex | SinkKind::UnionTag => (0..n)
                    .map(|k| {
                        match w {
                            1 => u32::from(data[k]),
                            2 => u32::from(u16::from_ne_bytes(
                                data[k * 2..k * 2 + 2].try_into().unwrap(),
                            )),
                            _ => u32::from_ne_bytes(data[k * 4..k * 4 + 4].try_into().unwrap()),
                        }
                        .to_string()
                    })
                    .collect(),
                SinkKind::ArrayOffsets | SinkKind::MapOffsets => (0..n)
                    .map(|k| {
                        u64::from_ne_bytes(data[k * 8..k * 8 + 8].try_into().unwrap()).to_string()
                    })
                    .collect(),
            };
            out.push_str(&format!("{}:{}\n", i, vals.join(" ")));
        }
        out
    }
}

pub struct DecodeStats {
    pub rows: u64,
    pub calls: u32,
    pub need_space: u32,
}

/// Decodes `input` completely, `rows_per_call` rows at a time, growing the sinks
/// by `growth` elements whenever the executor asks. Returns the number of rows and
/// the number of input bytes consumed.
pub fn drive(
    program: &Program,
    input: &[u8],
    max_rows: u64,
    rows_per_call: u64,
    initial: usize,
    growth: usize,
    bufs: &mut Buffers,
) -> Result<(DecodeStats, usize)> {
    let mut state = State::new(0);
    let mut pos = 0usize;
    let mut stats = DecodeStats {
        rows: 0,
        calls: 0,
        need_space: 0,
    };
    let _ = initial;
    loop {
        let want = rows_per_call.min(max_rows - stats.rows);
        let p = bufs.with_sinks(|sinks| decode(program, &mut state, &input[pos..], want, sinks))?;
        stats.calls += 1;
        stats.rows += p.rows_done;
        pos += p.consumed;
        match p.status {
            Status::NeedSpace {
                sink,
                min_cap,
                min_cap2,
            } => {
                stats.need_space += 1;
                let d = program.sinks()[sink as usize];
                let cur_cap = bufs.data_len(sink as usize) / d.width().max(1);
                let cur_cap2 = if d.width2() > 0 {
                    bufs.data2_len(sink as usize) / d.width2()
                } else {
                    0
                };
                let new_cap = if (min_cap as usize) > cur_cap {
                    min_cap.max((cur_cap + growth) as u64)
                } else {
                    min_cap
                };
                let new_cap2 = if (min_cap2 as usize) > cur_cap2 {
                    min_cap2.max((cur_cap2 + growth) as u64)
                } else {
                    min_cap2
                };
                bufs.grow(sink as usize, new_cap, new_cap2);
            }
            Status::RowLimit => {
                if stats.rows >= max_rows {
                    return Ok((stats, pos));
                }
            }
            Status::BlockDone => return Ok((stats, pos)),
            Status::NeedInput => unreachable!("streaming mode is off"),
        }
    }
}

/// Like [`drive`], but the way a caller that decodes a block window by window does: only the first `first`
/// bytes of `input` are visible at the start and `step` more become visible whenever the executor stops
/// because the visible bytes ended inside a row or exactly at a row boundary. The unconsumed bytes are
/// presented again together with the new ones. The rows that completed before an incomplete row stay in the
/// sinks; an input that ends inside a row is `Eof` or `CountExceedsPayload` and is retried with a longer window,
/// unless the whole input is already visible. Returns the number of rows, the bytes consumed and the number of
/// incomplete rows met (rows decoded again after a window boundary).
pub fn drive_windows(
    program: &Program,
    input: &[u8],
    max_rows: u64,
    first: usize,
    step: usize,
    bufs: &mut Buffers,
) -> Result<(u64, usize, u32)> {
    use _ch_rust_avro::ErrorCode;
    let mut state = State::new(0);
    let (mut pos, mut rows, mut incomplete) = (0usize, 0u64, 0u32);
    let mut visible = first.min(input.len());
    loop {
        let more = visible < input.len();
        let result = bufs.with_sinks(|sinks| {
            decode_partial(
                program,
                &mut state,
                &input[pos..visible],
                max_rows - rows,
                sinks,
            )
        });
        match result {
            Ok(p) => {
                rows += p.rows_done;
                pos += p.consumed;
                match p.status {
                    Status::NeedSpace {
                        sink,
                        min_cap,
                        min_cap2,
                    } => bufs.grow(sink as usize, min_cap, min_cap2),
                    Status::RowLimit => {
                        if rows >= max_rows {
                            return Ok((rows, pos, incomplete));
                        }
                    }
                    Status::NeedInput => unreachable!("streaming mode is off"),
                    Status::BlockDone => {
                        if rows >= max_rows {
                            return Ok((rows, pos, incomplete));
                        }
                        if !more {
                            return Err(_ch_rust_avro::Error::new(ErrorCode::Eof, "EOF reached"));
                        }
                        visible = (visible + step).min(input.len());
                    }
                }
            }
            Err(e) => {
                if !more
                    || !matches!(
                        e.error.code,
                        ErrorCode::Eof | ErrorCode::CountExceedsPayload
                    )
                {
                    return Err(e.error);
                }
                rows += e.rows_done;
                pos += e.consumed;
                incomplete += 1;
                visible = (visible + step).min(input.len());
            }
        }
    }
}

/// What a caller in streaming mode did, see [`drive_stream`].
#[derive(Debug, Default, PartialEq, Eq)]
pub struct StreamStats {
    pub rows: u64,
    /// Bytes of the input that were consumed: all of them, after a complete block.
    pub consumed: usize,
    /// The longest window (bytes passed to one call) of all the calls.
    pub max_window: usize,
    /// Calls that reported `NeedInput`, and the ones of them that left a row paused.
    pub need_input: u32,
    pub paused_for_input: u32,
    /// Calls that reported `NeedSpace`.
    pub need_space: u32,
}

/// Decodes `input` as a caller in streaming mode does: `first` bytes are visible at the start, and `step` more
/// whenever the executor needs input. The bytes that the executor reports as consumed are dropped from the window,
/// so the window is what a caller holds in memory. A row that is paused for input or space continues with
/// `decode_resumed`, as the C ABI does. When all of the input is visible streaming is switched off, so that
/// the end of a row that is cut by the end of the input is the error it is without streaming.
pub fn drive_stream(
    program: &Program,
    input: &[u8],
    max_rows: u64,
    rows_per_call: u64,
    first: usize,
    step: usize,
    bufs: &mut Buffers,
) -> Result<StreamStats> {
    use _ch_rust_avro::exec::decode_resumed;
    let mut state = State::new(0);
    let mut stats = StreamStats::default();
    let mut base = 0usize;
    let mut visible = first.min(input.len());
    // A row is paused, for space or for input.
    let mut paused = false;
    loop {
        state.set_streaming(visible < input.len());
        stats.max_window = stats.max_window.max(visible - base);
        let want = rows_per_call.min(max_rows - stats.rows);
        let result = bufs.with_sinks(|sinks| {
            if std::mem::take(&mut paused) {
                decode_resumed(program, &mut state, &input[base..visible], want, sinks)
            } else {
                decode_partial(program, &mut state, &input[base..visible], want, sinks)
            }
        });
        let p = result.map_err(|e| e.error)?;
        stats.rows += p.rows_done;
        base += p.consumed;
        match p.status {
            Status::NeedSpace { .. } => {
                stats.need_space += 1;
                paused = p.rows_done == 0;
                for need in state.needs() {
                    bufs.grow(need.sink as usize, need.min_cap, need.min_cap2);
                }
            }
            Status::NeedInput => {
                stats.need_input += 1;
                assert!(visible < input.len(), "NeedInput without streaming");
                if p.rows_done == 0 {
                    stats.paused_for_input += 1;
                    paused = true;
                }
                visible = (visible + step).min(input.len());
            }
            Status::RowLimit => {
                if stats.rows >= max_rows {
                    stats.consumed = base;
                    return Ok(stats);
                }
            }
            Status::BlockDone => {
                if stats.rows >= max_rows {
                    stats.consumed = base;
                    return Ok(stats);
                }
                if visible == input.len() {
                    return Err(_ch_rust_avro::Error::new(
                        _ch_rust_avro::ErrorCode::Eof,
                        "EOF reached",
                    ));
                }
                visible = (visible + step).min(input.len());
            }
        }
    }
}

impl Buffers {
    fn data_len(&self, sink: usize) -> usize {
        self.data[sink].len()
    }

    fn data2_len(&self, sink: usize) -> usize {
        self.data2[sink].len()
    }

    /// The capacities of `sink` in elements: primary and secondary buffer.
    pub fn capacity(&self, sink: usize) -> (usize, usize) {
        let d = self.descs[sink];
        (
            self.data[sink].len() / d.width().max(1),
            self.data2[sink].len() / d.width2().max(1),
        )
    }
}

// ---------------------------------------------------------------------------
// Container files, using only the crate's ocf module.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
pub enum OcfMode {
    /// Decode every value into a sink-less program (validation only).
    SkipAll,
    /// Decode every leaf into the universal plan and dump the columns.
    Dump,
    /// Check headers, sizes and sync markers only; payloads are not decoded. The
    /// only mode that accepts a compressed codec, since decompression is the
    /// caller's job.
    FramingOnly,
}

/// Reads a whole container file. Bytes left in a block after its last record are
/// accepted and ignored, as the C++ reader does. Returns the number of rows.
pub fn read_ocf_skip_all(file: &[u8]) -> Result<u64> {
    read_ocf(file, OcfMode::SkipAll).map(|(rows, _)| rows)
}

/// Like [`read_ocf_skip_all`] but also returns the dump of a universal decode.
pub fn read_ocf_dump(file: &[u8]) -> Result<(u64, String)> {
    read_ocf(file, OcfMode::Dump)
}

pub fn read_ocf(file: &[u8], mode: OcfMode) -> Result<(u64, String)> {
    let limits = ocf::OcfLimits::default();
    let (header, mut pos) = match ocf::parse_header(file, true, &limits)? {
        Parsed::Done { value, consumed } => (value, consumed),
        Parsed::NeedMore { .. } => unreachable!("eof was set"),
    };
    let schema = Schema::parse(header.schema_json(), 256)?;
    let u = if mode == OcfMode::Dump {
        universal_plan(&schema)
    } else {
        skip_plan(&schema)
    };
    let program = Program::compile(&schema, &u.sinks, &u.plan)?;
    let mut bufs = Buffers::new(&u.sinks, 4);
    let ctx = BlockCtx {
        codec: header.codec,
        min_record_size: schema.min_encoded_size(schema.root()),
        max_block_bytes: 0,
    };
    let mut rows = 0u64;
    loop {
        let (obj, bytes) = match ocf::parse_block_header(&file[pos..], true, &ctx)? {
            Parsed::Done {
                value: BlockHeader::End,
                ..
            } => break,
            Parsed::Done {
                value:
                    BlockHeader::Block {
                        object_count,
                        byte_count,
                    },
                consumed,
            } => {
                pos += consumed;
                (object_count, byte_count)
            }
            Parsed::NeedMore { .. } => unreachable!("eof was set"),
        };
        let end = pos
            .checked_add(bytes as usize)
            .filter(|e| *e <= file.len())
            .ok_or_else(|| Error::new(_ch_rust_avro::ErrorCode::Eof, "EOF reached"))?;
        if mode == OcfMode::FramingOnly {
            rows += obj;
        } else {
            if header.codec != Codec::Null {
                return Err(Error::new(
                    _ch_rust_avro::ErrorCode::UnknownCodec,
                    "fixture needs decompression",
                ));
            }
            let payload = &file[pos..end];
            let (stats, _used) = drive(&program, payload, obj, 1000, 4, 16, &mut bufs)?;
            if stats.rows != obj {
                return Err(Error::new(
                    _ch_rust_avro::ErrorCode::Eof,
                    format!("block holds {} rows, header says {obj}", stats.rows),
                ));
            }
            rows += stats.rows;
        }
        pos = end;
        match ocf::check_sync(&file[pos..], &header.sync, true)? {
            Parsed::Done { consumed, .. } => pos += consumed,
            Parsed::NeedMore { .. } => unreachable!("eof was set"),
        }
    }
    Ok((rows, bufs.dump()))
}

// ---------------------------------------------------------------------------
// Fixture discovery
// ---------------------------------------------------------------------------

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repository root")
}

pub fn fixtures_out_dir() -> PathBuf {
    match std::env::var_os("AVRO_FIXTURES_DIR") {
        Some(p) => PathBuf::from(p),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("target/avro-fixtures"),
    }
}

pub fn find_files(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = rd.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.path());
    for e in entries {
        let p = e.path();
        if p.is_dir() {
            find_files(&p, ext, out);
        } else if p.extension().is_some_and(|x| x == ext) {
            out.push(p);
        }
    }
}
