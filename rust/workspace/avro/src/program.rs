//! Compiles a schema plus a description of the caller's output buffers into a
//! flat list of ops that [`crate::exec`] runs.
//!
//! # Sinks and the plan
//!
//! The caller owns all output memory. It declares a list of *sinks*
//! ([`SinkDesc`]): typed buffers such as "an `Int64` column" or "string chars
//! plus offsets". It then describes, as a pre-order list of [`PlanNode`]s that
//! mirrors the schema tree, what to do with each schema node: skip it, decode it
//! into a sink, or recurse into its children and decode some of them.
//!
//! A sink receives only the values that are actually decoded for it. A field
//! inside a union branch gets one value per row that took that branch; the
//! items of an array arrive flattened, and an offsets sink records where each
//! array ends. Expanding such columns to row alignment (for `Nullable` or
//! `Variant`) is the caller's job and uses the `NullMap` and `UnionTag` sinks.
//!
//! # Skipping
//!
//! Skipping a subtree is not free of validation: `int` values are range
//! checked, booleans must be 0 or 1, enum and union indexes must be in range,
//! and lengths must fit the input. Named records that are shared (in-degree
//! above one in the schema graph, which includes every recursive record) are
//! compiled once into a subroutine, so the program size is linear in the schema
//! even for schemas that would expand exponentially as a tree.
//!
//! The compiler is iterative: nesting depth costs heap, not native stack.

use std::collections::{HashMap, HashSet};

use crate::schema::{Kind, NodeId, Schema};
use crate::{Error, ErrorCode, Result};

pub const NO_SINK: u32 = u32::MAX;
pub(crate) const NO_CHECK: u32 = u32::MAX;

/// Upper bound on the number of ops (and plan nodes) of one program.
pub const MAX_OPS: usize = 1 << 22;

/// `SinkDesc::flags` bit for [`SinkKind::Bytes`]: append a zero byte after each
/// value and count it in the offset. Optional, for consumers whose string column
/// stores a terminator; the default (0) writes plain value bytes.
pub const SINK_TERMINATED: u32 = 1;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkKind {
    /// `i32`, host byte order.
    Int32 = 1,
    /// `i64`.
    Int64 = 2,
    Float32 = 3,
    Float64 = 4,
    /// One byte, 0 or 1.
    Bool = 5,
    /// Primary buffer: value bytes. Secondary buffer: one `u64` end offset per
    /// value.
    Bytes = 6,
    /// `arg` bytes per value (the `fixed` size).
    FixedBytes = 7,
    /// Two's complement integer of `arg` bytes (4, 8, 16, 32), host byte order,
    /// sign extended from the big-endian Avro value.
    Decimal = 8,
    /// Enum symbol index in `arg` bytes (1, 2, 4), host byte order.
    EnumIndex = 9,
    /// One byte per union value: 1 when the null branch was taken.
    NullMap = 10,
    /// Branch index of a union value in `arg` bytes (1, 2, 4).
    UnionTag = 11,
    /// One `u64` end offset per array value, counting items.
    ArrayOffsets = 12,
    /// One `u64` end offset per map value, counting entries.
    MapOffsets = 13,
}

impl SinkKind {
    pub fn from_u32(v: u32) -> Option<SinkKind> {
        Some(match v {
            1 => SinkKind::Int32,
            2 => SinkKind::Int64,
            3 => SinkKind::Float32,
            4 => SinkKind::Float64,
            5 => SinkKind::Bool,
            6 => SinkKind::Bytes,
            7 => SinkKind::FixedBytes,
            8 => SinkKind::Decimal,
            9 => SinkKind::EnumIndex,
            10 => SinkKind::NullMap,
            11 => SinkKind::UnionTag,
            12 => SinkKind::ArrayOffsets,
            13 => SinkKind::MapOffsets,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SinkDesc {
    pub kind: SinkKind,
    /// Width or size, see [`SinkKind`]; `0` where it does not apply.
    pub arg: u32,
    pub flags: u32,
}

impl SinkDesc {
    pub fn new(kind: SinkKind, arg: u32) -> SinkDesc {
        SinkDesc {
            kind,
            arg,
            flags: 0,
        }
    }

    /// Bytes per element of the primary buffer.
    pub fn width(&self) -> usize {
        match self.kind {
            SinkKind::Int32 | SinkKind::Float32 => 4,
            SinkKind::Int64 | SinkKind::Float64 | SinkKind::ArrayOffsets | SinkKind::MapOffsets => {
                8
            }
            SinkKind::Bool | SinkKind::Bytes | SinkKind::NullMap => 1,
            SinkKind::FixedBytes | SinkKind::Decimal | SinkKind::EnumIndex | SinkKind::UnionTag => {
                self.arg as usize
            }
        }
    }

    /// Bytes per element of the secondary buffer, `0` when there is none.
    pub fn width2(&self) -> usize {
        if self.kind == SinkKind::Bytes {
            8
        } else {
            0
        }
    }

    fn validate(&self, index: usize) -> Result<()> {
        let ok = match self.kind {
            SinkKind::FixedBytes => self.arg >= 1 && self.arg <= i32::MAX as u32,
            SinkKind::Decimal => matches!(self.arg, 4 | 8 | 16 | 32),
            SinkKind::EnumIndex | SinkKind::UnionTag => matches!(self.arg, 1 | 2 | 4),
            _ => true,
        };
        if !ok {
            return Err(plan_error(format!(
                "sink {index}: invalid argument {} for {:?}",
                self.arg, self.kind
            )));
        }
        if self.flags & !SINK_TERMINATED != 0 || (self.flags != 0 && self.kind != SinkKind::Bytes) {
            return Err(plan_error(format!(
                "sink {index}: invalid flags {:#x} for {:?}",
                self.flags, self.kind
            )));
        }
        Ok(())
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Skip the whole value. No children.
    Skip = 0,
    /// Decode a primitive, enum or fixed into `sink`. No children.
    Value = 1,
    /// A record: one child per field, in field order.
    Record = 2,
    /// An array: one child for the items. `sink` is an `ArrayOffsets` sink or
    /// `NO_SINK`.
    Array = 3,
    /// A map: one child for the values. `sink` is a `MapOffsets` sink or
    /// `NO_SINK`; `sink2` is a `Bytes` sink for the keys or `NO_SINK` to skip
    /// them.
    Map = 4,
    /// A union: one child per branch. `sink` is a `NullMap` sink (then `arg` is
    /// the index of the null branch), a `UnionTag` sink, or `NO_SINK`.
    Union = 5,
}

impl Action {
    pub fn from_u32(v: u32) -> Option<Action> {
        Some(match v {
            0 => Action::Skip,
            1 => Action::Value,
            2 => Action::Record,
            3 => Action::Array,
            4 => Action::Map,
            5 => Action::Union,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanNode {
    pub action: Action,
    /// The schema node this entry describes; checked against the schema.
    pub schema_node: NodeId,
    pub sink: u32,
    pub sink2: u32,
    pub arg: u32,
    pub n_children: u32,
    /// For `Value` into an `Int32` or `Int64` sink: reject values outside
    /// `[check_min, check_max]`.
    pub check: bool,
    pub check_min: i64,
    pub check_max: i64,
}

impl PlanNode {
    pub fn new(action: Action, schema_node: NodeId, n_children: u32) -> PlanNode {
        PlanNode {
            action,
            schema_node,
            sink: NO_SINK,
            sink2: NO_SINK,
            arg: 0,
            n_children,
            check: false,
            check_min: 0,
            check_max: 0,
        }
    }

    pub fn skip(schema_node: NodeId) -> PlanNode {
        PlanNode::new(Action::Skip, schema_node, 0)
    }

    pub fn value(schema_node: NodeId, sink: u32) -> PlanNode {
        PlanNode {
            sink,
            ..PlanNode::new(Action::Value, schema_node, 0)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum UnionMode {
    /// Validate the index only.
    None,
    /// Write the index into a `UnionTag` sink of this many bytes.
    Tag(u8),
    /// Write 1 if the index equals this value, else 0, into a `NullMap` sink.
    NullMap(u32),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Op {
    I32 {
        sink: u32,
        chk: u32,
    },
    I32As64 {
        sink: u32,
        chk: u32,
    },
    I64 {
        sink: u32,
        chk: u32,
    },
    F32 {
        sink: u32,
    },
    F32As64 {
        sink: u32,
    },
    F64 {
        sink: u32,
    },
    Bool {
        sink: u32,
    },
    Bytes {
        sink: u32,
        terminated: bool,
    },
    Fixed {
        sink: u32,
        n: u32,
    },
    DecBytes {
        sink: u32,
        width: u8,
    },
    DecFixed {
        sink: u32,
        n: u32,
        width: u8,
    },
    Enum {
        sink: u32,
        symbols: u32,
        width: u8,
    },
    SkipInt,
    SkipLong,
    SkipBool,
    SkipFixed {
        n: u64,
    },
    SkipString,
    SkipEnum {
        symbols: u32,
    },
    Union {
        sink: u32,
        mode: UnionMode,
        n: u32,
        table: u32,
    },
    Jump {
        to: u32,
    },
    LoopBegin {
        sink: u32,
        /// `Schema::min_encoded_size` of one item, kept exact so the block count check never weakens.
        item_min: u64,
        end: u32,
    },
    LoopNext {
        begin: u32,
        /// Index in [`Program::reserves`] of what the items of the loop write, or [`NO_RESERVE`].
        reserve: u32,
    },
    Call {
        to: u32,
    },
    Return,
    EndRow,
}

#[derive(Debug)]
pub struct Program {
    pub(crate) ops: Vec<Op>,
    pub(crate) jump_table: Vec<u32>,
    pub(crate) checks: Vec<(i64, i64)>,
    /// What the loops write per item, see [`plan_reserves`].
    pub(crate) reserves: Vec<Reserve>,
    pub(crate) sinks: Vec<SinkDesc>,
    pub(crate) row_min: u64,
}

/// No [`Reserve`] list: the `reserve` of a `LoopNext` whose loop does not have one.
pub(crate) const NO_RESERVE: u32 = u32::MAX;

/// What every item of a loop writes to a sink, for the loop to ask for the room of a whole block at once.
/// A list ends with an entry whose `sink` is [`NO_SINK`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Reserve {
    pub sink: u32,
    /// Elements of the primary buffer: one, or for `Bytes` one per value that is terminated.
    pub elems: u32,
    /// Elements of the secondary buffer: one for `Bytes`.
    pub elems2: u32,
}

/// For each loop that has a size for its items: the sinks that every item writes to, found at the start of
/// the body up to the first op that is not unconditional (a union, a call). A nested loop writes one value
/// to its offsets sink per item, whatever it holds, and is stepped over. The list is where the `LoopNext` of
/// the loop points to, so that a loop that stops for room can ask for all of a block.
///
/// A sink has one op (a plan uses a sink once), so no two entries of a list name the same sink.
fn plan_reserves(ops: &mut [Op]) -> Vec<Reserve> {
    let mut table = Vec::new();
    for begin in 0..ops.len() {
        let Op::LoopBegin { item_min, end, .. } = ops[begin] else {
            continue;
        };
        // Items that occupy no bytes are not bounded by the payload, and nothing is asked for them.
        if item_min == 0 || end as usize <= begin + 1 || end as usize > ops.len() {
            continue;
        }
        let next = end as usize - 1;
        let first = table.len();
        let mut i = begin + 1;
        while i < next {
            let entry = match ops[i] {
                Op::I32 { sink, .. }
                | Op::I32As64 { sink, .. }
                | Op::I64 { sink, .. }
                | Op::F32 { sink }
                | Op::F32As64 { sink }
                | Op::F64 { sink }
                | Op::Bool { sink }
                | Op::Fixed { sink, .. }
                | Op::DecBytes { sink, .. }
                | Op::DecFixed { sink, .. }
                | Op::Enum { sink, .. } => Some(Reserve {
                    sink,
                    elems: 1,
                    elems2: 0,
                }),
                Op::Bytes { sink, terminated } => Some(Reserve {
                    sink,
                    elems: u32::from(terminated),
                    elems2: 1,
                }),
                Op::SkipInt
                | Op::SkipLong
                | Op::SkipBool
                | Op::SkipFixed { .. }
                | Op::SkipString
                | Op::SkipEnum { .. } => None,
                Op::LoopBegin { sink, end, .. } => {
                    i = end as usize;
                    if sink != NO_SINK {
                        table.push(Reserve {
                            sink,
                            elems: 1,
                            elems2: 0,
                        });
                    }
                    continue;
                }
                _ => break,
            };
            table.extend(entry);
            i += 1;
        }
        if table.len() > first {
            table.push(Reserve {
                sink: NO_SINK,
                elems: 0,
                elems2: 0,
            });
            if let Op::LoopNext { reserve, .. } = &mut ops[next] {
                *reserve = first as u32;
            }
        }
    }
    table
}

impl Program {
    /// Compiles `plan` (see [`PlanNode`]) for `schema` and `sinks`.
    pub fn compile(schema: &Schema, sinks: &[SinkDesc], plan: &[PlanNode]) -> Result<Program> {
        if plan.len() > MAX_OPS {
            return Err(Error::new(
                ErrorCode::ProgramTooLarge,
                "The plan has too many nodes",
            ));
        }
        for (i, s) in sinks.iter().enumerate() {
            s.validate(i)?;
        }
        let mut g = Gen {
            schema,
            sinks,
            plan,
            ops: Vec::new(),
            table: Vec::new(),
            checks: Vec::new(),
            used: vec![false; sinks.len()],
            pos: 0,
            barrier: 0,
            routines: HashMap::new(),
            queued: HashSet::new(),
            queue: Vec::new(),
            calls: Vec::new(),
        };
        g.compile_tree(Start::Plan)?;
        if g.pos != plan.len() {
            return Err(plan_error(format!(
                "the plan has {} entries but the schema walk used {}",
                plan.len(),
                g.pos
            )));
        }
        g.emit(Op::EndRow)?;
        while let Some(node) = g.queue.pop() {
            let entry = g.ops.len() as u32;
            g.mark_target();
            g.routines.insert(node, entry);
            g.compile_tree(Start::Routine(node))?;
            g.emit(Op::Return)?;
        }
        for (at, node) in std::mem::take(&mut g.calls) {
            let to = g.routines[&node];
            g.ops[at] = Op::Call { to };
        }
        let reserves = plan_reserves(&mut g.ops);
        Ok(Program {
            ops: g.ops,
            jump_table: g.table,
            checks: g.checks,
            reserves,
            sinks: sinks.to_vec(),
            row_min: schema.min_encoded_size(schema.root()),
        })
    }

    pub fn sinks(&self) -> &[SinkDesc] {
        &self.sinks
    }

    /// Lower bound on the encoded size of one row. When `0`, rows can occupy no
    /// bytes and the executor cannot tell the end of a block from the input.
    pub fn min_row_size(&self) -> u64 {
        self.row_min
    }

    pub fn op_count(&self) -> usize {
        self.ops.len()
    }

    /// The bytes that this program holds on the heap, plus its own size: what a cache of programs has to count.
    pub fn memory_size(&self) -> usize {
        use std::mem::size_of;
        size_of::<Program>()
            + self.ops.capacity() * size_of::<Op>()
            + self.jump_table.capacity() * size_of::<u32>()
            + self.checks.capacity() * size_of::<(i64, i64)>()
            + self.reserves.capacity() * size_of::<Reserve>()
            + self.sinks.capacity() * size_of::<SinkDesc>()
    }
}

fn plan_error(message: impl Into<String>) -> Error {
    Error::new(
        ErrorCode::PlanInvalid,
        format!("Invalid decode plan: {}", message.into()),
    )
}

enum Start {
    Plan,
    Routine(NodeId),
}

enum Item {
    /// The next plan entry, which must describe this schema node.
    Plan(NodeId),
    /// An implicit skip of this schema node.
    Skip(NodeId),
    /// The body of a routine: the fields of this shared record.
    Body(NodeId),
}

struct Frame {
    kind: Kind,
    node: NodeId,
    skip: bool,
    next: u32,
    total: u32,
    /// Index of the `LoopBegin` or `Union` op.
    begin: usize,
    /// First slot of this union's jump table.
    table: usize,
    /// `Jump` ops to patch with the join point.
    fixups: Vec<usize>,
    /// Jump table slots of branches that emitted nothing: patched to the join.
    empty: Vec<usize>,
    branch_start: usize,
}

struct Gen<'a> {
    schema: &'a Schema,
    sinks: &'a [SinkDesc],
    plan: &'a [PlanNode],
    ops: Vec<Op>,
    table: Vec<u32>,
    checks: Vec<(i64, i64)>,
    used: Vec<bool>,
    pos: usize,
    /// Ops at an index below this are jump targets: they must not be merged.
    barrier: usize,
    routines: HashMap<NodeId, u32>,
    queued: HashSet<NodeId>,
    queue: Vec<NodeId>,
    calls: Vec<(usize, NodeId)>,
}

impl Gen<'_> {
    fn emit(&mut self, op: Op) -> Result<usize> {
        if self.ops.len() >= MAX_OPS {
            return Err(Error::new(
                ErrorCode::ProgramTooLarge,
                "The decode program has too many ops",
            ));
        }
        self.ops.push(op);
        Ok(self.ops.len() - 1)
    }

    /// Marks the next op as a jump target.
    fn mark_target(&mut self) {
        self.barrier = self.ops.len();
    }

    fn skip_fixed(&mut self, n: u64) -> Result<()> {
        if n == 0 {
            return Ok(());
        }
        if self.ops.len() > self.barrier {
            if let Some(Op::SkipFixed { n: prev }) = self.ops.last_mut() {
                *prev = prev.saturating_add(n);
                return Ok(());
            }
        }
        self.emit(Op::SkipFixed { n })?;
        Ok(())
    }

    fn routine_worthy(&self, node: NodeId) -> bool {
        self.schema.kind(node) == Kind::Record && self.schema.in_degree(node) > 1
    }

    fn open(
        &mut self,
        kind: Kind,
        node: NodeId,
        skip: bool,
        total: u32,
        begin: usize,
        table: usize,
    ) -> Frame {
        Frame {
            kind,
            node,
            skip,
            next: 0,
            total,
            begin,
            table,
            fixups: Vec::new(),
            empty: Vec::new(),
            branch_start: self.ops.len(),
        }
    }

    fn use_sink(&mut self, sink: u32, what: &str, allowed: &[SinkKind]) -> Result<SinkDesc> {
        let Some(desc) = self.sinks.get(sink as usize).copied() else {
            return Err(plan_error(format!("{what}: sink {sink} does not exist")));
        };
        if !allowed.contains(&desc.kind) {
            return Err(plan_error(format!(
                "{what}: sink {sink} is a {:?} sink, expected one of {allowed:?}",
                desc.kind
            )));
        }
        if std::mem::replace(&mut self.used[sink as usize], true) {
            return Err(plan_error(format!("{what}: sink {sink} is used twice")));
        }
        Ok(desc)
    }

    fn check_index(&mut self, e: &PlanNode, kind: SinkKind) -> Result<u32> {
        if !e.check {
            return Ok(NO_CHECK);
        }
        if !matches!(kind, SinkKind::Int32 | SinkKind::Int64) || e.check_min > e.check_max {
            return Err(plan_error(
                "a range check needs an Int32 or Int64 sink and min <= max",
            ));
        }
        self.checks.push((e.check_min, e.check_max));
        Ok((self.checks.len() - 1) as u32)
    }

    /// The op that decodes `node` into the sink named by `e`.
    fn value_op(&mut self, e: &PlanNode, node: NodeId) -> Result<Op> {
        use SinkKind as K;
        let n = self.schema.node(node);
        let what = format!("value of schema node {node} ({})", n.kind.name());
        let allowed: &[SinkKind] = match n.kind
        {
            Kind::Int => &[K::Int32, K::Int64],
            Kind::Long => &[K::Int64],
            Kind::Float => &[K::Float32, K::Float64],
            Kind::Double => &[K::Float64],
            Kind::Boolean => &[K::Bool],
            Kind::String => &[K::Bytes],
            Kind::Bytes => &[K::Bytes, K::Decimal],
            Kind::Fixed => &[K::FixedBytes, K::Decimal],
            Kind::Enum => &[K::EnumIndex],
            _ => return Err(plan_error(format!("{what}: only primitives, enums and fixed can be decoded into a sink; use Skip, Record, Array, Map or Union"))),
        };
        let desc = self.use_sink(e.sink, &what, allowed)?;
        let chk = self.check_index(e, desc.kind)?;
        let sink = e.sink;
        Ok(match (n.kind, desc.kind) {
            (Kind::Int, K::Int32) => Op::I32 { sink, chk },
            (Kind::Int, K::Int64) => Op::I32As64 { sink, chk },
            (Kind::Long, K::Int64) => Op::I64 { sink, chk },
            (Kind::Float, K::Float32) => Op::F32 { sink },
            (Kind::Float, K::Float64) => Op::F32As64 { sink },
            (Kind::Double, K::Float64) => Op::F64 { sink },
            (Kind::Boolean, K::Bool) => Op::Bool { sink },
            (Kind::String | Kind::Bytes, K::Bytes) => Op::Bytes {
                sink,
                terminated: desc.flags & SINK_TERMINATED != 0,
            },
            (Kind::Bytes, K::Decimal) => Op::DecBytes {
                sink,
                width: desc.arg as u8,
            },
            (Kind::Fixed, K::FixedBytes) => {
                if desc.arg != n.fixed_size {
                    return Err(plan_error(format!(
                        "{what}: the sink holds {} bytes per value, the fixed has {}",
                        desc.arg, n.fixed_size
                    )));
                }
                Op::Fixed {
                    sink,
                    n: n.fixed_size,
                }
            }
            (Kind::Fixed, K::Decimal) => {
                if n.fixed_size > desc.arg {
                    return Err(Error::new(
                        ErrorCode::DecimalTooWide,
                        format!("Invalid decode plan: {what}: a fixed of {} bytes does not fit a {} byte decimal", n.fixed_size, desc.arg),
                    ));
                }
                Op::DecFixed {
                    sink,
                    n: n.fixed_size,
                    width: desc.arg as u8,
                }
            }
            (Kind::Enum, K::EnumIndex) => {
                let symbols = self.schema.symbols(node).len();
                let capacity = if desc.arg >= 4 {
                    u64::MAX
                } else {
                    1u64 << (8 * desc.arg)
                };
                if symbols as u64 > capacity {
                    return Err(plan_error(format!(
                        "{what}: {symbols} symbols do not fit in {} byte(s)",
                        desc.arg
                    )));
                }
                Op::Enum {
                    sink,
                    symbols: symbols as u32,
                    width: desc.arg as u8,
                }
            }
            _ => unreachable!("allowed sink kinds were checked above"),
        })
    }

    fn compile_tree(&mut self, start: Start) -> Result<()> {
        let schema = self.schema;
        let mut stack: Vec<Frame> = Vec::new();
        let mut pending = Some(match start {
            Start::Plan => Item::Plan(schema.root()),
            Start::Routine(n) => Item::Body(n),
        });
        loop {
            if let Some(item) = pending.take() {
                let (action, node, entry, body) = match item {
                    Item::Body(n) => (Action::Skip, n, None, true),
                    Item::Skip(n) => (Action::Skip, n, None, false),
                    Item::Plan(want) => {
                        let Some(e) = self.plan.get(self.pos).copied() else {
                            return Err(plan_error("the plan is shorter than the schema walk"));
                        };
                        self.pos += 1;
                        if e.schema_node != want {
                            return Err(plan_error(format!(
                                "plan entry {} describes schema node {} but the schema walk is at node {want}",
                                self.pos - 1,
                                e.schema_node
                            )));
                        }
                        (e.action, want, Some(e), false)
                    }
                };
                let kind = schema.kind(node);
                // Containers whose frame is pushed here; everything else is a leaf for the walk.
                let mut pushed: Option<Frame> = None;
                match action {
                    Action::Skip => {
                        if let Some(e) = entry.as_ref() {
                            if e.n_children != 0 || e.sink != NO_SINK || e.sink2 != NO_SINK {
                                return Err(plan_error(
                                    "a Skip entry has no children and no sinks",
                                ));
                            }
                        }
                        match kind {
                            Kind::Null => {}
                            Kind::Boolean => {
                                self.emit(Op::SkipBool)?;
                            }
                            Kind::Int => {
                                self.emit(Op::SkipInt)?;
                            }
                            Kind::Long => {
                                self.emit(Op::SkipLong)?;
                            }
                            Kind::Float => self.skip_fixed(4)?,
                            Kind::Double => self.skip_fixed(8)?,
                            Kind::Bytes | Kind::String => {
                                self.emit(Op::SkipString)?;
                            }
                            Kind::Enum => {
                                let symbols = schema.symbols(node).len() as u32;
                                self.emit(Op::SkipEnum { symbols })?;
                            }
                            Kind::Fixed => {
                                self.skip_fixed(u64::from(schema.node(node).fixed_size))?
                            }
                            Kind::Record => {
                                if !body && self.routine_worthy(node) {
                                    let at = self.emit(Op::Call { to: 0 })?;
                                    self.calls.push((at, node));
                                    if self.queued.insert(node) {
                                        self.queue.push(node);
                                    }
                                } else if schema.child_count(node) > 0 {
                                    pushed = Some(self.open(
                                        Kind::Record,
                                        node,
                                        true,
                                        schema.child_count(node) as u32,
                                        0,
                                        0,
                                    ));
                                }
                            }
                            Kind::Array => {
                                let item_min = schema.min_encoded_size(schema.child(node, 0));
                                let begin = self.emit(Op::LoopBegin {
                                    sink: NO_SINK,
                                    item_min,
                                    end: 0,
                                })?;
                                self.mark_target();
                                pushed = Some(self.open(Kind::Array, node, true, 1, begin, 0));
                            }
                            Kind::Map => {
                                let item_min = schema
                                    .min_encoded_size(schema.child(node, 0))
                                    .saturating_add(1);
                                let begin = self.emit(Op::LoopBegin {
                                    sink: NO_SINK,
                                    item_min,
                                    end: 0,
                                })?;
                                self.mark_target();
                                self.emit(Op::SkipString)?;
                                pushed = Some(self.open(Kind::Map, node, true, 1, begin, 0));
                            }
                            Kind::Union => {
                                pushed = self.open_union(node, true, UnionMode::None)?;
                            }
                        }
                    }
                    Action::Value => {
                        let e = entry.expect("a plan item has an entry");
                        if e.n_children != 0 || e.sink2 != NO_SINK {
                            return Err(plan_error("a Value entry has no children and one sink"));
                        }
                        let op = self.value_op(&e, node)?;
                        self.emit(op)?;
                    }
                    Action::Record => {
                        let e = entry.expect("a plan item has an entry");
                        if kind != Kind::Record
                            || e.sink != NO_SINK
                            || e.sink2 != NO_SINK
                            || e.n_children as usize != schema.child_count(node)
                        {
                            return Err(plan_error(format!("Record entry for schema node {node}: needs a record with one child per field and no sinks")));
                        }
                        if e.n_children > 0 {
                            pushed = Some(self.open(Kind::Record, node, false, e.n_children, 0, 0));
                        }
                    }
                    Action::Array => {
                        let e = entry.expect("a plan item has an entry");
                        if kind != Kind::Array || e.sink2 != NO_SINK || e.n_children != 1 {
                            return Err(plan_error(format!("Array entry for schema node {node}: needs an array with one child and no second sink")));
                        }
                        if e.sink != NO_SINK {
                            self.use_sink(e.sink, "array offsets", &[SinkKind::ArrayOffsets])?;
                        }
                        let item_min = schema.min_encoded_size(schema.child(node, 0));
                        let begin = self.emit(Op::LoopBegin {
                            sink: e.sink,
                            item_min,
                            end: 0,
                        })?;
                        self.mark_target();
                        pushed = Some(self.open(Kind::Array, node, false, 1, begin, 0));
                    }
                    Action::Map => {
                        let e = entry.expect("a plan item has an entry");
                        if kind != Kind::Map || e.n_children != 1 {
                            return Err(plan_error(format!(
                                "Map entry for schema node {node}: needs a map with one child"
                            )));
                        }
                        if e.sink != NO_SINK {
                            self.use_sink(e.sink, "map offsets", &[SinkKind::MapOffsets])?;
                        }
                        let item_min = schema
                            .min_encoded_size(schema.child(node, 0))
                            .saturating_add(1);
                        let begin = self.emit(Op::LoopBegin {
                            sink: e.sink,
                            item_min,
                            end: 0,
                        })?;
                        self.mark_target();
                        if e.sink2 == NO_SINK {
                            self.emit(Op::SkipString)?;
                        } else {
                            let desc = self.use_sink(e.sink2, "map keys", &[SinkKind::Bytes])?;
                            self.emit(Op::Bytes {
                                sink: e.sink2,
                                terminated: desc.flags & SINK_TERMINATED != 0,
                            })?;
                        }
                        pushed = Some(self.open(Kind::Map, node, false, 1, begin, 0));
                    }
                    Action::Union => {
                        let e = entry.expect("a plan item has an entry");
                        let n = schema.child_count(node);
                        if kind != Kind::Union || e.sink2 != NO_SINK || e.n_children as usize != n {
                            return Err(plan_error(format!("Union entry for schema node {node}: needs a union with one child per branch")));
                        }
                        let mode = if e.sink == NO_SINK {
                            UnionMode::None
                        } else {
                            let desc = self.use_sink(
                                e.sink,
                                "union",
                                &[SinkKind::NullMap, SinkKind::UnionTag],
                            )?;
                            if desc.kind == SinkKind::NullMap {
                                if e.arg as usize >= n
                                    || schema.kind(schema.child(node, e.arg as usize)) != Kind::Null
                                {
                                    return Err(plan_error(format!("union at schema node {node}: branch {} is not the null branch", e.arg)));
                                }
                                UnionMode::NullMap(e.arg)
                            } else {
                                let capacity = if desc.arg >= 4 {
                                    u64::MAX
                                } else {
                                    1u64 << (8 * desc.arg)
                                };
                                if n as u64 > capacity {
                                    return Err(plan_error(format!("union at schema node {node}: {n} branches do not fit in {} byte(s)", desc.arg)));
                                }
                                UnionMode::Tag(desc.arg as u8)
                            }
                        };
                        let sink = e.sink;
                        pushed = self.open_union_with_sink(node, false, mode, sink)?;
                    }
                }
                if let Some(f) = pushed {
                    let first = if f.skip {
                        Item::Skip(schema.child(f.node, 0))
                    } else {
                        Item::Plan(schema.child(f.node, 0))
                    };
                    stack.push(f);
                    pending = Some(first);
                    continue;
                }
            }

            // A subtree finished: unwind to the next child that is waiting.
            loop {
                let Some(top) = stack.last_mut() else {
                    return Ok(());
                };
                if top.kind == Kind::Union {
                    if self.ops.len() == top.branch_start {
                        top.empty.push(top.table + top.next as usize);
                    } else {
                        let at = self.emit(Op::Jump { to: 0 })?;
                        top.fixups.push(at);
                    }
                }
                top.next += 1;
                if top.next < top.total {
                    if top.kind == Kind::Union {
                        self.table[top.table + top.next as usize] = self.ops.len() as u32;
                        top.branch_start = self.ops.len();
                        self.barrier = self.ops.len();
                    }
                    let child = schema.child(top.node, top.next as usize);
                    pending = Some(if top.skip {
                        Item::Skip(child)
                    } else {
                        Item::Plan(child)
                    });
                    break;
                }
                let f = stack.pop().expect("the stack is not empty");
                self.close(f)?;
            }
        }
    }

    fn open_union(&mut self, node: NodeId, skip: bool, mode: UnionMode) -> Result<Option<Frame>> {
        self.open_union_with_sink(node, skip, mode, NO_SINK)
    }

    fn open_union_with_sink(
        &mut self,
        node: NodeId,
        skip: bool,
        mode: UnionMode,
        sink: u32,
    ) -> Result<Option<Frame>> {
        let n = self.schema.child_count(node);
        let table = self.table.len();
        if self.table.len() + n > MAX_OPS {
            return Err(Error::new(
                ErrorCode::ProgramTooLarge,
                "The decode program has too many union branches",
            ));
        }
        self.table.resize(table + n, 0);
        let begin = self.emit(Op::Union {
            sink,
            mode,
            n: n as u32,
            table: table as u32,
        })?;
        if n == 0 {
            return Ok(None);
        }
        self.table[table] = self.ops.len() as u32;
        self.mark_target();
        let f = self.open(Kind::Union, node, skip, n as u32, begin, table);
        Ok(Some(f))
    }

    fn close(&mut self, f: Frame) -> Result<()> {
        match f.kind {
            Kind::Array | Kind::Map => {
                self.emit(Op::LoopNext {
                    begin: f.begin as u32,
                    reserve: NO_RESERVE,
                })?;
                let end = self.ops.len() as u32;
                if let Op::LoopBegin { end: e, .. } = &mut self.ops[f.begin] {
                    *e = end;
                }
                self.mark_target();
            }
            Kind::Union => {
                let join = self.ops.len() as u32;
                for at in f.fixups {
                    self.ops[at] = Op::Jump { to: join };
                }
                for slot in f.empty {
                    self.table[slot] = join;
                }
                self.mark_target();
            }
            _ => {}
        }
        Ok(())
    }
}
