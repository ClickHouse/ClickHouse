//! Avro schema compiler (Apache Avro specification 1.12).
//!
//! The schema is a graph in an arena: nodes refer to each other by index, and
//! a named type referenced by name (including from inside its own body, which
//! is how recursion is expressed) is the same node as its definition. There are
//! no reference counted pointers, so no cycles to leak.
//!
//! Parsing never recurses: nested schemas are compiled with an explicit frame
//! stack. The depth limit has the fork's semantics: the root is at depth 0,
//! record fields, array items, map values and union branches are at depth + 1,
//! and entering any schema (including a bare name) at `depth > max` is the
//! error "Avro schema nesting depth exceeds the limit". `0` means "no
//! configured limit", which is the internal [`HARD_MAX_DEPTH`].
//!
//! What is checked, per the specification: primitive names; `record`, `enum`,
//! `fixed`, `array`, `map`; namespaces and full names; forward references
//! rejected; duplicate definitions of a name rejected; duplicate field names;
//! duplicate and empty enum symbols; fixed sizes; union rules (no nested
//! union, at most one unnamed schema of each type, distinct named branches);
//! logical types (invalid or unknown ones are ignored, as the specification
//! requires).
//!
//! What is parsed but not interpreted: `aliases`, `doc`, `default`, `order`,
//! and every unknown attribute. All attributes stay available through
//! [`Schema::node_attr`] and [`Schema::field_attr`], which is how Iceberg's
//! `field-id` and `element-id` reach the caller.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::OnceLock;

use crate::json::{JsonDoc, JsonId, JsonLimits, JsonValue};
use crate::{Error, ErrorCode, Result, HARD_MAX_DEPTH};

pub type NodeId = u32;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Null = 0,
    Boolean = 1,
    Int = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Bytes = 6,
    String = 7,
    Record = 8,
    Enum = 9,
    Array = 10,
    Map = 11,
    Union = 12,
    Fixed = 13,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Null => "null",
            Kind::Boolean => "boolean",
            Kind::Int => "int",
            Kind::Long => "long",
            Kind::Float => "float",
            Kind::Double => "double",
            Kind::Bytes => "bytes",
            Kind::String => "string",
            Kind::Record => "record",
            Kind::Enum => "enum",
            Kind::Array => "array",
            Kind::Map => "map",
            Kind::Union => "union",
            Kind::Fixed => "fixed",
        }
    }

    pub fn from_primitive(name: &str) -> Option<Kind> {
        Some(match name {
            "null" => Kind::Null,
            "boolean" => Kind::Boolean,
            "int" => Kind::Int,
            "long" => Kind::Long,
            "float" => Kind::Float,
            "double" => Kind::Double,
            "bytes" => Kind::Bytes,
            "string" => Kind::String,
            _ => return None,
        })
    }

    pub fn is_named(self) -> bool {
        matches!(self, Kind::Record | Kind::Enum | Kind::Fixed)
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Logical {
    None = 0,
    Decimal = 1,
    BigDecimal = 2,
    Uuid = 3,
    Date = 4,
    TimeMillis = 5,
    TimeMicros = 6,
    TimestampMillis = 7,
    TimestampMicros = 8,
    TimestampNanos = 9,
    LocalTimestampMillis = 10,
    LocalTimestampMicros = 11,
    LocalTimestampNanos = 12,
    Duration = 13,
}

#[derive(Debug)]
enum Body {
    None,
    Record { first: u32, count: u32 },
    Enum(Vec<Box<str>>),
    Array(NodeId),
    Map(NodeId),
    Union(Vec<NodeId>),
}

#[derive(Debug)]
pub struct Node {
    pub kind: Kind,
    pub logical: Logical,
    /// Decimal precision, `0` when not a decimal.
    pub precision: u32,
    pub scale: u32,
    /// Size of a `fixed`, `0` otherwise.
    pub fixed_size: u32,
    fullname: Option<Box<str>>,
    /// Byte offset of the simple name inside `fullname`.
    simple_start: u32,
    /// The JSON object this node was defined by, `None` for bare primitive names.
    json: Option<JsonId>,
    body: Body,
}

impl Node {
    /// Full name (`namespace.name`) of a record, enum or fixed.
    pub fn full_name(&self) -> Option<&str> {
        self.fullname.as_deref()
    }

    pub fn simple_name(&self) -> Option<&str> {
        self.fullname
            .as_deref()
            .map(|n| &n[self.simple_start as usize..])
    }

    /// Namespace of a named type, empty for the null namespace.
    pub fn namespace(&self) -> Option<&str> {
        self.fullname.as_deref().map(|n| {
            if self.simple_start == 0 {
                ""
            } else {
                &n[..self.simple_start as usize - 1]
            }
        })
    }
}

#[derive(Debug)]
pub struct Field {
    pub name: Box<str>,
    pub ty: NodeId,
    json: JsonId,
}

/// A value of an attribute that the schema compiler does not interpret.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AttrValue<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'a str),
    /// An array or object; the compiler keeps these only as structure.
    Composite,
}

#[derive(Debug)]
pub struct Schema {
    doc: JsonDoc,
    nodes: Vec<Node>,
    fields: Vec<Field>,
    root: NodeId,
    /// The JSON text that was parsed, byte for byte: with it the schema and everything derived from it are
    /// determined, which makes it an exact identity that is linear in the size of the input.
    source: Box<[u8]>,
    /// Parsing Canonical Form, computed by the first [`Schema::canonical_form`]. It can be far larger than the
    /// input (a named type that is referenced many times is printed as its full name each time), so parsing
    /// must not pay for it.
    canonical: OnceLock<String>,
    min_size: Vec<u64>,
    max_size: Vec<Option<u64>>,
    in_degree: Vec<u32>,
}

impl Schema {
    /// Parses and compiles a schema. `max_depth` is the nesting limit shared by
    /// the JSON text and the schema (`0`: only the internal hard cap). The JSON
    /// layer is stricter than the schema layer (every schema level costs at
    /// least one JSON level), so with a shared limit it is the one that fires.
    pub fn parse(input: &[u8], max_depth: usize) -> Result<Schema> {
        Schema::parse_with(
            input,
            max_depth,
            JsonLimits {
                max_depth,
                ..JsonLimits::default()
            },
        )
    }

    /// Like [`Schema::parse`] with explicit JSON limits; `schema_depth` is the
    /// limit of the schema layer alone.
    pub fn parse_with(input: &[u8], schema_depth: usize, limits: JsonLimits) -> Result<Schema> {
        let doc = JsonDoc::parse(input, limits)?;
        let depth_limit = if schema_depth == 0 {
            HARD_MAX_DEPTH
        } else {
            schema_depth.min(HARD_MAX_DEPTH)
        };
        let mut c = Compiler {
            doc: &doc,
            nodes: Vec::new(),
            fields: Vec::new(),
            names: HashMap::new(),
            primitives: [None; 8],
            max_depth: depth_limit,
            name_bytes: 0,
            name_budget: limits.max_bytes.max(1 << 16),
        };
        let root = c.compile(JsonDoc::ROOT)?;
        let Compiler { nodes, fields, .. } = c;
        let mut schema = Schema {
            doc,
            nodes,
            fields,
            root,
            source: Box::from(input),
            canonical: OnceLock::new(),
            min_size: Vec::new(),
            max_size: Vec::new(),
            in_degree: Vec::new(),
        };
        schema.min_size = schema.compute_min_sizes();
        schema.max_size = schema.compute_max_sizes();
        schema.in_degree = schema.compute_in_degrees();
        Ok(schema)
    }

    pub fn root(&self) -> NodeId {
        self.root
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id as usize]
    }

    pub fn kind(&self, id: NodeId) -> Kind {
        self.nodes[id as usize].kind
    }

    /// The fields of a record, empty for any other node.
    pub fn fields(&self, id: NodeId) -> &[Field] {
        match self.nodes[id as usize].body {
            Body::Record { first, count } => &self.fields[first as usize..(first + count) as usize],
            _ => &[],
        }
    }

    /// The symbols of an enum, empty for any other node.
    pub fn symbols(&self, id: NodeId) -> &[Box<str>] {
        match &self.nodes[id as usize].body {
            Body::Enum(s) => s,
            _ => &[],
        }
    }

    /// The branches of a union, empty for any other node.
    pub fn branches(&self, id: NodeId) -> &[NodeId] {
        match &self.nodes[id as usize].body {
            Body::Union(b) => b,
            _ => &[],
        }
    }

    /// Items of an array or values of a map.
    pub fn element(&self, id: NodeId) -> Option<NodeId> {
        match self.nodes[id as usize].body {
            Body::Array(e) | Body::Map(e) => Some(e),
            _ => None,
        }
    }

    pub fn child_count(&self, id: NodeId) -> usize {
        match &self.nodes[id as usize].body {
            Body::Record { count, .. } => *count as usize,
            Body::Union(b) => b.len(),
            Body::Array(_) | Body::Map(_) => 1,
            _ => 0,
        }
    }

    /// The `i`-th child: a record field's type, an array's items, a map's
    /// values, or a union branch.
    pub fn child(&self, id: NodeId, i: usize) -> NodeId {
        match &self.nodes[id as usize].body {
            Body::Record { first, .. } => self.fields[*first as usize + i].ty,
            Body::Union(b) => b[i],
            Body::Array(e) | Body::Map(e) => *e,
            _ => panic!("node {id} has no children"),
        }
    }

    /// Looks up an attribute of the JSON object that defined `id`. Bare
    /// primitive names (`"int"`) have no attributes.
    pub fn node_attr(&self, id: NodeId, key: &str) -> Option<AttrValue<'_>> {
        let j = self.nodes[id as usize].json?;
        self.attr_value(j, key)
    }

    /// Looks up an attribute of the `field`-th field of record `id`.
    pub fn field_attr(&self, id: NodeId, field: usize, key: &str) -> Option<AttrValue<'_>> {
        let f = self.fields(id).get(field)?;
        self.attr_value(f.json, key)
    }

    fn attr_value(&self, object: JsonId, key: &str) -> Option<AttrValue<'_>> {
        let v = self.doc.object_get(object, key)?;
        Some(match self.doc.get(v) {
            JsonValue::Null => AttrValue::Null,
            JsonValue::Bool(b) => AttrValue::Bool(*b),
            JsonValue::Int(i) => AttrValue::Int(*i),
            JsonValue::Float(f) => AttrValue::Float(*f),
            JsonValue::Str(s) => AttrValue::Str(s),
            JsonValue::Array(_) | JsonValue::Object(_) => AttrValue::Composite,
        })
    }

    /// The JSON text that this schema was parsed from.
    pub fn source(&self) -> &[u8] {
        &self.source
    }

    /// Parsing Canonical Form of the whole schema, computed on first use and kept for the life of the schema.
    ///
    /// Its size is not bounded by the size of the schema text: see [`Schema::canonical_prefix`] for a bounded form.
    pub fn canonical_form(&self) -> &str {
        self.canonical
            .get_or_init(|| crate::canonical::canonical_form(self, usize::MAX).0)
    }

    /// At most the first `limit` bytes of the Parsing Canonical Form (cut at a character boundary), and whether it
    /// was cut. Nothing beyond `limit` is generated and nothing is retained.
    pub fn canonical_prefix(&self, limit: usize) -> (String, bool) {
        match self.canonical.get() {
            Some(full) => crate::canonical::truncate_utf8(full, limit),
            None => crate::canonical::canonical_form(self, limit),
        }
    }

    /// A lower bound on the number of bytes one value of `id` occupies, exact
    /// for the cheapest value that exists. `u64::MAX` means no finite value
    /// exists (a type that can only be satisfied by infinite data).
    ///
    /// Recursive types get the bound of their cheapest non-recursive
    /// derivation, for example `record R { next: ["null", R] }` is 1 byte.
    pub fn min_encoded_size(&self, id: NodeId) -> u64 {
        self.min_size[id as usize]
    }

    /// An upper bound on the encoded size of one value of `id`, `None` when
    /// unbounded (strings, bytes, arrays, maps, and anything that contains one
    /// or is recursive).
    pub fn max_encoded_size(&self, id: NodeId) -> Option<u64> {
        self.max_size[id as usize]
    }

    /// Number of edges that point at `id`, counting the root once. A named type
    /// with an in-degree above one is a shared (possibly recursive) subtree.
    pub fn in_degree(&self, id: NodeId) -> u32 {
        self.in_degree[id as usize]
    }

    fn compute_in_degrees(&self) -> Vec<u32> {
        let mut d = vec![0u32; self.nodes.len()];
        d[self.root as usize] += 1;
        for id in 0..self.nodes.len() {
            for i in 0..self.child_count(id as NodeId) {
                let c = self.child(id as NodeId, i);
                d[c as usize] = d[c as usize].saturating_add(1);
            }
        }
        d
    }

    /// Knuth's generalisation of Dijkstra's algorithm: the least value of each
    /// node is the least fixed point of "record = sum of fields, union = 1 +
    /// least branch", which is monotone and superior, so nodes can be settled in
    /// increasing order of value. Nodes that are never settled have no finite
    /// value.
    fn compute_min_sizes(&self) -> Vec<u64> {
        let n = self.nodes.len();
        let mut value = vec![u64::MAX; n];
        let mut settled = vec![false; n];
        let mut remaining = vec![0u32; n];
        let mut acc = vec![0u64; n];
        let mut heap: BinaryHeap<Reverse<(u64, NodeId)>> = BinaryHeap::new();

        // Reverse edges (child -> parent) in compressed row form.
        let mut counts = vec![0u32; n + 1];
        for p in 0..n {
            for i in 0..self.child_count(p as NodeId) {
                counts[self.child(p as NodeId, i) as usize + 1] += 1;
            }
        }
        for i in 0..n {
            counts[i + 1] += counts[i];
        }
        let mut parents = vec![0u32; counts[n] as usize];
        let mut cursor = counts.clone();
        for p in 0..n {
            for i in 0..self.child_count(p as NodeId) {
                let c = self.child(p as NodeId, i) as usize;
                parents[cursor[c] as usize] = p as u32;
                cursor[c] += 1;
            }
        }

        for (id, node) in self.nodes.iter().enumerate() {
            let id = id as NodeId;
            match node.kind {
                Kind::Null => heap.push(Reverse((0, id))),
                Kind::Boolean
                | Kind::Int
                | Kind::Long
                | Kind::Enum
                | Kind::String
                | Kind::Bytes
                | Kind::Array
                | Kind::Map => heap.push(Reverse((1, id))),
                Kind::Float => heap.push(Reverse((4, id))),
                Kind::Double => heap.push(Reverse((8, id))),
                Kind::Fixed => heap.push(Reverse((u64::from(node.fixed_size), id))),
                Kind::Record => {
                    let c = self.child_count(id) as u32;
                    remaining[id as usize] = c;
                    if c == 0 {
                        heap.push(Reverse((0, id)));
                    }
                }
                Kind::Union => {
                    // The branch index itself is always present.
                    if self.child_count(id) == 0 {
                        heap.push(Reverse((1, id)));
                    }
                }
            }
        }

        while let Some(Reverse((v, id))) = heap.pop() {
            if settled[id as usize] {
                continue;
            }
            settled[id as usize] = true;
            value[id as usize] = v;
            for &p in &parents[counts[id as usize] as usize..counts[id as usize + 1] as usize] {
                if settled[p as usize] {
                    continue;
                }
                match self.nodes[p as usize].kind {
                    Kind::Record => {
                        remaining[p as usize] -= 1;
                        acc[p as usize] = acc[p as usize].saturating_add(v);
                        if remaining[p as usize] == 0 {
                            heap.push(Reverse((acc[p as usize], p)));
                        }
                    }
                    Kind::Union => heap.push(Reverse((v.saturating_add(1), p))),
                    // Arrays and maps always have the one-byte empty encoding.
                    _ => {}
                }
            }
        }
        value
    }

    fn compute_max_sizes(&self) -> Vec<Option<u64>> {
        #[derive(Clone, Copy, PartialEq)]
        enum Color {
            White,
            Gray,
            Black,
        }
        struct Frame {
            node: NodeId,
            next: usize,
            acc: u64,
            unbounded: bool,
        }
        // `Some(bound)` for kinds whose size does not depend on children; the
        // bound itself is `None` for variable-length kinds.
        fn leaf(node: &Node) -> Option<Option<u64>> {
            Some(match node.kind {
                Kind::Null => Some(0),
                Kind::Boolean => Some(1),
                Kind::Int | Kind::Enum => Some(5),
                Kind::Long => Some(10),
                Kind::Float => Some(4),
                Kind::Double => Some(8),
                Kind::Fixed => Some(u64::from(node.fixed_size)),
                Kind::Bytes | Kind::String | Kind::Array | Kind::Map => None,
                Kind::Record | Kind::Union => return None,
            })
        }
        fn fold(parent_kind: Kind, frame: &mut Frame, child: Option<u64>) {
            match child {
                None => frame.unbounded = true,
                Some(v) => {
                    frame.acc = if parent_kind == Kind::Record {
                        frame.acc.saturating_add(v)
                    } else {
                        frame.acc.max(v)
                    }
                }
            }
        }
        let n = self.nodes.len();
        let mut color = vec![Color::White; n];
        let mut result: Vec<Option<u64>> = vec![None; n];
        let mut stack: Vec<Frame> = Vec::new();

        for start in 0..n {
            if color[start] != Color::White {
                continue;
            }
            if let Some(l) = leaf(&self.nodes[start]) {
                result[start] = l;
                color[start] = Color::Black;
                continue;
            }
            color[start] = Color::Gray;
            stack.push(Frame {
                node: start as NodeId,
                next: 0,
                acc: 0,
                unbounded: false,
            });
            while let Some(top) = stack.len().checked_sub(1) {
                let id = stack[top].node;
                let kind = self.nodes[id as usize].kind;
                if stack[top].next < self.child_count(id) {
                    let c = self.child(id, stack[top].next);
                    stack[top].next += 1;
                    let known: Option<Option<u64>> = match color[c as usize] {
                        Color::Gray => Some(None),
                        Color::Black => Some(result[c as usize]),
                        Color::White => match leaf(&self.nodes[c as usize]) {
                            Some(l) => {
                                result[c as usize] = l;
                                color[c as usize] = Color::Black;
                                Some(l)
                            }
                            None => {
                                color[c as usize] = Color::Gray;
                                stack.push(Frame {
                                    node: c,
                                    next: 0,
                                    acc: 0,
                                    unbounded: false,
                                });
                                None
                            }
                        },
                    };
                    if let Some(v) = known {
                        fold(kind, &mut stack[top], v);
                    }
                    continue;
                }
                let frame = stack.pop().expect("stack is not empty");
                let total = if kind == Kind::Union {
                    frame.acc.saturating_add(10)
                } else {
                    frame.acc
                };
                let value = if frame.unbounded { None } else { Some(total) };
                result[id as usize] = value;
                color[id as usize] = Color::Black;
                if let Some(parent) = stack.last_mut() {
                    let pk = self.nodes[parent.node as usize].kind;
                    fold(pk, parent, value);
                }
            }
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Compiler
// ---------------------------------------------------------------------------

struct Compiler<'a> {
    doc: &'a JsonDoc,
    nodes: Vec<Node>,
    fields: Vec<Field>,
    names: HashMap<Box<str>, NodeId>,
    /// Shared nodes for bare primitive names, indexed by `Kind as usize - 0..8`.
    primitives: [Option<NodeId>; 8],
    max_depth: usize,
    name_bytes: usize,
    name_budget: usize,
}

enum Step {
    Start {
        json: JsonId,
        ns: Rc<str>,
        depth: usize,
    },
}

enum Frame {
    Record(RecordFrame),
    Array(NodeId),
    Map(NodeId),
    Union(UnionFrame),
}

struct RecordFrame {
    node: NodeId,
    fields_json: JsonId,
    next: usize,
    ns: Rc<str>,
    depth: usize,
    out: Vec<Field>,
    seen: HashSet<Box<str>>,
    current: Option<(Box<str>, JsonId)>,
}

struct UnionFrame {
    node: NodeId,
    branches_json: JsonId,
    next: usize,
    ns: Rc<str>,
    depth: usize,
    out: Vec<NodeId>,
    seen_unnamed: u16,
    seen_named: HashSet<NodeId>,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::SchemaInvalid, message)
}

fn depth_error() -> Error {
    Error::new(
        ErrorCode::SchemaDepth,
        "Avro schema nesting depth exceeds the limit",
    )
}

fn check_simple_name(name: &str) -> Result<()> {
    if name.is_empty() || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
        return Err(invalid(format!("Invalid name: {name}")));
    }
    Ok(())
}

fn check_namespace(ns: &str) -> Result<()> {
    if !ns.is_empty()
        && (ns.starts_with('.')
            || ns.ends_with('.')
            || !ns
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b'$'))
    {
        return Err(invalid(format!("Invalid namespace: {ns}")));
    }
    Ok(())
}

/// Splits a full name into namespace and simple name.
fn split_full_name(full: &str) -> (&str, &str) {
    match full.rfind('.') {
        Some(i) => (&full[..i], &full[i + 1..]),
        None => ("", full),
    }
}

/// Largest decimal precision that a `fixed` of `size` bytes can hold, as in the
/// specification's reference implementation.
fn max_fixed_precision(size: u32) -> u64 {
    (std::f64::consts::LOG10_2 * (8.0 * f64::from(size) - 1.0)).floor() as u64
}

impl Compiler<'_> {
    fn compile(&mut self, root: JsonId) -> Result<NodeId> {
        let doc = self.doc;
        let mut stack: Vec<Frame> = Vec::new();
        let mut step = Some(Step::Start {
            json: root,
            ns: Rc::from(""),
            depth: 0,
        });
        let mut returned: NodeId;
        loop {
            if let Some(Step::Start { json, ns, depth }) = step.take() {
                if depth > self.max_depth {
                    return Err(depth_error());
                }
                match doc.get(json) {
                    JsonValue::Str(s) => returned = self.resolve_reference(s, &ns)?,
                    JsonValue::Object(_) => {
                        match self.start_object(json, &ns, depth, &mut stack)? {
                            Started::Done(id) => returned = id,
                            Started::Child(next) => {
                                step = Some(next);
                                continue;
                            }
                        }
                    }
                    JsonValue::Array(branches) => {
                        let node = self.alloc(Node::new(Kind::Union, None, Body::None));
                        if branches.is_empty() {
                            self.nodes[node as usize].body = Body::Union(Vec::new());
                            returned = node;
                        } else {
                            let first = branches[0];
                            stack.push(Frame::Union(UnionFrame {
                                node,
                                branches_json: json,
                                next: 1,
                                ns: ns.clone(),
                                depth,
                                out: Vec::new(),
                                seen_unnamed: 0,
                                seen_named: HashSet::new(),
                            }));
                            step = Some(Step::Start {
                                json: first,
                                ns,
                                depth: depth + 1,
                            });
                            continue;
                        }
                    }
                    other => {
                        return Err(invalid(format!("Invalid Avro type: {}", describe(other))))
                    }
                }
            } else {
                unreachable!("compile loop always has a step or returns");
            }

            // A schema finished with `returned`; deliver it to the frame waiting for it.
            loop {
                let Some(frame) = stack.pop() else {
                    return Ok(returned);
                };
                match frame {
                    Frame::Array(node) => {
                        self.nodes[node as usize].body = Body::Array(returned);
                        returned = node;
                    }
                    Frame::Map(node) => {
                        self.nodes[node as usize].body = Body::Map(returned);
                        returned = node;
                    }
                    Frame::Union(mut f) => {
                        let kind = self.nodes[returned as usize].kind;
                        if kind == Kind::Union {
                            return Err(invalid("Unions may not immediately contain other unions"));
                        }
                        if kind.is_named() {
                            if !f.seen_named.insert(returned) {
                                let name = self.nodes[returned as usize].full_name().unwrap_or("");
                                return Err(invalid(format!("Duplicate branch in union: {name}")));
                            }
                        } else {
                            let bit = 1u16 << (kind as u8);
                            if f.seen_unnamed & bit != 0 {
                                return Err(invalid(format!(
                                    "Duplicate type in union: {}",
                                    kind.name()
                                )));
                            }
                            f.seen_unnamed |= bit;
                        }
                        f.out.push(returned);
                        let JsonValue::Array(branches) = doc.get(f.branches_json) else {
                            unreachable!("union frame points at an array");
                        };
                        if f.next < branches.len() {
                            let json = branches[f.next];
                            f.next += 1;
                            let (ns, depth) = (f.ns.clone(), f.depth + 1);
                            stack.push(Frame::Union(f));
                            step = Some(Step::Start { json, ns, depth });
                            break;
                        }
                        let node = f.node;
                        self.nodes[node as usize].body = Body::Union(f.out);
                        returned = node;
                    }
                    Frame::Record(mut f) => {
                        let (name, json) =
                            f.current.take().expect("record frame has a current field");
                        f.out.push(Field {
                            name,
                            ty: returned,
                            json,
                        });
                        match self.next_field(&mut f)? {
                            Some(next) => {
                                stack.push(Frame::Record(f));
                                step = Some(next);
                                break;
                            }
                            None => {
                                returned = self.finish_record(f);
                            }
                        }
                    }
                }
            }
        }
    }

    fn alloc(&mut self, node: Node) -> NodeId {
        let id = self.nodes.len() as NodeId;
        self.nodes.push(node);
        id
    }

    fn charge_name(&mut self, len: usize) -> Result<()> {
        self.name_bytes += len;
        if self.name_bytes > self.name_budget {
            return Err(Error::new(
                ErrorCode::SchemaTooLarge,
                "Schema names exceed the total size limit",
            ));
        }
        Ok(())
    }

    fn primitive_node(&mut self, kind: Kind) -> NodeId {
        if let Some(id) = self.primitives[kind as usize] {
            return id;
        }
        let id = self.alloc(Node::new(kind, None, Body::None));
        self.primitives[kind as usize] = Some(id);
        id
    }

    /// A bare string: a primitive name or a reference to a named type.
    fn resolve_reference(&mut self, s: &str, ns: &str) -> Result<NodeId> {
        if let Some(kind) = Kind::from_primitive(s) {
            return Ok(self.primitive_node(kind));
        }
        if s.contains('.') || ns.is_empty() {
            return match self.names.get(s) {
                Some(&id) => Ok(id),
                None => Err(invalid(format!("Unknown type: {s}"))),
            };
        }
        // An unqualified name is looked up in the enclosing namespace first and then in the
        // null namespace, as the Java reference implementation does.
        let full = format!("{ns}.{s}");
        match self.names.get(full.as_str()).or_else(|| self.names.get(s)) {
            Some(&id) => Ok(id),
            None => Err(invalid(format!("Unknown type: {full}"))),
        }
    }

    /// Computes the full name of the type defined by `object` (`name`,
    /// `namespace`, and the enclosing namespace, as in the specification).
    fn resolve_name(&mut self, object: JsonId, enclosing: &str) -> Result<Box<str>> {
        let doc = self.doc;
        let name = match doc.object_get(object, "name") {
            None => return Err(invalid("Missing Json field \"name\"")),
            Some(v) => doc
                .as_str(v)
                .ok_or_else(|| invalid("Json field \"name\" is not a string"))?,
        };
        let (ns, simple) = if name.contains('.') {
            // A dotted name is a full name: `namespace` and the enclosing one are ignored.
            split_full_name(name)
        } else {
            let ns = match doc.object_get(object, "namespace") {
                Some(v) => doc
                    .as_str(v)
                    .ok_or_else(|| invalid("Json field \"namespace\" is not a string"))?,
                None => enclosing,
            };
            (ns, name)
        };
        // No per-name length bound: the legacy C++ compiler accepts any length, and the total
        // name budget (`charge_name`) already bounds all names of the schema together.
        check_namespace(ns)?;
        check_simple_name(simple)?;
        let full: Box<str> = if ns.is_empty() {
            simple.into()
        } else {
            format!("{ns}.{simple}").into()
        };
        if Kind::from_primitive(&full).is_some() {
            return Err(invalid(format!(
                "Schemas may not be named after primitives: {full}"
            )));
        }
        self.charge_name(full.len())?;
        Ok(full)
    }

    fn register(&mut self, full: &str, id: NodeId) -> Result<()> {
        if self.names.insert(full.into(), id).is_some() {
            return Err(invalid(format!("Can't redefine: {full}")));
        }
        Ok(())
    }

    fn start_object(
        &mut self,
        json: JsonId,
        ns: &Rc<str>,
        depth: usize,
        stack: &mut Vec<Frame>,
    ) -> Result<Started> {
        let doc = self.doc;
        let type_name = match doc.object_get(json, "type") {
            None => return Err(invalid("Missing Json field \"type\"")),
            Some(v) => doc
                .as_str(v)
                .ok_or_else(|| invalid("Json field \"type\" is not a string"))?,
        };
        match type_name {
            "record" | "error" => {
                let full = self.resolve_name(json, ns)?;
                let fields_json = match doc.object_get(json, "fields") {
                    None => return Err(invalid("Missing Json field \"fields\"")),
                    Some(v) => v,
                };
                if !matches!(doc.get(fields_json), JsonValue::Array(_)) {
                    return Err(invalid("Json field \"fields\" is not an array"));
                }
                let simple_start =
                    (split_full_name(&full).0.len() + usize::from(full.contains('.'))) as u32;
                let rec_ns: Rc<str> = Rc::from(split_full_name(&full).0);
                let mut node = Node::new(Kind::Record, Some(json), Body::None);
                node.fullname = Some(full.clone());
                node.simple_start = simple_start;
                let id = self.alloc(node);
                self.register(&full, id)?;
                let mut frame = RecordFrame {
                    node: id,
                    fields_json,
                    next: 0,
                    ns: rec_ns,
                    depth: depth + 1,
                    out: Vec::new(),
                    seen: HashSet::new(),
                    current: None,
                };
                match self.next_field(&mut frame)? {
                    Some(step) => {
                        stack.push(Frame::Record(frame));
                        Ok(Started::Child(step))
                    }
                    None => Ok(Started::Done(self.finish_record(frame))),
                }
            }
            "enum" => {
                let full = self.resolve_name(json, ns)?;
                let symbols_json = doc
                    .object_get(json, "symbols")
                    .ok_or_else(|| invalid("Missing Json field \"symbols\""))?;
                let JsonValue::Array(items) = doc.get(symbols_json) else {
                    return Err(invalid("Json field \"symbols\" is not an array"));
                };
                if items.is_empty() {
                    return Err(invalid("Enum has no symbols"));
                }
                let mut symbols: Vec<Box<str>> = Vec::with_capacity(items.len());
                let mut seen: HashSet<&str> = HashSet::with_capacity(items.len());
                for &it in items {
                    let Some(s) = doc.as_str(it) else {
                        return Err(invalid("Enum symbol not a string"));
                    };
                    if !seen.insert(s) {
                        return Err(invalid(format!("Cannot add duplicate enum: {s}")));
                    }
                    symbols.push(s.into());
                }
                let simple_start =
                    (split_full_name(&full).0.len() + usize::from(full.contains('.'))) as u32;
                let mut node = Node::new(Kind::Enum, Some(json), Body::Enum(symbols));
                node.fullname = Some(full.clone());
                node.simple_start = simple_start;
                let id = self.alloc(node);
                self.register(&full, id)?;
                Ok(Started::Done(id))
            }
            "fixed" => {
                let full = self.resolve_name(json, ns)?;
                let size_json = doc
                    .object_get(json, "size")
                    .ok_or_else(|| invalid("Missing Json field \"size\""))?;
                let size = doc
                    .as_int(size_json)
                    .ok_or_else(|| invalid("Json field \"size\" is not a long"))?;
                if size <= 0 || size > i64::from(i32::MAX) {
                    return Err(invalid(format!(
                        "Size for fixed is not positive or too large: {size}"
                    )));
                }
                let simple_start =
                    (split_full_name(&full).0.len() + usize::from(full.contains('.'))) as u32;
                let mut node = Node::new(Kind::Fixed, Some(json), Body::None);
                node.fixed_size = size as u32;
                node.fullname = Some(full.clone());
                node.simple_start = simple_start;
                self.apply_logical(&mut node, json);
                let id = self.alloc(node);
                self.register(&full, id)?;
                Ok(Started::Done(id))
            }
            "array" | "map" => {
                let (key, kind) = if type_name == "array" {
                    ("items", Kind::Array)
                } else {
                    ("values", Kind::Map)
                };
                let child = doc
                    .object_get(json, key)
                    .ok_or_else(|| invalid(format!("Missing Json field \"{key}\"")))?;
                let id = self.alloc(Node::new(kind, Some(json), Body::None));
                stack.push(if kind == Kind::Array {
                    Frame::Array(id)
                } else {
                    Frame::Map(id)
                });
                Ok(Started::Child(Step::Start {
                    json: child,
                    ns: ns.clone(),
                    depth: depth + 1,
                }))
            }
            other => {
                let Some(kind) = Kind::from_primitive(other) else {
                    return Err(invalid(format!("Unknown type definition: {other}")));
                };
                let mut node = Node::new(kind, Some(json), Body::None);
                self.apply_logical(&mut node, json);
                Ok(Started::Done(self.alloc(node)))
            }
        }
    }

    /// Validates the next field of `f` and returns the step that compiles its
    /// type, or `None` when all fields were consumed.
    fn next_field(&mut self, f: &mut RecordFrame) -> Result<Option<Step>> {
        let doc = self.doc;
        let JsonValue::Array(items) = doc.get(f.fields_json) else {
            unreachable!("record frame points at an array");
        };
        let Some(&field_json) = items.get(f.next) else {
            return Ok(None);
        };
        f.next += 1;
        if !matches!(doc.get(field_json), JsonValue::Object(_)) {
            return Err(invalid("Record field is not an object"));
        }
        let name = match doc.object_get(field_json, "name") {
            None => return Err(invalid("Missing Json field \"name\"")),
            Some(v) => doc
                .as_str(v)
                .ok_or_else(|| invalid("Json field \"name\" is not a string"))?,
        };
        let ty = doc
            .object_get(field_json, "type")
            .ok_or_else(|| invalid("Missing Json field \"type\""))?;
        if !f.seen.insert(name.into()) {
            return Err(invalid(format!("Cannot add duplicate field: {name}")));
        }
        self.charge_name(name.len())?;
        f.current = Some((name.into(), field_json));
        Ok(Some(Step::Start {
            json: ty,
            ns: f.ns.clone(),
            depth: f.depth,
        }))
    }

    fn finish_record(&mut self, f: RecordFrame) -> NodeId {
        let first = self.fields.len() as u32;
        let count = f.out.len() as u32;
        self.fields.extend(f.out);
        self.nodes[f.node as usize].body = Body::Record { first, count };
        f.node
    }

    /// Reads `logicalType` from the defining object and applies it when it is
    /// valid for the node; anything else leaves the node without one.
    fn apply_logical(&self, node: &mut Node, json: JsonId) {
        let doc = self.doc;
        let Some(lt) = doc
            .object_get(json, "logicalType")
            .and_then(|v| doc.as_str(v))
        else {
            return;
        };
        let size = node.fixed_size;
        node.logical = match (lt, node.kind) {
            ("decimal", Kind::Bytes | Kind::Fixed) => {
                let precision = doc
                    .object_get(json, "precision")
                    .and_then(|v| doc.as_int(v));
                let scale = match doc.object_get(json, "scale") {
                    None => Some(0),
                    Some(v) => doc.as_int(v),
                };
                match (precision, scale) {
                    (Some(p), Some(s))
                        if p > 0
                            && p <= i64::from(u32::MAX)
                            && (0..=p).contains(&s)
                            && (node.kind == Kind::Bytes
                                || p as u64 <= max_fixed_precision(size)) =>
                    {
                        node.precision = p as u32;
                        node.scale = s as u32;
                        Logical::Decimal
                    }
                    _ => Logical::None,
                }
            }
            ("big-decimal", Kind::Bytes) => Logical::BigDecimal,
            ("uuid", Kind::String) => Logical::Uuid,
            ("uuid", Kind::Fixed) if size == 16 => Logical::Uuid,
            ("date", Kind::Int) => Logical::Date,
            ("time-millis", Kind::Int) => Logical::TimeMillis,
            ("time-micros", Kind::Long) => Logical::TimeMicros,
            ("timestamp-millis", Kind::Long) => Logical::TimestampMillis,
            ("timestamp-micros", Kind::Long) => Logical::TimestampMicros,
            ("timestamp-nanos", Kind::Long) => Logical::TimestampNanos,
            ("local-timestamp-millis", Kind::Long) => Logical::LocalTimestampMillis,
            ("local-timestamp-micros", Kind::Long) => Logical::LocalTimestampMicros,
            ("local-timestamp-nanos", Kind::Long) => Logical::LocalTimestampNanos,
            ("duration", Kind::Fixed) if size == 12 => Logical::Duration,
            _ => Logical::None,
        };
    }
}

enum Started {
    Done(NodeId),
    Child(Step),
}

impl Node {
    fn new(kind: Kind, json: Option<JsonId>, body: Body) -> Node {
        Node {
            kind,
            logical: Logical::None,
            precision: 0,
            scale: 0,
            fixed_size: 0,
            fullname: None,
            simple_start: 0,
            json,
            body,
        }
    }
}

fn describe(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Int(_) | JsonValue::Float(_) => "number",
        JsonValue::Str(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Schema> {
        Schema::parse(s.as_bytes(), 256)
    }

    #[test]
    fn primitives_and_forms() {
        for p in [
            "null", "boolean", "int", "long", "float", "double", "bytes", "string",
        ] {
            let s = parse(&format!("\"{p}\"")).unwrap();
            assert_eq!(s.kind(s.root()).name(), p);
            let s = parse(&format!("{{\"type\":\"{p}\"}}")).unwrap();
            assert_eq!(s.kind(s.root()).name(), p);
        }
        assert!(parse("\"integer\"").is_err());
        assert!(parse("{\"type\":{\"type\":\"long\"}}").is_err());
        assert!(parse("5").is_err());
        assert!(parse("{\"type\":\"int\"} x").is_err());
    }

    #[test]
    fn named_types_namespaces_and_references() {
        let s = parse(
            r#"{"type":"record","name":"R","namespace":"a.b","fields":[
                {"name":"x","type":{"type":"enum","name":"E","symbols":["A","B"]}},
                {"name":"y","type":"E"},
                {"name":"z","type":{"type":"fixed","name":"c.F","size":4}},
                {"name":"w","type":"c.F"},
                {"name":"self","type":["null","R"]}]}"#,
        )
        .unwrap();
        let r = s.root();
        assert_eq!(s.node(r).full_name(), Some("a.b.R"));
        assert_eq!(s.node(r).namespace(), Some("a.b"));
        assert_eq!(s.node(r).simple_name(), Some("R"));
        let f = s.fields(r);
        assert_eq!(f[0].ty, f[1].ty, "reference resolves to the definition");
        assert_eq!(s.node(f[0].ty).full_name(), Some("a.b.E"));
        assert_eq!(s.node(f[2].ty).full_name(), Some("c.F"));
        assert_eq!(f[2].ty, f[3].ty);
        let u = s.branches(f[4].ty);
        assert_eq!(u[1], r, "recursion is an edge back to the record");
    }

    #[test]
    fn rejects_bad_references_and_duplicates() {
        // Forward reference.
        assert!(parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"E"},{"name":"b","type":{"type":"enum","name":"E","symbols":["A"]}}]}"#).is_err());
        // Unknown reference.
        assert!(
            parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"Nope"}]}"#).is_err()
        );
        // Redefinition.
        assert!(parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":1}},{"name":"b","type":{"type":"fixed","name":"F","size":2}}]}"#).is_err());
        // Same simple name in different namespaces is fine.
        assert!(parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","namespace":"x","size":1}},{"name":"b","type":{"type":"fixed","name":"F","namespace":"y","size":2}}]}"#).is_ok());
        // Duplicate fields, symbols; empty enum; bad sizes; primitive names.
        assert!(parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"a","type":"int"}]}"#).is_err());
        assert!(parse(r#"{"type":"enum","name":"E","symbols":["A","A"]}"#).is_err());
        assert!(parse(r#"{"type":"enum","name":"E","symbols":[]}"#).is_err());
        assert!(parse(r#"{"type":"enum","name":"E","symbols":[1]}"#).is_err());
        assert!(parse(r#"{"type":"fixed","name":"F","size":0}"#).is_err());
        assert!(parse(r#"{"type":"fixed","name":"F","size":-1}"#).is_err());
        assert!(parse(r#"{"type":"fixed","name":"F","size":4294967301}"#).is_err());
        assert!(parse(r#"{"type":"fixed","name":"F","size":"4"}"#).is_err());
        assert!(parse(r#"{"type":"record","name":"int","fields":[]}"#).is_err());
        assert!(parse(r#"{"type":"record","name":"","fields":[]}"#).is_err());
        assert!(parse(r#"{"type":"record","name":"a-b","fields":[]}"#).is_err());
        assert!(parse(r#"{"type":"record","name":"a","namespace":".x","fields":[]}"#).is_err());
        assert!(parse(r#"{"type":"record","name":"a"}"#).is_err());
        assert!(parse(r#"{"type":"array"}"#).is_err());
        assert!(parse(r#"{"type":"map"}"#).is_err());
    }

    #[test]
    fn union_rules() {
        assert!(parse(r#"["null","string"]"#).is_ok());
        assert!(parse(r#"[]"#).is_ok());
        assert!(parse(r#"["string","string"]"#).is_err());
        assert!(parse(r#"["int",{"type":"int","logicalType":"date"}]"#).is_err());
        assert!(
            parse(r#"[{"type":"array","items":"int"},{"type":"array","items":"string"}]"#).is_err()
        );
        assert!(
            parse(r#"[{"type":"map","values":"int"},{"type":"map","values":"string"}]"#).is_err()
        );
        assert!(parse(r#"["null",["int"]]"#).is_err());
        // Distinct named types of the same kind are allowed.
        assert!(parse(
            r#"[{"type":"fixed","name":"A","size":1},{"type":"fixed","name":"B","size":1}]"#
        )
        .is_ok());
        // The same named type twice is not.
        assert!(parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"A","size":1}},{"name":"b","type":["A","A"]}]}"#).is_err());
    }

    #[test]
    fn logical_types() {
        let l = |s: &str| {
            let sc = parse(s).unwrap();
            let n = sc.node(sc.root());
            (n.logical, n.precision, n.scale)
        };
        assert_eq!(
            l(r#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"#),
            (Logical::Decimal, 10, 2)
        );
        assert_eq!(
            l(r#"{"type":"bytes","logicalType":"decimal","precision":10}"#),
            (Logical::Decimal, 10, 0)
        );
        assert_eq!(
            l(r#"{"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":18}"#),
            (Logical::Decimal, 18, 0)
        );
        // Invalid logical types are ignored, not errors.
        assert_eq!(
            l(r#"{"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":19}"#).0,
            Logical::None
        );
        assert_eq!(
            l(r#"{"type":"bytes","logicalType":"decimal","precision":3,"scale":4}"#).0,
            Logical::None
        );
        assert_eq!(
            l(r#"{"type":"bytes","logicalType":"decimal"}"#).0,
            Logical::None
        );
        assert_eq!(
            l(r#"{"type":"bytes","logicalType":"decimal","precision":0}"#).0,
            Logical::None
        );
        assert_eq!(
            l(r#"{"type":"bytes","logicalType":"decimal","precision":"5"}"#).0,
            Logical::None
        );
        assert_eq!(
            l(r#"{"type":"long","logicalType":"date"}"#).0,
            Logical::None
        );
        assert_eq!(
            l(r#"{"type":"string","logicalType":"made-up"}"#).0,
            Logical::None
        );
        assert_eq!(
            l(r#"{"type":"string","logicalType":"uuid"}"#).0,
            Logical::Uuid
        );
        assert_eq!(l(r#"{"type":"int","logicalType":"date"}"#).0, Logical::Date);
        assert_eq!(
            l(r#"{"type":"int","logicalType":"time-millis"}"#).0,
            Logical::TimeMillis
        );
        assert_eq!(
            l(r#"{"type":"long","logicalType":"time-micros"}"#).0,
            Logical::TimeMicros
        );
        assert_eq!(
            l(r#"{"type":"long","logicalType":"timestamp-millis"}"#).0,
            Logical::TimestampMillis
        );
        assert_eq!(
            l(r#"{"type":"long","logicalType":"timestamp-micros"}"#).0,
            Logical::TimestampMicros
        );
        assert_eq!(
            l(r#"{"type":"long","logicalType":"timestamp-nanos"}"#).0,
            Logical::TimestampNanos
        );
        assert_eq!(
            l(r#"{"type":"long","logicalType":"local-timestamp-millis"}"#).0,
            Logical::LocalTimestampMillis
        );
        assert_eq!(
            l(r#"{"type":"long","logicalType":"local-timestamp-micros"}"#).0,
            Logical::LocalTimestampMicros
        );
        assert_eq!(
            l(r#"{"type":"long","logicalType":"local-timestamp-nanos"}"#).0,
            Logical::LocalTimestampNanos
        );
        assert_eq!(
            l(r#"{"type":"fixed","name":"D","size":12,"logicalType":"duration"}"#).0,
            Logical::Duration
        );
        assert_eq!(
            l(r#"{"type":"fixed","name":"D","size":11,"logicalType":"duration"}"#).0,
            Logical::None
        );
        // The invalid annotation does not make the schema invalid and the raw attribute stays readable.
        let sc =
            parse(r#"{"type":"array","items":"int","logicalType":"map","element-id":7}"#).unwrap();
        assert_eq!(
            sc.node_attr(sc.root(), "logicalType"),
            Some(AttrValue::Str("map"))
        );
        assert_eq!(
            sc.node_attr(sc.root(), "element-id"),
            Some(AttrValue::Int(7))
        );
    }

    #[test]
    fn attributes_are_retained() {
        let s = parse(
            r#"{"type":"record","name":"R","fields":[
                {"name":"a","type":"int","field-id":42,"doc":"x","default":1,"aliases":["b"]},
                {"name":"b","type":"string"}]}"#,
        )
        .unwrap();
        assert_eq!(
            s.field_attr(s.root(), 0, "field-id"),
            Some(AttrValue::Int(42))
        );
        assert_eq!(s.field_attr(s.root(), 0, "doc"), Some(AttrValue::Str("x")));
        assert_eq!(
            s.field_attr(s.root(), 0, "aliases"),
            Some(AttrValue::Composite)
        );
        assert_eq!(s.field_attr(s.root(), 1, "field-id"), None);
        assert_eq!(s.field_attr(s.root(), 9, "field-id"), None);
    }

    #[test]
    fn depth_semantics_match_the_fork() {
        // `{"type":"array","items":X}` puts X at depth + 1, in JSON and in the schema.
        let nest = |n: usize| {
            let mut s = String::from("\"int\"");
            for _ in 0..n {
                s = format!("{{\"type\":\"array\",\"items\":{s}}}");
            }
            s
        };
        // Three arrays: "int" is at depth 3.
        assert!(Schema::parse(nest(3).as_bytes(), 3).is_ok());
        let e = Schema::parse(nest(3).as_bytes(), 2).unwrap_err();
        assert_eq!(e.message, "JSON nesting depth exceeds the limit");
        // Unlimited: deep nesting is fine and does not overflow the native stack.
        let deep = Schema::parse(nest(20_000).as_bytes(), 0).unwrap();
        assert_eq!(deep.node_count(), 20_001);
        // Record fields are at depth + 1 in the schema and 3 JSON levels below the record.
        let rec = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#;
        assert!(Schema::parse(rec.as_bytes(), 3).is_ok());
        assert_eq!(
            Schema::parse(rec.as_bytes(), 2).unwrap_err().code,
            ErrorCode::JsonDepth
        );
    }

    #[test]
    fn schema_layer_depth_error() {
        // With the JSON limit out of the way the schema layer reports its own error.
        let json = JsonLimits::default();
        let two = r#"{"type":"array","items":{"type":"array","items":"int"}}"#;
        // Inner "int" is at schema depth 2.
        assert!(Schema::parse_with(two.as_bytes(), 2, json).is_ok());
        let e = Schema::parse_with(two.as_bytes(), 1, json).unwrap_err();
        assert_eq!(e.message, "Avro schema nesting depth exceeds the limit");
        assert_eq!(e.code, ErrorCode::SchemaDepth);
        // Every union branch is one level deeper than the union, bare names included.
        assert!(Schema::parse_with(br#"["null","int"]"#, 1, json).is_ok());
        let e =
            Schema::parse_with(br#"{"type":"array","items":["null","int"]}"#, 1, json).unwrap_err();
        assert_eq!(e.code, ErrorCode::SchemaDepth);
        // The field type of a record is one level below the record.
        let rec = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#;
        assert!(Schema::parse_with(rec.as_bytes(), 1, json).is_ok());
        assert_eq!(
            Schema::parse_with(rec.as_bytes(), 0, json).map(|_| ()),
            Ok(())
        );
    }

    #[test]
    fn encoded_sizes() {
        let s = parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"double"},{"name":"c","type":["null","string"]}]}"#).unwrap();
        // int 1 + double 8 + union (1 + min(null 0, string 1)) = 10.
        assert_eq!(s.min_encoded_size(s.root()), 10);
        assert_eq!(s.max_encoded_size(s.root()), None);
        let s = parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"double"},{"name":"c","type":["null","long"]}]}"#).unwrap();
        assert_eq!(s.min_encoded_size(s.root()), 1 + 8 + 1);
        assert_eq!(s.max_encoded_size(s.root()), Some(5 + 8 + 10 + 10));
        let s = parse(r#"{"type":"record","name":"E","fields":[]}"#).unwrap();
        assert_eq!(s.min_encoded_size(s.root()), 0);
        assert_eq!(s.max_encoded_size(s.root()), Some(0));
        // Recursive: the minimum goes through the non-recursive branch.
        let s = parse(r#"{"type":"record","name":"L","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","L"]}]}"#).unwrap();
        assert_eq!(s.min_encoded_size(s.root()), 1 + 1);
        assert_eq!(s.max_encoded_size(s.root()), None);
        // Uninhabitable: the only way out is through itself.
        let s = parse(
            r#"{"type":"record","name":"L","fields":[{"name":"v","type":"long"},{"name":"next","type":"L2"}]}"#,
        );
        assert!(s.is_err());
        let s = parse(r#"{"type":"record","name":"L","fields":[{"name":"next","type":{"type":"array","items":"L"}}]}"#).unwrap();
        assert_eq!(s.min_encoded_size(s.root()), 1);
        let s = parse(r#"{"type":"fixed","name":"F","size":16}"#).unwrap();
        assert_eq!(s.min_encoded_size(s.root()), 16);
        assert_eq!(s.max_encoded_size(s.root()), Some(16));
    }

    #[test]
    fn namespace_rules_and_the_error_type() {
        // A dotted name is a full name: `namespace` and the enclosing namespace are ignored.
        let s = parse(
            r#"{"type":"record","name":"a.b.R","namespace":"ignored","fields":[
                {"name":"x","type":{"type":"fixed","name":"F","size":1}}]}"#,
        )
        .unwrap();
        assert_eq!(s.node(s.root()).full_name(), Some("a.b.R"));
        assert_eq!(s.node(s.fields(s.root())[0].ty).full_name(), Some("a.b.F"));
        // An empty `namespace` puts the type in the null namespace, and its children inherit that.
        let s = parse(
            r#"{"type":"record","name":"R","namespace":"outer","fields":[
                {"name":"x","type":{"type":"record","name":"In","namespace":"","fields":[
                    {"name":"y","type":{"type":"fixed","name":"G","size":2}}]}}]}"#,
        )
        .unwrap();
        let inner = s.fields(s.root())[0].ty;
        assert_eq!(s.node(inner).full_name(), Some("In"));
        assert_eq!(s.node(s.fields(inner)[0].ty).full_name(), Some("G"));
        // A bare reference resolves in the enclosing namespace first, then in the null namespace.
        let s = parse(
            r#"{"type":"record","name":"R","namespace":"n","fields":[
                {"name":"a","type":{"type":"fixed","name":"F","namespace":"","size":1}},
                {"name":"b","type":{"type":"fixed","name":"F","size":2}},
                {"name":"c","type":"F"}]}"#,
        )
        .unwrap();
        let f = s.fields(s.root());
        assert_eq!(f[2].ty, f[1].ty, "n.F wins over the null-namespace F");
        // `error` is a record for decoding purposes.
        let s = parse(r#"{"type":"error","name":"E","fields":[{"name":"m","type":"string"}]}"#)
            .unwrap();
        assert_eq!(s.kind(s.root()), Kind::Record);
        // Names must not start or end the namespace with a dot, or contain other characters.
        for bad in ["a b", "a-b", "", "a\u{e9}"] {
            let j = format!(r#"{{"type":"fixed","name":"{bad}","size":1}}"#);
            assert!(Schema::parse(j.as_bytes(), 0).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn in_degree_marks_shared_types() {
        let s = parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":1}},{"name":"b","type":"F"}]}"#).unwrap();
        let f = s.fields(s.root());
        assert_eq!(s.in_degree(f[0].ty), 2);
        assert_eq!(s.in_degree(s.root()), 1);
    }

    #[test]
    fn long_names_are_accepted() {
        // The legacy C++ compiler bounds only the characters of a name, not its length: a 4 KiB
        // record name, a long namespace and a long enum/fixed name are valid schemas.
        let long = "r".repeat(4096);
        let j = format!(
            r#"{{"type":"record","name":"{long}","fields":[
                {{"name":"id","type":"long"}},
                {{"name":"e","type":{{"type":"enum","name":"{long}.E","symbols":["A"]}}}},
                {{"name":"f","type":{{"type":"fixed","name":"{long}.F","size":1}}}}
            ]}}"#
        );
        let s = Schema::parse(j.as_bytes(), 0).unwrap();
        assert_eq!(s.node(s.root()).full_name().unwrap().len(), 4096);
    }

    #[test]
    fn name_bytes_are_budgeted() {
        // 100 nested records inheriting a 4000 byte namespace: names are copied per record, so the
        // total is charged against the budget instead of being unbounded.
        let ns = "a".repeat(4000);
        let mut s = String::from("\"int\"");
        for i in 0..100 {
            s = format!(
                r#"{{"type":"record","name":"r{i}","fields":[{{"name":"f","type":{s}}}]}}"#
            );
        }
        let s = s.replacen(
            "\"name\":\"r99\"",
            &format!("\"namespace\":\"{ns}\",\"name\":\"r99\""),
            1,
        );
        let limits = JsonLimits {
            max_bytes: 1 << 16,
            ..JsonLimits::default()
        };
        assert_eq!(
            Schema::parse_with(s.as_bytes(), 0, limits)
                .unwrap_err()
                .code,
            ErrorCode::SchemaTooLarge
        );
    }
}
