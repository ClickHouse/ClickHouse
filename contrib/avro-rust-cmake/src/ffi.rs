#![allow(unsafe_code)]
// The safety contract of every entry point is the one in `include/avro_rust.h`.
#![allow(clippy::missing_safety_doc)]
//! The C ABI. This is the only module that handles raw pointers; its contract
//! is documented in `include/avro_rust.h`, which is hand written and must be
//! kept in sync (the layout tests at the bottom pin the struct sizes).
//!
//! Conventions:
//!
//! * Every entry point returns an `int32_t`: `0` success, positive statuses
//!   (`1` need more input, `2` clean end, `3` not found) and `-code` for an
//!   error, where `code` is a [`crate::ErrorCode`]. The message goes to the
//!   caller's `err` buffer (NUL terminated, truncated to fit).
//! * Every entry point catches panics; a panic is reported as error
//!   `Internal` with the panic message and never unwinds into C++.
//! * Handles are owned by the caller and released with the matching `*_free`.
//!   Strings handed out point into a handle and live as long as it does.
//! * Nothing here is thread-hostile: handles are plain data and may be moved
//!   between threads, but one handle must not be used from two threads at
//!   once (`AvroState`), or mutated while shared (nothing is mutable except
//!   states and sinks).

use std::ffi::c_char;
use std::mem::MaybeUninit;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;

use crate::exec::{self, Sink, State, Status};
use crate::ocf::{self, BlockCtx, BlockHeader, Codec, Header, OcfLimits, Parsed};
use crate::program::{Action, PlanNode, Program, SinkDesc, SinkKind};
use crate::schema::{AttrValue, Schema};
use crate::{Error, ErrorCode, Result};

pub const AVRO_OK: i32 = 0;
pub const AVRO_NEED_MORE: i32 = 1;
pub const AVRO_END: i32 = 2;
pub const AVRO_NOT_FOUND: i32 = 3;

// Opaque handle types: C only ever sees pointers to these.
#[repr(C)]
pub struct AvroSchema {
    _private: [u8; 0],
}
#[repr(C)]
pub struct AvroProgram {
    _private: [u8; 0],
}
#[repr(C)]
pub struct AvroState {
    _private: [u8; 0],
}
#[repr(C)]
pub struct AvroOcfHeader {
    _private: [u8; 0],
}

fn write_error(err: *mut c_char, cap: usize, message: &str) {
    if err.is_null() || cap == 0 {
        return;
    }
    let mut n = message.len().min(cap - 1);
    while !message.is_char_boundary(n) {
        n -= 1;
    }
    // SAFETY: the caller promises `err` points to `cap` writable bytes and `n < cap`.
    unsafe {
        std::ptr::copy_nonoverlapping(message.as_ptr(), err as *mut u8, n);
        *err.add(n) = 0;
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    format!("panic in the Avro decoder: {message}")
}

/// Runs `f`, converting an error or a panic into a negative status and a message.
fn wrap(err: *mut c_char, cap: usize, f: impl FnOnce() -> Result<i32>) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            write_error(err, cap, &e.message);
            -(e.code as i32)
        }
        Err(p) => {
            write_error(err, cap, &panic_message(p.as_ref()));
            -(ErrorCode::Internal as i32)
        }
    }
}

fn invalid(what: &str) -> Error {
    Error::new(ErrorCode::InvalidArgument, what)
}

/// `n` bytes at `p`; `n == 0` may come with a null pointer.
///
/// SAFETY: when `n > 0`, `p` must point to `n` readable bytes that stay valid and
/// unmodified for `'a`.
unsafe fn bytes<'a>(p: *const u8, n: usize) -> Result<&'a [u8]> {
    if n == 0 {
        return Ok(&[]);
    }
    if p.is_null() || n > isize::MAX as usize {
        return Err(invalid("null pointer or oversized length"));
    }
    Ok(slice::from_raw_parts(p, n))
}

/// `n` writable bytes at `p`, which may be uninitialized; `n == 0` may come with a null pointer.
///
/// The bytes are typed `MaybeUninit<u8>`: a `&mut [u8]` over bytes that were never written (the caller
/// resizes its `PODArray`s before the executor fills them) would be undefined behavior.
///
/// SAFETY: when `n > 0`, `p` must point to `n` writable bytes that stay valid for `'a` and are not
/// aliased. They need not be initialized.
unsafe fn bytes_uninit_mut<'a>(p: *mut u8, n: usize) -> Result<&'a mut [MaybeUninit<u8>]> {
    if n == 0 {
        return Ok(&mut []);
    }
    if p.is_null() || n > isize::MAX as usize {
        return Err(invalid("null pointer or oversized length"));
    }
    Ok(slice::from_raw_parts_mut(p as *mut MaybeUninit<u8>, n))
}

fn to_usize(v: u64) -> Result<usize> {
    usize::try_from(v).map_err(|_| invalid("length does not fit in the address space"))
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// SAFETY (all `avro_schema_*` functions): `schema` comes from `avro_schema_parse` and has not been freed.
unsafe fn schema_ref<'a>(schema: *const AvroSchema) -> Result<&'a Schema> {
    if schema.is_null() {
        return Err(invalid("null schema"));
    }
    Ok(&*(schema as *const Schema))
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_parse(
    json: *const u8,
    len: usize,
    max_depth: u32,
    out: *mut *mut AvroSchema,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        if out.is_null() {
            return Err(invalid("null out pointer"));
        }
        let text = bytes(json, len)?;
        let schema = Schema::parse(text, max_depth as usize)?;
        *out = Box::into_raw(Box::new(schema)) as *mut AvroSchema;
        Ok(AVRO_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_free(schema: *mut AvroSchema) {
    if !schema.is_null() {
        drop(Box::from_raw(schema as *mut Schema));
    }
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_root(schema: *const AvroSchema) -> u32 {
    schema_ref(schema).map(|s| s.root()).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_node_count(schema: *const AvroSchema) -> u32 {
    schema_ref(schema)
        .map(|s| s.node_count() as u32)
        .unwrap_or(0)
}

#[repr(C)]
pub struct AvroNodeInfo {
    pub kind: u8,
    pub logical: u8,
    /// 1 when `max_size` is meaningful.
    pub has_max_size: u8,
    pub reserved: u8,
    pub precision: u32,
    pub scale: u32,
    pub fixed_size: u32,
    /// Fields of a record, branches of a union, 1 for an array or a map.
    pub child_count: u32,
    pub symbol_count: u32,
    pub in_degree: u32,
    pub name: *const c_char,
    pub name_len: usize,
    /// Offset of the simple name inside `name`.
    pub simple_name_offset: u32,
    pub reserved2: u32,
    pub min_size: u64,
    pub max_size: u64,
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_node(
    schema: *const AvroSchema,
    node: u32,
    out: *mut AvroNodeInfo,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let s = schema_ref(schema)?;
        if out.is_null() || node as usize >= s.node_count() {
            return Err(invalid("null out pointer or node out of range"));
        }
        let n = s.node(node);
        let (name, name_len, simple) = match n.full_name() {
            Some(f) => (
                f.as_ptr() as *const c_char,
                f.len(),
                (f.len() - n.simple_name().map_or(0, str::len)) as u32,
            ),
            None => (std::ptr::null(), 0, 0),
        };
        let max = s.max_encoded_size(node);
        *out = AvroNodeInfo {
            kind: n.kind as u8,
            logical: n.logical as u8,
            has_max_size: u8::from(max.is_some()),
            reserved: 0,
            precision: n.precision,
            scale: n.scale,
            fixed_size: n.fixed_size,
            child_count: s.child_count(node) as u32,
            symbol_count: s.symbols(node).len() as u32,
            in_degree: s.in_degree(node),
            name,
            name_len,
            simple_name_offset: simple,
            reserved2: 0,
            min_size: s.min_encoded_size(node),
            max_size: max.unwrap_or(0),
        };
        Ok(AVRO_OK)
    })
}

/// The `i`-th child of `node`: a record field's type (with the field name), a
/// union branch, or the items/values of an array/map (`i == 0`). `name` is set
/// only for records.
#[no_mangle]
pub unsafe extern "C" fn avro_schema_child(
    schema: *const AvroSchema,
    node: u32,
    i: u32,
    child: *mut u32,
    name: *mut *const c_char,
    name_len: *mut usize,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let s = schema_ref(schema)?;
        if child.is_null() || node as usize >= s.node_count() || i as usize >= s.child_count(node) {
            return Err(invalid(
                "null out pointer, node out of range or child index out of range",
            ));
        }
        *child = s.child(node, i as usize);
        if !name.is_null() && !name_len.is_null() {
            match s.fields(node).get(i as usize) {
                Some(f) => {
                    *name = f.name.as_ptr() as *const c_char;
                    *name_len = f.name.len();
                }
                None => {
                    *name = std::ptr::null();
                    *name_len = 0;
                }
            }
        }
        Ok(AVRO_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_symbol(
    schema: *const AvroSchema,
    node: u32,
    i: u32,
    symbol: *mut *const c_char,
    len: *mut usize,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let s = schema_ref(schema)?;
        if symbol.is_null() || len.is_null() || node as usize >= s.node_count() {
            return Err(invalid("null out pointer or node out of range"));
        }
        let Some(sym) = s.symbols(node).get(i as usize) else {
            return Err(invalid("symbol index out of range"));
        };
        *symbol = sym.as_ptr() as *const c_char;
        *len = sym.len();
        Ok(AVRO_OK)
    })
}

pub const AVRO_ATTR_NULL: u8 = 0;
pub const AVRO_ATTR_BOOL: u8 = 1;
pub const AVRO_ATTR_INT: u8 = 2;
pub const AVRO_ATTR_FLOAT: u8 = 3;
pub const AVRO_ATTR_STRING: u8 = 4;
pub const AVRO_ATTR_COMPOSITE: u8 = 5;

#[repr(C)]
pub struct AvroAttr {
    pub kind: u8,
    pub reserved: [u8; 7],
    /// The integer value, or 0/1 for a boolean.
    pub int_value: i64,
    pub float_value: f64,
    pub string: *const c_char,
    pub string_len: usize,
}

/// Looks up the attribute `key` of a node (`field < 0`) or of the `field`-th
/// field of a record. Returns `AVRO_NOT_FOUND` when there is no such
/// attribute. This is how Iceberg's `field-id` and `element-id` are read.
#[no_mangle]
pub unsafe extern "C" fn avro_schema_attr(
    schema: *const AvroSchema,
    node: u32,
    field: i64,
    key: *const u8,
    key_len: usize,
    out: *mut AvroAttr,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let s = schema_ref(schema)?;
        if out.is_null() || node as usize >= s.node_count() {
            return Err(invalid("null out pointer or node out of range"));
        }
        let key = std::str::from_utf8(bytes(key, key_len)?)
            .map_err(|_| invalid("attribute key is not UTF-8"))?;
        let found = if field < 0 {
            s.node_attr(node, key)
        } else {
            s.field_attr(node, field as usize, key)
        };
        let Some(v) = found else {
            return Ok(AVRO_NOT_FOUND);
        };
        let mut a = AvroAttr {
            kind: AVRO_ATTR_NULL,
            reserved: [0; 7],
            int_value: 0,
            float_value: 0.0,
            string: std::ptr::null(),
            string_len: 0,
        };
        match v {
            AttrValue::Null => {}
            AttrValue::Bool(b) => {
                a.kind = AVRO_ATTR_BOOL;
                a.int_value = i64::from(b);
            }
            AttrValue::Int(i) => {
                a.kind = AVRO_ATTR_INT;
                a.int_value = i;
            }
            AttrValue::Float(f) => {
                a.kind = AVRO_ATTR_FLOAT;
                a.float_value = f;
            }
            AttrValue::Str(t) => {
                a.kind = AVRO_ATTR_STRING;
                a.string = t.as_ptr() as *const c_char;
                a.string_len = t.len();
            }
            AttrValue::Composite => a.kind = AVRO_ATTR_COMPOSITE,
        }
        *out = a;
        Ok(AVRO_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_canonical(
    schema: *const AvroSchema,
    out: *mut *const c_char,
    len: *mut usize,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let s = schema_ref(schema)?;
        if out.is_null() || len.is_null() {
            return Err(invalid("null out pointer"));
        }
        let c = s.canonical_form();
        *out = c.as_ptr() as *const c_char;
        *len = c.len();
        Ok(AVRO_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_canonical_prefix(
    schema: *const AvroSchema,
    buf: *mut c_char,
    cap: usize,
    len: *mut usize,
    truncated: *mut u8,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let s = schema_ref(schema)?;
        if len.is_null() || truncated.is_null() || (cap > 0 && buf.is_null()) {
            return Err(invalid("null out pointer"));
        }
        let (prefix, cut) = s.canonical_prefix(cap);
        if prefix.len() > cap {
            return Err(invalid("canonical prefix longer than its limit"));
        }
        if !prefix.is_empty() {
            std::ptr::copy_nonoverlapping(prefix.as_ptr(), buf as *mut u8, prefix.len());
        }
        *len = prefix.len();
        *truncated = u8::from(cut);
        Ok(AVRO_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn avro_schema_source(
    schema: *const AvroSchema,
    out: *mut *const u8,
    len: *mut usize,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let s = schema_ref(schema)?;
        if out.is_null() || len.is_null() {
            return Err(invalid("null out pointer"));
        }
        let text = s.source();
        *out = text.as_ptr();
        *len = text.len();
        Ok(AVRO_OK)
    })
}

/// CRC-64-AVRO fingerprint of `bytes`, for cache bucketing only.
#[no_mangle]
pub unsafe extern "C" fn avro_fingerprint64(data: *const u8, len: usize) -> u64 {
    bytes(data, len)
        .map(crate::canonical::fingerprint64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct AvroSinkDesc {
    pub kind: u32,
    pub arg: u32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct AvroPlanNode {
    pub action: u32,
    pub schema_node: u32,
    pub sink: u32,
    pub sink2: u32,
    pub arg: u32,
    pub n_children: u32,
    /// Non-zero: range check `[check_min, check_max]`.
    pub check: u32,
    pub reserved: u32,
    pub check_min: i64,
    pub check_max: i64,
}

#[no_mangle]
pub unsafe extern "C" fn avro_program_compile(
    schema: *const AvroSchema,
    sinks: *const AvroSinkDesc,
    n_sinks: usize,
    plan: *const AvroPlanNode,
    n_plan: usize,
    out: *mut *mut AvroProgram,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let s = schema_ref(schema)?;
        if out.is_null() || (n_sinks > 0 && sinks.is_null()) || (n_plan > 0 && plan.is_null()) {
            return Err(invalid("null pointer"));
        }
        let mut descs = Vec::with_capacity(n_sinks.min(1 << 16));
        let sink_descs: &[AvroSinkDesc] = if n_sinks == 0 {
            &[]
        } else {
            slice::from_raw_parts(sinks, n_sinks)
        };
        for d in sink_descs {
            let kind = SinkKind::from_u32(d.kind).ok_or_else(|| {
                Error::new(
                    ErrorCode::PlanInvalid,
                    format!("Invalid decode plan: unknown sink kind {}", d.kind),
                )
            })?;
            descs.push(SinkDesc {
                kind,
                arg: d.arg,
                flags: d.flags,
            });
        }
        let mut nodes = Vec::with_capacity(n_plan.min(1 << 16));
        let plan_nodes: &[AvroPlanNode] = if n_plan == 0 {
            &[]
        } else {
            slice::from_raw_parts(plan, n_plan)
        };
        for p in plan_nodes {
            let action = Action::from_u32(p.action).ok_or_else(|| {
                Error::new(
                    ErrorCode::PlanInvalid,
                    format!("Invalid decode plan: unknown action {}", p.action),
                )
            })?;
            nodes.push(PlanNode {
                action,
                schema_node: p.schema_node,
                sink: p.sink,
                sink2: p.sink2,
                arg: p.arg,
                n_children: p.n_children,
                check: p.check != 0,
                check_min: p.check_min,
                check_max: p.check_max,
            });
        }
        let program = Program::compile(s, &descs, &nodes)?;
        *out = Box::into_raw(Box::new(program)) as *mut AvroProgram;
        Ok(AVRO_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn avro_program_free(program: *mut AvroProgram) {
    if !program.is_null() {
        drop(Box::from_raw(program as *mut Program));
    }
}

#[no_mangle]
pub unsafe extern "C" fn avro_program_min_row_size(program: *const AvroProgram) -> u64 {
    if program.is_null() {
        return 0;
    }
    (*(program as *const Program)).min_row_size()
}

#[no_mangle]
pub unsafe extern "C" fn avro_program_memory_size(program: *const AvroProgram) -> usize {
    if program.is_null() {
        return 0;
    }
    (*(program as *const Program)).memory_size()
}

// ---------------------------------------------------------------------------
// State and decode
// ---------------------------------------------------------------------------

/// What an `AvroState` is: the executor's state, and what the C ABI keeps between the calls that continue
/// a row that was paused for space.
struct FfiState {
    exec: State,
    /// The sinks of the call that paused a row, as long as that row is paused.
    paused: Option<PausedSinks>,
    /// An empty vector with room for the sinks of a call, so that a call does not allocate one.
    spare: Vec<Sink<'static>>,
    stats: FfiStats,
}

/// The work of the C ABI that does not depend on the rows, as counters for the tests.
#[derive(Default, Clone, Copy)]
struct FfiStats {
    /// Sinks converted from the caller's `AvroSink` to the executor's [`Sink`]: all of them when a call
    /// starts a row, those that were to grow when it continues a paused row.
    sinks_converted: u64,
}

/// The converted sinks of a row paused for space or input. Their fill levels are the paused row's: for a row
/// paused for space only the sinks that were to grow get their levels back in the caller's array until the
/// row is done or has failed, for a row paused for input none does.
///
/// The slices point into the caller's buffers and are not used between the calls. The caller may only
/// change the buffers of the sinks it was asked to grow (`State::needs`); those are converted again before
/// the next use.
struct PausedSinks {
    sinks: Vec<Sink<'static>>,
    /// The caller's array: the paused row must be continued with it.
    origin: *mut AvroSink,
}

#[no_mangle]
pub extern "C" fn avro_state_new(max_depth: u32) -> *mut AvroState {
    Box::into_raw(Box::new(FfiState {
        exec: State::new(max_depth as usize),
        paused: None,
        spare: Vec::new(),
        stats: FfiStats::default(),
    })) as *mut AvroState
}

#[no_mangle]
pub unsafe extern "C" fn avro_state_free(state: *mut AvroState) {
    if !state.is_null() {
        drop(Box::from_raw(state as *mut FfiState));
    }
}

/// Back to the start of a row: use for a new block and after an error.
#[no_mangle]
pub unsafe extern "C" fn avro_state_reset(state: *mut AvroState) {
    if !state.is_null() {
        let st = &mut *(state as *mut FfiState);
        st.exec.reset();
        st.paused = None;
    }
}

/// Streaming mode of the decoder (see `avro_decode` in the header): `1` when the caller has more bytes of the
/// block than the input of the next calls, so that an input that ends inside a row is `AVRO_STATUS_NEED_INPUT`
/// and not an error. `0` for the last call, and by default. May be changed between the calls that continue a
/// paused row.
#[no_mangle]
pub unsafe extern "C" fn avro_state_set_streaming(state: *mut AvroState, streaming: i32) {
    if !state.is_null() {
        (*(state as *mut FfiState))
            .exec
            .set_streaming(streaming != 0);
    }
}

#[repr(C)]
pub struct AvroSink {
    /// Primary buffer and its size in bytes.
    pub data: *mut u8,
    pub data_size: u64,
    /// Secondary buffer (offsets of a `Bytes` sink) and its size in bytes.
    pub data2: *mut u8,
    pub data2_size: u64,
    /// Elements written to the primary buffer (bytes for `Bytes`): in/out.
    pub len: u64,
    /// Elements written to the secondary buffer: in/out.
    pub len2: u64,
    /// Running end offset for `Bytes` and the offsets sinks: in/out.
    pub offset: u64,
}

pub const AVRO_STATUS_BLOCK_DONE: u32 = 0;
pub const AVRO_STATUS_NEED_SPACE: u32 = 1;
pub const AVRO_STATUS_ROW_LIMIT: u32 = 2;
/// Only in `AvroProgress` after a failed call: `rows_done`/`consumed` are what was committed.
pub const AVRO_STATUS_ERROR: u32 = 3;
/// Only in streaming mode: the input ended inside a row (see `avro_decode` in the header).
pub const AVRO_STATUS_NEED_INPUT: u32 = 4;

/// A sink that lacks room: the capacity, in elements, its primary buffer must reach and the capacity its
/// secondary buffer must reach (`AvroNeed` in the header).
pub type AvroNeed = exec::Need;

#[repr(C)]
pub struct AvroProgress {
    pub rows_done: u64,
    pub consumed: u64,
    pub status: u32,
    /// With `AVRO_STATUS_NEED_SPACE`: the first of the sinks that lack room and the capacity,
    /// in elements, it must reach (`need_cap2` for the secondary buffer). With
    /// `rows_done == 0` the first row is paused and the call that follows with
    /// the same input continues it (see `avro_decode` in the header).
    pub need_sink: u32,
    pub need_cap: u64,
    pub need_cap2: u64,
    /// With `AVRO_STATUS_NEED_SPACE`: how many sinks lack room, and the array of them (`need_sink` and the
    /// capacities above are its first entry), valid until the next call with the state. Zero and null for
    /// every other status.
    pub need_count: u64,
    pub needs: *const AvroNeed,
}

/// `c` as the executor's sink. Checked as [`bytes_uninit_mut`] does.
///
/// SAFETY: `c.data` and `c.data2` are valid for the sizes they declare for as long as the result is used,
/// and not aliased; they need not be initialized (the executor only writes them). The lifetime is the
/// caller's promise: [`PausedSinks`] keeps sinks across calls.
unsafe fn convert_sink(c: &AvroSink) -> Result<Sink<'static>> {
    Ok(Sink {
        data: bytes_uninit_mut(c.data, to_usize(c.data_size)?)?.into(),
        data2: bytes_uninit_mut(c.data2, to_usize(c.data2_size)?)?.into(),
        len: to_usize(c.len)?,
        len2: to_usize(c.len2)?,
        offset: c.offset,
    })
}

fn write_back(c: &mut AvroSink, r: &Sink<'_>) {
    c.len = r.len as u64;
    c.len2 = r.len2 as u64;
    c.offset = r.offset;
}

#[no_mangle]
pub unsafe extern "C" fn avro_decode(
    state: *mut AvroState,
    program: *const AvroProgram,
    input: *const u8,
    input_len: usize,
    max_rows: u64,
    sinks: *mut AvroSink,
    n_sinks: usize,
    out: *mut AvroProgress,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        if state.is_null() || program.is_null() || out.is_null() || (n_sinks > 0 && sinks.is_null())
        {
            return Err(invalid("null pointer"));
        }
        // Whatever happens below, the caller sees "error, nothing committed" rather than garbage.
        *out = AvroProgress {
            rows_done: 0,
            consumed: 0,
            status: AVRO_STATUS_ERROR,
            need_sink: 0,
            need_cap: 0,
            need_cap2: 0,
            need_count: 0,
            needs: std::ptr::null(),
        };
        let st = &mut *(state as *mut FfiState);
        let prog = &*(program as *const Program);
        let input = bytes(input, input_len)?;
        let c_sinks: &mut [AvroSink] = if n_sinks == 0 {
            &mut []
        } else {
            slice::from_raw_parts_mut(sinks, n_sinks)
        };
        // A row that the last call paused continues with the sinks that call converted: only the sinks it
        // asked to grow have other buffers now. Converting and checking every sink again for every sink that
        // has to grow would make a wide row cost its number of sinks times the number of growths.
        let (mut rs, resumed) = match st.paused.take() {
            Some(mut paused) => {
                if paused.origin != sinks || paused.sinks.len() != n_sinks {
                    st.exec.reset();
                    return Err(invalid("a paused row was resumed with other sinks"));
                }
                // A row paused for space resumes with the sinks that were to grow converted again; a row
                // paused for input has no sink that changed.
                for need in st.exec.needs() {
                    let grown = need.sink as usize;
                    match convert_sink(&c_sinks[grown]) {
                        Ok(sink) => paused.sinks[grown] = sink,
                        Err(e) => {
                            st.paused = Some(paused);
                            return Err(e);
                        }
                    }
                    st.stats.sinks_converted += 1;
                }
                (paused.sinks, true)
            }
            None => {
                if !st.exec.at_row_boundary() {
                    // A paused row without its sinks: the fill levels of the caller are not the row's.
                    st.exec.reset();
                    return Err(invalid("a paused row was resumed without its sinks"));
                }
                let mut rs = std::mem::take(&mut st.spare);
                rs.clear();
                rs.reserve(n_sinks.min(1 << 16));
                for c in c_sinks.iter() {
                    rs.push(convert_sink(c)?);
                }
                st.stats.sinks_converted += n_sinks as u64;
                (rs, false)
            }
        };
        let result = if resumed {
            exec::decode_resumed(prog, &mut st.exec, input, max_rows, &mut rs)
        } else {
            exec::decode_partial(prog, &mut st.exec, input, max_rows, &mut rs)
        };
        let p = match result {
            Ok(p) => p,
            Err(e) => {
                // Committed rows survive the error: report them with the status ERROR, and hand the fill
                // levels back: they are the committed ones.
                for (c, r) in c_sinks.iter_mut().zip(&rs) {
                    write_back(c, r);
                }
                rs.clear();
                st.spare = rs;
                *out = AvroProgress {
                    rows_done: e.rows_done,
                    consumed: e.consumed as u64,
                    status: AVRO_STATUS_ERROR,
                    need_sink: 0,
                    need_cap: 0,
                    need_cap2: 0,
                    need_count: 0,
                    needs: std::ptr::null(),
                };
                return Err(e.error);
            }
        };
        let (status, need_sink, need_cap, need_cap2) = match p.status {
            Status::BlockDone => (AVRO_STATUS_BLOCK_DONE, 0, 0, 0),
            Status::RowLimit => (AVRO_STATUS_ROW_LIMIT, 0, 0, 0),
            Status::NeedSpace {
                sink,
                min_cap,
                min_cap2,
            } => (AVRO_STATUS_NEED_SPACE, sink, min_cap, min_cap2),
            Status::NeedInput => (AVRO_STATUS_NEED_INPUT, 0, 0, 0),
        };
        let needs = st.exec.needs();
        let (need_count, needs) = if status == AVRO_STATUS_NEED_SPACE {
            (needs.len() as u64, needs.as_ptr())
        } else {
            (0, std::ptr::null())
        };
        if (status == AVRO_STATUS_NEED_SPACE || status == AVRO_STATUS_NEED_INPUT)
            && p.rows_done == 0
        {
            // The row is paused: the next call continues it with `rs`, so the fill levels stay there
            // and only the sinks that are about to grow get their own back.
            if status == AVRO_STATUS_NEED_SPACE {
                for need in st.exec.needs() {
                    let grown = need.sink as usize;
                    write_back(&mut c_sinks[grown], &rs[grown]);
                }
            }
            st.paused = Some(PausedSinks {
                sinks: rs,
                origin: sinks,
            });
        } else {
            // Hand the fill levels back: the rows that were committed are in the sinks up to them.
            for (c, r) in c_sinks.iter_mut().zip(&rs) {
                write_back(c, r);
            }
            rs.clear();
            st.spare = rs;
        }
        *out = AvroProgress {
            rows_done: p.rows_done,
            consumed: p.consumed as u64,
            status,
            need_sink,
            need_cap,
            need_cap2,
            need_count,
            needs,
        };
        Ok(AVRO_OK)
    })
}

/// Index of the first `Int32` (host byte order) in `data` outside `[min, max]`.
/// Returns 1 and sets `*bad_index` if there is one, 0 if all are in range.
#[no_mangle]
pub unsafe extern "C" fn avro_first_out_of_range_i32(
    data: *const u8,
    n_values: usize,
    min: i32,
    max: i32,
    bad_index: *mut u64,
) -> i32 {
    let Some(len) = n_values.checked_mul(4) else {
        return -(ErrorCode::InvalidArgument as i32);
    };
    let Ok(d) = bytes(data, len) else {
        return -(ErrorCode::InvalidArgument as i32);
    };
    match exec::first_out_of_range_i32(d, min, max) {
        Some(i) => {
            if !bad_index.is_null() {
                *bad_index = i as u64;
            }
            1
        }
        None => 0,
    }
}

// ---------------------------------------------------------------------------
// Object Container Files
// ---------------------------------------------------------------------------

/// Parses the file header at the start of the window `buf[0..len)`.
///
/// Returns `AVRO_OK` (`*out` is a new header and `*consumed` its size),
/// `AVRO_NEED_MORE` (`*needed` is the total window size required), or an
/// error. With `eof != 0` a short window is the error "EOF reached".
/// `max_header_bytes == 0` means no header-size limit: the caller's buffer bounds memory.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_header_parse(
    buf: *const u8,
    len: usize,
    eof: i32,
    max_header_bytes: u64,
    out: *mut *mut AvroOcfHeader,
    consumed: *mut u64,
    needed: *mut u64,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        if out.is_null() || consumed.is_null() || needed.is_null() {
            return Err(invalid("null out pointer"));
        }
        let mut limits = OcfLimits::default();
        if max_header_bytes != 0 {
            limits.max_header_bytes = to_usize(max_header_bytes)?;
        }
        match ocf::parse_header(bytes(buf, len)?, eof != 0, &limits)? {
            Parsed::Done { value, consumed: n } => {
                *out = Box::into_raw(Box::new(value)) as *mut AvroOcfHeader;
                *consumed = n as u64;
                Ok(AVRO_OK)
            }
            Parsed::NeedMore { needed: n } => {
                *needed = n as u64;
                Ok(AVRO_NEED_MORE)
            }
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn avro_ocf_header_free(h: *mut AvroOcfHeader) {
    if !h.is_null() {
        drop(Box::from_raw(h as *mut Header));
    }
}

unsafe fn header_ref<'a>(h: *const AvroOcfHeader) -> Result<&'a Header> {
    if h.is_null() {
        return Err(invalid("null header"));
    }
    Ok(&*(h as *const Header))
}

/// The codec of the file: 0 null, 1 deflate, 2 snappy, 3 zstandard, 4 bzip2, 5 xz.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_header_codec(h: *const AvroOcfHeader) -> u32 {
    header_ref(h).map(|h| h.codec as u32).unwrap_or(0)
}

/// Copies the 16 byte sync marker to `out`.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_header_sync(h: *const AvroOcfHeader, out: *mut u8) {
    if let (Ok(h), false) = (header_ref(h), out.is_null()) {
        std::ptr::copy_nonoverlapping(h.sync.as_ptr(), out, h.sync.len());
    }
}

/// The `avro.schema` JSON.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_header_schema(
    h: *const AvroOcfHeader,
    out: *mut *const u8,
    len: *mut usize,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let h = header_ref(h)?;
        if out.is_null() || len.is_null() {
            return Err(invalid("null out pointer"));
        }
        let s = h.schema_json();
        *out = s.as_ptr();
        *len = s.len();
        Ok(AVRO_OK)
    })
}

/// Looks up a metadata value by key; `AVRO_NOT_FOUND` if the key is absent.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_header_meta(
    h: *const AvroOcfHeader,
    key: *const u8,
    key_len: usize,
    value: *mut *const u8,
    value_len: *mut usize,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let h = header_ref(h)?;
        if value.is_null() || value_len.is_null() {
            return Err(invalid("null out pointer"));
        }
        match h.get(bytes(key, key_len)?) {
            Some(v) => {
                *value = v.as_ptr();
                *value_len = v.len();
                Ok(AVRO_OK)
            }
            None => Ok(AVRO_NOT_FOUND),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn avro_ocf_header_meta_count(h: *const AvroOcfHeader) -> u64 {
    header_ref(h)
        .map(|h| h.metadata().len() as u64)
        .unwrap_or(0)
}

/// The `i`-th metadata entry in order of first appearance.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_header_meta_at(
    h: *const AvroOcfHeader,
    i: u64,
    key: *mut *const u8,
    key_len: *mut usize,
    value: *mut *const u8,
    value_len: *mut usize,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        let h = header_ref(h)?;
        if key.is_null() || key_len.is_null() || value.is_null() || value_len.is_null() {
            return Err(invalid("null out pointer"));
        }
        let Some((k, v)) = h.metadata().get(to_usize(i)?) else {
            return Err(invalid("metadata index out of range"));
        };
        *key = k.as_ptr();
        *key_len = k.len();
        *value = v.as_ptr();
        *value_len = v.len();
        Ok(AVRO_OK)
    })
}

#[repr(C)]
pub struct AvroBlockHeader {
    pub object_count: u64,
    pub byte_count: u64,
    /// Size of the block header itself: the payload starts this many bytes into the window.
    pub header_len: u64,
}

/// Parses a block header at the start of the window. Returns `AVRO_OK`,
/// `AVRO_NEED_MORE` (`*needed`), `AVRO_END` (the window is empty and `eof` is
/// set) or an error. `min_record_size` is `avro_schema_node(root).min_size`.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_block_header(
    buf: *const u8,
    len: usize,
    eof: i32,
    codec: u32,
    min_record_size: u64,
    max_block_bytes: u64,
    out: *mut AvroBlockHeader,
    needed: *mut u64,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        if out.is_null() || needed.is_null() {
            return Err(invalid("null out pointer"));
        }
        let codec = match codec {
            0 => Codec::Null,
            1 => Codec::Deflate,
            2 => Codec::Snappy,
            3 => Codec::Zstandard,
            4 => Codec::Bzip2,
            5 => Codec::Xz,
            _ => return Err(invalid("unknown codec number")),
        };
        let ctx = BlockCtx {
            codec,
            min_record_size,
            max_block_bytes,
        };
        match ocf::parse_block_header(bytes(buf, len)?, eof != 0, &ctx)? {
            Parsed::Done {
                value: BlockHeader::End,
                ..
            } => Ok(AVRO_END),
            Parsed::Done {
                value:
                    BlockHeader::Block {
                        object_count,
                        byte_count,
                    },
                consumed,
            } => {
                *out = AvroBlockHeader {
                    object_count,
                    byte_count,
                    header_len: consumed as u64,
                };
                Ok(AVRO_OK)
            }
            Parsed::NeedMore { needed: n } => {
                *needed = n as u64;
                Ok(AVRO_NEED_MORE)
            }
        }
    })
}

/// Checks the 16 sync bytes that follow a block payload.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_check_sync(
    buf: *const u8,
    len: usize,
    eof: i32,
    sync: *const u8,
    needed: *mut u64,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        if sync.is_null() || needed.is_null() {
            return Err(invalid("null pointer"));
        }
        let mut expected = [0u8; ocf::SYNC_LEN];
        expected.copy_from_slice(slice::from_raw_parts(sync, ocf::SYNC_LEN));
        match ocf::check_sync(bytes(buf, len)?, &expected, eof != 0)? {
            Parsed::Done { .. } => Ok(AVRO_OK),
            Parsed::NeedMore { needed: n } => {
                *needed = n as u64;
                Ok(AVRO_NEED_MORE)
            }
        }
    })
}

/// Checks a block's object count against its decompressed payload size.
#[no_mangle]
pub unsafe extern "C" fn avro_ocf_check_object_count(
    object_count: u64,
    payload_len: u64,
    min_record_size: u64,
    err: *mut c_char,
    err_cap: usize,
) -> i32 {
    wrap(err, err_cap, || {
        ocf::check_object_count(object_count, payload_len, min_record_size).map(|()| AVRO_OK)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, size_of};
    use std::ptr::{null, null_mut};

    fn errbuf() -> ([c_char; 256], usize) {
        ([0; 256], 256)
    }

    fn msg(buf: &[c_char]) -> String {
        let bytes: Vec<u8> = buf
            .iter()
            .take_while(|c| **c != 0)
            .map(|c| *c as u8)
            .collect();
        String::from_utf8(bytes).unwrap()
    }

    unsafe fn parse(json: &str) -> *mut AvroSchema {
        let (mut e, cap) = errbuf();
        let mut out = null_mut();
        let rc = avro_schema_parse(
            json.as_ptr(),
            json.len(),
            256,
            &mut out,
            e.as_mut_ptr(),
            cap,
        );
        assert_eq!(rc, 0, "{}", msg(&e));
        out
    }

    /// Every exported function is declared in the header and every function declared in the
    /// header is exported, so the two cannot drift apart unnoticed.
    #[test]
    fn header_declares_exactly_the_exported_functions() {
        let source = include_str!("ffi.rs");
        let production = &source[..source.find("#[cfg(test)]").unwrap()];
        let mut exported: Vec<&str> = production
            .lines()
            .filter_map(|l| {
                l.strip_prefix("pub unsafe extern \"C\" fn ")
                    .or_else(|| l.strip_prefix("pub extern \"C\" fn "))
            })
            .map(|l| l.split('(').next().unwrap())
            .collect();
        let header = include_str!("../include/avro_rust.h");
        // A prototype's name is the last identifier before the first opening parenthesis on its first line.
        let mut declared: Vec<&str> = header
            .lines()
            .filter(|l| !l.trim_start().starts_with("//") && !l.starts_with("#define"))
            .filter_map(|l| l.split_once('(').map(|(head, _)| head))
            .filter_map(|head| {
                head.split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .next_back()
            })
            .filter(|w| w.starts_with("avro_"))
            .collect();
        exported.sort_unstable();
        declared.sort_unstable();
        assert_eq!(exported.len(), 33);
        assert_eq!(exported, declared);
    }

    #[test]
    fn struct_layouts_match_the_header() {
        assert_eq!(
            (size_of::<AvroSinkDesc>(), align_of::<AvroSinkDesc>()),
            (12, 4)
        );
        assert_eq!(
            (size_of::<AvroPlanNode>(), align_of::<AvroPlanNode>()),
            (48, 8)
        );
        assert_eq!((size_of::<AvroSink>(), align_of::<AvroSink>()), (56, 8));
        assert_eq!(
            (size_of::<AvroProgress>(), align_of::<AvroProgress>()),
            (56, 8)
        );
        assert_eq!((size_of::<AvroNeed>(), align_of::<AvroNeed>()), (24, 8));
        assert_eq!(
            (size_of::<AvroBlockHeader>(), align_of::<AvroBlockHeader>()),
            (24, 8)
        );
        assert_eq!((size_of::<AvroAttr>(), align_of::<AvroAttr>()), (40, 8));
        assert_eq!(
            (size_of::<AvroNodeInfo>(), align_of::<AvroNodeInfo>()),
            (72, 8)
        );
    }

    #[test]
    fn schema_introspection() {
        let json = r#"{"type":"record","name":"R","namespace":"ns","fields":[
            {"name":"a","type":"long","field-id":7},
            {"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},
            {"name":"u","type":["null",{"type":"enum","name":"E","symbols":["X","Y"]}]}]}"#;
        unsafe {
            let s = parse(json);
            let (mut e, cap) = errbuf();
            // record, long, decimal bytes, union, the shared null, enum
            assert_eq!(avro_schema_node_count(s), 6);
            let root = avro_schema_root(s);
            let mut info = std::mem::MaybeUninit::<AvroNodeInfo>::zeroed();
            assert_eq!(
                avro_schema_node(s, root, info.as_mut_ptr(), e.as_mut_ptr(), cap),
                0
            );
            let info = info.assume_init();
            assert_eq!(
                (info.kind, info.child_count, info.min_size),
                (8, 3, 1 + 1 + 1)
            );
            assert_eq!(
                std::str::from_utf8(slice::from_raw_parts(info.name as *const u8, info.name_len))
                    .unwrap(),
                "ns.R"
            );
            assert_eq!(info.simple_name_offset, 3);
            // children
            let (mut child, mut name, mut name_len) = (0u32, null(), 0usize);
            assert_eq!(
                avro_schema_child(
                    s,
                    root,
                    1,
                    &mut child,
                    &mut name,
                    &mut name_len,
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            assert_eq!(
                std::str::from_utf8(slice::from_raw_parts(name as *const u8, name_len)).unwrap(),
                "d"
            );
            let mut dinfo = std::mem::MaybeUninit::<AvroNodeInfo>::zeroed();
            assert_eq!(
                avro_schema_node(s, child, dinfo.as_mut_ptr(), e.as_mut_ptr(), cap),
                0
            );
            let dinfo = dinfo.assume_init();
            assert_eq!(
                (dinfo.kind, dinfo.logical, dinfo.precision, dinfo.scale),
                (6, 1, 9, 2)
            );
            assert_eq!(
                avro_schema_child(
                    s,
                    root,
                    3,
                    &mut child,
                    &mut name,
                    &mut name_len,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::InvalidArgument as i32)
            );
            // attributes
            let mut attr = std::mem::MaybeUninit::<AvroAttr>::zeroed();
            assert_eq!(
                avro_schema_attr(
                    s,
                    root,
                    0,
                    b"field-id".as_ptr(),
                    8,
                    attr.as_mut_ptr(),
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            let attr = attr.assume_init();
            assert_eq!((attr.kind, attr.int_value), (AVRO_ATTR_INT, 7));
            let mut attr2 = std::mem::MaybeUninit::<AvroAttr>::zeroed();
            assert_eq!(
                avro_schema_attr(
                    s,
                    root,
                    1,
                    b"field-id".as_ptr(),
                    8,
                    attr2.as_mut_ptr(),
                    e.as_mut_ptr(),
                    cap
                ),
                AVRO_NOT_FOUND
            );
            // symbols of the enum behind the union
            let mut u = 0u32;
            assert_eq!(
                avro_schema_child(
                    s,
                    root,
                    2,
                    &mut u,
                    null_mut(),
                    null_mut(),
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            let mut en = 0u32;
            assert_eq!(
                avro_schema_child(
                    s,
                    u,
                    1,
                    &mut en,
                    null_mut(),
                    null_mut(),
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            let (mut sym, mut sl) = (null(), 0usize);
            assert_eq!(
                avro_schema_symbol(s, en, 1, &mut sym, &mut sl, e.as_mut_ptr(), cap),
                0
            );
            assert_eq!(
                std::str::from_utf8(slice::from_raw_parts(sym as *const u8, sl)).unwrap(),
                "Y"
            );
            // The source text, and a prefix of the canonical form before the form was ever computed.
            let (mut src, mut src_len) = (null(), 0usize);
            assert_eq!(
                avro_schema_source(s, &mut src, &mut src_len, e.as_mut_ptr(), cap),
                0
            );
            assert_eq!(slice::from_raw_parts(src, src_len), json.as_bytes());
            let mut prefix = [0 as c_char; 16];
            let (mut prefix_len, mut cut) = (0usize, 0u8);
            assert_eq!(
                avro_schema_canonical_prefix(
                    s,
                    prefix.as_mut_ptr(),
                    prefix.len(),
                    &mut prefix_len,
                    &mut cut,
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            assert_eq!((prefix_len, cut), (16, 1));
            assert_eq!(
                slice::from_raw_parts(prefix.as_ptr() as *const u8, prefix_len),
                b"{\"name\":\"ns.R\",\""
            );
            assert_eq!(
                avro_schema_canonical_prefix(
                    s,
                    null_mut(),
                    0,
                    &mut prefix_len,
                    &mut cut,
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            assert_eq!((prefix_len, cut), (0, 1));
            // canonical form
            let (mut c, mut cl) = (null(), 0usize);
            assert_eq!(
                avro_schema_canonical(s, &mut c, &mut cl, e.as_mut_ptr(), cap),
                0
            );
            let canon = std::str::from_utf8(slice::from_raw_parts(c as *const u8, cl)).unwrap();
            assert!(
                canon.starts_with(
                    r#"{"name":"ns.R","type":"record","fields":[{"name":"a","type":"long"}"#
                ),
                "{canon}"
            );
            assert_eq!(
                avro_fingerprint64(b"\"null\"".as_ptr(), 6),
                0x63dd_24e7_cc25_8f8a
            );
            avro_schema_free(s);
            avro_schema_free(null_mut());
        }
    }

    #[test]
    fn errors_and_messages() {
        unsafe {
            let (mut e, cap) = errbuf();
            let mut out = null_mut();
            let json = b"{\"type\":\"record\"";
            assert_eq!(
                avro_schema_parse(
                    json.as_ptr(),
                    json.len(),
                    256,
                    &mut out,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::JsonSyntax as i32)
            );
            assert!(out.is_null());
            assert!(msg(&e).starts_with("JSON parse error"), "{}", msg(&e));
            // Truncation to a tiny buffer keeps NUL termination.
            let mut small = [0x7f as c_char; 8];
            assert_eq!(
                avro_schema_parse(
                    json.as_ptr(),
                    json.len(),
                    256,
                    &mut out,
                    small.as_mut_ptr(),
                    8
                ),
                -(ErrorCode::JsonSyntax as i32)
            );
            assert_eq!(msg(&small).len(), 7);
            assert_eq!(
                avro_schema_parse(json.as_ptr(), json.len(), 256, &mut out, null_mut(), 0),
                -(ErrorCode::JsonSyntax as i32)
            );
            // Null pointers are errors, not crashes.
            assert_eq!(
                avro_schema_parse(null(), 5, 256, &mut out, e.as_mut_ptr(), cap),
                -(ErrorCode::InvalidArgument as i32)
            );
            assert_eq!(
                avro_schema_parse(json.as_ptr(), 1, 256, null_mut(), e.as_mut_ptr(), cap),
                -(ErrorCode::InvalidArgument as i32)
            );
            // An empty slice with a null pointer is an empty input (a JSON error), not a crash.
            assert_eq!(
                avro_schema_parse(null(), 0, 256, &mut out, e.as_mut_ptr(), cap),
                -(ErrorCode::JsonSyntax as i32)
            );
            // The depth error text is the fork's.
            let deep = "[".repeat(300) + &"]".repeat(300);
            assert_eq!(
                avro_schema_parse(
                    deep.as_ptr(),
                    deep.len(),
                    256,
                    &mut out,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::JsonDepth as i32)
            );
            assert_eq!(msg(&e), "JSON nesting depth exceeds the limit");
            assert_eq!(avro_schema_root(null()), 0);
            assert_eq!(avro_schema_node_count(null()), 0);
        }
    }

    #[test]
    fn panics_are_caught_and_reported() {
        let (mut e, cap) = errbuf();
        let rc = wrap(e.as_mut_ptr(), cap, || -> Result<i32> {
            panic!("boom {}", 7)
        });
        assert_eq!(rc, -(ErrorCode::Internal as i32));
        assert_eq!(msg(&e), "panic in the Avro decoder: boom 7");
        let rc = wrap(e.as_mut_ptr(), cap, || -> Result<i32> {
            let v: Vec<i32> = Vec::new();
            Ok(v[3])
        });
        assert_eq!(rc, -(ErrorCode::Internal as i32));
        assert!(msg(&e).contains("index out of bounds"), "{}", msg(&e));
    }

    #[test]
    fn compile_and_decode_through_the_c_abi() {
        let json = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"s","type":"string"},{"name":"tags","type":{"type":"array","items":"int"}}]}"#;
        unsafe {
            let s = parse(json);
            let (mut e, cap) = errbuf();
            let sinks = [
                AvroSinkDesc {
                    kind: SinkKind::Int64 as u32,
                    arg: 0,
                    flags: 0,
                },
                AvroSinkDesc {
                    kind: SinkKind::Bytes as u32,
                    arg: 0,
                    flags: 0,
                },
                AvroSinkDesc {
                    kind: SinkKind::ArrayOffsets as u32,
                    arg: 0,
                    flags: 0,
                },
                AvroSinkDesc {
                    kind: SinkKind::Int32 as u32,
                    arg: 0,
                    flags: 0,
                },
            ];
            let n = |action: Action, node: u32, sink: u32, children: u32| AvroPlanNode {
                action: action as u32,
                schema_node: node,
                sink,
                sink2: u32::MAX,
                arg: 0,
                n_children: children,
                check: 0,
                reserved: 0,
                check_min: 0,
                check_max: 0,
            };
            let root = avro_schema_root(s);
            let kid = |i: u32| {
                let mut c = 0u32;
                assert_eq!(
                    avro_schema_child(s, root, i, &mut c, null_mut(), null_mut(), null_mut(), 0),
                    0
                );
                c
            };
            let mut items = 0u32;
            assert_eq!(
                avro_schema_child(
                    s,
                    kid(2),
                    0,
                    &mut items,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                    0
                ),
                0
            );
            let plan = [
                n(Action::Record, root, u32::MAX, 3),
                n(Action::Value, kid(0), 0, 0),
                n(Action::Value, kid(1), 1, 0),
                n(Action::Array, kid(2), 2, 1),
                n(Action::Value, items, 3, 0),
            ];
            let mut prog = null_mut();
            assert_eq!(
                avro_program_compile(
                    s,
                    sinks.as_ptr(),
                    sinks.len(),
                    plan.as_ptr(),
                    plan.len(),
                    &mut prog,
                    e.as_mut_ptr(),
                    cap
                ),
                0,
                "{}",
                msg(&e)
            );
            assert_eq!(avro_program_min_row_size(prog), 3);
            assert!(avro_program_memory_size(prog) > 0);
            assert_eq!(avro_program_memory_size(null()), 0);
            // A bad plan is an error with a message.
            let mut bad = plan;
            bad[1].schema_node = 99;
            let mut prog2 = null_mut();
            assert_eq!(
                avro_program_compile(
                    s,
                    sinks.as_ptr(),
                    sinks.len(),
                    bad.as_ptr(),
                    bad.len(),
                    &mut prog2,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::PlanInvalid as i32)
            );
            assert!(msg(&e).starts_with("Invalid decode plan"), "{}", msg(&e));
            assert!(prog2.is_null());

            // Two rows: (1, "ab", [5, 6]) and (-1, "", []).
            let input: Vec<u8> = vec![
                0x02, 0x04, b'a', b'b', 0x04, 0x0a, 0x0c, 0x00, 0x01, 0x00, 0x00,
            ];
            let (mut ids, mut chars, mut offs, mut aoffs, mut vals) =
                ([0u8; 16], [0u8; 2], [0u8; 16], [0u8; 16], [0u8; 8]);
            let mut c_sinks = [
                AvroSink {
                    data: ids.as_mut_ptr(),
                    data_size: 16,
                    data2: null_mut(),
                    data2_size: 0,
                    len: 0,
                    len2: 0,
                    offset: 0,
                },
                AvroSink {
                    data: chars.as_mut_ptr(),
                    data_size: 2,
                    data2: offs.as_mut_ptr(),
                    data2_size: 16,
                    len: 0,
                    len2: 0,
                    offset: 0,
                },
                AvroSink {
                    data: aoffs.as_mut_ptr(),
                    data_size: 16,
                    data2: null_mut(),
                    data2_size: 0,
                    len: 0,
                    len2: 0,
                    offset: 0,
                },
                // Room for one value only.
                AvroSink {
                    data: vals.as_mut_ptr(),
                    data_size: 4,
                    data2: null_mut(),
                    data2_size: 0,
                    len: 0,
                    len2: 0,
                    offset: 0,
                },
            ];
            let state = avro_state_new(100);
            let mut p = std::mem::MaybeUninit::<AvroProgress>::zeroed();
            assert_eq!(
                avro_decode(
                    state,
                    prog,
                    input.as_ptr(),
                    input.len(),
                    2,
                    c_sinks.as_mut_ptr(),
                    4,
                    p.as_mut_ptr(),
                    e.as_mut_ptr(),
                    cap
                ),
                0,
                "{}",
                msg(&e)
            );
            let pr = p.assume_init();
            // The first row needs two values in the last sink and has room for one: the row is paused,
            // nothing is committed. The fill levels are past the row boundary, where it stopped, in the
            // state; the array gets back those of the sink to grow, and keeps those of the others.
            assert_eq!(
                (
                    pr.rows_done,
                    pr.consumed,
                    pr.status,
                    pr.need_sink,
                    pr.need_cap
                ),
                (0, 0, AVRO_STATUS_NEED_SPACE, 3, 2)
            );
            assert_eq!(
                c_sinks
                    .iter()
                    .map(|s| (s.len, s.len2, s.offset))
                    .collect::<Vec<_>>(),
                vec![(0, 0, 0), (0, 0, 0), (0, 0, 0), (1, 0, 0)]
            );
            assert_eq!((*(state as *mut FfiState)).stats.sinks_converted, 4);
            c_sinks[3].data_size = 8;
            let mut p = std::mem::MaybeUninit::<AvroProgress>::zeroed();
            assert_eq!(
                avro_decode(
                    state,
                    prog,
                    input.as_ptr(),
                    input.len(),
                    2,
                    c_sinks.as_mut_ptr(),
                    4,
                    p.as_mut_ptr(),
                    e.as_mut_ptr(),
                    cap
                ),
                0,
                "{}",
                msg(&e)
            );
            let pr = p.assume_init();
            assert_eq!(
                (pr.rows_done, pr.consumed, pr.status),
                (2, 11, AVRO_STATUS_ROW_LIMIT)
            );
            assert_eq!(i64::from_ne_bytes(ids[..8].try_into().unwrap()), 1);
            assert_eq!(i64::from_ne_bytes(ids[8..].try_into().unwrap()), -1);
            assert_eq!(&chars, b"ab");
            let u64s = |b: &[u8]| -> Vec<u64> {
                b.chunks(8)
                    .map(|c| u64::from_ne_bytes(c.try_into().unwrap()))
                    .collect()
            };
            assert_eq!(u64s(&offs), vec![2, 2]);
            assert_eq!(u64s(&aoffs), vec![2, 2]);
            assert_eq!(
                (
                    i32::from_ne_bytes(vals[..4].try_into().unwrap()),
                    i32::from_ne_bytes(vals[4..].try_into().unwrap())
                ),
                (5, 6)
            );
            assert_eq!(
                (c_sinks[1].len, c_sinks[1].len2, c_sinks[1].offset),
                (2, 2, 2)
            );
            assert_eq!((*(state as *mut FfiState)).stats.sinks_converted, 4 + 1);
            // Wrong sink count and null pointers are errors.
            let mut p = std::mem::MaybeUninit::<AvroProgress>::zeroed();
            avro_state_reset(state);
            assert_eq!(
                avro_decode(
                    state,
                    prog,
                    input.as_ptr(),
                    input.len(),
                    1,
                    c_sinks.as_mut_ptr(),
                    3,
                    p.as_mut_ptr(),
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::InvalidArgument as i32)
            );
            assert_eq!(
                avro_decode(
                    null_mut(),
                    prog,
                    input.as_ptr(),
                    input.len(),
                    1,
                    c_sinks.as_mut_ptr(),
                    4,
                    p.as_mut_ptr(),
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::InvalidArgument as i32)
            );
            avro_state_free(state);
            avro_program_free(prog);
            avro_schema_free(s);
        }
    }

    fn zigzag(v: i64) -> Vec<u8> {
        let mut u = ((v << 1) ^ (v >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let b = (u & 0x7f) as u8;
            u >>= 7;
            if u == 0 {
                out.push(b);
                return out;
            }
            out.push(b | 0x80);
        }
    }

    /// `fields` arrays of `items` longs in one row, one offsets and one value sink for each array, and the
    /// sinks of a C caller that starts every buffer with room for 16 elements.
    struct Wide {
        program: Program,
        input: Vec<u8>,
        descs: Vec<SinkDesc>,
        bufs: Vec<Vec<u8>>,
        c_sinks: Vec<AvroSink>,
    }

    impl Wide {
        fn new(fields: usize, items: usize) -> Wide {
            let json = format!(
                r#"{{"type":"record","name":"R","fields":[{}]}}"#,
                (0..fields)
                    .map(|i| format!(
                        r#"{{"name":"f{i}","type":{{"type":"array","items":"long"}}}}"#
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let schema = Schema::parse(json.as_bytes(), 0).unwrap();
            let root = schema.root();
            let mut descs = Vec::new();
            let mut plan = vec![PlanNode::new(Action::Record, root, fields as u32)];
            let mut input = Vec::new();
            for i in 0..fields {
                let array = schema.child(root, i);
                let mut node = PlanNode::new(Action::Array, array, 1);
                node.sink = descs.len() as u32;
                descs.push(SinkDesc::new(SinkKind::ArrayOffsets, 0));
                plan.push(node);
                plan.push(PlanNode::value(schema.child(array, 0), descs.len() as u32));
                descs.push(SinkDesc::new(SinkKind::Int64, 0));
                input.extend(zigzag(items as i64));
                for j in 0..items {
                    input.extend(zigzag((i * 1000 + j) as i64));
                }
                input.push(0);
            }
            Wide::finish(&schema, descs, plan, input)
        }

        /// One array of `items` records of `fields` long fields: every field sink lacks room for the block.
        fn records(fields: usize, items: usize) -> Wide {
            let json = format!(
                r#"{{"type":"record","name":"R","fields":[{{"name":"x","type":{{"type":"array","items":
                {{"type":"record","name":"T","fields":[{}]}}}}}}]}}"#,
                (0..fields)
                    .map(|i| format!(r#"{{"name":"f{i}","type":"long"}}"#))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let schema = Schema::parse(json.as_bytes(), 0).unwrap();
            let root = schema.root();
            let array = schema.child(root, 0);
            let record = schema.child(array, 0);
            let mut descs = vec![SinkDesc::new(SinkKind::ArrayOffsets, 0)];
            let mut array_node = PlanNode::new(Action::Array, array, 1);
            array_node.sink = 0;
            let mut plan = vec![
                PlanNode::new(Action::Record, root, 1),
                array_node,
                PlanNode::new(Action::Record, record, fields as u32),
            ];
            let mut input = zigzag(items as i64);
            for j in 0..items {
                for i in 0..fields {
                    input.extend(zigzag((i * 1000 + j) as i64));
                }
            }
            input.push(0);
            for i in 0..fields {
                plan.push(PlanNode::value(schema.child(record, i), descs.len() as u32));
                descs.push(SinkDesc::new(SinkKind::Int64, 0));
            }
            Wide::finish(&schema, descs, plan, input)
        }

        /// With the program of `plan`, and sinks that hold 16 elements each, as the C++ caller starts them.
        fn finish(
            schema: &Schema,
            descs: Vec<SinkDesc>,
            plan: Vec<PlanNode>,
            input: Vec<u8>,
        ) -> Wide {
            let program = Program::compile(schema, &descs, &plan).unwrap();
            let mut bufs: Vec<Vec<u8>> = descs.iter().map(|d| vec![0u8; d.width() * 16]).collect();
            let c_sinks = bufs
                .iter_mut()
                .map(|b| AvroSink {
                    data: b.as_mut_ptr(),
                    data_size: b.len() as u64,
                    data2: null_mut(),
                    data2_size: 0,
                    len: 0,
                    len2: 0,
                    offset: 0,
                })
                .collect();
            Wide {
                program,
                input,
                descs,
                bufs,
                c_sinks,
            }
        }

        /// One call of the row, with the sinks of this array: the status.
        unsafe fn call(
            &mut self,
            state: *mut AvroState,
            sinks: *mut AvroSink,
        ) -> (i32, AvroProgress) {
            let (mut e, cap) = errbuf();
            let mut p = std::mem::MaybeUninit::<AvroProgress>::zeroed();
            let rc = avro_decode(
                state,
                &self.program as *const Program as *const AvroProgram,
                self.input.as_ptr(),
                self.input.len(),
                1,
                sinks,
                self.c_sinks.len(),
                p.as_mut_ptr(),
                e.as_mut_ptr(),
                cap,
            );
            (rc, p.assume_init())
        }

        /// What the caller does about `NEED_SPACE`: grow the buffer of every sink that lacks room and tell it
        /// the sink.
        fn grow(&mut self, p: &AvroProgress) {
            assert!(p.need_count >= 1);
            let needs = unsafe { slice::from_raw_parts(p.needs, p.need_count as usize) };
            assert_eq!(
                (needs[0].sink, needs[0].min_cap, needs[0].min_cap2),
                (p.need_sink, p.need_cap, p.need_cap2)
            );
            for need in needs {
                let k = need.sink as usize;
                let want = need.min_cap as usize * self.descs[k].width();
                assert!(want > self.bufs[k].len());
                self.bufs[k].resize(want, 0);
                self.c_sinks[k].data = self.bufs[k].as_mut_ptr();
                self.c_sinks[k].data_size = self.bufs[k].len() as u64;
            }
        }
    }

    /// A row paused for space continues with the sinks that the state kept, and only the sink that was grown
    /// is converted again: the conversions are the sinks of the call that started the row, plus one for every
    /// pause, whatever the number of sinks.
    #[test]
    fn resuming_a_paused_row_converts_one_sink() {
        for fields in [1usize, 8, 64, 250] {
            let items = 100;
            let mut w = Wide::new(fields, items);
            let n_sinks = w.c_sinks.len() as u64;
            let state = avro_state_new(0);
            let mut pauses = 0u64;
            loop {
                let sinks = w.c_sinks.as_mut_ptr();
                let (rc, p) = unsafe { w.call(state, sinks) };
                assert_eq!(rc, 0);
                match p.status {
                    AVRO_STATUS_NEED_SPACE => {
                        assert_eq!((p.rows_done, p.consumed), (0, 0));
                        pauses += 1;
                        w.grow(&p);
                    }
                    AVRO_STATUS_ROW_LIMIT => {
                        assert_eq!((p.rows_done, p.consumed as usize), (1, w.input.len()));
                        break;
                    }
                    s => panic!("status {s}"),
                }
            }
            // The block of every array is asked for at once: one pause for each value sink.
            assert_eq!(pauses, fields as u64);
            assert_eq!(
                unsafe { (*(state as *mut FfiState)).stats.sinks_converted },
                n_sinks + pauses
            );
            for i in 0..fields {
                assert_eq!(
                    (w.c_sinks[2 * i].len, w.c_sinks[2 * i + 1].len),
                    (1, items as u64)
                );
                let values: Vec<i64> = w.bufs[2 * i + 1]
                    .chunks_exact(8)
                    .take(items)
                    .map(|c| i64::from_ne_bytes(c.try_into().unwrap()))
                    .collect();
                assert_eq!(
                    values,
                    (0..items)
                        .map(|j| (i * 1000 + j) as i64)
                        .collect::<Vec<_>>()
                );
            }
            unsafe { avro_state_free(state) };
        }
    }

    /// An array of records lacks room for its block in every field sink at once: one stop names all of them
    /// (`need_count`, `needs`, the first also in `need_sink`), and the call that continues the row converts
    /// each of them again, and no other: two calls and `n_sinks + fields` conversions, whatever `fields` is.
    #[test]
    fn a_record_of_many_fields_is_one_stop() {
        for fields in [1usize, 8, 64, 250] {
            let items = 100;
            let mut w = Wide::records(fields, items);
            let n_sinks = w.c_sinks.len() as u64;
            let state = avro_state_new(0);
            let (mut calls, mut stops) = (0u64, 0u64);
            loop {
                let sinks = w.c_sinks.as_mut_ptr();
                let (rc, p) = unsafe { w.call(state, sinks) };
                calls += 1;
                assert_eq!(rc, 0);
                match p.status {
                    AVRO_STATUS_NEED_SPACE => {
                        assert_eq!((p.rows_done, p.consumed), (0, 0));
                        assert_eq!(p.need_count, fields as u64);
                        let needs = unsafe { slice::from_raw_parts(p.needs, fields) };
                        // The field sinks, in the order of the program: the offsets sink has room.
                        let named: Vec<u32> = needs.iter().map(|n| n.sink).collect();
                        assert_eq!(named, (1..=fields as u32).collect::<Vec<_>>());
                        assert!(needs.iter().all(|n| n.min_cap == items as u64));
                        stops += 1;
                        w.grow(&p);
                    }
                    AVRO_STATUS_ROW_LIMIT => {
                        assert_eq!((p.rows_done, p.consumed as usize), (1, w.input.len()));
                        assert_eq!((p.need_count, p.needs), (0, std::ptr::null()));
                        break;
                    }
                    s => panic!("status {s}"),
                }
            }
            assert_eq!((calls, stops), (2, 1), "fields={fields}");
            assert_eq!(
                unsafe { (*(state as *mut FfiState)).stats.sinks_converted },
                n_sinks + fields as u64
            );
            assert_eq!((w.c_sinks[0].len, w.c_sinks[0].offset), (1, items as u64));
            for i in 0..fields {
                assert_eq!(w.c_sinks[1 + i].len, items as u64);
                let values: Vec<i64> = w.bufs[1 + i]
                    .chunks_exact(8)
                    .take(items)
                    .map(|c| i64::from_ne_bytes(c.try_into().unwrap()))
                    .collect();
                assert_eq!(
                    values,
                    (0..items)
                        .map(|j| (i * 1000 + j) as i64)
                        .collect::<Vec<_>>()
                );
            }
            unsafe { avro_state_free(state) };
        }
    }

    /// A paused row can only continue with the array of sinks it was paused with.
    #[test]
    fn a_paused_row_is_not_resumed_with_other_sinks() {
        let mut w = Wide::new(4, 100);
        let state = avro_state_new(0);
        let sinks = w.c_sinks.as_mut_ptr();
        let (rc, p) = unsafe { w.call(state, sinks) };
        assert_eq!((rc, p.status), (0, AVRO_STATUS_NEED_SPACE));
        w.grow(&p);
        let mut copy: Vec<AvroSink> = w
            .c_sinks
            .iter()
            .map(|s| AvroSink {
                data: s.data,
                data_size: s.data_size,
                data2: s.data2,
                data2_size: s.data2_size,
                len: s.len,
                len2: s.len2,
                offset: s.offset,
            })
            .collect();
        let (rc, _) = unsafe { w.call(state, copy.as_mut_ptr()) };
        assert_eq!(rc, -(ErrorCode::InvalidArgument as i32));
        // The state is reset: the row starts again from the beginning with the sinks at their start.
        let mut fresh = Wide::new(4, 100);
        let sinks = fresh.c_sinks.as_mut_ptr();
        let (rc, p) = unsafe { fresh.call(state, sinks) };
        assert_eq!((rc, p.status), (0, AVRO_STATUS_NEED_SPACE));
        unsafe { avro_state_free(state) };
    }

    #[test]
    fn ocf_through_the_c_abi() {
        // Header: schema "long", null codec, a custom entry; one block of two longs (1, -1).
        let sync = [7u8; 16];
        let mut file = b"Obj\x01".to_vec();
        file.push(0x04); // two entries
        for (k, v) in [(&b"avro.schema"[..], &b"\"long\""[..]), (b"x", b"y")] {
            file.push((k.len() * 2) as u8);
            file.extend_from_slice(k);
            file.push((v.len() * 2) as u8);
            file.extend_from_slice(v);
        }
        file.push(0);
        file.extend_from_slice(&sync);
        let header_len = file.len();
        file.extend_from_slice(&[0x04, 0x04, 0x02, 0x01]);
        file.extend_from_slice(&sync);
        unsafe {
            let (mut e, cap) = errbuf();
            let (mut h, mut consumed, mut needed) = (null_mut(), 0u64, 0u64);
            // A short window needs more.
            assert_eq!(
                avro_ocf_header_parse(
                    file.as_ptr(),
                    5,
                    0,
                    0,
                    &mut h,
                    &mut consumed,
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                AVRO_NEED_MORE
            );
            assert!(needed > 5 && needed as usize <= header_len);
            // With eof it is an error.
            assert_eq!(
                avro_ocf_header_parse(
                    file.as_ptr(),
                    5,
                    1,
                    0,
                    &mut h,
                    &mut consumed,
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::Eof as i32)
            );
            assert_eq!(
                avro_ocf_header_parse(
                    file.as_ptr(),
                    file.len(),
                    0,
                    0,
                    &mut h,
                    &mut consumed,
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            assert_eq!(consumed as usize, header_len);
            assert_eq!(avro_ocf_header_codec(h), 0);
            let mut s16 = [0u8; 16];
            avro_ocf_header_sync(h, s16.as_mut_ptr());
            assert_eq!(s16, sync);
            let (mut p, mut l) = (null(), 0usize);
            assert_eq!(
                avro_ocf_header_schema(h, &mut p, &mut l, e.as_mut_ptr(), cap),
                0
            );
            assert_eq!(slice::from_raw_parts(p, l), b"\"long\"");
            assert_eq!(
                avro_ocf_header_meta(h, b"x".as_ptr(), 1, &mut p, &mut l, e.as_mut_ptr(), cap),
                0
            );
            assert_eq!(slice::from_raw_parts(p, l), b"y");
            assert_eq!(
                avro_ocf_header_meta(h, b"nope".as_ptr(), 4, &mut p, &mut l, e.as_mut_ptr(), cap),
                AVRO_NOT_FOUND
            );
            assert_eq!(avro_ocf_header_meta_count(h), 2);
            let (mut k, mut kl) = (null(), 0usize);
            assert_eq!(
                avro_ocf_header_meta_at(h, 1, &mut k, &mut kl, &mut p, &mut l, e.as_mut_ptr(), cap),
                0
            );
            assert_eq!(slice::from_raw_parts(k, kl), b"x");
            assert_eq!(
                avro_ocf_header_meta_at(h, 2, &mut k, &mut kl, &mut p, &mut l, e.as_mut_ptr(), cap),
                -(ErrorCode::InvalidArgument as i32)
            );
            // block header
            let rest = &file[header_len..];
            let mut b = std::mem::MaybeUninit::<AvroBlockHeader>::zeroed();
            assert_eq!(
                avro_ocf_block_header(
                    rest.as_ptr(),
                    rest.len(),
                    0,
                    0,
                    1,
                    0,
                    b.as_mut_ptr(),
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            let b = b.assume_init();
            assert_eq!((b.object_count, b.byte_count, b.header_len), (2, 2, 2));
            assert_eq!(
                avro_ocf_check_sync(
                    rest[4..].as_ptr(),
                    16,
                    0,
                    sync.as_ptr(),
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                0
            );
            assert_eq!(
                avro_ocf_check_sync(
                    rest[4..].as_ptr(),
                    15,
                    0,
                    sync.as_ptr(),
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                AVRO_NEED_MORE
            );
            assert_eq!(needed, 16);
            assert_eq!(
                avro_ocf_check_sync(
                    rest[3..].as_ptr(),
                    16,
                    0,
                    sync.as_ptr(),
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::SyncMismatch as i32)
            );
            let mut bh = std::mem::MaybeUninit::<AvroBlockHeader>::zeroed();
            assert_eq!(
                avro_ocf_block_header(
                    null(),
                    0,
                    1,
                    0,
                    1,
                    0,
                    bh.as_mut_ptr(),
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                AVRO_END
            );
            assert_eq!(
                avro_ocf_block_header(
                    rest.as_ptr(),
                    rest.len(),
                    0,
                    9,
                    1,
                    0,
                    bh.as_mut_ptr(),
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::InvalidArgument as i32)
            );
            // 3 objects cannot fit in 2 bytes of 1 byte records.
            let lying = [0x06u8, 0x04, 0x02, 0x01];
            assert_eq!(
                avro_ocf_block_header(
                    lying.as_ptr(),
                    4,
                    0,
                    0,
                    1,
                    0,
                    bh.as_mut_ptr(),
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::ObjectCountExceedsPayload as i32)
            );
            assert_eq!(
                avro_ocf_check_object_count(3, 2, 1, e.as_mut_ptr(), cap),
                -(ErrorCode::ObjectCountExceedsPayload as i32)
            );
            assert_eq!(avro_ocf_check_object_count(2, 2, 1, e.as_mut_ptr(), cap), 0);
            avro_ocf_header_free(h);
            avro_ocf_header_free(null_mut());
            // Bad magic.
            let mut h2 = null_mut();
            let bad = b"Obj\x02xxxxxxxx";
            assert_eq!(
                avro_ocf_header_parse(
                    bad.as_ptr(),
                    bad.len(),
                    1,
                    0,
                    &mut h2,
                    &mut consumed,
                    &mut needed,
                    e.as_mut_ptr(),
                    cap
                ),
                -(ErrorCode::BadMagic as i32)
            );
            assert_eq!(msg(&e), "Invalid data file. Magic does not match: ");
        }
    }

    #[test]
    fn range_helper() {
        let data: Vec<u8> = [1i32, 2, 300, 4]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let mut bad = 0u64;
        unsafe {
            assert_eq!(
                avro_first_out_of_range_i32(data.as_ptr(), 4, 0, 100, &mut bad),
                1
            );
            assert_eq!(bad, 2);
            assert_eq!(
                avro_first_out_of_range_i32(data.as_ptr(), 4, 0, 1000, &mut bad),
                0
            );
            assert_eq!(avro_first_out_of_range_i32(null(), 0, 0, 0, &mut bad), 0);
            assert_eq!(
                avro_first_out_of_range_i32(null(), 4, 0, 0, &mut bad),
                -(ErrorCode::InvalidArgument as i32)
            );
            assert_eq!(
                avro_first_out_of_range_i32(data.as_ptr(), usize::MAX, 0, 0, &mut bad),
                -(ErrorCode::InvalidArgument as i32)
            );
        }
    }
}
