//! The C ABI of `dwarf_rust.h`.
//!
//! This is the only module that contains `unsafe` code. It does exactly three things:
//!
//! 1. exposes `Box`ed handles as opaque pointers and takes them back;
//! 2. reconstructs `&[u8]` over the caller's immutable input for the duration of one call;
//! 3. reconstructs `&mut [MaybeUninit<u8>]` over the caller's output buffers for the duration of
//!    one write, after every descriptor was validated.
//!
//! No lifetime is erased to `'static`: the parser (`UnitState`) never stores a reference into the
//! input or into an output buffer; both are passed in per call. Every entry point contains panics
//! (`catch_unwind`) and reports them as `CH_DWARF_ERR_INTERNAL`; no unwind crosses the C ABI.

#![allow(unsafe_code)]
#![allow(clippy::missing_safety_doc)]

use crate::control::{Control, Poll};
use crate::error::{DwarfError, Result, StatusCode};
use crate::file::FileInner;
use crate::names::{write_full_name_table, NameTableKind};
use crate::outbuf::{
    checked_required_cap, BatchSinkAccess, ProjectionPlan, SinkId, StringSinkAccess,
    COL_ANCESTOR_OFFSETS, COL_ANCESTOR_TAGS, COL_ATTR_FORM, COL_ATTR_INT, COL_ATTR_NAME,
    COL_ATTR_STR, COL_DECL_FILE, COL_DECL_LINE, COL_LINKAGE_NAME, COL_NAME, COL_OFFSET, COL_RANGES,
    COL_SIZE, COL_TAG, SINK_COUNT,
};
use crate::unit::UnitState;
use core::mem::MaybeUninit;
use std::ffi::{c_char, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

pub const CH_DWARF_ERROR_BUF_SIZE: usize = 256;

pub const CH_DWARF_CANCEL_CONTINUE: i32 = 0;
pub const CH_DWARF_CANCEL_STOP: i32 = 1;
pub const CH_DWARF_CANCEL_FAILED: i32 = 2;
pub const CH_DWARF_RESERVE_OK: i32 = 0;
pub const CH_DWARF_RESERVE_FAILED: i32 = 1;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChDwarfOptions {
    pub flags: u32,
    pub reserved: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChDwarfProjection {
    pub column_mask: u32,
    pub need_ancestor_offsets_vec: u32,
    pub need_attr_offsets_vec: u32,
    pub reserved: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChDwarfSink {
    pub data: *mut u8,
    pub len: usize,
    pub cap: usize,
    pub elem_size: u32,
    pub reserved: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChDwarfStringSink {
    pub chars: ChDwarfSink,
    pub offsets: ChDwarfSink,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChDwarfBatchSinks {
    pub sinks: [ChDwarfSink; SINK_COUNT],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChDwarfReserveRequest {
    pub sink_id: u32,
    pub reserved: u32,
    pub min_cap: usize,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChDwarfProgress {
    pub rows: usize,
    pub bytes_advanced: u64,
    pub unit_name_ptr: *const u8,
    pub unit_name_len: usize,
    pub unit_done: u32,
    pub reserved: u32,
}

pub type ChDwarfReserveFn = Option<
    unsafe extern "C" fn(
        opaque: *mut c_void,
        sinks: *mut ChDwarfBatchSinks,
        requests: *const ChDwarfReserveRequest,
        request_count: usize,
    ) -> i32,
>;

pub type ChDwarfStringReserveFn = Option<
    unsafe extern "C" fn(
        opaque: *mut c_void,
        sink: *mut ChDwarfStringSink,
        min_chars_cap: usize,
        min_offsets_cap: usize,
    ) -> i32,
>;

pub type ChDwarfCancelFn = Option<unsafe extern "C" fn(opaque: *mut c_void) -> i32>;

/// Opaque file handle (`ChDwarfFile`).
pub struct ChDwarfFile {
    inner: Arc<FileInner>,
    /// The caller's input span, exactly as passed to `ch_dwarf_file_open` (keeps its provenance).
    input: *const u8,
}

/// Opaque unit handle (`ChDwarfUnit`). `Send`, not `Sync`.
pub struct ChDwarfUnit {
    state: UnitState,
}

// ---------------------------------------------------------------------------------------------
// Error reporting and panic containment
// ---------------------------------------------------------------------------------------------

/// Writes `msg` into the caller's `char[256]`, truncated, always NUL-terminated.
fn write_error(buf: *mut c_char, msg: &str) {
    if buf.is_null() {
        return;
    }
    let bytes = msg.as_bytes();
    let n = bytes.len().min(CH_DWARF_ERROR_BUF_SIZE - 1);
    // SAFETY: the caller guarantees `buf` points to CH_DWARF_ERROR_BUF_SIZE writable bytes, and
    // `n + 1 <= CH_DWARF_ERROR_BUF_SIZE`.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), buf.cast::<u8>(), n);
        *buf.add(n) = 0;
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

/// Runs `f`, converting an error or a panic into a status plus message. `error` is cleared first.
fn guarded<F>(error: *mut c_char, f: F) -> i32
where
    F: FnOnce() -> Result<StatusCode>,
{
    write_error(error, "");
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(code)) => code as i32,
        Ok(Err(e)) => {
            if e.code != StatusCode::Cancelled {
                write_error(error, &e.message);
            }
            e.code as i32
        }
        Err(payload) => {
            write_error(
                error,
                &format!("internal error: panic: {}", panic_message(&*payload)),
            );
            StatusCode::ErrInternal as i32
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Sink descriptors
// ---------------------------------------------------------------------------------------------

/// Largest byte size a sink may describe; mirrors the `isize::MAX` bound of Rust slices.
const MAX_SINK_BYTES: usize = isize::MAX as usize;

fn sink_bytes(s: &ChDwarfSink) -> Option<usize> {
    s.cap
        .checked_mul(s.elem_size as usize)
        .filter(|&b| b <= MAX_SINK_BYTES)
}

/// Checks one descriptor; `expected` is the element width the sink must have.
fn validate_sink(s: &ChDwarfSink, expected: u32, what: &str) -> Result<()> {
    if s.elem_size != expected {
        return Err(DwarfError::invalid_arg(format!(
            "sink {what}: element size {} (expected {expected})",
            s.elem_size
        )));
    }
    if s.len > s.cap {
        return Err(DwarfError::invalid_arg(format!(
            "sink {what}: len exceeds cap"
        )));
    }
    if s.data.is_null() && s.cap != 0 {
        return Err(DwarfError::invalid_arg(format!(
            "sink {what}: null data with non-zero capacity"
        )));
    }
    if sink_bytes(s).is_none() {
        return Err(DwarfError::invalid_arg(format!(
            "sink {what}: capacity is too large"
        )));
    }
    Ok(())
}

/// The writable bytes of a validated sink.
#[inline(always)]
fn sink_buffer(s: &mut ChDwarfSink) -> &mut [MaybeUninit<u8>] {
    // `validate_sink` established (at call start and after every reserve callback) that
    // `s.cap * s.elem_size <= isize::MAX` and `s.data` is non-null whenever `cap > 0`.
    let bytes = s.cap.wrapping_mul(s.elem_size as usize);
    if s.data.is_null() || bytes == 0 {
        return &mut [];
    }
    // SAFETY: validated at call entry and after every reserve callback.
    unsafe { core::slice::from_raw_parts_mut(s.data.cast::<MaybeUninit<u8>>(), bytes) }
}

/// `BatchSinkAccess` over the caller's `ChDwarfBatchSinks`.
struct FfiBatch<'a> {
    sinks: &'a mut ChDwarfBatchSinks,
    plan: ProjectionPlan,
    reserve_cb: unsafe extern "C" fn(
        *mut c_void,
        *mut ChDwarfBatchSinks,
        *const ChDwarfReserveRequest,
        usize,
    ) -> i32,
    opaque: *mut c_void,
}

const ALL_SINKS: [SinkId; SINK_COUNT] = [
    SinkId::Offset,
    SinkId::Size,
    SinkId::Tag,
    SinkId::AncestorTags,
    SinkId::AncestorOffsets,
    SinkId::AncestorArrayOffsets,
    SinkId::NameChars,
    SinkId::NameOffsets,
    SinkId::LinkageNameChars,
    SinkId::LinkageNameOffsets,
    SinkId::DeclFileChars,
    SinkId::DeclFileOffsets,
    SinkId::DeclLine,
    SinkId::RangesStart,
    SinkId::RangesEnd,
    SinkId::RangesOffsets,
    SinkId::AttrName,
    SinkId::AttrForm,
    SinkId::AttrInt,
    SinkId::AttrStrChars,
    SinkId::AttrStrOffsets,
    SinkId::AttrOffsets,
];

impl<'a> FfiBatch<'a> {
    /// Validates every active descriptor. With `fresh`, every active sink must also be empty.
    fn validate(&self, fresh: bool) -> Result<()> {
        for id in ALL_SINKS {
            if !self.plan.sink_active(id) {
                continue;
            }
            let s = &self.sinks.sinks[id as usize];
            validate_sink(s, id.expected_elem_size(), &format!("{id:?}"))?;
            if fresh && s.len != 0 {
                return Err(DwarfError::invalid_arg(format!(
                    "sink {id:?} must be empty at the start of a call"
                )));
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn active(&mut self, id: SinkId) -> Result<&mut ChDwarfSink> {
        if !self.plan.sink_active(id) {
            return Err(Self::inactive_err(id));
        }
        Ok(&mut self.sinks.sinks[id as usize])
    }

    #[cold]
    #[inline(never)]
    fn inactive_err(id: SinkId) -> DwarfError {
        DwarfError::internal(format!("write to inactive sink {id:?}"))
    }

    #[cold]
    #[inline(never)]
    fn full_err(msg: &'static str) -> DwarfError {
        DwarfError::internal(msg)
    }

    #[cold]
    #[inline(never)]
    fn ensure_slow(&mut self, adds: &[(SinkId, usize)]) -> Result<()> {
        let mut requests = [ChDwarfReserveRequest {
            sink_id: 0,
            reserved: 0,
            min_cap: 0,
        }; SINK_COUNT];
        let mut n = 0usize;
        for &(id, add) in adds {
            let s = self.active(id)?;
            if let Some(min_cap) = checked_required_cap(s.len, s.cap, add)? {
                if requests[..n].iter().any(|r| r.sink_id == id as u32) {
                    return Err(DwarfError::internal("duplicate reserve request"));
                }
                requests[n] = ChDwarfReserveRequest {
                    sink_id: id as u32,
                    reserved: 0,
                    min_cap,
                };
                n += 1;
            }
        }
        if n == 0 {
            return Ok(());
        }

        let mut lens = [0usize; SINK_COUNT];
        for id in ALL_SINKS {
            lens[id as usize] = self.sinks.sinks[id as usize].len;
        }
        let sinks_ptr: *mut ChDwarfBatchSinks = &mut *self.sinks;
        let rc = unsafe { (self.reserve_cb)(self.opaque, sinks_ptr, requests.as_ptr(), n) };
        if rc != CH_DWARF_RESERVE_OK {
            return Err(DwarfError::callback("reserve callback failed"));
        }

        self.validate(false)?;
        for id in ALL_SINKS {
            if self.plan.sink_active(id) && self.sinks.sinks[id as usize].len != lens[id as usize] {
                return Err(DwarfError::callback(
                    "reserve callback changed the initialized length of a sink",
                ));
            }
        }
        for r in &requests[..n] {
            if self.sinks.sinks[r.sink_id as usize].cap < r.min_cap {
                return Err(DwarfError::callback(
                    "reserve callback did not provide the requested capacity",
                ));
            }
        }
        Ok(())
    }
}
impl BatchSinkAccess for FfiBatch<'_> {
    #[inline]
    fn plan(&self) -> ProjectionPlan {
        self.plan
    }

    #[inline(always)]
    fn sink_len(&self, id: SinkId) -> usize {
        self.sinks.sinks[id as usize].len
    }

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn has_row_space(
        &self,
        ancestors: usize,
        name_len: usize,
        linkage_len: usize,
        decl_file_len: usize,
        ranges: usize,
        attrs: usize,
        attr_str_chars: usize,
    ) -> bool {
        let p = self.plan;
        let s = &self.sinks.sinks;
        macro_rules! ok {
            ($id:expr, $add:expr) => {{
                let d = &s[$id as usize];
                d.len <= d.cap && d.cap - d.len >= $add
            }};
        }
        if p.has(COL_OFFSET) && !ok!(SinkId::Offset, 1) {
            return false;
        }
        if p.has(COL_SIZE) && !ok!(SinkId::Size, 1) {
            return false;
        }
        if p.has(COL_TAG) && !ok!(SinkId::Tag, 1) {
            return false;
        }
        if p.need_ancestor_array_offsets {
            if !ok!(SinkId::AncestorArrayOffsets, 1) {
                return false;
            }
            if p.has(COL_ANCESTOR_TAGS) && !ok!(SinkId::AncestorTags, ancestors) {
                return false;
            }
            if p.has(COL_ANCESTOR_OFFSETS) && !ok!(SinkId::AncestorOffsets, ancestors) {
                return false;
            }
        }
        if p.has(COL_NAME) && (!ok!(SinkId::NameOffsets, 1) || !ok!(SinkId::NameChars, name_len)) {
            return false;
        }
        if p.has(COL_LINKAGE_NAME)
            && (!ok!(SinkId::LinkageNameOffsets, 1) || !ok!(SinkId::LinkageNameChars, linkage_len))
        {
            return false;
        }
        if p.has(COL_DECL_FILE)
            && (!ok!(SinkId::DeclFileOffsets, 1) || !ok!(SinkId::DeclFileChars, decl_file_len))
        {
            return false;
        }
        if p.has(COL_DECL_LINE) && !ok!(SinkId::DeclLine, 1) {
            return false;
        }
        if p.has(COL_RANGES)
            && (!ok!(SinkId::RangesOffsets, 1)
                || !ok!(SinkId::RangesStart, ranges)
                || !ok!(SinkId::RangesEnd, ranges))
        {
            return false;
        }
        if p.need_attr_offsets {
            if !ok!(SinkId::AttrOffsets, 1) {
                return false;
            }
            if p.has(COL_ATTR_NAME) && !ok!(SinkId::AttrName, attrs) {
                return false;
            }
            if p.has(COL_ATTR_FORM) && !ok!(SinkId::AttrForm, attrs) {
                return false;
            }
            if p.has(COL_ATTR_INT) && !ok!(SinkId::AttrInt, attrs) {
                return false;
            }
            if p.has(COL_ATTR_STR)
                && (!ok!(SinkId::AttrStrOffsets, attrs)
                    || !ok!(SinkId::AttrStrChars, attr_str_chars))
            {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    fn ensure(&mut self, adds: &[(SinkId, usize)]) -> Result<()> {
        for &(id, add) in adds {
            if !self.plan.sink_active(id) {
                return self.ensure_slow(adds);
            }
            let s = &self.sinks.sinks[id as usize];
            if s.len > s.cap || s.cap - s.len < add {
                return self.ensure_slow(adds);
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn push_u64(&mut self, id: SinkId, val: u64) -> Result<()> {
        let s = self.active(id)?;
        let at = s.len;
        if at >= s.cap {
            return Err(Self::full_err("sink is full (missing reservation)"));
        }
        let buf = sink_buffer(s);
        let bytes = val.to_ne_bytes();
        // SAFETY: `at < s.cap` and `validate_sink` verified `s.cap * 8 == buf.len()`.
        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr().cast::<MaybeUninit<u8>>(),
                buf.as_mut_ptr().add(at * 8),
                8,
            );
        }
        s.len = at + 1;
        Ok(())
    }

    #[inline(always)]
    fn push_u32(&mut self, id: SinkId, val: u32) -> Result<()> {
        let s = self.active(id)?;
        let at = s.len;
        if at >= s.cap {
            return Err(Self::full_err("sink is full (missing reservation)"));
        }
        let buf = sink_buffer(s);
        let bytes = val.to_ne_bytes();
        // SAFETY: `at < s.cap` and `validate_sink` verified `s.cap * 4 == buf.len()`.
        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr().cast::<MaybeUninit<u8>>(),
                buf.as_mut_ptr().add(at * 4),
                4,
            );
        }
        s.len = at + 1;
        Ok(())
    }

    #[inline(always)]
    fn push_u16(&mut self, id: SinkId, val: u16) -> Result<()> {
        let s = self.active(id)?;
        let at = s.len;
        if at >= s.cap {
            return Err(Self::full_err("sink is full (missing reservation)"));
        }
        let buf = sink_buffer(s);
        let bytes = val.to_ne_bytes();
        // SAFETY: `at < s.cap` and `validate_sink` verified `s.cap * 2 == buf.len()`.
        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr().cast::<MaybeUninit<u8>>(),
                buf.as_mut_ptr().add(at * 2),
                2,
            );
        }
        s.len = at + 1;
        Ok(())
    }

    #[inline(always)]
    fn push_string(&mut self, chars_id: SinkId, offsets_id: SinkId, bytes: &[u8]) -> Result<()> {
        let end = {
            let chars = self.active(chars_id)?;
            if !bytes.is_empty() {
                let at = chars.len;
                let Some(end) = at.checked_add(bytes.len()).filter(|&e| e <= chars.cap) else {
                    return Err(Self::full_err("chars sink is full (missing reservation)"));
                };
                let buf = sink_buffer(chars);
                // SAFETY: `at + bytes.len() <= chars.cap == buf.len()`.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        bytes.as_ptr().cast::<MaybeUninit<u8>>(),
                        buf.as_mut_ptr().add(at),
                        bytes.len(),
                    );
                }
                chars.len = end;
                end
            } else {
                chars.len
            }
        };
        self.push_u64(offsets_id, end as u64)
    }
}

/// `StringSinkAccess` over the caller's `ChDwarfStringSink` (the name tables).
struct FfiStringSink<'a> {
    sink: &'a mut ChDwarfStringSink,
    reserve_cb: unsafe extern "C" fn(*mut c_void, *mut ChDwarfStringSink, usize, usize) -> i32,
    opaque: *mut c_void,
}

impl FfiStringSink<'_> {
    fn validate(&self) -> Result<()> {
        validate_sink(&self.sink.chars, 1, "chars")?;
        validate_sink(&self.sink.offsets, 8, "offsets")
    }
}

impl StringSinkAccess for FfiStringSink<'_> {
    fn reserve_exact(&mut self, add_chars: usize, add_offsets: usize) -> Result<()> {
        let chars = checked_required_cap(self.sink.chars.len, self.sink.chars.cap, add_chars)?;
        let offsets =
            checked_required_cap(self.sink.offsets.len, self.sink.offsets.cap, add_offsets)?;
        if chars.is_none() && offsets.is_none() {
            return Ok(());
        }
        let min_chars = chars.unwrap_or(self.sink.chars.cap);
        let min_offsets = offsets.unwrap_or(self.sink.offsets.cap);
        let (chars_len, offsets_len) = (self.sink.chars.len, self.sink.offsets.len);
        let ptr: *mut ChDwarfStringSink = &mut *self.sink;
        // SAFETY: see `FfiBatch::ensure`.
        let rc = unsafe { (self.reserve_cb)(self.opaque, ptr, min_chars, min_offsets) };
        if rc != CH_DWARF_RESERVE_OK {
            return Err(DwarfError::callback("name table reserve callback failed"));
        }
        self.validate()?;
        if self.sink.chars.len != chars_len
            || self.sink.offsets.len != offsets_len
            || self.sink.chars.cap < min_chars
            || self.sink.offsets.cap < min_offsets
        {
            return Err(DwarfError::callback(
                "name table reserve callback did not honor the request",
            ));
        }
        Ok(())
    }

    #[inline(always)]
    fn push_str_unchecked(&mut self, bytes: &[u8]) -> Result<()> {
        let at = self.sink.chars.len;
        let Some(end) = at
            .checked_add(bytes.len())
            .filter(|&e| e <= self.sink.chars.cap)
        else {
            return Err(FfiBatch::full_err("name table chars sink is full"));
        };
        let off_at = self.sink.offsets.len;
        if off_at >= self.sink.offsets.cap {
            return Err(FfiBatch::full_err("name table offsets sink is full"));
        }
        if !bytes.is_empty() {
            let cbuf = sink_buffer(&mut self.sink.chars);
            unsafe {
                core::ptr::copy_nonoverlapping(
                    bytes.as_ptr().cast::<MaybeUninit<u8>>(),
                    cbuf.as_mut_ptr().add(at),
                    bytes.len(),
                );
            }
        }
        self.sink.chars.len = end;
        let obuf = sink_buffer(&mut self.sink.offsets);
        let obytes = (end as u64).to_ne_bytes();
        unsafe {
            core::ptr::copy_nonoverlapping(
                obytes.as_ptr().cast::<MaybeUninit<u8>>(),
                obuf.as_mut_ptr().add(off_at * 8),
                8,
            );
        }
        self.sink.offsets.len = off_at + 1;
        Ok(())
    }
}

/// `Control` over the caller's cancel callback.
struct FfiControl {
    cancel_cb: unsafe extern "C" fn(*mut c_void) -> i32,
    opaque: *mut c_void,
}

impl Control for FfiControl {
    fn poll(&mut self) -> Result<Poll> {
        // SAFETY: the callback is a non-throwing C function that accepts `opaque`.
        match unsafe { (self.cancel_cb)(self.opaque) } {
            CH_DWARF_CANCEL_CONTINUE => Ok(Poll::Continue),
            CH_DWARF_CANCEL_STOP => Ok(Poll::Stop),
            CH_DWARF_CANCEL_FAILED => Err(DwarfError::callback("cancel callback failed")),
            other => Err(DwarfError::callback(format!(
                "cancel callback returned invalid status {other}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Input reconstruction
// ---------------------------------------------------------------------------------------------

/// The caller's input for the duration of one call.
///
/// SAFETY contract (header): the bytes passed to `ch_dwarf_file_open` stay mapped, immutable and
/// unmoved until `ch_dwarf_file_free` returns, and no call overlaps a free.
fn input_of(file: &ChDwarfFile) -> &[u8] {
    let len = file.inner.raw_len;
    if len == 0 {
        return &[];
    }
    // SAFETY: see above; `input`/`len` were captured from a valid non-null slice at open.
    unsafe { core::slice::from_raw_parts(file.input, len) }
}

// ---------------------------------------------------------------------------------------------
// Exported functions
// ---------------------------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn ch_dwarf_file_open(
    bytes: *const u8,
    len: usize,
    options: *const ChDwarfOptions,
    out_file: *mut *mut ChDwarfFile,
    error: *mut c_char,
) -> i32 {
    guarded(error, || {
        if out_file.is_null() {
            return Err(DwarfError::invalid_arg("out_file is null"));
        }
        // SAFETY: non-null, caller-owned out pointer.
        unsafe { *out_file = core::ptr::null_mut() };
        // SAFETY: a non-null `options` points to a valid ChDwarfOptions.
        let options = unsafe { options.as_ref() }
            .ok_or_else(|| DwarfError::invalid_arg("options is null"))?;
        if options.flags != 0 || options.reserved != 0 {
            return Err(DwarfError::invalid_arg("unsupported option flags"));
        }
        if bytes.is_null() && len != 0 {
            return Err(DwarfError::invalid_arg(
                "bytes is null with non-zero length",
            ));
        }
        let input: &[u8] = if len == 0 {
            &[]
        } else {
            // SAFETY: `bytes` is non-null and the caller guarantees `len` readable bytes.
            unsafe { core::slice::from_raw_parts(bytes, len) }
        };
        let inner = FileInner::open(input, bytes as usize)?;
        let handle = Box::new(ChDwarfFile {
            inner,
            input: bytes,
        });
        // SAFETY: `out_file` was checked above.
        unsafe { *out_file = Box::into_raw(handle) };
        Ok(StatusCode::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn ch_dwarf_file_unit_count(
    file: *const ChDwarfFile,
    out_count: *mut usize,
) -> i32 {
    guarded(core::ptr::null_mut(), || {
        // SAFETY: non-null `file` is a live handle from `ch_dwarf_file_open`.
        let file =
            unsafe { file.as_ref() }.ok_or_else(|| DwarfError::invalid_arg("file is null"))?;
        if out_count.is_null() {
            return Err(DwarfError::invalid_arg("out_count is null"));
        }
        // SAFETY: checked non-null.
        unsafe { *out_count = file.inner.units.len() };
        Ok(StatusCode::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn ch_dwarf_unit_open(
    file: *const ChDwarfFile,
    unit_index: usize,
    out_unit: *mut *mut ChDwarfUnit,
    out_unit_offset: *mut u64,
    error: *mut c_char,
) -> i32 {
    guarded(error, || {
        if out_unit.is_null() || out_unit_offset.is_null() {
            return Err(DwarfError::invalid_arg("output pointer is null"));
        }
        // SAFETY: checked non-null.
        unsafe { *out_unit = core::ptr::null_mut() };
        // SAFETY: non-null `file` is a live handle from `ch_dwarf_file_open`.
        let file =
            unsafe { file.as_ref() }.ok_or_else(|| DwarfError::invalid_arg("file is null"))?;
        let state = UnitState::open(&file.inner, input_of(file), unit_index)?;
        let offset = state.unit_offset();
        let handle = Box::new(ChDwarfUnit { state });
        // SAFETY: checked non-null.
        unsafe {
            *out_unit_offset = offset;
            *out_unit = Box::into_raw(handle);
        }
        Ok(StatusCode::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn ch_dwarf_unit_parse_chunk(
    file: *const ChDwarfFile,
    unit: *mut ChDwarfUnit,
    projection: *const ChDwarfProjection,
    sinks: *mut ChDwarfBatchSinks,
    reserve_cb: ChDwarfReserveFn,
    cancel_cb: ChDwarfCancelFn,
    opaque: *mut c_void,
    max_rows: usize,
    out_progress: *mut ChDwarfProgress,
    error: *mut c_char,
) -> i32 {
    guarded(error, || {
        // SAFETY: non-null handles/pointers are live and exclusively ours for the call (a unit is
        // never entered concurrently; the caller owns the sink descriptors and the progress).
        let file =
            unsafe { file.as_ref() }.ok_or_else(|| DwarfError::invalid_arg("file is null"))?;
        let unit =
            unsafe { unit.as_mut() }.ok_or_else(|| DwarfError::invalid_arg("unit is null"))?;
        let projection = unsafe { projection.as_ref() }
            .ok_or_else(|| DwarfError::invalid_arg("projection is null"))?;
        let sinks =
            unsafe { sinks.as_mut() }.ok_or_else(|| DwarfError::invalid_arg("sinks is null"))?;
        let progress = unsafe { out_progress.as_mut() }
            .ok_or_else(|| DwarfError::invalid_arg("out_progress is null"))?;
        let reserve_cb = reserve_cb.ok_or_else(|| DwarfError::invalid_arg("reserve_cb is null"))?;
        let cancel_cb = cancel_cb.ok_or_else(|| DwarfError::invalid_arg("cancel_cb is null"))?;

        // Identity: the unit must belong to this file (and so to this input span).
        if unit.state.file_generation() != file.inner.generation {
            return Err(DwarfError::invalid_arg("unit does not belong to this file"));
        }
        if max_rows == 0 {
            return Err(DwarfError::invalid_arg("max_rows must be positive"));
        }
        let plan = ProjectionPlan::validate_and_build(
            projection.column_mask,
            projection.need_ancestor_offsets_vec,
            projection.need_attr_offsets_vec,
            projection.reserved,
        )?;

        let mut batch = FfiBatch {
            sinks,
            plan,
            reserve_cb,
            opaque,
        };
        batch.validate(true)?;
        let mut control = FfiControl { cancel_cb, opaque };

        let result = unit
            .state
            .parse_chunk(input_of(file), &mut batch, &mut control, max_rows)?;

        let name = unit.state.unit_name();
        *progress = ChDwarfProgress {
            rows: result.rows,
            bytes_advanced: result.bytes_advanced,
            unit_name_ptr: name.as_ptr(),
            unit_name_len: name.len(),
            unit_done: u32::from(result.done),
            reserved: 0,
        };
        Ok(if result.done {
            StatusCode::UnitDone
        } else {
            StatusCode::ChunkReady
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn ch_dwarf_write_name_table(
    kind: i32,
    string_sink: *mut ChDwarfStringSink,
    reserve_cb: ChDwarfStringReserveFn,
    opaque: *mut c_void,
    error: *mut c_char,
) -> i32 {
    guarded(error, || {
        let kind = NameTableKind::from_i32(kind)
            .ok_or_else(|| DwarfError::invalid_arg("unknown name table kind"))?;
        // SAFETY: non-null `string_sink` is a caller-owned descriptor.
        let sink = unsafe { string_sink.as_mut() }
            .ok_or_else(|| DwarfError::invalid_arg("string_sink is null"))?;
        let reserve_cb = reserve_cb.ok_or_else(|| DwarfError::invalid_arg("reserve_cb is null"))?;
        let mut out = FfiStringSink {
            sink,
            reserve_cb,
            opaque,
        };
        out.validate()?;
        if out.sink.chars.len != 0 || out.sink.offsets.len != 0 {
            return Err(DwarfError::invalid_arg("name table sink must be empty"));
        }
        write_full_name_table(kind, &mut out)?;
        Ok(StatusCode::Ok)
    })
}

#[no_mangle]
pub unsafe extern "C" fn ch_dwarf_unit_free(unit: *mut ChDwarfUnit) {
    if !unit.is_null() {
        // SAFETY: a non-null handle came from `Box::into_raw` in `ch_dwarf_unit_open` and is freed
        // once. Dropping cannot unwind in practice; contain it regardless.
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(unit) })));
    }
}

#[no_mangle]
pub unsafe extern "C" fn ch_dwarf_file_free(file: *mut ChDwarfFile) {
    if !file.is_null() {
        // SAFETY: as for `ch_dwarf_unit_free`; units hold their own `Arc` to the file state, so
        // freeing the file handle first is safe for Rust, though the caller's contract orders it
        // after the units.
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(file) })));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(buf: &[c_char; CH_DWARF_ERROR_BUF_SIZE]) -> Vec<u8> {
        buf.iter()
            .take_while(|&&c| c != 0)
            .map(|&c| c as u8)
            .collect()
    }

    #[test]
    fn error_text_is_truncated_and_always_nul_terminated() {
        for len in [0usize, 1, 100, 254, 255, 256, 257, 10_000] {
            let mut buf = [0x55 as c_char; CH_DWARF_ERROR_BUF_SIZE];
            write_error(buf.as_mut_ptr(), &"e".repeat(len));
            let t = text(&buf);
            assert_eq!(t.len(), len.min(CH_DWARF_ERROR_BUF_SIZE - 1), "len {len}");
            assert!(t.iter().all(|&b| b == b'e'));
            assert_eq!(buf[t.len()], 0, "terminator at {}", t.len());
        }
        // A null buffer is tolerated.
        write_error(core::ptr::null_mut(), "ignored");
    }

    #[test]
    fn guarded_contains_panics_and_reports_errors() {
        let mut buf = [0x55 as c_char; CH_DWARF_ERROR_BUF_SIZE];
        assert_eq!(guarded(buf.as_mut_ptr(), || Ok(StatusCode::Ok)), 0);
        assert!(text(&buf).is_empty(), "success clears stale text");

        let rc = guarded(buf.as_mut_ptr(), || -> Result<StatusCode> {
            panic!("boom {}", 7)
        });
        assert_eq!(rc, StatusCode::ErrInternal as i32);
        let t = String::from_utf8(text(&buf)).unwrap();
        assert!(t.contains("panic") && t.contains("boom 7"), "{t}");

        let rc = guarded(buf.as_mut_ptr(), || -> Result<StatusCode> {
            Err(DwarfError::dwarf("bad"))
        });
        assert_eq!(rc, StatusCode::ErrDwarf as i32);
        assert_eq!(text(&buf), b"bad");

        // Cancellation is a status, not an error text.
        let rc = guarded(buf.as_mut_ptr(), || -> Result<StatusCode> {
            Err(DwarfError::cancelled())
        });
        assert_eq!(rc, StatusCode::Cancelled as i32);
        assert!(text(&buf).is_empty());

        // Non-string panic payloads.
        let rc = guarded(buf.as_mut_ptr(), || -> Result<StatusCode> {
            std::panic::panic_any(5u32)
        });
        assert_eq!(rc, StatusCode::ErrInternal as i32);
    }
}
