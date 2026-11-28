//! Shared harness for the integration tests: a C-ABI client with Vec-backed sinks that behaves like
//! the ClickHouse adapter (geometric growth through `reserve_cb`, cancel polling), a decoder for
//! the produced columns, and an ELF/DWARF builder for synthetic inputs.
#![allow(dead_code, clippy::missing_safety_doc, unsafe_code)]

use _ch_rust_dwarf::error::StatusCode;
use _ch_rust_dwarf::ffi::*;
use _ch_rust_dwarf::outbuf::*;
use std::ffi::{c_char, c_void};

pub const OK: i32 = 0;
pub const CHUNK_READY: i32 = 1;
pub const UNIT_DONE: i32 = 2;
pub const CANCELLED: i32 = 3;
pub const ERR_INVALID_ARG: i32 = -1;
pub const ERR_ELF: i32 = -2;
pub const ERR_MISSING_REQUIRED_SECTION: i32 = -3;
pub const ERR_DWARF: i32 = -4;
pub const ERR_MISSING_AUX_SECTION: i32 = -5;
pub const ERR_CALLBACK: i32 = -6;
pub const ERR_INTERNAL: i32 = -7;

/// ELF-open failures the C++ adapter maps to `CANNOT_PARSE_ELF`.
pub fn is_elf_class(status: i32) -> bool {
    status == ERR_ELF || status == ERR_MISSING_REQUIRED_SECTION
}

/// DWARF failures the C++ adapter maps to `CANNOT_PARSE_DWARF`.
pub fn is_dwarf_class(status: i32) -> bool {
    status == ERR_DWARF || status == ERR_MISSING_AUX_SECTION
}

pub fn status_name(status: i32) -> StatusCode {
    match status {
        0 => StatusCode::Ok,
        1 => StatusCode::ChunkReady,
        2 => StatusCode::UnitDone,
        3 => StatusCode::Cancelled,
        -1 => StatusCode::ErrInvalidArg,
        -2 => StatusCode::ErrElf,
        -3 => StatusCode::ErrMissingRequiredSection,
        -4 => StatusCode::ErrDwarf,
        -5 => StatusCode::ErrMissingAuxSection,
        -6 => StatusCode::ErrCallback,
        _ => StatusCode::ErrInternal,
    }
}

pub fn err_string(buf: &[c_char; 256]) -> String {
    let bytes: Vec<u8> = buf
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

pub const ALL_SINK_IDS: [SinkId; SINK_COUNT] = [
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

pub fn projection(mask: u32) -> ChDwarfProjection {
    ChDwarfProjection {
        column_mask: mask,
        need_ancestor_offsets_vec: u32::from(
            mask & (COL_ANCESTOR_TAGS | COL_ANCESTOR_OFFSETS) != 0,
        ),
        need_attr_offsets_vec: u32::from(
            mask & (COL_ATTR_NAME | COL_ATTR_FORM | COL_ATTR_INT | COL_ATTR_STR) != 0,
        ),
        reserved: 0,
    }
}

pub fn plan_of(mask: u32) -> ProjectionPlan {
    let p = projection(mask);
    ProjectionPlan::validate_and_build(
        p.column_mask,
        p.need_ancestor_offsets_vec,
        p.need_attr_offsets_vec,
        0,
    )
    .unwrap()
}

/// Mirrors the adapter side of the callbacks.
pub struct Collector {
    pub bufs: Vec<Vec<u8>>,
    pub reserve_calls: usize,
    pub reserve_requests: Vec<Vec<(usize, usize)>>,
    pub polls: usize,
    /// Answer `STOP` from the n-th poll on (1-based).
    pub stop_at_poll: Option<usize>,
    /// Answer `FAILED` from the n-th poll on.
    pub fail_at_poll: Option<usize>,
    /// Fail the n-th reserve call (1-based).
    pub fail_at_reserve: Option<usize>,
    /// Misbehave in reserve: report success without growing.
    pub ignore_reserve: bool,
    /// Misbehave in reserve: change element size of the first requested sink.
    pub corrupt_elem_size: bool,
    pub pointer_changes: usize,
    /// Initial capacity (elements) to hand out on the first reservation of each sink.
    pub min_growth: usize,
}

impl Collector {
    pub fn new() -> Self {
        Self {
            bufs: vec![Vec::new(); SINK_COUNT],
            reserve_calls: 0,
            reserve_requests: Vec::new(),
            polls: 0,
            stop_at_poll: None,
            fail_at_poll: None,
            fail_at_reserve: None,
            ignore_reserve: false,
            corrupt_elem_size: false,
            pointer_changes: 0,
            min_growth: 4,
        }
    }
}

pub unsafe extern "C" fn reserve_cb(
    opaque: *mut c_void,
    sinks: *mut ChDwarfBatchSinks,
    requests: *const ChDwarfReserveRequest,
    count: usize,
) -> i32 {
    let st = &mut *opaque.cast::<Collector>();
    st.reserve_calls += 1;
    let reqs = std::slice::from_raw_parts(requests, count);
    st.reserve_requests.push(
        reqs.iter()
            .map(|r| (r.sink_id as usize, r.min_cap))
            .collect(),
    );
    if st.fail_at_reserve == Some(st.reserve_calls) {
        return CH_DWARF_RESERVE_FAILED;
    }
    if st.ignore_reserve {
        return CH_DWARF_RESERVE_OK;
    }
    let sinks = &mut *sinks;
    for r in reqs {
        let id = r.sink_id as usize;
        let s = &mut sinks.sinks[id];
        let elem = s.elem_size as usize;
        // Geometric growth, like the ClickHouse adapter.
        let new_cap = r.min_cap.max(s.cap * 2).max(st.min_growth);
        let buf = &mut st.bufs[id];
        let before = buf.as_ptr();
        // Unwritten capacity is poisoned so a read of uninitialized data shows up in tests.
        buf.resize(new_cap * elem, 0xAA);
        if buf.as_ptr() != before {
            st.pointer_changes += 1;
        }
        s.data = buf.as_mut_ptr();
        s.cap = new_cap;
        if st.corrupt_elem_size {
            s.elem_size += 1;
        }
    }
    CH_DWARF_RESERVE_OK
}

pub unsafe extern "C" fn cancel_cb(opaque: *mut c_void) -> i32 {
    let st = &mut *opaque.cast::<Collector>();
    st.polls += 1;
    if st.fail_at_poll.is_some_and(|n| st.polls >= n) {
        return CH_DWARF_CANCEL_FAILED;
    }
    if st.stop_at_poll.is_some_and(|n| st.polls >= n) {
        return CH_DWARF_CANCEL_STOP;
    }
    CH_DWARF_CANCEL_CONTINUE
}

/// Decoded columns of one chunk; `None` for columns that were not projected.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Decoded {
    pub rows: usize,
    pub offset: Option<Vec<u64>>,
    pub size: Option<Vec<u32>>,
    pub tag: Option<Vec<u16>>,
    pub ancestor_tags: Option<Vec<Vec<u16>>>,
    pub ancestor_offsets: Option<Vec<Vec<u64>>>,
    pub name: Option<Vec<Vec<u8>>>,
    pub linkage_name: Option<Vec<Vec<u8>>>,
    pub decl_file: Option<Vec<Vec<u8>>>,
    pub decl_line: Option<Vec<u32>>,
    pub ranges: Option<Vec<Vec<(u64, u64)>>>,
    pub attr_name: Option<Vec<Vec<u16>>>,
    pub attr_form: Option<Vec<Vec<u16>>>,
    pub attr_int: Option<Vec<Vec<u64>>>,
    pub attr_str: Option<Vec<Vec<Vec<u8>>>>,
}

fn raw<'a>(st: &'a Collector, sinks: &ChDwarfBatchSinks, id: SinkId) -> (&'a [u8], usize) {
    let s = &sinks.sinks[id as usize];
    let elem = s.elem_size as usize;
    assert!(s.len <= s.cap, "{id:?}: len {} > cap {}", s.len, s.cap);
    (&st.bufs[id as usize][..s.len * elem], s.len)
}

fn u64s(st: &Collector, sinks: &ChDwarfBatchSinks, id: SinkId) -> Vec<u64> {
    raw(st, sinks, id)
        .0
        .chunks_exact(8)
        .map(|c| u64::from_ne_bytes(c.try_into().unwrap()))
        .collect()
}

fn u32s(st: &Collector, sinks: &ChDwarfBatchSinks, id: SinkId) -> Vec<u32> {
    raw(st, sinks, id)
        .0
        .chunks_exact(4)
        .map(|c| u32::from_ne_bytes(c.try_into().unwrap()))
        .collect()
}

fn u16s(st: &Collector, sinks: &ChDwarfBatchSinks, id: SinkId) -> Vec<u16> {
    raw(st, sinks, id)
        .0
        .chunks_exact(2)
        .map(|c| u16::from_ne_bytes(c.try_into().unwrap()))
        .collect()
}

/// Splits `values` by cumulative `ends` and checks every ColumnArray invariant.
fn split_by_ends<T: Clone>(values: &[T], ends: &[u64], rows: usize, what: &str) -> Vec<Vec<T>> {
    assert_eq!(ends.len(), rows, "{what}: one offset per row");
    let mut out = Vec::with_capacity(rows);
    let mut prev = 0u64;
    for &e in ends {
        assert!(e >= prev, "{what}: offsets must not decrease");
        assert!(
            e as usize <= values.len(),
            "{what}: offset beyond the element sink"
        );
        out.push(values[prev as usize..e as usize].to_vec());
        prev = e;
    }
    assert_eq!(
        prev as usize,
        values.len(),
        "{what}: last offset equals element count"
    );
    out
}

fn strings(
    st: &Collector,
    sinks: &ChDwarfBatchSinks,
    chars: SinkId,
    offsets: SinkId,
    rows: usize,
) -> Vec<Vec<u8>> {
    let data = raw(st, sinks, chars).0;
    let ends = u64s(st, sinks, offsets);
    let parts = split_by_ends(data, &ends, rows, &format!("{offsets:?}"));
    parts
}

/// Decodes every active sink of the chunk and verifies the structural invariants of the layout.
pub fn decode(
    st: &Collector,
    sinks: &ChDwarfBatchSinks,
    plan: ProjectionPlan,
    rows: usize,
) -> Decoded {
    let mut d = Decoded {
        rows,
        ..Decoded::default()
    };
    for id in ALL_SINK_IDS {
        let s = &sinks.sinks[id as usize];
        if !plan.sink_active(id) {
            assert_eq!((s.len, s.cap), (0, 0), "inactive sink {id:?} was touched");
        }
    }
    let per_row = |id: SinkId| {
        assert_eq!(
            sinks.sinks[id as usize].len, rows,
            "{id:?} has one element per row"
        );
    };
    if plan.has(COL_OFFSET) {
        per_row(SinkId::Offset);
        d.offset = Some(u64s(st, sinks, SinkId::Offset));
    }
    if plan.has(COL_SIZE) {
        per_row(SinkId::Size);
        d.size = Some(u32s(st, sinks, SinkId::Size));
    }
    if plan.has(COL_TAG) {
        per_row(SinkId::Tag);
        d.tag = Some(u16s(st, sinks, SinkId::Tag));
    }
    if plan.need_ancestor_array_offsets {
        per_row(SinkId::AncestorArrayOffsets);
        let ends = u64s(st, sinks, SinkId::AncestorArrayOffsets);
        let total = *ends.last().unwrap_or(&0) as usize;
        if plan.has(COL_ANCESTOR_TAGS) {
            let v = u16s(st, sinks, SinkId::AncestorTags);
            assert_eq!(v.len(), total);
            d.ancestor_tags = Some(split_by_ends(&v, &ends, rows, "ancestor_tags"));
        }
        if plan.has(COL_ANCESTOR_OFFSETS) {
            let v = u64s(st, sinks, SinkId::AncestorOffsets);
            assert_eq!(v.len(), total);
            d.ancestor_offsets = Some(split_by_ends(&v, &ends, rows, "ancestor_offsets"));
        }
    }
    if plan.has(COL_NAME) {
        d.name = Some(strings(
            st,
            sinks,
            SinkId::NameChars,
            SinkId::NameOffsets,
            rows,
        ));
    }
    if plan.has(COL_LINKAGE_NAME) {
        d.linkage_name = Some(strings(
            st,
            sinks,
            SinkId::LinkageNameChars,
            SinkId::LinkageNameOffsets,
            rows,
        ));
    }
    if plan.has(COL_DECL_FILE) {
        d.decl_file = Some(strings(
            st,
            sinks,
            SinkId::DeclFileChars,
            SinkId::DeclFileOffsets,
            rows,
        ));
    }
    if plan.has(COL_DECL_LINE) {
        per_row(SinkId::DeclLine);
        d.decl_line = Some(u32s(st, sinks, SinkId::DeclLine));
    }
    if plan.has(COL_RANGES) {
        per_row(SinkId::RangesOffsets);
        let ends = u64s(st, sinks, SinkId::RangesOffsets);
        let starts = u64s(st, sinks, SinkId::RangesStart);
        let stops = u64s(st, sinks, SinkId::RangesEnd);
        assert_eq!(starts.len(), stops.len());
        let pairs: Vec<(u64, u64)> = starts.into_iter().zip(stops).collect();
        d.ranges = Some(split_by_ends(&pairs, &ends, rows, "ranges"));
    }
    if plan.need_attr_offsets {
        per_row(SinkId::AttrOffsets);
        let ends = u64s(st, sinks, SinkId::AttrOffsets);
        let total = *ends.last().unwrap_or(&0) as usize;
        if plan.has(COL_ATTR_NAME) {
            let v = u16s(st, sinks, SinkId::AttrName);
            assert_eq!(v.len(), total);
            d.attr_name = Some(split_by_ends(&v, &ends, rows, "attr_name"));
        }
        if plan.has(COL_ATTR_FORM) {
            let v = u16s(st, sinks, SinkId::AttrForm);
            assert_eq!(v.len(), total);
            d.attr_form = Some(split_by_ends(&v, &ends, rows, "attr_form"));
        }
        if plan.has(COL_ATTR_INT) {
            let v = u64s(st, sinks, SinkId::AttrInt);
            assert_eq!(v.len(), total);
            d.attr_int = Some(split_by_ends(&v, &ends, rows, "attr_int"));
        }
        if plan.has(COL_ATTR_STR) {
            let flat = strings(
                st,
                sinks,
                SinkId::AttrStrChars,
                SinkId::AttrStrOffsets,
                total,
            );
            d.attr_str = Some(split_by_ends(&flat, &ends, rows, "attr_str"));
        }
    }
    d
}

impl Decoded {
    /// Appends `other` (a later chunk of the same unit).
    pub fn extend(&mut self, other: Decoded) {
        fn ext<T>(a: &mut Option<Vec<T>>, b: Option<Vec<T>>) {
            match (a.as_mut(), b) {
                (Some(a), Some(b)) => a.extend(b),
                (None, Some(b)) => *a = Some(b),
                _ => {}
            }
        }
        self.rows += other.rows;
        ext(&mut self.offset, other.offset);
        ext(&mut self.size, other.size);
        ext(&mut self.tag, other.tag);
        ext(&mut self.ancestor_tags, other.ancestor_tags);
        ext(&mut self.ancestor_offsets, other.ancestor_offsets);
        ext(&mut self.name, other.name);
        ext(&mut self.linkage_name, other.linkage_name);
        ext(&mut self.decl_file, other.decl_file);
        ext(&mut self.decl_line, other.decl_line);
        ext(&mut self.ranges, other.ranges);
        ext(&mut self.attr_name, other.attr_name);
        ext(&mut self.attr_form, other.attr_form);
        ext(&mut self.attr_int, other.attr_int);
        ext(&mut self.attr_str, other.attr_str);
    }
}

#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub status: i32,
    pub rows: usize,
    pub bytes_advanced: u64,
    pub unit_name: Vec<u8>,
    pub unit_done: u32,
}

pub struct TestFile {
    /// Keeps the input alive; never resized after `open`.
    pub bytes: Vec<u8>,
    pub file: *mut ChDwarfFile,
}

impl Drop for TestFile {
    fn drop(&mut self) {
        unsafe { ch_dwarf_file_free(self.file) };
    }
}

pub fn open_raw(bytes: &[u8]) -> (i32, String, *mut ChDwarfFile) {
    let mut err = [0 as c_char; 256];
    let mut file: *mut ChDwarfFile = std::ptr::null_mut();
    let opts = ChDwarfOptions {
        flags: 0,
        reserved: 0,
    };
    let rc = unsafe {
        ch_dwarf_file_open(
            bytes.as_ptr(),
            bytes.len(),
            &opts,
            &mut file,
            err.as_mut_ptr(),
        )
    };
    (rc, err_string(&err), file)
}

impl TestFile {
    pub fn open(bytes: Vec<u8>) -> Result<Self, (i32, String)> {
        let (rc, msg, file) = open_raw(&bytes);
        if rc != OK {
            assert!(file.is_null(), "failed open must not return a handle");
            return Err((rc, msg));
        }
        assert!(!file.is_null());
        Ok(Self { bytes, file })
    }

    pub fn unit_count(&self) -> usize {
        let mut n = usize::MAX;
        assert_eq!(unsafe { ch_dwarf_file_unit_count(self.file, &mut n) }, OK);
        n
    }

    pub fn open_unit(&self, index: usize) -> Result<TestUnit, (i32, String)> {
        let mut err = [0 as c_char; 256];
        let mut unit: *mut ChDwarfUnit = std::ptr::null_mut();
        let mut offset = u64::MAX;
        let rc = unsafe {
            ch_dwarf_unit_open(self.file, index, &mut unit, &mut offset, err.as_mut_ptr())
        };
        if rc != OK {
            assert!(unit.is_null());
            return Err((rc, err_string(&err)));
        }
        Ok(TestUnit { unit, offset })
    }
}

pub struct TestUnit {
    pub unit: *mut ChDwarfUnit,
    pub offset: u64,
}

impl Drop for TestUnit {
    fn drop(&mut self) {
        unsafe { ch_dwarf_unit_free(self.unit) };
    }
}

pub fn fresh_sinks(plan: ProjectionPlan) -> ChDwarfBatchSinks {
    let empty = ChDwarfSink {
        data: std::ptr::null_mut(),
        len: 0,
        cap: 0,
        elem_size: 0,
        reserved: 0,
    };
    let mut sinks = ChDwarfBatchSinks {
        sinks: [empty; SINK_COUNT],
    };
    for id in ALL_SINK_IDS {
        if plan.sink_active(id) {
            sinks.sinks[id as usize].elem_size = id.expected_elem_size();
        }
    }
    sinks
}

/// One `ch_dwarf_unit_parse_chunk` call with fresh sinks. Returns the status, the decoded chunk (if
/// the call produced one) and progress.
pub fn parse_one(
    file: &TestFile,
    unit: &mut TestUnit,
    mask: u32,
    max_rows: usize,
    st: &mut Collector,
) -> (i32, String, Option<(Decoded, ChunkInfo)>) {
    let plan = plan_of(mask);
    let proj = projection(mask);
    for b in &mut st.bufs {
        b.clear();
    }
    let mut sinks = fresh_sinks(plan);
    let mut progress = ChDwarfProgress {
        rows: usize::MAX,
        bytes_advanced: u64::MAX,
        unit_name_ptr: std::ptr::null(),
        unit_name_len: usize::MAX,
        unit_done: u32::MAX,
        reserved: 0,
    };
    let mut err = [0 as c_char; 256];
    let rc = unsafe {
        ch_dwarf_unit_parse_chunk(
            file.file,
            unit.unit,
            &proj,
            &mut sinks,
            Some(reserve_cb),
            Some(cancel_cb),
            (st as *mut Collector).cast(),
            max_rows,
            &mut progress,
            err.as_mut_ptr(),
        )
    };
    let msg = err_string(&err);
    if rc != CHUNK_READY && rc != UNIT_DONE {
        return (rc, msg, None);
    }
    let unit_name =
        unsafe { std::slice::from_raw_parts(progress.unit_name_ptr, progress.unit_name_len) }
            .to_vec();
    let decoded = decode(st, &sinks, plan, progress.rows);
    let info = ChunkInfo {
        status: rc,
        rows: progress.rows,
        bytes_advanced: progress.bytes_advanced,
        unit_name,
        unit_done: progress.unit_done,
    };
    (rc, msg, Some((decoded, info)))
}

/// Reads one whole unit; returns all chunks' rows concatenated, or the failing status.
pub fn read_unit(
    file: &TestFile,
    index: usize,
    mask: u32,
    max_rows: usize,
    st: &mut Collector,
) -> Result<(Decoded, Vec<ChunkInfo>), (i32, String)> {
    let mut unit = file.open_unit(index)?;
    let mut all = Decoded::default();
    let mut infos = Vec::new();
    loop {
        let (rc, msg, chunk) = parse_one(file, &mut unit, mask, max_rows, st);
        match chunk {
            None => return Err((rc, msg)),
            Some((d, info)) => {
                assert!(d.rows > 0 || rc == UNIT_DONE, "CHUNK_READY must carry rows");
                assert!(d.rows <= max_rows);
                all.extend(d);
                let done = rc == UNIT_DONE;
                assert_eq!(info.unit_done, u32::from(done));
                infos.push(info);
                if done {
                    return Ok((all, infos));
                }
                assert_eq!(
                    infos.last().unwrap().rows,
                    max_rows,
                    "a non-final chunk is full"
                );
            }
        }
    }
}

pub const ALL_COLS: u32 = ALL_COLUMNS_MASK;

// ---------------------------------------------------------------------------------------------
// Synthetic ELF / DWARF builder
// ---------------------------------------------------------------------------------------------

pub fn uleb(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return out;
        }
        out.push(b | 0x80);
    }
}

pub fn sleb(mut v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        let done = (v == 0 && b & 0x40 == 0) || (v == -1 && b & 0x40 != 0);
        if done {
            out.push(b);
            return out;
        }
        out.push(b | 0x80);
    }
}

/// One abbreviation declaration.
pub fn abbrev(code: u64, tag: u16, children: bool, attrs: &[(u16, u16, Option<i64>)]) -> Vec<u8> {
    let mut v = uleb(code);
    v.extend(uleb(u64::from(tag)));
    v.push(u8::from(children));
    for &(name, form, implicit) in attrs {
        v.extend(uleb(u64::from(name)));
        v.extend(uleb(u64::from(form)));
        if let Some(c) = implicit {
            v.extend(sleb(c));
        }
    }
    v.extend([0, 0]);
    v
}

/// A complete abbreviation table (declarations plus terminator).
pub fn abbrev_table(decls: &[Vec<u8>]) -> Vec<u8> {
    let mut v = decls.concat();
    v.push(0);
    v
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Fmt {
    Dwarf32,
    Dwarf64,
}

/// A unit: header per `version`, then `dies`. `unit_type` is only used for version 5.
pub fn unit(version: u16, fmt: Fmt, addr_size: u8, abbrev_offset: u64, dies: &[u8]) -> Vec<u8> {
    let off = |v: u64| -> Vec<u8> {
        match fmt {
            Fmt::Dwarf32 => (v as u32).to_le_bytes().to_vec(),
            Fmt::Dwarf64 => v.to_le_bytes().to_vec(),
        }
    };
    let mut body = version.to_le_bytes().to_vec();
    if version >= 5 {
        body.push(1); // DW_UT_compile
        body.push(addr_size);
        body.extend(off(abbrev_offset));
    } else {
        body.extend(off(abbrev_offset));
        body.push(addr_size);
    }
    body.extend_from_slice(dies);
    let mut out = Vec::new();
    match fmt {
        Fmt::Dwarf32 => out.extend((body.len() as u32).to_le_bytes()),
        Fmt::Dwarf64 => {
            out.extend(0xffff_ffffu32.to_le_bytes());
            out.extend((body.len() as u64).to_le_bytes());
        }
    }
    out.extend(body);
    out
}

#[derive(Default, Clone)]
pub struct Elf {
    pub info: Vec<u8>,
    pub abbrev: Vec<u8>,
    /// Extra sections (name, bytes).
    pub extra: Vec<(String, Vec<u8>)>,
    pub phnum: u16,
}

impl Elf {
    pub fn new(info: Vec<u8>, abbrev: Vec<u8>) -> Self {
        Self {
            info,
            abbrev,
            extra: Vec::new(),
            phnum: 1,
        }
    }

    pub fn with(mut self, name: &str, bytes: Vec<u8>) -> Self {
        self.extra.push((name.to_string(), bytes));
        self
    }

    /// Minimal valid ELF64 little-endian executable with one program header.
    pub fn build(&self) -> Vec<u8> {
        let mut sections: Vec<(String, Vec<u8>)> = vec![
            (".debug_abbrev".into(), self.abbrev.clone()),
            (".debug_info".into(), self.info.clone()),
        ];
        sections.extend(self.extra.iter().cloned());
        let mut shstrtab = vec![0u8];
        let mut name_offsets = Vec::new();
        for (n, _) in &sections {
            name_offsets.push(shstrtab.len() as u32);
            shstrtab.extend(n.as_bytes());
            shstrtab.push(0);
        }
        let shstr_name = shstrtab.len() as u32;
        shstrtab.extend(b".shstrtab\0");

        let mut body = vec![0u8; 64 + 56];
        let mut offsets = Vec::new();
        for (_, data) in &sections {
            offsets.push(body.len() as u64);
            body.extend(data);
        }
        let shstr_off = body.len() as u64;
        body.extend(&shstrtab);
        while !body.len().is_multiple_of(8) {
            body.push(0);
        }
        let shoff = body.len() as u64;
        let sh = |name: u32, ty: u32, off: u64, size: u64| -> Vec<u8> {
            let mut s = Vec::new();
            s.extend(name.to_le_bytes());
            s.extend(ty.to_le_bytes());
            s.extend(0u64.to_le_bytes());
            s.extend(0u64.to_le_bytes());
            s.extend(off.to_le_bytes());
            s.extend(size.to_le_bytes());
            s.extend(0u32.to_le_bytes());
            s.extend(0u32.to_le_bytes());
            s.extend(1u64.to_le_bytes());
            s.extend(0u64.to_le_bytes());
            s
        };
        let mut table = sh(0, 0, 0, 0);
        for (i, (_, data)) in sections.iter().enumerate() {
            table.extend(sh(name_offsets[i], 1, offsets[i], data.len() as u64));
        }
        table.extend(sh(shstr_name, 3, shstr_off, shstrtab.len() as u64));
        let shnum = (sections.len() + 2) as u16;
        body.extend(table);

        let mut hdr = vec![0x7f, b'E', b'L', b'F', 2, 1, 1];
        hdr.extend([0u8; 9]);
        hdr.extend(2u16.to_le_bytes()); // ET_EXEC
        hdr.extend(0x3eu16.to_le_bytes());
        hdr.extend(1u32.to_le_bytes());
        hdr.extend(0u64.to_le_bytes()); // entry
        hdr.extend((if self.phnum == 0 { 0u64 } else { 64 }).to_le_bytes()); // phoff
        hdr.extend(shoff.to_le_bytes());
        hdr.extend(0u32.to_le_bytes());
        hdr.extend(64u16.to_le_bytes());
        hdr.extend(56u16.to_le_bytes());
        hdr.extend(self.phnum.to_le_bytes());
        hdr.extend(64u16.to_le_bytes());
        hdr.extend(shnum.to_le_bytes());
        hdr.extend((shnum - 1).to_le_bytes()); // shstrndx
        assert_eq!(hdr.len(), 64);
        body[..64].copy_from_slice(&hdr);
        // One PT_LOAD program header (contents are never read).
        let mut ph = Vec::new();
        ph.extend(1u32.to_le_bytes());
        ph.extend(4u32.to_le_bytes());
        ph.extend([0u8; 40]);
        ph.extend(8u64.to_le_bytes());
        body[64..64 + 56].copy_from_slice(&ph);
        body
    }
}

pub fn tiny_dwarf(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/queries/0_stateless/data_dwarf")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

// ---------------------------------------------------------------------------------------------
// DIE / section builders for synthetic inputs
// ---------------------------------------------------------------------------------------------

pub fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

pub fn le16(v: u16) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

pub fn le32(v: u32) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

pub fn le64(v: u64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

/// A DIE: abbreviation code then its encoded attribute values.
pub fn die(code: u64, parts: &[&[u8]]) -> Vec<u8> {
    let mut v = uleb(code);
    for p in parts {
        v.extend_from_slice(p);
    }
    v
}

/// Size of the unit header in front of the first DIE for 32-bit DWARF.
pub fn header_size(version: u16) -> u64 {
    if version >= 5 {
        12
    } else {
        11
    }
}

/// A one-unit file: `dies` are the unit's DIE bytes (including terminating nulls).
pub fn one_unit(
    version: u16,
    addr_size: u8,
    decls: &[Vec<u8>],
    dies: &[u8],
    extra: Vec<(&str, Vec<u8>)>,
) -> TestFile {
    let mut elf = Elf::new(
        unit(version, Fmt::Dwarf32, addr_size, 0, dies),
        abbrev_table(decls),
    );
    for (n, b) in extra {
        elf = elf.with(n, b);
    }
    TestFile::open(elf.build()).unwrap_or_else(|e| panic!("synthetic file must open: {e:?}"))
}

/// Reads unit 0 fully with all columns.
pub fn rows_of(file: &TestFile) -> Decoded {
    let mut st = Collector::new();
    read_unit(file, 0, ALL_COLS, 65536, &mut st)
        .unwrap_or_else(|e| panic!("unit must parse: {e:?}"))
        .0
}

/// Status of the first failing step (open unit or chunk) for unit 0.
pub fn fail_of(file: &TestFile) -> i32 {
    let mut st = Collector::new();
    match read_unit(file, 0, ALL_COLS, 65536, &mut st) {
        Ok(_) => panic!("expected a failure"),
        Err((rc, _)) => rc,
    }
}

fn line_prologue(version: u16, tables: &[u8], addr_size: u8, seg: u8, program: &[u8]) -> Vec<u8> {
    // min_inst, max_ops (DWARF >= 4), default_is_stmt, line_base, line_range, opcode_base
    let mut hdr = vec![1u8];
    if version >= 4 {
        hdr.push(1);
    }
    hdr.extend([1u8, 0xfb, 14, 13]);
    hdr.extend([0u8, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 1]); // standard_opcode_lengths
    hdr.extend_from_slice(tables);
    let mut body = le16(version);
    if version >= 5 {
        body.push(addr_size);
        body.push(seg);
    }
    body.extend(le32(hdr.len() as u32));
    body.extend(hdr);
    body.extend_from_slice(program);
    let mut out = le32(body.len() as u32);
    out.extend(body);
    out
}

/// A DWARF 2-4 line table prologue with the given directories and file names.
pub fn line_v4(version: u16, dirs: &[&str], files: &[&str]) -> Vec<u8> {
    let mut t = Vec::new();
    for d in dirs {
        t.extend(cstr(d));
    }
    t.push(0);
    for f in files {
        t.extend(cstr(f));
        t.extend([0, 0, 0]);
    }
    t.push(0);
    line_prologue(version, &t, 8, 0, &[])
}

/// A DWARF 5 line table prologue whose paths are the raw encoded values in `dirs`/`files`
/// (encoded with `path_form`) and whose file entries carry a `DW_FORM_udata` directory index.
pub fn line_v5(path_form: u16, dirs: &[Vec<u8>], files: &[Vec<u8>]) -> Vec<u8> {
    let mut t = Vec::new();
    t.push(1); // directory_entry_format_count
    t.extend(uleb(1)); // DW_LNCT_path
    t.extend(uleb(u64::from(path_form)));
    t.extend(uleb(dirs.len() as u64));
    for d in dirs {
        t.extend_from_slice(d);
    }
    t.push(2); // file_name_entry_format_count
    t.extend(uleb(1)); // DW_LNCT_path
    t.extend(uleb(u64::from(path_form)));
    t.extend(uleb(2)); // DW_LNCT_directory_index
    t.extend(uleb(0x0f)); // DW_FORM_udata
    t.extend(uleb(files.len() as u64));
    for f in files {
        t.extend_from_slice(f);
        t.push(0); // directory index
    }
    line_prologue(5, &t, 8, 0, &[])
}
