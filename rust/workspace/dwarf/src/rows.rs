#![forbid(unsafe_code)]

//! Row materialization: one DIE (or null entry) becomes one element in every active sink.
//!
//! The parser decodes a whole DIE first (validation never depends on the projection), then calls
//! [`write_row`], which computes the exact additional capacity every active sink needs, asks the
//! sink to reserve it in one go, and appends. Per-row work is therefore one reservation check,
//! not one per value.

use crate::error::Result;
use crate::line::FilenameTable;
use crate::outbuf::{
    BatchSinkAccess, SinkId, COL_ANCESTOR_OFFSETS, COL_ANCESTOR_TAGS, COL_ATTR_FORM, COL_ATTR_INT,
    COL_ATTR_NAME, COL_ATTR_STR, COL_DECL_FILE, COL_DECL_LINE, COL_LINKAGE_NAME, COL_NAME,
    COL_OFFSET, COL_RANGES, COL_SIZE, COL_TAG, SINK_COUNT,
};

/// An ancestor with children, as kept on the explicit DIE stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StackEntry {
    pub offset: u64,
    pub tag: u16,
}

/// A string-valued output that is either empty, borrowed from the input, or an entry of the
/// unit's filename table.
#[derive(Debug, Clone, Copy)]
pub enum StrSrc<'a> {
    Empty,
    Bytes(&'a [u8]),
    Table(u64),
}

#[derive(Debug, Clone, Copy)]
pub struct AttrOut<'a> {
    pub name: u16,
    /// The *declared* form (not the one resolved through `DW_FORM_indirect`).
    pub form: u16,
    pub int: u64,
    pub str_: StrSrc<'a>,
}

/// Everything one output row needs.
pub struct Row<'a, 'v> {
    pub offset: u64,
    pub size: u32,
    pub tag: u16,
    pub name: &'a [u8],
    pub linkage_name: &'a [u8],
    pub decl_file: StrSrc<'a>,
    pub decl_line: u32,
    pub ranges: &'v [(u64, u64)],
    pub attrs: &'v [AttrOut<'a>],
    pub attr_str_chars: usize,
    /// Innermost ancestor last (the DIE stack); written innermost first.
    pub ancestors: &'v [StackEntry],
}

/// Running cumulative element counts for the array columns of one chunk.
#[derive(Debug, Default, Clone, Copy)]
pub struct Counters {
    pub ancestors: u64,
    pub ranges: u64,
    pub attrs: u64,
}

#[inline]
fn resolve<'x>(src: StrSrc<'x>, table: Option<&'x FilenameTable>) -> &'x [u8] {
    match src {
        StrSrc::Empty => b"",
        StrSrc::Bytes(b) => b,
        StrSrc::Table(i) => table.map_or(&b""[..], |t| t.get(i)),
    }
}

#[cold]
#[inline(never)]
fn ensure_row_slow<S: BatchSinkAccess>(
    sink: &mut S,
    row: &Row<'_, '_>,
    decl_file_len: usize,
    attr_str_chars: usize,
) -> Result<()> {
    let plan = sink.plan();
    let mut adds = [(SinkId::Offset, 0usize); SINK_COUNT];
    let mut n = 0usize;
    let mut want = |id: SinkId, add: usize| {
        if add != 0 && plan.sink_active(id) {
            adds[n] = (id, add);
            n += 1;
        }
    };
    want(SinkId::Offset, 1);
    want(SinkId::Size, 1);
    want(SinkId::Tag, 1);
    want(SinkId::AncestorTags, row.ancestors.len());
    want(SinkId::AncestorOffsets, row.ancestors.len());
    want(SinkId::AncestorArrayOffsets, 1);
    want(SinkId::NameChars, row.name.len());
    want(SinkId::NameOffsets, 1);
    want(SinkId::LinkageNameChars, row.linkage_name.len());
    want(SinkId::LinkageNameOffsets, 1);
    want(SinkId::DeclFileChars, decl_file_len);
    want(SinkId::DeclFileOffsets, 1);
    want(SinkId::DeclLine, 1);
    want(SinkId::RangesStart, row.ranges.len());
    want(SinkId::RangesEnd, row.ranges.len());
    want(SinkId::RangesOffsets, 1);
    want(SinkId::AttrName, row.attrs.len());
    want(SinkId::AttrForm, row.attrs.len());
    want(SinkId::AttrInt, row.attrs.len());
    want(SinkId::AttrStrOffsets, row.attrs.len());
    want(SinkId::AttrOffsets, 1);
    if plan.has(COL_ATTR_STR) {
        want(SinkId::AttrStrChars, attr_str_chars);
    }
    sink.ensure(&adds[..n])
}

/// Appends `row` to every active sink of `sink`.
#[inline]
pub fn write_row<S: BatchSinkAccess>(
    sink: &mut S,
    row: &Row<'_, '_>,
    table: Option<&FilenameTable>,
    cnt: &mut Counters,
) -> Result<()> {
    let plan = sink.plan();
    let decl_file = if plan.has(COL_DECL_FILE) {
        resolve(row.decl_file, table)
    } else {
        b""
    };
    let attr_str_chars = row.attr_str_chars;

    if !sink.has_row_space(
        row.ancestors.len(),
        row.name.len(),
        row.linkage_name.len(),
        decl_file.len(),
        row.ranges.len(),
        row.attrs.len(),
        attr_str_chars,
    ) {
        ensure_row_slow(sink, row, decl_file.len(), attr_str_chars)?;
    }

    if plan.has(COL_OFFSET) {
        sink.push_u64(SinkId::Offset, row.offset)?;
    }
    if plan.has(COL_SIZE) {
        sink.push_u32(SinkId::Size, row.size)?;
    }
    if plan.has(COL_TAG) {
        sink.push_u16(SinkId::Tag, row.tag)?;
    }
    if plan.need_ancestor_array_offsets {
        if plan.has(COL_ANCESTOR_TAGS) {
            for a in row.ancestors.iter().rev() {
                sink.push_u16(SinkId::AncestorTags, a.tag)?;
            }
        }
        if plan.has(COL_ANCESTOR_OFFSETS) {
            for a in row.ancestors.iter().rev() {
                sink.push_u64(SinkId::AncestorOffsets, a.offset)?;
            }
        }
        cnt.ancestors += row.ancestors.len() as u64;
        sink.push_u64(SinkId::AncestorArrayOffsets, cnt.ancestors)?;
    }
    if plan.has(COL_NAME) {
        sink.push_string(SinkId::NameChars, SinkId::NameOffsets, row.name)?;
    }
    if plan.has(COL_LINKAGE_NAME) {
        sink.push_string(
            SinkId::LinkageNameChars,
            SinkId::LinkageNameOffsets,
            row.linkage_name,
        )?;
    }
    if plan.has(COL_DECL_FILE) {
        sink.push_string(SinkId::DeclFileChars, SinkId::DeclFileOffsets, decl_file)?;
    }
    if plan.has(COL_DECL_LINE) {
        sink.push_u32(SinkId::DeclLine, row.decl_line)?;
    }
    if plan.has(COL_RANGES) {
        for &(start, end) in row.ranges {
            sink.push_u64(SinkId::RangesStart, start)?;
            sink.push_u64(SinkId::RangesEnd, end)?;
        }
        cnt.ranges += row.ranges.len() as u64;
        sink.push_u64(SinkId::RangesOffsets, cnt.ranges)?;
    }
    if plan.need_attr_offsets {
        if plan.has(COL_ATTR_NAME) {
            for a in row.attrs {
                sink.push_u16(SinkId::AttrName, a.name)?;
            }
        }
        if plan.has(COL_ATTR_FORM) {
            for a in row.attrs {
                sink.push_u16(SinkId::AttrForm, a.form)?;
            }
        }
        if plan.has(COL_ATTR_INT) {
            for a in row.attrs {
                sink.push_u64(SinkId::AttrInt, a.int)?;
            }
        }
        if plan.has(COL_ATTR_STR) {
            for a in row.attrs {
                sink.push_string(
                    SinkId::AttrStrChars,
                    SinkId::AttrStrOffsets,
                    resolve(a.str_, table),
                )?;
            }
        }
        cnt.attrs += row.attrs.len() as u64;
        sink.push_u64(SinkId::AttrOffsets, cnt.attrs)?;
    }
    Ok(())
}
