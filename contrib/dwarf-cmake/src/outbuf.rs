#![forbid(unsafe_code)]

use crate::error::{DwarfError, Result};
use core::mem::MaybeUninit;

pub const COL_OFFSET: u32 = 1 << 0;
pub const COL_SIZE: u32 = 1 << 1;
pub const COL_TAG: u32 = 1 << 2;
pub const COL_UNIT_NAME: u32 = 1 << 3;
pub const COL_UNIT_OFFSET: u32 = 1 << 4;
pub const COL_ANCESTOR_TAGS: u32 = 1 << 5;
pub const COL_ANCESTOR_OFFSETS: u32 = 1 << 6;
pub const COL_NAME: u32 = 1 << 7;
pub const COL_LINKAGE_NAME: u32 = 1 << 8;
pub const COL_DECL_FILE: u32 = 1 << 9;
pub const COL_DECL_LINE: u32 = 1 << 10;
pub const COL_RANGES: u32 = 1 << 11;
pub const COL_ATTR_NAME: u32 = 1 << 12;
pub const COL_ATTR_FORM: u32 = 1 << 13;
pub const COL_ATTR_INT: u32 = 1 << 14;
pub const COL_ATTR_STR: u32 = 1 << 15;
pub const ALL_COLUMNS_MASK: u32 = (1 << 16) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum SinkId {
    Offset = 0,
    Size = 1,
    Tag = 2,
    AncestorTags = 3,
    AncestorOffsets = 4,
    AncestorArrayOffsets = 5,
    NameChars = 6,
    NameOffsets = 7,
    LinkageNameChars = 8,
    LinkageNameOffsets = 9,
    DeclFileChars = 10,
    DeclFileOffsets = 11,
    DeclLine = 12,
    RangesStart = 13,
    RangesEnd = 14,
    RangesOffsets = 15,
    AttrName = 16,
    AttrForm = 17,
    AttrInt = 18,
    AttrStrChars = 19,
    AttrStrOffsets = 20,
    AttrOffsets = 21,
}

pub const SINK_COUNT: usize = 22;

impl SinkId {
    #[inline]
    pub const fn expected_elem_size(self) -> u32 {
        match self {
            Self::Offset
            | Self::AncestorOffsets
            | Self::AncestorArrayOffsets
            | Self::NameOffsets
            | Self::LinkageNameOffsets
            | Self::DeclFileOffsets
            | Self::RangesStart
            | Self::RangesEnd
            | Self::RangesOffsets
            | Self::AttrInt
            | Self::AttrStrOffsets
            | Self::AttrOffsets => 8,
            Self::Size | Self::DeclLine => 4,
            Self::Tag | Self::AncestorTags | Self::AttrName | Self::AttrForm => 2,
            Self::NameChars | Self::LinkageNameChars | Self::DeclFileChars | Self::AttrStrChars => {
                1
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionPlan {
    pub mask: u32,
    pub need_ancestor_array_offsets: bool,
    pub need_attr_offsets: bool,
}

impl ProjectionPlan {
    pub fn validate_and_build(
        column_mask: u32,
        need_ancestor_offsets_vec: u32,
        need_attr_offsets_vec: u32,
        reserved: u32,
    ) -> Result<Self> {
        if (column_mask & !ALL_COLUMNS_MASK) != 0 || reserved != 0 {
            return Err(DwarfError::invalid_arg("invalid column projection mask"));
        }
        if need_ancestor_offsets_vec > 1 || need_attr_offsets_vec > 1 {
            return Err(DwarfError::invalid_arg(
                "invalid projection dependency flag",
            ));
        }
        let expected_anc = (column_mask & (COL_ANCESTOR_TAGS | COL_ANCESTOR_OFFSETS)) != 0;
        let expected_attr =
            (column_mask & (COL_ATTR_NAME | COL_ATTR_FORM | COL_ATTR_INT | COL_ATTR_STR)) != 0;
        if (need_ancestor_offsets_vec != 0) != expected_anc
            || (need_attr_offsets_vec != 0) != expected_attr
        {
            return Err(DwarfError::invalid_arg(
                "projection dependency flags do not match column mask",
            ));
        }
        Ok(Self {
            mask: column_mask,
            need_ancestor_array_offsets: expected_anc,
            need_attr_offsets: expected_attr,
        })
    }

    #[inline]
    pub const fn has(self, bit: u32) -> bool {
        (self.mask & bit) != 0
    }

    #[inline]
    pub const fn sink_active(self, sink: SinkId) -> bool {
        match sink {
            SinkId::Offset => self.has(COL_OFFSET),
            SinkId::Size => self.has(COL_SIZE),
            SinkId::Tag => self.has(COL_TAG),
            SinkId::AncestorTags => self.has(COL_ANCESTOR_TAGS),
            SinkId::AncestorOffsets => self.has(COL_ANCESTOR_OFFSETS),
            SinkId::AncestorArrayOffsets => self.need_ancestor_array_offsets,
            SinkId::NameChars | SinkId::NameOffsets => self.has(COL_NAME),
            SinkId::LinkageNameChars | SinkId::LinkageNameOffsets => self.has(COL_LINKAGE_NAME),
            SinkId::DeclFileChars | SinkId::DeclFileOffsets => self.has(COL_DECL_FILE),
            SinkId::DeclLine => self.has(COL_DECL_LINE),
            SinkId::RangesStart | SinkId::RangesEnd | SinkId::RangesOffsets => self.has(COL_RANGES),
            SinkId::AttrName => self.has(COL_ATTR_NAME),
            SinkId::AttrForm => self.has(COL_ATTR_FORM),
            SinkId::AttrInt => self.has(COL_ATTR_INT),
            SinkId::AttrStrChars | SinkId::AttrStrOffsets => self.has(COL_ATTR_STR),
            SinkId::AttrOffsets => self.need_attr_offsets,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReserveNeed {
    pub sink_id: SinkId,
    pub min_cap: usize,
}

#[cold]
#[inline(never)]
fn cold_invalid_arg(msg: &'static str) -> DwarfError {
    DwarfError::invalid_arg(msg)
}

#[cold]
#[inline(never)]
fn cold_internal_err(msg: &'static str) -> DwarfError {
    DwarfError::internal(msg)
}

#[inline(always)]
pub fn checked_required_cap(len: usize, cap: usize, add: usize) -> Result<Option<usize>> {
    if len > cap {
        return Err(cold_invalid_arg("sink len exceeds cap"));
    }
    let Some(needed) = len.checked_add(add) else {
        return Err(cold_internal_err("sink capacity overflow"));
    };
    if needed <= cap {
        Ok(None)
    } else {
        Ok(Some(needed))
    }
}

#[inline(always)]
pub fn write_u16_ne(buf: &mut [MaybeUninit<u8>], elem_index: usize, val: u16) -> Result<()> {
    let Some(start) = elem_index.checked_mul(2) else {
        return Err(cold_internal_err("u16 sink byte offset overflow"));
    };
    let Some(end) = start.checked_add(2) else {
        return Err(cold_internal_err("u16 sink byte end overflow"));
    };
    let Some(dst) = buf.get_mut(start..end) else {
        return Err(cold_internal_err("u16 sink write out of bounds"));
    };
    let bytes = val.to_ne_bytes();
    dst[0] = MaybeUninit::new(bytes[0]);
    dst[1] = MaybeUninit::new(bytes[1]);
    Ok(())
}

#[inline(always)]
pub fn write_u32_ne(buf: &mut [MaybeUninit<u8>], elem_index: usize, val: u32) -> Result<()> {
    let Some(start) = elem_index.checked_mul(4) else {
        return Err(cold_internal_err("u32 sink byte offset overflow"));
    };
    let Some(end) = start.checked_add(4) else {
        return Err(cold_internal_err("u32 sink byte end overflow"));
    };
    let Some(dst) = buf.get_mut(start..end) else {
        return Err(cold_internal_err("u32 sink write out of bounds"));
    };
    let bytes = val.to_ne_bytes();
    for (slot, b) in dst.iter_mut().zip(bytes) {
        *slot = MaybeUninit::new(b);
    }
    Ok(())
}

#[inline(always)]
pub fn write_u64_ne(buf: &mut [MaybeUninit<u8>], elem_index: usize, val: u64) -> Result<()> {
    let Some(start) = elem_index.checked_mul(8) else {
        return Err(cold_internal_err("u64 sink byte offset overflow"));
    };
    let Some(end) = start.checked_add(8) else {
        return Err(cold_internal_err("u64 sink byte end overflow"));
    };
    let Some(dst) = buf.get_mut(start..end) else {
        return Err(cold_internal_err("u64 sink write out of bounds"));
    };
    let bytes = val.to_ne_bytes();
    for (slot, b) in dst.iter_mut().zip(bytes) {
        *slot = MaybeUninit::new(b);
    }
    Ok(())
}

#[inline(always)]
pub fn write_bytes(buf: &mut [MaybeUninit<u8>], start: usize, payload: &[u8]) -> Result<()> {
    let Some(end) = start.checked_add(payload.len()) else {
        return Err(cold_internal_err("byte sink offset overflow"));
    };
    let Some(dst) = buf.get_mut(start..end) else {
        return Err(cold_internal_err("byte sink write out of bounds"));
    };
    for (slot, &b) in dst.iter_mut().zip(payload) {
        *slot = MaybeUninit::new(b);
    }
    Ok(())
}

pub trait StringSinkAccess {
    fn reserve_exact(&mut self, add_chars: usize, add_offsets: usize) -> Result<()>;
    fn push_str_unchecked(&mut self, bytes: &[u8]) -> Result<()>;
}

/// Destination of one chunk. Implementations must never touch sinks that the plan marks inactive.
pub trait BatchSinkAccess {
    fn plan(&self) -> ProjectionPlan;
    /// Number of initialized elements in `id`.
    fn sink_len(&self, id: SinkId) -> usize;
    /// Fast check whether all active sinks already have capacity for a row with the given counts.
    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    fn has_row_space(
        &self,
        _ancestors: usize,
        _name_len: usize,
        _linkage_len: usize,
        _decl_file_len: usize,
        _ranges: usize,
        _attrs: usize,
        _attr_str_chars: usize,
    ) -> bool {
        false
    }
    /// Guarantees room for `add` more elements in every listed sink. Implementations invoke the
    /// reserve callback at most once per call, naming every sink that is short.
    fn ensure(&mut self, adds: &[(SinkId, usize)]) -> Result<()>;
    fn push_u64(&mut self, id: SinkId, val: u64) -> Result<()>;
    fn push_u32(&mut self, id: SinkId, val: u32) -> Result<()>;
    fn push_u16(&mut self, id: SinkId, val: u16) -> Result<()>;
    /// Appends `bytes` (no terminator) to `chars_id` and the new cumulative end to `offsets_id`.
    fn push_string(&mut self, chars_id: SinkId, offsets_id: SinkId, bytes: &[u8]) -> Result<()>;
}
