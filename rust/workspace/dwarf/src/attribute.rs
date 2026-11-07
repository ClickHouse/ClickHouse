#![forbid(unsafe_code)]

//! Attribute value decoder: the single exhaustive `match` over DWARF forms that reproduces
//! LLVM 22.1.1's `DWARFFormValue::extractValue` wire decoding and the accessors the old format
//! applied to its result (ledger section 5).
//!
//! Everything here is a bounded read over a `gimli::EndianSlice` cursor. `gimli`'s unit/DIE value
//! model cannot represent `DW_FORM_LLVM_addrx_offset`, the `data16` quirk (`uval = 16`), or the
//! string-form behaviors of the ClickHouse format, so only `gimli::Reader` primitives are used.

use crate::error::{DwarfError, Result};
use crate::file::Sections;
use crate::probe::describe;
use gimli::constants::*;
use gimli::{DwForm, EndianSlice, LittleEndian, Reader};

pub type Cursor<'a> = EndianSlice<'a, LittleEndian>;

/// `DW_FORM_LLVM_addrx_offset` (LLVM `Dwarf.def`, `HANDLE_DW_FORM(0x2001, ...)`).
#[allow(non_upper_case_globals)]
pub const DW_FORM_LLVM_addrx_offset: DwForm = DwForm(0x2001);

/// LLVM's `dwarf::FormParams` for one unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormParams {
    pub version: u16,
    pub address_size: u8,
    /// 4 for 32-bit DWARF, 8 for 64-bit DWARF.
    pub offset_size: u8,
}

impl FormParams {
    pub fn from_encoding(e: gimli::Encoding) -> Self {
        Self {
            version: e.version,
            address_size: e.address_size,
            offset_size: e.format.word_size(),
        }
    }

    /// `FormParams::getRefAddrByteSize`: the address size in DWARF 2, the offset size otherwise.
    #[inline]
    pub fn ref_addr_size(&self) -> u8 {
        if self.version == 2 {
            self.address_size
        } else {
            self.offset_size
        }
    }
}

/// A decoded attribute value (LLVM `DWARFFormValue`): the *resolved* form (after
/// `DW_FORM_indirect`), the raw 64-bit payload, and the bytes of block/inline-string forms.
#[derive(Debug, Clone, Copy)]
pub struct FormValue<'a> {
    pub form: DwForm,
    /// `getRawUValue()`: zero-extended or sign-extended-as-bits constant, block length, offset or
    /// index. For `DW_FORM_string` LLVM leaves a pointer here; this decoder uses `u64::MAX`.
    pub uval: u64,
    /// Block bytes (`block*`, `exprloc`, `data16`) or the inline string without its NUL.
    pub data: &'a [u8],
}

/// Output group of the old `switch (val.getForm())` in `parseEntries`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Group {
    Const,
    Addr,
    Block,
    Str,
    Ref,
    Other,
}

#[inline]
pub fn group(form: DwForm) -> Group {
    match form {
        DW_FORM_data1
        | DW_FORM_data2
        | DW_FORM_data4
        | DW_FORM_data8
        | DW_FORM_sdata
        | DW_FORM_udata
        | DW_FORM_data16
        | DW_FORM_flag
        | DW_FORM_flag_present
        | DW_FORM_loclistx
        | DW_FORM_rnglistx
        | DW_FORM_sec_offset
        | DW_FORM_implicit_const => Group::Const,
        DW_FORM_addr
        | DW_FORM_addrx
        | DW_FORM_addrx1
        | DW_FORM_addrx2
        | DW_FORM_addrx3
        | DW_FORM_addrx4
        | DW_FORM_GNU_addr_index
        | DW_FORM_LLVM_addrx_offset => Group::Addr,
        DW_FORM_block1 | DW_FORM_block2 | DW_FORM_block4 | DW_FORM_block | DW_FORM_exprloc => {
            Group::Block
        }
        DW_FORM_string
        | DW_FORM_strp
        | DW_FORM_strx
        | DW_FORM_strp_sup
        | DW_FORM_line_strp
        | DW_FORM_strx1
        | DW_FORM_strx2
        | DW_FORM_strx3
        | DW_FORM_strx4
        | DW_FORM_GNU_str_index
        | DW_FORM_GNU_strp_alt => Group::Str,
        DW_FORM_ref_addr | DW_FORM_ref1 | DW_FORM_ref2 | DW_FORM_ref4 | DW_FORM_ref8
        | DW_FORM_ref_udata | DW_FORM_ref_sup4 | DW_FORM_ref_sig8 | DW_FORM_ref_sup8
        | DW_FORM_GNU_ref_alt => Group::Ref,
        _ => Group::Other,
    }
}

/// LLVM `doesFormBelongToClass(Form, FC_SectionOffset, Version)`.
#[inline]
pub fn is_section_offset_class(form: DwForm, version: u16) -> bool {
    match form {
        DW_FORM_sec_offset | DW_FORM_loclistx | DW_FORM_rnglistx | DW_FORM_strp
        | DW_FORM_line_strp => true,
        // In DWARF 3 and below data4/data8 doubled as section offsets.
        DW_FORM_data4 | DW_FORM_data8 => version <= 3,
        _ => false,
    }
}

/// `DWARFFormValue::getAsSectionOffset` for a value read in a unit of `version`.
#[inline]
pub fn as_section_offset(v: &FormValue<'_>, version: u16) -> Option<u64> {
    is_section_offset_class(v.form, version).then_some(v.uval)
}

#[inline]
fn read_sized(cur: &mut Cursor<'_>, size: u8) -> gimli::Result<u64> {
    cur.read_address(size)
}

fn eof(what: &str, e: gimli::Error) -> DwarfError {
    DwarfError::dwarf(format!("failed to read {what}: {}", describe(&e)))
}

/// Reads a length-prefixed block of `len` bytes.
#[inline]
fn take_block<'a>(cur: &mut Cursor<'a>, len: u64) -> Result<&'a [u8]> {
    let len = usize::try_from(len).map_err(|_| DwarfError::dwarf("block length exceeds usize"))?;
    cur.split(len)
        .map(|b| b.slice())
        .map_err(|e| eof("block", e))
}

/// LLVM `DWARFFormValue::extractValue` for one attribute specification.
///
/// `implicit` is the `implicit_const` value of the declaration (only meaningful if `declared` is
/// `DW_FORM_implicit_const`). The cursor must be bounded by the unit.
pub fn extract_value<'a>(
    declared: DwForm,
    implicit: Option<i64>,
    cur: &mut Cursor<'a>,
    p: &FormParams,
) -> Result<FormValue<'a>> {
    let mut form = declared;
    // `DWARFFormValue::createFromSValue(attr.Form, implicit_const ? value : 0)`.
    let mut uval: u64 = implicit.unwrap_or(0) as u64;
    let mut data: &'a [u8] = &[];
    // Every `DW_FORM_indirect` iteration consumes at least one byte, so the loop is bounded by
    // the unit size.
    loop {
        match form {
            DW_FORM_addr => {
                uval = read_sized(cur, p.address_size).map_err(|e| eof("DW_FORM_addr", e))?;
            }
            DW_FORM_ref_addr => {
                uval =
                    read_sized(cur, p.ref_addr_size()).map_err(|e| eof("DW_FORM_ref_addr", e))?;
            }
            DW_FORM_exprloc | DW_FORM_block => {
                let len = cur.read_uleb128().map_err(|e| eof("block length", e))?;
                uval = len;
                data = take_block(cur, len)?;
            }
            DW_FORM_block1 => {
                let len = u64::from(cur.read_u8().map_err(|e| eof("block1 length", e))?);
                uval = len;
                data = take_block(cur, len)?;
            }
            DW_FORM_block2 => {
                let len = u64::from(cur.read_u16().map_err(|e| eof("block2 length", e))?);
                uval = len;
                data = take_block(cur, len)?;
            }
            DW_FORM_block4 => {
                let len = u64::from(cur.read_u32().map_err(|e| eof("block4 length", e))?);
                uval = len;
                data = take_block(cur, len)?;
            }
            DW_FORM_data1 | DW_FORM_ref1 | DW_FORM_flag | DW_FORM_strx1 | DW_FORM_addrx1 => {
                uval = u64::from(cur.read_u8().map_err(|e| eof("1-byte form", e))?);
            }
            DW_FORM_data2 | DW_FORM_ref2 | DW_FORM_strx2 | DW_FORM_addrx2 => {
                uval = u64::from(cur.read_u16().map_err(|e| eof("2-byte form", e))?);
            }
            DW_FORM_strx3 | DW_FORM_addrx3 => {
                uval = cur.read_uint(3).map_err(|e| eof("3-byte form", e))?;
            }
            DW_FORM_data4 | DW_FORM_ref4 | DW_FORM_ref_sup4 | DW_FORM_strx4 | DW_FORM_addrx4 => {
                uval = u64::from(cur.read_u32().map_err(|e| eof("4-byte form", e))?);
            }
            DW_FORM_data8 | DW_FORM_ref8 | DW_FORM_ref_sup8 | DW_FORM_ref_sig8 => {
                uval = cur.read_u64().map_err(|e| eof("8-byte form", e))?;
            }
            DW_FORM_data16 => {
                // LLVM treats this as a 16-byte block whose numeric value is the length 16.
                uval = 16;
                data = take_block(cur, 16)?;
            }
            DW_FORM_sdata => {
                uval = cur.read_sleb128().map_err(|e| eof("sdata", e))? as u64;
            }
            DW_FORM_udata
            | DW_FORM_ref_udata
            | DW_FORM_rnglistx
            | DW_FORM_loclistx
            | DW_FORM_GNU_addr_index
            | DW_FORM_GNU_str_index
            | DW_FORM_addrx
            | DW_FORM_strx => {
                uval = cur.read_uleb128().map_err(|e| eof("ULEB128 form", e))?;
            }
            DW_FORM_LLVM_addrx_offset => {
                let index = cur
                    .read_uleb128()
                    .map_err(|e| eof("addrx_offset index", e))?;
                let offset = cur.read_u32().map_err(|e| eof("addrx_offset offset", e))?;
                uval = (index << 32) | u64::from(offset);
            }
            DW_FORM_string => {
                data = cur
                    .read_null_terminated_slice()
                    .map(|s| s.slice())
                    .map_err(|e| eof("inline string", e))?;
                uval = u64::MAX;
            }
            DW_FORM_indirect => {
                let code = cur.read_uleb128().map_err(|e| eof("indirect form", e))?;
                let code = u16::try_from(code)
                    .map_err(|_| DwarfError::dwarf("DW_FORM_indirect form code exceeds 16 bits"))?;
                form = DwForm(code);
                continue;
            }
            DW_FORM_strp | DW_FORM_sec_offset | DW_FORM_GNU_ref_alt | DW_FORM_GNU_strp_alt
            | DW_FORM_line_strp | DW_FORM_strp_sup => {
                uval = read_sized(cur, p.offset_size).map_err(|e| eof("offset form", e))?;
            }
            DW_FORM_flag_present => uval = 1,
            // The value was installed from the abbreviation declaration (or stays 0 when reached
            // through `DW_FORM_indirect`).
            DW_FORM_implicit_const => {}
            other => {
                return Err(DwarfError::dwarf(format!(
                    "unknown or unsupported attribute form 0x{:x}",
                    other.0
                )));
            }
        }
        return Ok(FormValue { form, uval, data });
    }
}

/// `StrOffsetsContributionDescriptor` (the pieces `getStringOffsetSectionItem` uses).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrOffsets {
    pub base: u64,
    pub size: u64,
    /// Format of the contribution, which may differ from the unit's format.
    pub dwarf64: bool,
}

impl StrOffsets {
    #[inline]
    fn item_size(&self) -> u8 {
        if self.dwarf64 {
            8
        } else {
            4
        }
    }
}

/// Everything needed to turn a [`FormValue`] into the values the format exposes. Cheap to copy;
/// rebuilt per call because it borrows the call's input.
#[derive(Debug, Clone, Copy)]
pub struct Ctx<'a> {
    pub sections: Sections<'a>,
    pub params: FormParams,
    pub unit_offset: u64,
    pub str_offsets: Option<StrOffsets>,
    /// `DWARFUnit::AddrOffsetSectionBase` (standard base, else GNU base).
    pub addr_base: Option<u64>,
}

/// LLVM `DataExtractor::getCStr`: the bytes from `offset` up to the first NUL, which must exist.
#[inline(always)]
fn cstr_at(section: &[u8], offset: u64) -> Option<&[u8]> {
    let offset = usize::try_from(offset).ok()?;
    let tail = section.get(offset..)?;
    core::ffi::CStr::from_bytes_until_nul(tail)
        .ok()
        .map(core::ffi::CStr::to_bytes)
}

/// Little-endian value of `size` bytes at `offset`, or `None` if out of bounds.
#[inline(always)]
fn read_le(data: &[u8], offset: u64, size: u64) -> Option<u64> {
    let start = usize::try_from(offset).ok()?;
    match size {
        4 => {
            let b: [u8; 4] = data.get(start..start.checked_add(4)?)?.try_into().ok()?;
            Some(u64::from(u32::from_le_bytes(b)))
        }
        8 => {
            let b: [u8; 8] = data.get(start..start.checked_add(8)?)?.try_into().ok()?;
            Some(u64::from_le_bytes(b))
        }
        2 => {
            let b: [u8; 2] = data.get(start..start.checked_add(2)?)?.try_into().ok()?;
            Some(u64::from(u16::from_le_bytes(b)))
        }
        _ => {
            let end = start.checked_add(usize::try_from(size).ok()?)?;
            let bytes = data.get(start..end)?;
            let mut v = 0u64;
            for (i, b) in bytes.iter().enumerate() {
                v |= u64::from(*b) << (8 * i);
            }
            Some(v)
        }
    }
}
impl<'a> Ctx<'a> {
    /// `DWARFUnit::getStringOffsetSectionItem`: the `.debug_str_offsets` entry for a `strx` index.
    /// LLVM takes the index as `uint32_t` and computes `Index * ItemSize` in 32 bits.
    fn str_offset_item(&self, index: u64) -> Result<u64> {
        let c = self.str_offsets.ok_or_else(|| {
            DwarfError::dwarf("DW_FORM_strx used without a valid string offsets table")
        })?;
        let index = index as u32;
        let item = u64::from(c.item_size());
        let offset = c
            .base
            .wrapping_add(u64::from(index.wrapping_mul(u32::from(c.item_size()))));
        if (self.sections.str_offsets.len() as u64) < offset.wrapping_add(item) {
            return Err(DwarfError::dwarf(format!(
                "DW_FORM_strx uses index {index}, which is too large"
            )));
        }
        read_le(self.sections.str_offsets, offset, item)
            .ok_or_else(|| DwarfError::dwarf("string offsets entry is out of bounds"))
    }

    /// `DWARFFormValue::getAsCString` for a value of any form. Errors reproduce LLVM's: a
    /// non-string form, `DW_FORM_GNU_strp_alt`, a missing offsets table, or an offset with no NUL
    /// terminator before the end of the string section.
    pub fn resolve_string(&self, v: &FormValue<'a>) -> Result<&'a [u8]> {
        let (section, offset, what) = match v.form {
            DW_FORM_string => return Ok(v.data),
            DW_FORM_GNU_strp_alt => {
                return Err(DwarfError::dwarf("Unsupported form for string attribute"));
            }
            DW_FORM_strx
            | DW_FORM_strx1
            | DW_FORM_strx2
            | DW_FORM_strx3
            | DW_FORM_strx4
            | DW_FORM_GNU_str_index => (
                self.sections.str_,
                self.str_offset_item(v.uval)?,
                ".debug_str",
            ),
            DW_FORM_strp | DW_FORM_strp_sup => (self.sections.str_, v.uval, ".debug_str"),
            DW_FORM_line_strp => (self.sections.line_str, v.uval, ".debug_line_str"),
            _ => return Err(DwarfError::dwarf("Invalid form for string attribute")),
        };
        cstr_at(section, offset).ok_or_else(|| {
            DwarfError::dwarf(format!(
                "string offset {offset} is beyond {what} bounds or unterminated"
            ))
        })
    }

    /// `DWARFUnit::getAddrOffsetSectionItem`, without the DWO fallback (units are never DWO).
    fn addr_item(&self, index: u32) -> Option<u64> {
        let base = self.addr_base?;
        let size = self.params.address_size;
        // `Index * getAddressByteSize()` is evaluated in 32 bits, the sum in 64 bits.
        let offset = base.wrapping_add(u64::from(index.wrapping_mul(u32::from(size))));
        let data = self.sections.addr_llvm;
        if (data.len() as u64) < offset.wrapping_add(u64::from(size)) {
            return None;
        }
        // In bounds unless the 64-bit sum wrapped; LLVM then reads zero.
        Some(read_le(data, offset, u64::from(size)).unwrap_or(0))
    }

    /// `DWARFFormValue::getAsAddress` for address-group values.
    pub fn as_address(&self, v: &FormValue<'a>) -> Option<u64> {
        match v.form {
            DW_FORM_addr => Some(v.uval),
            DW_FORM_addrx
            | DW_FORM_addrx1
            | DW_FORM_addrx2
            | DW_FORM_addrx3
            | DW_FORM_addrx4
            | DW_FORM_GNU_addr_index => self.addr_item(v.uval as u32),
            DW_FORM_LLVM_addrx_offset => self
                .addr_item((v.uval >> 32) as u32)
                .map(|a| a.wrapping_add(v.uval & 0xffff_ffff)),
            _ => None,
        }
    }

    /// Reference-group `attr_int`: unit-relative forms are rebased to `.debug_info`, `ref_addr` is
    /// already absolute, the supplementary/signature forms are dropped (0).
    pub fn reference_value(&self, v: &FormValue<'a>) -> u64 {
        match v.form {
            DW_FORM_ref1 | DW_FORM_ref2 | DW_FORM_ref4 | DW_FORM_ref8 | DW_FORM_ref_udata => {
                self.unit_offset.wrapping_add(v.uval)
            }
            DW_FORM_ref_addr => v.uval,
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> FormParams {
        FormParams {
            version: 4,
            address_size: 8,
            offset_size: 4,
        }
    }

    fn extract(form: DwForm, bytes: &[u8]) -> (Result<FormValue<'_>>, usize) {
        let mut cur = Cursor::new(bytes, LittleEndian);
        let r = extract_value(form, None, &mut cur, &params());
        (r, bytes.len() - cur.len())
    }

    #[test]
    fn fixed_width_forms() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9];
        assert_eq!(extract(DW_FORM_data1, &bytes).0.unwrap().uval, 1);
        assert_eq!(extract(DW_FORM_data2, &bytes).0.unwrap().uval, 0x0201);
        assert_eq!(extract(DW_FORM_strx3, &bytes).0.unwrap().uval, 0x030201);
        assert_eq!(extract(DW_FORM_data4, &bytes).0.unwrap().uval, 0x0403_0201);
        assert_eq!(
            extract(DW_FORM_data8, &bytes).0.unwrap().uval,
            0x0807_0605_0403_0201
        );
        let (r, used) = extract(DW_FORM_flag_present, &bytes);
        assert_eq!((r.unwrap().uval, used), (1, 0));
    }

    #[test]
    fn data16_is_a_block_with_value_16() {
        let bytes: Vec<u8> = (0..16).collect();
        let (r, used) = extract(DW_FORM_data16, &bytes);
        let v = r.unwrap();
        assert_eq!((v.uval, v.data.len(), used), (16, 16, 16));
        assert!(extract(DW_FORM_data16, &bytes[..15]).0.is_err());
    }

    #[test]
    fn indirect_resolves_and_reports_resolved_form() {
        // indirect -> data1 (0x0b), value 0x2a
        let (r, used) = extract(DW_FORM_indirect, &[0x0b, 0x2a]);
        let v = r.unwrap();
        assert_eq!((v.form, v.uval, used), (DW_FORM_data1, 0x2a, 2));
        // indirect -> indirect -> implicit_const consumes only the two form codes
        let (r, used) = extract(DW_FORM_indirect, &[0x16, 0x21]);
        let v = r.unwrap();
        assert_eq!((v.form, v.uval, used), (DW_FORM_implicit_const, 0, 2));
        // form code above 16 bits is rejected
        assert!(extract(DW_FORM_indirect, &[0x80, 0x80, 0x04]).0.is_err());
    }

    #[test]
    fn unknown_form_and_truncation_are_errors() {
        assert!(extract(DwForm(0x7777), &[0; 8]).0.is_err());
        assert!(extract(DW_FORM_data4, &[1, 2, 3]).0.is_err());
        assert!(extract(DW_FORM_udata, &[0x80]).0.is_err());
        assert!(extract(DW_FORM_string, b"abc").0.is_err());
        assert!(extract(DW_FORM_block1, &[5, 1, 2]).0.is_err());
    }

    #[test]
    fn addrx_offset_packs_index_and_offset() {
        let (r, used) = extract(DW_FORM_LLVM_addrx_offset, &[0x03, 0x10, 0, 0, 0]);
        let v = r.unwrap();
        assert_eq!((v.uval, used), ((3u64 << 32) | 0x10, 5));
    }

    #[test]
    fn implicit_const_uses_declared_value() {
        let mut cur = Cursor::new(&[], LittleEndian);
        let v = extract_value(DW_FORM_implicit_const, Some(-2), &mut cur, &params()).unwrap();
        assert_eq!(v.uval as i64, -2);
    }

    #[test]
    fn section_offset_class_depends_on_version() {
        assert!(is_section_offset_class(DW_FORM_sec_offset, 5));
        assert!(is_section_offset_class(DW_FORM_strp, 5));
        assert!(is_section_offset_class(DW_FORM_data4, 3));
        assert!(!is_section_offset_class(DW_FORM_data4, 4));
        assert!(!is_section_offset_class(DW_FORM_implicit_const, 5));
    }
}
