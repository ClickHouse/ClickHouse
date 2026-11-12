#![forbid(unsafe_code)]

//! One `.debug_info` unit: root-DIE initialization and the chunked DIE walk.
//!
//! The DIE cursor is bounded by the unit, keeps its ancestors in an explicit vector, emits null
//! entries as rows, and requires the DIE tree to close exactly at the unit end. Attribute values
//! are decoded for every attribute of every DIE whatever the projection; the projection only
//! controls which values are written (and whether range lists are decoded).

use crate::attribute::{
    as_section_offset, extract_value, group, Ctx, Cursor, FormParams, FormValue, Group, StrOffsets,
};
use crate::control::{Control, Work};
use crate::error::{DwarfError, Result};
use crate::file::{FileInner, Sections};
use crate::line::{parse_filename_table, FilenameTable};
use crate::names::{lookup_encoding_name, lookup_language_name};
use crate::outbuf::{
    BatchSinkAccess, COL_ATTR_STR, COL_DECL_FILE, COL_DECL_LINE, COL_LINKAGE_NAME, COL_NAME,
    COL_RANGES,
};
use crate::range::{fetch_from_debug_addr, parse_ranges, RangeState, UNSET};
use crate::rows::{write_row, AttrOut, Counters, Row, StackEntry, StrSrc};
use core::cell::Cell;
use core::marker::PhantomData;
use gimli::constants::*;
use gimli::{Abbreviation, Abbreviations, DwAt, DwForm, LittleEndian, Reader};
use std::sync::Arc;

/// LLVM `isValidOffsetForDataOfSize(offset, length)` over a buffer of `size` bytes, including its
/// quirks: wrapped sums are invalid, and a zero-length range at offset 0 is invalid.
#[inline]
fn valid_range(size: u64, offset: u64, length: u64) -> bool {
    let end = offset.wrapping_add(length);
    end >= offset && size > end.wrapping_sub(1)
}

/// `StrOffsetsContributionDescriptor::validateContributionSize`.
fn validate_contribution(section: &[u8], c: StrOffsets) -> Result<StrOffsets> {
    let entry_size: u64 = if c.dwarf64 { 8 } else { 4 };
    // `alignTo(Size, EntrySize)` wraps for sizes near 2^64; the `>=` guard rejects those.
    let validation_size = c.size.wrapping_add(entry_size - 1) / entry_size * entry_size;
    if validation_size >= c.size
        && valid_range(
            section.len() as u64,
            u64::from(c.base as u32),
            validation_size,
        )
    {
        Ok(c)
    } else {
        Err(DwarfError::dwarf(
            "invalid reference to or invalid content in .debug_str_offsets: length exceeds section size",
        ))
    }
}

/// `DWARFUnit::determineStringOffsetsTableContribution`: validates the contribution header that
/// precedes `base` (the value of `DW_AT_str_offsets_base`) exactly as LLVM 22.1.1 does.
pub fn str_offsets_contribution(
    section: &[u8],
    unit_dwarf64: bool,
    base: u64,
) -> Result<StrOffsets> {
    let size = section.len() as u64;
    let bad = |what: &str| {
        DwarfError::dwarf(format!(
            "invalid reference to or invalid content in .debug_str_offsets: {what}"
        ))
    };
    let u16_at = |off: u64| -> u16 {
        u16::from_le_bytes([section[off as usize], section[off as usize + 1]])
    };
    let u32_at = |off: u64| -> u32 {
        let o = off as usize;
        u32::from_le_bytes([section[o], section[o + 1], section[o + 2], section[o + 3]])
    };
    let u64_at = |off: u64| -> u64 {
        let o = off as usize;
        let mut b = [0u8; 8];
        b.copy_from_slice(&section[o..o + 8]);
        u64::from_le_bytes(b)
    };
    let desc = if unit_dwarf64 {
        if base < 16 {
            return Err(bad("insufficient space for 64 bit header prefix"));
        }
        let at = base - 16;
        if !valid_range(size, at, 16) {
            return Err(bad("section offset exceeds section size"));
        }
        if u32_at(at) != 0xffff_ffff {
            return Err(bad("32 bit contribution referenced from a 64 bit unit"));
        }
        let len = u64_at(at + 4);
        // The version (a u16 narrowed to u8 by LLVM) and padding are not interpreted.
        let _version = u16_at(at + 12);
        StrOffsets {
            base,
            size: len.wrapping_sub(4),
            dwarf64: true,
        }
    } else {
        if base < 8 {
            return Err(bad("insufficient space for 32 bit header prefix"));
        }
        let at = base - 8;
        if !valid_range(size, at, 8) {
            return Err(bad("section offset exceeds section size"));
        }
        let len = u32_at(at);
        if len >= 0xffff_fff0 {
            return Err(bad("invalid length"));
        }
        StrOffsets {
            base,
            // `ContributionSize - 4` is evaluated in 32 bits.
            size: u64::from(len.wrapping_sub(4)),
            dwarf64: false,
        }
    };
    validate_contribution(section, desc)
}

/// Mutable walk state.
struct Walk {
    /// Absolute `.debug_info` offset of the next DIE.
    pos: u64,
    done: bool,
    failed: bool,
    stack: Vec<StackEntry>,
    unit_name: Vec<u8>,
    filename_table: Option<FilenameTable>,
    debug_addr_base: u64,
    rnglists_base: u64,
    base_address: u64,
}

/// A unit being read. `Send` (it may move between worker threads between calls) but not `Sync`:
/// a unit is never entered concurrently.
pub struct UnitState {
    file: Arc<FileInner>,
    abbrevs: Option<Arc<Abbreviations>>,
    params: FormParams,
    unit_offset: u64,
    end_offset: u64,
    str_offsets: Option<StrOffsets>,
    addr_base: Option<u64>,
    walk: Walk,
    _not_sync: PhantomData<Cell<()>>,
}

/// What one `parse_chunk` call produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkResult {
    pub rows: usize,
    /// `.debug_info` bytes the walk advanced in this call.
    pub bytes_advanced: u64,
    pub done: bool,
}

/// First attribute of the root DIE with a given name (`DWARFDie::find` semantics).
fn first_attr<'a>(seen: &mut [Option<FormValue<'a>>; 3], name: DwAt, v: &FormValue<'a>) {
    let slot = match name {
        DW_AT_str_offsets_base => 0,
        DW_AT_addr_base => 1,
        DW_AT_GNU_addr_base => 2,
        _ => return,
    };
    if seen[slot].is_none() {
        seen[slot] = Some(*v);
    }
}

impl UnitState {
    /// Opens unit `index` of `file` (`bytes` is the input span the file was opened with) and runs
    /// LLVM's root-DIE initialization (`DWARFUnit::tryExtractDIEsIfNeeded(CUDieOnly)`).
    pub fn open(file: &Arc<FileInner>, bytes: &[u8], index: usize) -> Result<Self> {
        let desc = file
            .units
            .get(index)
            .ok_or_else(|| DwarfError::invalid_arg("unit index out of range"))?;
        let sections = file.sections(bytes)?;
        let params = FormParams::from_encoding(desc.encoding);
        let initial_pos = desc.unit_offset + desc.header_size;

        let unit_bytes = usize::try_from(desc.end_offset)
            .ok()
            .and_then(|end| sections.info.get(..end))
            .ok_or_else(|| DwarfError::internal("unit end outside .debug_info"))?;
        let mut cur = Cursor::new(
            unit_bytes
                .get(initial_pos as usize..)
                .ok_or_else(|| DwarfError::internal("unit start outside .debug_info"))?,
            LittleEndian,
        );

        // The root DIE: it must exist, and decode completely.
        let code = cur.read_uleb128().map_err(|e| {
            DwarfError::dwarf(format!("Failed to parse compilation unit entry: {e}"))
        })?;
        if code == 0 {
            return Err(DwarfError::dwarf(
                "Failed to parse compilation unit entry: the root DIE is a null entry",
            ));
        }
        let abbrevs = file.abbreviations_for(sections.abbrev, desc.abbrev_offset)?;
        let abbrev = abbrevs.get(code).ok_or_else(|| {
            DwarfError::dwarf(format!(
                "Failed to parse compilation unit entry: abbreviation code {code} is not declared"
            ))
        })?;
        let mut seen: [Option<FormValue<'_>>; 3] = [None; 3];
        for spec in abbrev.attributes() {
            let v = extract_value(spec.form(), spec.implicit_const_value(), &mut cur, &params)
                .map_err(|e| {
                    DwarfError::dwarf(format!("Failed to parse compilation unit entry: {e}"))
                })?;
            first_attr(&mut seen, spec.name(), &v);
        }
        let sec_off =
            |slot: Option<FormValue<'_>>| slot.and_then(|v| as_section_offset(&v, params.version));

        // `DW_AT_str_offsets_base` is order independent; DWARF 5 units only (units here are never
        // DWO).
        let str_offsets = if params.version >= 5 {
            match sec_off(seen[0]) {
                Some(base) => Some(str_offsets_contribution(
                    sections.str_offsets,
                    params.offset_size == 8,
                    base,
                )?),
                None => None,
            }
        } else {
            None
        };
        // The standard address base wins; the GNU one is only consulted when it is absent.
        let addr_base = sec_off(seen[1]).or_else(|| sec_off(seen[2]));

        Ok(Self {
            file: Arc::clone(file),
            abbrevs: Some(abbrevs),
            params,
            unit_offset: desc.unit_offset,
            end_offset: desc.end_offset,
            str_offsets,
            addr_base,
            walk: Walk {
                pos: initial_pos,
                done: false,
                failed: false,
                stack: Vec::new(),
                unit_name: Vec::new(),
                filename_table: None,
                debug_addr_base: UNSET,
                rnglists_base: UNSET,
                base_address: UNSET,
            },
            _not_sync: PhantomData,
        })
    }

    #[inline]
    pub fn unit_offset(&self) -> u64 {
        self.unit_offset
    }

    /// The compilation unit name as of the last chunk (last `DW_AT_name` of any
    /// `DW_TAG_compile_unit` DIE walked so far).
    #[inline]
    pub fn unit_name(&self) -> &[u8] {
        &self.walk.unit_name
    }

    #[inline]
    pub fn file_generation(&self) -> u64 {
        self.file.generation
    }

    #[inline]
    pub fn is_done(&self) -> bool {
        self.walk.done
    }

    /// Walks DIEs, appending one row each to `sink`, until `max_rows` rows were produced or the
    /// unit's tree closed. Any error (or cancellation) leaves the unit unusable and the partially
    /// written chunk meaningless.
    pub fn parse_chunk<S: BatchSinkAccess>(
        &mut self,
        bytes: &[u8],
        sink: &mut S,
        ctl: &mut dyn Control,
        max_rows: usize,
    ) -> Result<ChunkResult> {
        if self.walk.failed {
            return Err(DwarfError::invalid_arg(
                "unit cannot be read after a failed or cancelled call",
            ));
        }
        if self.walk.done {
            return Err(DwarfError::invalid_arg("unit is already complete"));
        }
        if max_rows == 0 {
            return Err(DwarfError::invalid_arg("max_rows must be positive"));
        }
        let r = self.parse_chunk_inner(bytes, sink, ctl, max_rows);
        if r.is_err() {
            self.walk.failed = true;
        }
        r
    }

    fn parse_chunk_inner<S: BatchSinkAccess>(
        &mut self,
        bytes: &[u8],
        sink: &mut S,
        ctl: &mut dyn Control,
        max_rows: usize,
    ) -> Result<ChunkResult> {
        let sections = self.file.sections(bytes)?;
        let ctx = Ctx {
            sections,
            params: self.params,
            unit_offset: self.unit_offset,
            str_offsets: self.str_offsets,
            addr_base: self.addr_base,
        };
        let plan = sink.plan();
        let want_name = plan.has(COL_NAME);
        let want_linkage = plan.has(COL_LINKAGE_NAME);
        let want_decl_file = plan.has(COL_DECL_FILE);
        let want_decl_line = plan.has(COL_DECL_LINE);
        let want_ranges = plan.has(COL_RANGES);
        let want_attr_str = plan.has(COL_ATTR_STR);
        let want_attrs = plan.need_attr_offsets;

        let unit_end = self.end_offset;
        let unit_bytes = usize::try_from(unit_end)
            .ok()
            .and_then(|end| sections.info.get(..end))
            .ok_or_else(|| DwarfError::internal("unit end outside .debug_info"))?;
        let abbrevs = self
            .abbrevs
            .as_ref()
            .ok_or_else(|| DwarfError::internal("unit abbreviations missing"))?;
        let params = self.params;
        let w = &mut self.walk;

        let mut work = Work::new(ctl);
        work.check()?;
        let start_pos = w.pos;
        let mut counters = Counters::default();
        let mut attrs: Vec<AttrOut<'_>> = Vec::new();
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        let mut rows = 0usize;

        while rows < max_rows {
            rows += 1;
            work.tick(1)?;
            let die_offset = w.pos;
            if die_offset >= unit_end {
                return Err(DwarfError::dwarf(format!(
                    "DIE tree is not closed at the end of the unit at offset {unit_end} ({} open)",
                    w.stack.len()
                )));
            }
            let mut cur = Cursor::new(&unit_bytes[die_offset as usize..], LittleEndian);
            let code = cur
                .read_uleb128()
                .map_err(|e| DwarfError::dwarf(format!("Failed to parse DIE header: {e}")))?;

            if code == 0 {
                let end_pos = unit_end - cur.len() as u64;
                let row = Row {
                    offset: die_offset,
                    size: (end_pos - die_offset) as u32,
                    tag: 0,
                    name: b"",
                    linkage_name: b"",
                    decl_file: StrSrc::Empty,
                    decl_line: 0,
                    ranges: &[],
                    attrs: &[],
                    attr_str_chars: 0,
                    ancestors: &w.stack,
                };
                write_row(sink, &row, &mut counters)?;
                if w.stack.pop().is_none() {
                    return Err(DwarfError::dwarf("Stack underflow"));
                }
                w.pos = end_pos;
            } else {
                let abbrev: &Abbreviation = abbrevs.get(code).ok_or_else(|| {
                    DwarfError::dwarf(format!(
                        "Abbrev code in DIE header is out of bounds: {code}, offset {die_offset}"
                    ))
                })?;
                let tag = abbrev.tag();
                let is_cu = tag == DW_TAG_compile_unit;
                if is_cu {
                    prepass(w, abbrev, cur, &ctx, &mut work)?;
                }

                attrs.clear();
                ranges.clear();
                let mut attr_str_chars = 0usize;
                let mut need_name = want_name;
                let mut need_linkage = want_linkage;
                let mut need_decl_file = want_decl_file;
                let mut need_decl_line = want_decl_line;
                let mut name: &[u8] = b"";
                let mut linkage: &[u8] = b"";
                let mut decl_file = StrSrc::Empty;
                let mut decl_line = 0u32;
                let mut ranges_value: Option<u64> = None;
                let mut ranges_rnglistx = false;
                let mut low_pc: Option<u64> = None;
                let mut high_pc: Option<u64> = None;
                let mut relative_high_pc = false;

                for spec in abbrev.attributes() {
                    work.tick(1)?;
                    let v =
                        extract_value(spec.form(), spec.implicit_const_value(), &mut cur, &params)?;
                    let aname = spec.name();

                    if aname == DW_AT_stmt_list && w.filename_table.is_none() {
                        // Expected before any attribute that points into the table; parsed once.
                        if let Some(offset) = as_section_offset(&v, params.version) {
                            w.filename_table = Some(parse_filename_table(&ctx, offset, &mut work)?);
                        }
                    }

                    let mut int = 0u64;
                    let mut str_ = StrSrc::Empty;
                    match group(v.form) {
                        Group::Const => {
                            int = v.uval;
                            if aname == DW_AT_decl_line
                                && core::mem::replace(&mut need_decl_line, false)
                            {
                                decl_line = v.uval as u32;
                            }
                            if aname == DW_AT_high_pc {
                                high_pc = Some(v.uval);
                                relative_high_pc = true;
                            }
                            if aname == DW_AT_ranges {
                                ranges_value = Some(v.uval);
                                ranges_rnglistx = v.form == DW_FORM_rnglistx;
                            }
                            // Some values index lookup tables that can be stringified.
                            if let Some(table) = w.filename_table.as_ref() {
                                if (aname == DW_AT_decl_file || aname == DW_AT_call_file)
                                    && v.uval < table.size()
                                {
                                    let resolved = table.get(v.uval + 1, &ctx.sections);
                                    if aname == DW_AT_decl_file
                                        && core::mem::replace(&mut need_decl_file, false)
                                    {
                                        decl_file = StrSrc::Bytes(resolved);
                                    }
                                    if want_attr_str {
                                        str_ = StrSrc::Bytes(resolved);
                                    }
                                }
                            }
                            if matches!(str_, StrSrc::Empty) && want_attr_str {
                                if aname == DW_AT_language {
                                    str_ = StrSrc::Bytes(
                                        lookup_language_name(v.uval as u32).as_bytes(),
                                    );
                                } else if aname == DW_AT_encoding {
                                    str_ = StrSrc::Bytes(
                                        lookup_encoding_name(v.uval as u32).as_bytes(),
                                    );
                                }
                            }
                        }
                        Group::Addr => {
                            int = ctx.as_address(&v).unwrap_or(0);
                            if want_ranges && (aname == DW_AT_low_pc || aname == DW_AT_high_pc) {
                                let addr =
                                    parse_address(&ctx.sections, w.debug_addr_base, aname, &v)?;
                                if aname == DW_AT_low_pc {
                                    low_pc = Some(addr);
                                } else {
                                    high_pc = Some(addr);
                                }
                            }
                        }
                        Group::Block => {
                            str_ = StrSrc::Bytes(v.data);
                        }
                        Group::Str => {
                            let res = ctx.resolve_string(&v).map_err(|e| {
                                DwarfError::dwarf(format!("Error parsing string attribute: {e}"))
                            })?;
                            if aname == DW_AT_name {
                                if core::mem::replace(&mut need_name, false) {
                                    name = res;
                                }
                                if is_cu {
                                    work.tick(res.len() / 4096)?;
                                    w.unit_name.clear();
                                    w.unit_name.extend_from_slice(res);
                                }
                            }
                            if aname == DW_AT_linkage_name
                                && core::mem::replace(&mut need_linkage, false)
                            {
                                linkage = res;
                            }
                            str_ = StrSrc::Bytes(res);
                        }
                        Group::Ref => {
                            int = ctx.reference_value(&v);
                        }
                        Group::Other => {}
                    }

                    if want_attrs {
                        if want_attr_str {
                            attr_str_chars += match str_ {
                                StrSrc::Empty => 0,
                                StrSrc::Bytes(b) => b.len(),
                            };
                        }
                        attrs.push(AttrOut {
                            name: aname.0,
                            form: spec.form().0,
                            int,
                            str_,
                        });
                    }
                }

                let end_pos = unit_end - cur.len() as u64;

                if want_ranges {
                    if let Some(value) = ranges_value {
                        let st = RangeState {
                            params,
                            debug_addr_base: w.debug_addr_base,
                            rnglists_base: w.rnglists_base,
                            base_address: w.base_address,
                        };
                        parse_ranges(
                            &st,
                            &ctx.sections,
                            value,
                            ranges_rnglistx,
                            &mut ranges,
                            &mut work,
                        )?;
                    } else if let Some(low) = low_pc {
                        let high = match high_pc {
                            None => low.wrapping_add(1),
                            Some(h) if relative_high_pc => low.wrapping_add(h),
                            Some(h) => h,
                        };
                        ranges.push((low, high));
                    }
                }

                let row = Row {
                    offset: die_offset,
                    size: (end_pos - die_offset) as u32,
                    tag: tag.0,
                    name,
                    linkage_name: linkage,
                    decl_file,
                    decl_line,
                    ranges: &ranges,
                    attrs: &attrs,
                    attr_str_chars,
                    ancestors: &w.stack,
                };
                write_row(sink, &row, &mut counters)?;

                if abbrev.has_children() {
                    w.stack.push(StackEntry {
                        offset: die_offset,
                        tag: tag.0,
                    });
                }
                w.pos = end_pos;
            }

            if w.stack.is_empty() {
                if w.pos != unit_end {
                    return Err(DwarfError::dwarf(format!(
                        "Unexpected end of DIE tree at offset {} != {unit_end}",
                        w.pos
                    )));
                }
                w.done = true;
                w.filename_table = None;
                w.stack = Vec::new();
                self.abbrevs = None;
                break;
            }
        }

        Ok(ChunkResult {
            rows,
            bytes_advanced: w.pos - start_pos,
            done: w.done,
        })
    }
}

/// The old format's pass over every `DW_TAG_compile_unit` DIE: pick up the standard
/// `DW_AT_addr_base`, `DW_AT_rnglists_base` (raw value of whatever form) and `DW_AT_low_pc`
/// before the DIE's attributes are walked, because ranges and addresses may depend on them.
///
/// The old code skipped the other attributes with `skipValue`; decoding them strictly can only
/// fail where the walk that follows fails anyway.
fn prepass(
    w: &mut Walk,
    abbrev: &Abbreviation,
    mut cur: Cursor<'_>,
    ctx: &Ctx<'_>,
    work: &mut Work<'_>,
) -> Result<()> {
    let mut low_pc: Option<FormValue<'_>> = None;
    for spec in abbrev.attributes() {
        work.tick(1)?;
        let v = extract_value(
            spec.form(),
            spec.implicit_const_value(),
            &mut cur,
            &ctx.params,
        )?;
        match spec.name() {
            DW_AT_addr_base => w.debug_addr_base = v.uval,
            DW_AT_rnglists_base => w.rnglists_base = v.uval,
            DW_AT_low_pc => low_pc = Some(v),
            _ => {}
        }
    }
    // May use the address base just read.
    if let Some(v) = low_pc {
        w.base_address = parse_address(&ctx.sections, w.debug_addr_base, DW_AT_low_pc, &v)?;
    }
    Ok(())
}

/// Old `parseAddress`: only `DW_FORM_addr` and the standard `addrx*` forms are accepted.
fn parse_address(
    sections: &Sections<'_>,
    debug_addr_base: u64,
    attr: DwAt,
    v: &FormValue<'_>,
) -> Result<u64> {
    match v.form {
        DW_FORM_addr => Ok(v.uval),
        DW_FORM_addrx | DW_FORM_addrx1 | DW_FORM_addrx2 | DW_FORM_addrx3 | DW_FORM_addrx4 => {
            fetch_from_debug_addr(sections, debug_addr_base, v.uval)
        }
        other => Err(DwarfError::dwarf(format!(
            "Form {} for {} is not supported",
            form_name(other),
            attr.static_string().unwrap_or("attribute")
        ))),
    }
}

fn form_name(f: DwForm) -> String {
    f.static_string()
        .map_or_else(|| format!("0x{:x}", f.0), str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_range_quirks() {
        assert!(valid_range(8, 0, 8));
        assert!(!valid_range(8, 1, 8));
        assert!(!valid_range(8, 0, 0)); // zero length at offset 0
        assert!(valid_range(8, 8, 0)); // zero length at the end
        assert!(!valid_range(8, 9, 0));
        assert!(!valid_range(8, u64::MAX, 2)); // wraps
    }

    /// LLVM's `isValidOffsetForDataOfSize` over small values (no 64-bit wrap), restated with
    /// exact arithmetic: a non-empty range must lie inside the buffer; an empty range is valid
    /// only strictly after offset 0 and up to the end (`isValidOffset(offset - 1)`).
    fn reference(size: u64, off: u64, len: u64) -> bool {
        if len == 0 {
            off >= 1 && off <= size
        } else {
            off + len <= size
        }
    }

    fn check_valid_range(f: impl Fn(u64, u64, u64) -> bool) -> Result<(), String> {
        for size in 0..=24u64 {
            for off in 0..=26u64 {
                for len in 0..=26u64 {
                    if f(size, off, len) != reference(size, off, len) {
                        return Err(format!("size {size} off {off} len {len}"));
                    }
                }
            }
        }
        // Near the top of the 64-bit range sums wrap; wrapped ranges are never valid.
        for off in [u64::MAX, u64::MAX - 1, u64::MAX / 2 + 1] {
            for len in [1, 2, 3, u64::MAX, u64::MAX / 2 + 1] {
                if off.checked_add(len).is_none() && f(1 << 40, off, len) {
                    return Err(format!("wrapped off {off:#x} len {len:#x}"));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn valid_range_is_exhaustively_exact() {
        check_valid_range(valid_range).unwrap();
    }

    #[test]
    fn valid_range_mutants_are_caught() {
        // Off by one at the end.
        assert!(
            check_valid_range(|s, o, l| o.wrapping_add(l) >= o && s > o.wrapping_add(l)).is_err()
        );
        // Forgets the wrap guard.
        assert!(check_valid_range(|s, o, l| s > o.wrapping_add(l).wrapping_sub(1)).is_err());
        // Treats a zero-length range at offset 0 as valid.
        assert!(check_valid_range(|s, o, l| o.checked_add(l).is_some_and(|e| e <= s)).is_err());
    }

    fn contribution_32(len: u32, entries: u32) -> Vec<u8> {
        let mut v = len.to_le_bytes().to_vec();
        v.extend([5, 0, 0, 0]); // version 5, padding
        v.extend(vec![0u8; entries as usize * 4]);
        v
    }

    #[test]
    fn contribution_header_validation() {
        let sec = contribution_32(4 + 8, 2);
        let c = str_offsets_contribution(&sec, false, 8).unwrap();
        assert_eq!(
            c,
            StrOffsets {
                base: 8,
                size: 8,
                dwarf64: false
            }
        );
        // base without room for the header prefix
        assert!(str_offsets_contribution(&sec, false, 4).is_err());
        // length claims more than the section holds
        let long = contribution_32(4 + 64, 2);
        assert!(str_offsets_contribution(&long, false, 8).is_err());
        // reserved length
        let reserved = contribution_32(0xffff_fff0, 0);
        assert!(str_offsets_contribution(&reserved, false, 8).is_err());
        // length below 4 wraps in 32 bits and cannot fit
        let tiny = contribution_32(2, 0);
        assert!(str_offsets_contribution(&tiny, false, 8).is_err());
        // a 32-bit header referenced from a 64-bit unit
        let mut padded = vec![0u8; 8];
        padded.extend(&sec);
        assert!(str_offsets_contribution(&padded, true, 24).is_err());
        // empty section
        assert!(str_offsets_contribution(&[], false, 8).is_err());
    }

    #[test]
    fn contribution_64_bit() {
        let mut sec = vec![0xff, 0xff, 0xff, 0xff];
        sec.extend((4u64 + 16).to_le_bytes());
        sec.extend([5, 0, 0, 0]);
        sec.extend(vec![0u8; 16]);
        let c = str_offsets_contribution(&sec, true, 16).unwrap();
        assert_eq!(
            c,
            StrOffsets {
                base: 16,
                size: 16,
                dwarf64: true
            }
        );
    }
}
