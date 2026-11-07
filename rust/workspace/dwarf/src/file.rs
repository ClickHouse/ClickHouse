#![forbid(unsafe_code)]

//! ELF64 container, debug-section selection and the `.debug_info` unit index.
//!
//! Section selection deliberately reproduces two different lookup rules of the old parser
//! (ledger E-10, E-13):
//!
//! * the format's own lookups (`.debug_info` walk, `.debug_line` prologue, `.debug_addr` pool for
//!   ranges, `.debug_ranges`, `.debug_rnglists`) use the exact dotted name, first match wins;
//! * LLVM's `DWARFContext` section map strips one leading dot and keeps the first section per
//!   stripped key (`StringMap::try_emplace`). LLVM supplies `.debug_abbrev`, `.debug_str`,
//!   `.debug_line_str`, `.debug_str_offsets`, and the `.debug_addr` pool used by indexed
//!   `attr_int` values.

use crate::error::{DwarfError, Result};
use crate::probe::{describe, Probe, ProbeFlags};
use gimli::{Abbreviations, DebugAbbrev, DebugAbbrevOffset, DebugInfo, Encoding, LittleEndian};
use object::elf;
use object::read::elf::{FileHeader as _, SectionHeader as _};
use object::LittleEndian as ObjLe;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT_FILE_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionRange {
    pub offset: usize,
    pub len: usize,
}

impl SectionRange {
    #[inline]
    pub fn slice<'a>(&self, file_bytes: &'a [u8]) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(self.len)
            .ok_or_else(|| DwarfError::internal("section range overflow"))?;
        file_bytes
            .get(self.offset..end)
            .ok_or_else(|| DwarfError::internal("section range out of bounds"))
    }
}

#[derive(Debug, Clone)]
pub struct UnitDescriptor {
    pub unit_offset: u64,
    pub header_size: u64,
    pub end_offset: u64,
    pub encoding: Encoding,
    pub abbrevs: Arc<Abbreviations>,
}

/// Borrowed views of every debug section, valid for one call only.
#[derive(Debug, Clone, Copy)]
pub struct Sections<'a> {
    /// `.debug_info`, exact dotted name.
    pub info: &'a [u8],
    /// `.debug_line`, exact dotted name.
    pub line: Option<&'a [u8]>,
    /// `.debug_addr`, exact dotted name (the format's own fixed 8-byte pool lookups).
    pub addr: Option<&'a [u8]>,
    /// `.debug_addr` as seen by LLVM's section map (indexed `attr_int` values); empty if absent.
    pub addr_llvm: &'a [u8],
    /// `.debug_ranges`, exact dotted name.
    pub ranges: Option<&'a [u8]>,
    /// `.debug_rnglists`, exact dotted name.
    pub rnglists: Option<&'a [u8]>,
    /// LLVM-map `.debug_str`; empty if absent.
    pub str_: &'a [u8],
    /// LLVM-map `.debug_line_str`; empty if absent.
    pub line_str: &'a [u8],
    /// LLVM-map `.debug_str_offsets`; empty if absent.
    pub str_offsets: &'a [u8],
}

#[derive(Debug)]
pub struct FileInner {
    pub generation: u64,
    pub raw_ptr: usize,
    pub raw_len: usize,
    pub debug_info: SectionRange,
    pub debug_abbrev: SectionRange,
    pub debug_line: Option<SectionRange>,
    pub debug_addr: Option<SectionRange>,
    pub debug_addr_llvm: Option<SectionRange>,
    pub debug_ranges: Option<SectionRange>,
    pub debug_rnglists: Option<SectionRange>,
    pub debug_str: Option<SectionRange>,
    pub debug_line_str: Option<SectionRange>,
    pub debug_str_offsets: Option<SectionRange>,
    pub units: Vec<UnitDescriptor>,
}

#[inline]
fn range_within(offset: u64, length: u64, total: usize) -> Option<(usize, usize)> {
    let total_u64 = total as u64;
    if offset <= total_u64 && length <= total_u64 - offset {
        Some((offset as usize, length as usize))
    } else {
        None
    }
}

/// Parses the abbreviation table at `abbrev_offset` with `gimli`, then applies the LLVM 22.1.1
/// acceptance rules that `gimli` is more lenient about (ledger U-07, U-11, U-14).
fn parse_abbreviations(abbrev_bytes: &[u8], abbrev_offset: u64) -> Result<Arc<Abbreviations>> {
    let off = usize::try_from(abbrev_offset)
        .map_err(|_| DwarfError::dwarf("abbreviation offset overflows usize"))?;
    if off >= abbrev_bytes.len() {
        return Err(DwarfError::dwarf(format!(
            "abbreviation table offset {abbrev_offset} is outside .debug_abbrev"
        )));
    }
    let flags = ProbeFlags::default();
    let debug_abbrev = DebugAbbrev::from(Probe::new(abbrev_bytes, &flags));
    let parsed = debug_abbrev
        .abbreviations(DebugAbbrevOffset(off))
        .map_err(|e| {
            DwarfError::dwarf(format!("malformed abbreviation table: {}", describe(&e)))
        })?;
    if flags.saw_empty.get() {
        return Err(DwarfError::dwarf(
            "abbreviation table is not terminated by a zero code before the end of .debug_abbrev",
        ));
    }
    if flags.wide_uleb.get() {
        return Err(DwarfError::dwarf("abbreviation code exceeds UINT32_MAX"));
    }
    Ok(Arc::new(parsed))
}

/// Name -> first section in header order, by the two rules described in the module docs.
#[derive(Default)]
struct SectionTable {
    // Format-owned lookups: exact dotted names.
    info: Option<(usize, SectionRange)>,
    abbrev_present: bool,
    line: Option<SectionRange>,
    addr: Option<SectionRange>,
    ranges: Option<SectionRange>,
    rnglists: Option<SectionRange>,
    // LLVM section map: one leading dot stripped, first wins.
    llvm_info: Option<(usize, SectionRange)>,
    llvm_abbrev: Option<SectionRange>,
    llvm_addr: Option<SectionRange>,
    llvm_str: Option<SectionRange>,
    llvm_line_str: Option<SectionRange>,
    llvm_str_offsets: Option<SectionRange>,
}

impl SectionTable {
    fn add(&mut self, idx: usize, name: &[u8], range: SectionRange) {
        let first = |slot: &mut Option<SectionRange>| {
            if slot.is_none() {
                *slot = Some(range);
            }
        };
        match name {
            b".debug_info" => {
                if self.info.is_none() {
                    self.info = Some((idx, range));
                }
            }
            b".debug_abbrev" => self.abbrev_present = true,
            b".debug_line" => first(&mut self.line),
            b".debug_addr" => first(&mut self.addr),
            b".debug_ranges" => first(&mut self.ranges),
            b".debug_rnglists" => first(&mut self.rnglists),
            _ => {}
        }
        let key = name.strip_prefix(b".").unwrap_or(name);
        match key {
            b"debug_info" => {
                if self.llvm_info.is_none() {
                    self.llvm_info = Some((idx, range));
                }
            }
            b"debug_abbrev" => first(&mut self.llvm_abbrev),
            b"debug_addr" => first(&mut self.llvm_addr),
            b"debug_str" => first(&mut self.llvm_str),
            b"debug_line_str" => first(&mut self.llvm_line_str),
            b"debug_str_offsets" => first(&mut self.llvm_str_offsets),
            _ => {}
        }
    }
}

impl FileInner {
    /// `raw_ptr` is the address of `bytes`; it is recorded so later calls can prove they were given
    /// the same input.
    pub fn open(bytes: &[u8], raw_ptr: usize) -> Result<Arc<Self>> {
        let table = Self::parse_elf(bytes)?;

        let (info_idx, debug_info) = table
            .info
            .ok_or_else(|| DwarfError::missing_required_section("No .debug_info section"))?;
        if !table.abbrev_present {
            return Err(DwarfError::missing_required_section(
                "No .debug_abbrev section",
            ));
        }
        let debug_abbrev = table
            .llvm_abbrev
            .ok_or_else(|| DwarfError::internal("section map lost .debug_abbrev"))?;
        // LLVM enumerates units in the section it keys as `debug_info` while the format walks the
        // dotted `.debug_info`; they only differ in a file with a second, dotless `debug_info`
        // section placed first. Offsets from one would index the other, so refuse (ledger E-13).
        match table.llvm_info {
            Some((idx, _)) if idx == info_idx => {}
            _ => {
                return Err(DwarfError::elf(
                    "ambiguous debug_info sections: `debug_info` and `.debug_info` name different sections",
                ));
            }
        }

        let info_bytes = debug_info.slice(bytes)?;
        let abbrev_bytes = debug_abbrev.slice(bytes)?;

        let mut abbrev_cache: BTreeMap<u64, Arc<Abbreviations>> = BTreeMap::new();
        let mut units = Vec::new();
        let mut headers = DebugInfo::new(info_bytes, LittleEndian).units();
        while let Some(header) = headers.next().map_err(|e| {
            DwarfError::dwarf(format!("malformed DWARF unit header: {}", describe(&e)))
        })? {
            let encoding = header.encoding();
            if !matches!(encoding.address_size, 2 | 4 | 8) {
                return Err(DwarfError::dwarf(format!(
                    "unsupported DWARF unit address size {}",
                    encoding.address_size
                )));
            }
            let unit_offset = header
                .debug_info_offset()
                .ok_or_else(|| DwarfError::internal("unit outside .debug_info"))?
                .0 as u64;
            let header_size = header.header_size() as u64;
            let total_len = header.length_including_self() as u64;
            let end_offset = unit_offset
                .checked_add(total_len)
                .ok_or_else(|| DwarfError::dwarf("unit end offset overflow"))?;
            if header_size >= total_len {
                return Err(DwarfError::dwarf("DWARF unit has no DIE payload bytes"));
            }
            if let gimli::UnitType::Type { type_offset, .. }
            | gimli::UnitType::SplitType { type_offset, .. } = header.type_()
            {
                let toff = type_offset.0 as u64;
                if toff < header_size || toff >= total_len {
                    return Err(DwarfError::dwarf(
                        "DWARF type unit type_offset out of bounds",
                    ));
                }
            }

            let abbrev_offset = header.debug_abbrev_offset().0 as u64;
            let abbrevs = match abbrev_cache.get(&abbrev_offset) {
                Some(cached) => Arc::clone(cached),
                None => {
                    let parsed = parse_abbreviations(abbrev_bytes, abbrev_offset)?;
                    abbrev_cache.insert(abbrev_offset, Arc::clone(&parsed));
                    parsed
                }
            };

            units.push(UnitDescriptor {
                unit_offset,
                header_size,
                end_offset,
                encoding,
                abbrevs,
            });
        }

        Ok(Arc::new(Self {
            generation: NEXT_FILE_GENERATION.fetch_add(1, Ordering::Relaxed),
            raw_ptr,
            raw_len: bytes.len(),
            debug_info,
            debug_abbrev,
            debug_line: table.line,
            debug_addr: table.addr,
            debug_addr_llvm: table.llvm_addr,
            debug_ranges: table.ranges,
            debug_rnglists: table.rnglists,
            debug_str: table.llvm_str,
            debug_line_str: table.llvm_line_str,
            debug_str_offsets: table.llvm_str_offsets,
            units,
        }))
    }

    /// Views of every section inside `bytes`, which must be the span given to [`FileInner::open`].
    pub fn sections<'a>(&self, bytes: &'a [u8]) -> Result<Sections<'a>> {
        if bytes.len() != self.raw_len {
            return Err(DwarfError::invalid_arg("input length changed since open"));
        }
        let opt = |r: &Option<SectionRange>| -> Result<Option<&'a [u8]>> {
            r.map(|r| r.slice(bytes)).transpose()
        };
        let or_empty =
            |r: &Option<SectionRange>| -> Result<&'a [u8]> { Ok(opt(r)?.unwrap_or(&[])) };
        Ok(Sections {
            info: self.debug_info.slice(bytes)?,
            line: opt(&self.debug_line)?,
            addr: opt(&self.debug_addr)?,
            addr_llvm: or_empty(&self.debug_addr_llvm)?,
            ranges: opt(&self.debug_ranges)?,
            rnglists: opt(&self.debug_rnglists)?,
            str_: or_empty(&self.debug_str)?,
            line_str: or_empty(&self.debug_line_str)?,
            str_offsets: or_empty(&self.debug_str_offsets)?,
        })
    }

    fn parse_elf(bytes: &[u8]) -> Result<SectionTable> {
        if bytes.len() < 64 {
            return Err(DwarfError::elf(
                "ELF file is smaller than 64-byte ELF64 header",
            ));
        }
        if &bytes[0..4] != b"\x7fELF" {
            return Err(DwarfError::elf("invalid ELF magic"));
        }
        if bytes[4] != elf::ELFCLASS64
            || bytes[5] != elf::ELFDATA2LSB
            || bytes[6] != elf::EV_CURRENT
        {
            return Err(DwarfError::elf(
                "unsupported ELF ident (expected ELF64 little-endian EV_CURRENT)",
            ));
        }

        let header = elf::FileHeader64::<ObjLe>::parse(bytes)
            .map_err(|e| DwarfError::elf(format!("failed to parse ELF64 header: {e}")))?;
        if header.e_version(ObjLe) != u32::from(elf::EV_CURRENT) || header.e_ehsize(ObjLe) != 64 {
            return Err(DwarfError::elf("invalid ELF64 version or ehsize"));
        }

        let shoff = header.e_shoff(ObjLe);
        let shnum = header.e_shnum(ObjLe);
        let shentsize = header.e_shentsize(ObjLe);
        let shstrndx = header.e_shstrndx(ObjLe);
        if shoff == 0
            || shnum == 0
            || shnum >= elf::SHN_LORESERVE
            || shstrndx == elf::SHN_XINDEX
            || shentsize != 64
        {
            return Err(DwarfError::elf(
                "missing or invalid ELF64 section header table",
            ));
        }
        let sh_table_bytes = u64::from(shnum) * 64;
        if range_within(shoff, sh_table_bytes, bytes.len()).is_none() {
            return Err(DwarfError::elf(
                "ELF section header table extends past end of file",
            ));
        }
        if shstrndx >= shnum {
            return Err(DwarfError::elf("ELF e_shstrndx is out of bounds"));
        }

        let sections: &[elf::SectionHeader64<ObjLe>] =
            object::ReadRef::read_slice_at(bytes, shoff, usize::from(shnum))
                .map_err(|_| DwarfError::elf("invalid ELF64 section header table"))?;

        let shstr_sec = &sections[usize::from(shstrndx)];
        if shstr_sec.sh_type(ObjLe) != elf::SHT_STRTAB {
            return Err(DwarfError::elf("ELF shstrndx section is not SHT_STRTAB"));
        }
        let (shstr_off, shstr_len) = range_within(
            shstr_sec.sh_offset(ObjLe),
            shstr_sec.sh_size(ObjLe),
            bytes.len(),
        )
        .ok_or_else(|| DwarfError::elf("ELF section-name string table extends past end of file"))?;
        let shstrtab = &bytes[shstr_off..shstr_off + shstr_len];
        if shstrtab.last() != Some(&0) {
            return Err(DwarfError::elf(
                "ELF section-name string table is empty or not NUL-terminated",
            ));
        }

        // `DB::Elf` requires a non-empty, in-bounds program header table.
        let phoff = header.e_phoff(ObjLe);
        let phnum = header.e_phnum(ObjLe);
        if phoff == 0 || phnum == 0 || phnum == elf::PN_XNUM || header.e_phentsize(ObjLe) != 56 {
            return Err(DwarfError::elf(
                "missing or invalid ELF64 program header table",
            ));
        }
        if range_within(phoff, u64::from(phnum) * 56, bytes.len()).is_none() {
            return Err(DwarfError::elf(
                "ELF program header table extends past end of file",
            ));
        }

        let mut table = SectionTable::default();
        for (idx, sec) in sections.iter().enumerate() {
            let name_off = usize::try_from(sec.sh_name(ObjLe))
                .map_err(|_| DwarfError::elf("section name offset overflow"))?;
            let name_tail = shstrtab
                .get(name_off..)
                .ok_or_else(|| DwarfError::elf("section name offset is out of bounds"))?;
            let nul = name_tail
                .iter()
                .position(|&b| b == 0)
                .ok_or_else(|| DwarfError::elf("section name is not NUL-terminated"))?;
            let name = &name_tail[..nul];

            // NOBITS sections occupy no file space; treated as absent (ledger E-11).
            if sec.sh_type(ObjLe) == elf::SHT_NOBITS {
                continue;
            }
            let (offset, len) = range_within(sec.sh_offset(ObjLe), sec.sh_size(ObjLe), bytes.len())
                .ok_or_else(|| DwarfError::elf("ELF section extends past end of file"))?;
            table.add(idx, name, SectionRange { offset, len });
        }
        Ok(table)
    }
}
