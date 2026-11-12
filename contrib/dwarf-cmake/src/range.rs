#![forbid(unsafe_code)]

//! Address ranges (ledger section 8): `.debug_ranges` (DWARF <= 4) and `.debug_rnglists`
//! (DWARF 5) lists, and the format's own fixed 8-byte `.debug_addr` pool lookups.
//!
//! Lists are iterated raw with `gimli::RangeLists::raw_ranges` using a synthetic 8-byte address
//! size (the old parser's extractors ignore the unit's address size). The conversion to absolute
//! ranges reproduces LLVM's `getAbsoluteRanges`: it does *not* filter or normalize like
//! `gimli`'s `RangeLists::ranges`.

use crate::attribute::FormParams;
use crate::control::Work;
use crate::error::{DwarfError, Result};
use crate::file::Sections;
use crate::probe::{describe, Probe, ProbeFlags};
use gimli::{
    DebugAddrIndex, DebugRanges, DebugRngLists, Encoding, Format, RangeLists, RangeListsOffset,
    RawRngListEntry,
};

/// "Unset" marker of the old `UnitState` fields.
pub const UNSET: u64 = u64::MAX;

/// Per-unit state the range conversion depends on.
#[derive(Debug, Clone, Copy)]
pub struct RangeState {
    pub params: FormParams,
    /// `DW_AT_addr_base` seen by the compile-unit pre-pass (`UNSET` if none).
    pub debug_addr_base: u64,
    /// `DW_AT_rnglists_base` seen by the pre-pass (`UNSET` if none).
    pub rnglists_base: u64,
    /// `DW_AT_low_pc` of the last compile-unit DIE (`UNSET` if none).
    pub base_address: u64,
}

/// Old `fetchFromDebugAddr`: fixed 8-byte entries, throwing on any problem.
pub fn fetch_from_debug_addr(sections: &Sections<'_>, addr_base: u64, idx: u64) -> Result<u64> {
    let pool = sections
        .addr
        .ok_or_else(|| DwarfError::missing_aux_section("Missing .debug_addr section."))?;
    if addr_base == UNSET {
        return Err(DwarfError::dwarf("Missing DW_AT_addr_base"));
    }
    let size = pool.len() as u64;
    if addr_base > size || idx >= (size - addr_base) / 8 {
        return Err(DwarfError::dwarf(format!(
            ".debug_addr offset out of bounds: addr_base {addr_base}, idx {idx} vs {size}."
        )));
    }
    // `idx < (size - addr_base) / 8` keeps `addr_base + idx * 8 + 8 <= size`.
    let offset = (addr_base + idx * 8) as usize;
    let bytes: [u8; 8] = pool[offset..offset + 8]
        .try_into()
        .map_err(|_| DwarfError::internal("debug_addr slice"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn le_uint(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .enumerate()
        .fold(0u64, |acc, (i, b)| acc | (u64::from(*b) << (8 * i)))
}

/// Old `parseRanges`: appends the DIE's `(start, end)` ranges for a `DW_AT_ranges` value.
///
/// `value` is the raw attribute value (a section offset, or an index when `form_rnglistx`).
pub fn parse_ranges(
    st: &RangeState,
    sections: &Sections<'_>,
    value: u64,
    form_rnglistx: bool,
    out: &mut Vec<(u64, u64)>,
    work: &mut Work<'_>,
) -> Result<()> {
    let base = (st.base_address != UNSET).then_some(st.base_address);
    let flags = ProbeFlags::default();
    let no_bytes: &[u8] = &[];
    // The synthetic encoding gives raw iteration the old parser's fixed 8-byte addresses.
    let mut encoding = Encoding {
        format: Format::Dwarf32,
        version: st.params.version,
        address_size: 8,
    };

    if st.params.version < 5 {
        let section = sections
            .ranges
            .ok_or_else(|| DwarfError::missing_aux_section("Missing .debug_ranges section"))?;
        // The list must start inside the section.
        if value >= section.len() as u64 {
            return Err(DwarfError::dwarf(format!(
                "invalid range list offset 0x{value:x}"
            )));
        }
        let lists = RangeLists::new(
            DebugRanges::from(Probe::new(section, &flags)),
            DebugRngLists::from(Probe::new(no_bytes, &flags)),
        );
        encoding.version = st.params.version;
        let mut iter = lists
            .raw_ranges(RangeListsOffset(value as usize), encoding)
            .map_err(|e| {
                DwarfError::dwarf(format!(
                    "Error parsing .debug_ranges list: {}",
                    describe(&e)
                ))
            })?;
        // Entries equal to `UINT64_MAX - 1` are tombstones (`computeTombstoneAddress(8) - 1`).
        let tombstone = u64::MAX - 1;
        let mut base = base;
        loop {
            work.tick(1)?;
            match iter.next() {
                Ok(Some(RawRngListEntry::BaseAddress { addr })) => base = Some(addr),
                Ok(Some(RawRngListEntry::AddressOrOffsetPair { begin, end })) => {
                    if begin == tombstone {
                        continue;
                    }
                    let (mut low, mut high) = (begin, end);
                    if let Some(b) = base {
                        if b == tombstone {
                            continue;
                        }
                        low = low.wrapping_add(b);
                        high = high.wrapping_add(b);
                    }
                    out.push((low, high));
                }
                Ok(Some(_)) => {
                    return Err(DwarfError::internal(
                        "unexpected DWARF 5 entry in .debug_ranges",
                    ));
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(DwarfError::dwarf(format!(
                        "Error parsing .debug_ranges list: {}",
                        describe(&e)
                    )));
                }
            }
        }
        // `gimli` also stops silently at the end of the section; LLVM requires the (0, 0) entry.
        if !flags.emptied.get() {
            return Err(DwarfError::dwarf(
                "Error parsing .debug_ranges list: invalid range list entry (no end of list marker)",
            ));
        }
        return Ok(());
    }

    let section = sections
        .rnglists
        .ok_or_else(|| DwarfError::missing_aux_section("Missing .debug_rnglists section"))?;
    let section_size = section.len() as u64;
    let mut offset = value;
    if form_rnglistx {
        // Fetch the offset from the unit's offsets table.
        if st.rnglists_base == UNSET {
            return Err(DwarfError::dwarf("Missing DW_AT_rnglists_base"));
        }
        let entry_size = u64::from(st.params.offset_size);
        if entry_size == 0
            || st.rnglists_base > section_size
            || offset >= (section_size - st.rnglists_base) / entry_size
        {
            return Err(DwarfError::dwarf(format!(
                "DW_FORM_rnglistx offset out of bounds: rnglists_base {}, idx {offset} vs {section_size}",
                st.rnglists_base
            )));
        }
        let lists_offset = (st.rnglists_base + offset * entry_size) as usize;
        let list_entry = le_uint(&section[lists_offset..lists_offset + entry_size as usize]);
        // The offset read from the table is untrusted; the subtraction also stops
        // `rnglists_base + list_entry` from wrapping.
        if list_entry > section_size - st.rnglists_base {
            return Err(DwarfError::dwarf(format!(
                "DW_FORM_rnglistx points out of bounds: rnglists_base {}, entry {list_entry} vs {section_size}",
                st.rnglists_base
            )));
        }
        offset = st.rnglists_base + list_entry;
    }
    if offset >= section_size {
        return Err(DwarfError::dwarf(format!(
            "Error parsing .debug_rnglists list: invalid rnglists list offset 0x{offset:x}"
        )));
    }

    let lists = RangeLists::new(
        DebugRanges::from(Probe::new(no_bytes, &flags)),
        DebugRngLists::from(Probe::new(section, &flags)),
    );
    let mut iter = lists
        .raw_ranges(RangeListsOffset(offset as usize), encoding)
        .map_err(|e| {
            DwarfError::dwarf(format!(
                "Error parsing .debug_rnglists list: {}",
                describe(&e)
            ))
        })?;
    // `computeTombstoneAddress(8)`.
    let tombstone = u64::MAX;
    let pooled = |index: DebugAddrIndex| -> Result<u64> {
        // The lookup callback takes `uint32_t`.
        fetch_from_debug_addr(sections, st.debug_addr_base, u64::from(index.0 as u32))
    };
    let mut base = base;
    loop {
        work.tick(1)?;
        let entry = match iter.next() {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(e) => {
                return Err(DwarfError::dwarf(format!(
                    "Error parsing .debug_rnglists list: {}",
                    describe(&e)
                )));
            }
        };
        let (low, high) = match entry {
            RawRngListEntry::BaseAddress { addr } => {
                base = Some(addr);
                continue;
            }
            RawRngListEntry::BaseAddressx { addr } => {
                base = Some(pooled(addr)?);
                continue;
            }
            RawRngListEntry::OffsetPair { begin, end } => {
                if begin == tombstone {
                    continue;
                }
                match base {
                    Some(b) if b == tombstone => continue,
                    Some(b) => (begin.wrapping_add(b), end.wrapping_add(b)),
                    None => (begin, end),
                }
            }
            RawRngListEntry::StartEnd { begin, end } => (begin, end),
            RawRngListEntry::StartLength { begin, length } => (begin, begin.wrapping_add(length)),
            RawRngListEntry::StartxLength { begin, length } => {
                let low = pooled(begin)?;
                (low, low.wrapping_add(length))
            }
            RawRngListEntry::StartxEndx { begin, end } => (pooled(begin)?, pooled(end)?),
            RawRngListEntry::AddressOrOffsetPair { .. } => {
                return Err(DwarfError::internal(
                    "unexpected DWARF 4 entry in .debug_rnglists",
                ));
            }
        };
        if low == tombstone {
            continue;
        }
        out.push((low, high));
    }
    // `gimli` also stops silently at the end of the section; LLVM requires DW_RLE_end_of_list.
    if !flags.emptied.get() {
        return Err(DwarfError::dwarf(
            "Error parsing .debug_rnglists list: no end of list marker detected at end of the table",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{Control, Poll};

    struct Never;
    impl Control for Never {
        fn poll(&mut self) -> Result<Poll> {
            Ok(Poll::Continue)
        }
    }

    fn sections<'a>(
        ranges: Option<&'a [u8]>,
        rnglists: Option<&'a [u8]>,
        addr: Option<&'a [u8]>,
    ) -> Sections<'a> {
        Sections {
            info: &[],
            abbrev: &[],
            line: None,
            addr,
            addr_llvm: &[],
            ranges,
            rnglists,
            str_: &[],
            line_str: &[],
            str_offsets: &[],
        }
    }

    fn state(version: u16) -> RangeState {
        RangeState {
            params: FormParams {
                version,
                address_size: 8,
                offset_size: 4,
            },
            debug_addr_base: UNSET,
            rnglists_base: UNSET,
            base_address: UNSET,
        }
    }

    fn run(
        st: &RangeState,
        s: &Sections<'_>,
        value: u64,
        rnglistx: bool,
    ) -> Result<Vec<(u64, u64)>> {
        let mut ctl = Never;
        let mut work = Work::new(&mut ctl);
        let mut out = Vec::new();
        parse_ranges(st, s, value, rnglistx, &mut out, &mut work)?;
        Ok(out)
    }

    fn pair(a: u64, b: u64) -> Vec<u8> {
        let mut v = a.to_le_bytes().to_vec();
        v.extend_from_slice(&b.to_le_bytes());
        v
    }

    #[test]
    fn debug_ranges_base_selection_tombstone_and_wrap() {
        let mut sec = Vec::new();
        sec.extend(pair(0x10, 0x20)); // relative to unit base 0x1000
        sec.extend(pair(u64::MAX, 0x5000)); // base address selection
        sec.extend(pair(0x1, 0x3)); // relative to 0x5000
        sec.extend(pair(u64::MAX - 1, 0x9)); // tombstone start: dropped
        sec.extend(pair(u64::MAX - 2, 0x9)); // wraps with the base
        sec.extend(pair(0, 0)); // end of list
        let mut st = state(4);
        st.base_address = 0x1000;
        let s = sections(Some(&sec), None, None);
        let got = run(&st, &s, 0, false).unwrap();
        assert_eq!(
            got,
            vec![
                (0x1010, 0x1020),
                (0x5001, 0x5003),
                (
                    (u64::MAX - 2).wrapping_add(0x5000),
                    9u64.wrapping_add(0x5000)
                ),
            ]
        );
        // Without a unit base address entries are not offset.
        st.base_address = UNSET;
        assert_eq!(run(&st, &s, 0, false).unwrap()[0], (0x10, 0x20));
    }

    #[test]
    fn debug_ranges_requires_terminator_and_valid_offset() {
        let sec = pair(1, 2);
        let s = sections(Some(&sec), None, None);
        assert!(run(&state(4), &s, 0, false).is_err());
        assert!(run(&state(4), &s, 16, false).is_err());
        let sec2 = [pair(1, 2), vec![0u8; 15]].concat();
        let s2 = sections(Some(&sec2), None, None);
        assert!(run(&state(4), &s2, 0, false).is_err());
        // Missing section.
        assert!(run(&state(4), &sections(None, None, None), 0, false).is_err());
    }

    #[test]
    fn rnglists_entry_kinds() {
        let mut sec = vec![];
        sec.push(5); // base_address
        sec.extend(0x4000u64.to_le_bytes());
        sec.push(4); // offset_pair 1..2 => 0x4001..0x4002
        sec.extend([1, 2]);
        sec.push(6); // start_end
        sec.extend(pair(0x100, 0x200));
        sec.push(7); // start_length
        sec.extend(0x300u64.to_le_bytes());
        sec.push(0x10);
        sec.push(6); // start_end with the tombstone start: dropped
        sec.extend(pair(u64::MAX, 0));
        sec.push(0); // end_of_list
        let st = state(5);
        let s = sections(None, Some(&sec), None);
        assert_eq!(
            run(&st, &s, 0, false).unwrap(),
            vec![(0x4001, 0x4002), (0x100, 0x200), (0x300, 0x310)]
        );
        // Truncated list: end of section before DW_RLE_end_of_list.
        assert!(run(
            &st,
            &sections(None, Some(&sec[..sec.len() - 1]), None),
            0,
            false
        )
        .is_err());
        // Offset outside the section.
        assert!(run(&st, &s, sec.len() as u64, false).is_err());
        // Unknown entry kind.
        assert!(run(&st, &sections(None, Some(&[0x42, 0]), None), 0, false).is_err());
    }

    #[test]
    fn rnglists_indexed_entries_use_the_8_byte_addr_pool() {
        let mut pool = Vec::new();
        for a in [0x1000u64, 0x2000, 0x3000] {
            pool.extend(a.to_le_bytes());
        }
        let sec = vec![
            1, 1, // base_addressx 1
            4, 0x10, 0x20, // offset_pair
            2, 0, 2, // startx_endx 0..2
            3, 1, 5, // startx_length 1, 5
            0,
        ];
        let mut st = state(5);
        st.debug_addr_base = 0;
        let s = sections(None, Some(&sec), Some(&pool));
        assert_eq!(
            run(&st, &s, 0, false).unwrap(),
            vec![(0x2010, 0x2020), (0x1000, 0x3000), (0x2000, 0x2005)]
        );
        // No base / out-of-range index are errors, never silently skipped.
        st.debug_addr_base = UNSET;
        assert!(run(&st, &s, 0, false).is_err());
        st.debug_addr_base = 16;
        assert!(run(&st, &s, 0, false).is_err());
    }

    #[test]
    fn rnglistx_indirection_is_checked() {
        // Header-less layout: base 0, one 4-byte entry pointing at offset 4.
        let mut sec = vec![];
        sec.extend(4u32.to_le_bytes());
        sec.push(6);
        sec.extend(pair(1, 2));
        sec.push(0);
        let mut st = state(5);
        st.rnglists_base = 0;
        let s = sections(None, Some(&sec), None);
        assert_eq!(run(&st, &s, 0, true).unwrap(), vec![(1, 2)]);
        assert!(run(&st, &s, 5, true).is_err());
        st.rnglists_base = UNSET;
        assert!(run(&st, &s, 0, true).is_err());
        // Entry beyond the section.
        let mut bad = sec.clone();
        bad[0] = 0xff;
        st.rnglists_base = 0;
        assert!(run(&st, &sections(None, Some(&bad), None), 0, true).is_err());
    }

    #[test]
    fn fetch_from_debug_addr_bounds() {
        let pool = [1u8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
        let s = sections(None, None, Some(&pool));
        assert_eq!(fetch_from_debug_addr(&s, 0, 1).unwrap(), 2);
        assert!(fetch_from_debug_addr(&s, 0, 2).is_err());
        assert!(fetch_from_debug_addr(&s, 17, 0).is_err());
        assert!(fetch_from_debug_addr(&s, UNSET, 0).is_err());
        assert!(fetch_from_debug_addr(&sections(None, None, None), 0, 0).is_err());
    }
}
