//! A transparent `gimli::Reader` wrapper that lets callers observe three events that `gimli`
//! handles more leniently than LLVM 22.1.1 (the behavioral oracle of the `DWARF` format).
//!
//! * `gimli` ends an abbreviation table, a `.debug_ranges` list or a `.debug_rnglists` list silently
//!   at the end of the section when the explicit terminator is missing; LLVM reports an error.
//!   `gimli` signals that situation by calling `Reader::is_empty` (abbreviations) or by
//!   *not* calling `Reader::empty` (range lists) at the end of iteration.
//! * LLVM's abbreviation table stores the declaration code in 32 bits, so a larger code is
//!   ambiguous. `Reader::read_uleb128` (the 64-bit variant) is only used by `gimli` to read abbreviation
//!   codes while parsing a table, which lets the probe flag codes above `u32::MAX`.
//!
//! The wrapper delegates every byte read to `gimli::EndianSlice`; it parses nothing itself.

#![forbid(unsafe_code)]

use core::cell::Cell;
use gimli::{EndianSlice, Error, LittleEndian, Reader, ReaderOffsetId, Result};
use std::borrow::Cow;

/// Shared flags observed through [`Probe`] readers (all clones share one set of cells).
#[derive(Debug, Default)]
pub struct ProbeFlags {
    /// `is_empty` returned `true`: the parser looked for more data and found none.
    pub saw_empty: Cell<bool>,
    /// `empty` was called: the parser consumed a terminator or aborted after an error.
    pub emptied: Cell<bool>,
    /// `read_uleb128` produced a value above `u32::MAX`.
    pub wide_uleb: Cell<bool>,
}

#[derive(Clone, Debug)]
pub struct Probe<'a> {
    inner: EndianSlice<'a, LittleEndian>,
    flags: &'a ProbeFlags,
}

impl<'a> Probe<'a> {
    pub fn new(bytes: &'a [u8], flags: &'a ProbeFlags) -> Self {
        Self {
            inner: EndianSlice::new(bytes, LittleEndian),
            flags,
        }
    }
}

impl<'a> Reader for Probe<'a> {
    type Endian = LittleEndian;
    type Offset = usize;

    #[inline]
    fn endian(&self) -> LittleEndian {
        LittleEndian
    }

    #[inline]
    fn len(&self) -> usize {
        Reader::len(&self.inner)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        let empty = Reader::is_empty(&self.inner);
        if empty {
            self.flags.saw_empty.set(true);
        }
        empty
    }

    #[inline]
    fn empty(&mut self) {
        self.flags.emptied.set(true);
        Reader::empty(&mut self.inner);
    }

    #[inline]
    fn truncate(&mut self, len: usize) -> Result<()> {
        Reader::truncate(&mut self.inner, len)
    }

    #[inline]
    fn offset_from(&self, base: &Self) -> usize {
        Reader::offset_from(&self.inner, &base.inner)
    }

    #[inline]
    fn offset_id(&self) -> ReaderOffsetId {
        Reader::offset_id(&self.inner)
    }

    #[inline]
    fn lookup_offset_id(&self, id: ReaderOffsetId) -> Option<usize> {
        Reader::lookup_offset_id(&self.inner, id)
    }

    #[inline]
    fn find(&self, byte: u8) -> Result<usize> {
        Reader::find(&self.inner, byte)
    }

    #[inline]
    fn skip(&mut self, len: usize) -> Result<()> {
        Reader::skip(&mut self.inner, len)
    }

    #[inline]
    fn split(&mut self, len: usize) -> Result<Self> {
        Ok(Self {
            inner: Reader::split(&mut self.inner, len)?,
            flags: self.flags,
        })
    }

    #[inline]
    fn to_slice(&self) -> Result<Cow<'_, [u8]>> {
        Reader::to_slice(&self.inner)
    }

    #[inline]
    fn to_string(&self) -> Result<Cow<'_, str>> {
        Reader::to_string(&self.inner)
    }

    #[inline]
    fn to_string_lossy(&self) -> Result<Cow<'_, str>> {
        Reader::to_string_lossy(&self.inner)
    }

    #[inline]
    fn read_slice(&mut self, buf: &mut [u8]) -> Result<()> {
        Reader::read_slice(&mut self.inner, buf)
    }

    fn read_uleb128(&mut self) -> Result<u64> {
        let value = Reader::read_uleb128(&mut self.inner)?;
        if value > u64::from(u32::MAX) {
            self.flags.wide_uleb.set(true);
        }
        Ok(value)
    }
}

/// Converts a `gimli` failure into a short message.
pub fn describe(e: &Error) -> String {
    e.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_follow_reads() {
        let flags = ProbeFlags::default();
        let bytes = [0xffu8, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x01];
        let mut p = Probe::new(&bytes, &flags);
        assert!(!p.is_empty());
        assert_eq!(p.read_uleb128().unwrap(), (1u64 << 39) - 1);
        assert!(flags.wide_uleb.get());
        assert!(!flags.saw_empty.get());
        p.skip(1).unwrap();
        assert!(p.is_empty());
        assert!(flags.saw_empty.get());
        p.empty();
        assert!(flags.emptied.get());
    }
}
