#![allow(unsafe_code)]
//! Caller-owned output bytes that may not be initialized.
//!
//! The C++ caller sizes its columns with `PODArray::resize`, which makes the bytes of the new elements part
//! of the array without writing them. The executor therefore must never form a `&mut [u8]` (or read) over a
//! buffer it has not written yet: that is undefined behavior for uninitialized bytes. [`OutBuf`] holds the
//! buffer as `&mut [MaybeUninit<u8>]` and offers writes only, so no code path of the executor can observe an
//! unwritten byte, and the same type serves a buffer that is fully initialized (the tests') at no cost.

use std::mem::MaybeUninit;
use std::ops::Range;

/// A buffer the executor writes to. The bytes may be uninitialized; it is never read.
///
/// The only way to put bytes into it from outside the crate is [`OutBuf::put`], which writes initialized
/// bytes. That is what makes `From<&mut [u8]>` sound: the caller keeps its initialized slice and the
/// executor can only leave it initialized.
pub struct OutBuf<'a>(&'a mut [MaybeUninit<u8>]);

impl<'a> OutBuf<'a> {
    /// A buffer of no bytes.
    pub fn empty() -> OutBuf<'a> {
        OutBuf(&mut [])
    }

    /// Size of the buffer in bytes.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Writes `src` at byte `at`. Panics if it does not fit; the executor checks the room before it writes.
    #[inline(always)]
    pub fn put(&mut self, at: usize, src: &[u8]) {
        self.0[at..at + src.len()].write_copy_of_slice(src);
    }

    /// Writes `b` at byte `at`. Panics if it does not fit.
    #[inline(always)]
    pub fn put_byte(&mut self, at: usize, b: u8) {
        self.0[at].write(b);
    }

    /// The bytes `range` to be written, for the writers that fill them one by one. Not public: a caller
    /// could store an uninitialized byte through it into a buffer that came from an initialized slice.
    #[inline(always)]
    pub(crate) fn slot(&mut self, range: Range<usize>) -> &mut [MaybeUninit<u8>] {
        &mut self.0[range]
    }
}

impl<'a> From<&'a mut [u8]> for OutBuf<'a> {
    #[inline]
    fn from(data: &'a mut [u8]) -> OutBuf<'a> {
        // SAFETY: `MaybeUninit<u8>` has the size, alignment and validity of `u8` plus the uninitialized
        // state, so every initialized byte is a valid `MaybeUninit<u8>`. The new reference is the only
        // access to the bytes for `'a`, and `OutBuf` writes initialized bytes only (`put`, `put_byte` and
        // the crate-private `slot`, whose users initialize everything they were given), so when `'a` ends
        // the bytes are still initialized as `data` promises.
        OutBuf(unsafe { &mut *(data as *mut [u8] as *mut [MaybeUninit<u8>]) })
    }
}

impl<'a> From<&'a mut [MaybeUninit<u8>]> for OutBuf<'a> {
    #[inline]
    fn from(data: &'a mut [MaybeUninit<u8>]) -> OutBuf<'a> {
        OutBuf(data)
    }
}
