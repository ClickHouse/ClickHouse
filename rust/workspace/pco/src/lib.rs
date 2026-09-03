//! C FFI over the `pco` (pcodec) crate for the ClickHouse `PCO` compression
//! codec.
//!
//! The heavy lifting lives in the `pco` crate (patched in the ClickHouse fork of
//! pcodec to add runtime CPU-feature dispatch of its hot loops). This wrapper
//! only bridges to C: it reinterprets ClickHouse's raw byte buffers as typed
//! number slices, dispatches on a pcodec number-type byte, and translates
//! results and panics into simple status codes.
//!
//! The produced payload is a standalone `.pco` stream, wire-compatible with the
//! reference pcodec implementation.

use std::io::{self, Write};
use std::panic::{self, AssertUnwindSafe};
use std::ptr;
use std::slice;

use pco::data_types::Number;
use pco::ChunkConfig;

// Status codes; keep in sync with `PcoStatus` in include/pco.h.
const PCO_OK: i32 = 0;
const PCO_WONT_FIT: i32 = 1;
const PCO_ERROR: i32 = -1;

/// A `Write` sink over a fixed-capacity buffer that refuses to grow: once the
/// buffer is exhausted it records the overflow and returns an error instead of
/// writing partial data. Lets `pco` stream straight into ClickHouse's
/// destination buffer while giving us a clean "did not fit" signal.
struct SliceWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
    overflowed: &'a std::cell::Cell<bool>,
}

impl Write for SliceWriter<'_> {
    #[inline]
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let end = match self.pos.checked_add(data.len()) {
            Some(end) if end <= self.buf.len() => end,
            _ => {
                self.overflowed.set(true);
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
        };
        self.buf[self.pos..end].copy_from_slice(data);
        self.pos = end;
        Ok(data.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Copies `n` unaligned `T` values out of a raw byte pointer into an owned,
/// properly aligned `Vec<T>`. ClickHouse substream buffers are not guaranteed to
/// be aligned to the element type.
unsafe fn read_unaligned_vec<T: Copy>(src: *const u8, n: usize) -> Vec<T> {
    let mut v: Vec<T> = Vec::with_capacity(n);
    ptr::copy_nonoverlapping(src, v.as_mut_ptr() as *mut u8, n * size_of::<T>());
    v.set_len(n);
    v
}

/// ClickHouse serializes numbers as little-endian bytes regardless of the host
/// architecture (see `SerializationNumber`), while `pco` operates on
/// native-endian values, so on a big-endian host every element's bytes must be
/// reversed when crossing this boundary — in both directions, which one helper
/// covers because byte reversal is its own inverse. On little-endian hosts it
/// is a no-op the compiler removes.
fn convert_le_bytes_in_place<T>(values: &mut [T]) {
    if cfg!(target_endian = "big") {
        let bytes = unsafe {
            slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, values.len() * size_of::<T>())
        };
        for element in bytes.chunks_exact_mut(size_of::<T>()) {
            element.reverse();
        }
    }
}

unsafe fn compress_typed<T: Number>(
    src: *const u8,
    n_values: usize,
    level: usize,
    enable_8_bit: bool,
    dst: *mut u8,
    dst_capacity: usize,
    out_size: *mut u64,
) -> i32 {
    // On a little-endian host an aligned source is borrowed in place; an
    // unaligned source, or any source on a big-endian host (which must
    // byte-swap the little-endian input), is copied into an owned aligned
    // buffer first.
    let owned: Vec<T>;
    let nums: &[T] = if cfg!(target_endian = "little") && (src as usize) % align_of::<T>() == 0 {
        slice::from_raw_parts(src as *const T, n_values)
    } else {
        let mut copied = read_unaligned_vec::<T>(src, n_values);
        convert_le_bytes_in_place(&mut copied);
        owned = copied;
        &owned
    };

    let config = ChunkConfig::default()
        .with_compression_level(level)
        .with_enable_8_bit(enable_8_bit);

    let overflowed = std::cell::Cell::new(false);
    let writer = SliceWriter {
        buf: slice::from_raw_parts_mut(dst, dst_capacity),
        pos: 0,
        overflowed: &overflowed,
    };

    match pco::standalone::simple_compress_into(nums, &config, writer) {
        Ok(writer) => {
            *out_size = writer.pos as u64;
            PCO_OK
        }
        Err(_) if overflowed.get() => PCO_WONT_FIT,
        Err(_) => PCO_ERROR,
    }
}

/// Decodes the chunks of an already-opened standalone stream into `dst`,
/// requiring that the stream carries exactly `dst.len()` values and that
/// nothing follows the termination marker.
/// `pco::standalone::simple_decompress_into` cannot prove the latter: it stops
/// at `DecompressorItem::EndOfData(rest)` and discards `rest`, so a valid
/// stream with trailing garbage appended would still decode. Driving
/// `FileDecompressor` directly keeps `rest` in hand and lets us fail closed on
/// trailing bytes, on a short stream, and on excess values alike. The caller
/// passes the `FileDecompressor` in (with `src` positioned right after the
/// header) so the stream header is parsed once, shared with the number-type
/// peek, instead of reparsed per decode.
fn decompress_exact<T: Number>(
    file_decompressor: &pco::standalone::FileDecompressor,
    mut src: &[u8],
    mut dst: &mut [T],
) -> Result<(), ()> {
    loop {
        match file_decompressor.chunk_decompressor::<T, _>(src).map_err(|_| ())? {
            pco::standalone::DecompressorItem::Chunk(mut chunk_decompressor) => {
                let n = chunk_decompressor.n();
                // More values than expected: mismatched block, fail closed
                // without decoding past the destination.
                if n > dst.len() {
                    return Err(());
                }
                // `dst[..n]` covers the whole chunk, satisfying `read`'s
                // "at least the numbers remaining in the chunk" contract.
                let progress = chunk_decompressor.read(&mut dst[..n]).map_err(|_| ())?;
                if !progress.finished || progress.n_processed != n {
                    return Err(());
                }
                dst = &mut dst[n..];
                src = chunk_decompressor.into_src();
            }
            pco::standalone::DecompressorItem::EndOfData(rest) => {
                // Fewer values than expected, or bytes after the termination
                // marker: fail closed.
                return if dst.is_empty() && rest.is_empty() { Ok(()) } else { Err(()) };
            }
        }
    }
}

unsafe fn decompress_typed<T: Number>(
    file_decompressor: &pco::standalone::FileDecompressor,
    src: &[u8],
    dst: *mut u8,
    n_values: usize,
    dst_capacity: usize,
) -> i32 {
    // The destination must be able to hold the requested values.
    let needed = match n_values.checked_mul(size_of::<T>()) {
        Some(needed) if needed <= dst_capacity => needed,
        _ => return PCO_ERROR,
    };

    if dst as usize % align_of::<T>() == 0 {
        // Aligned: decode straight into the destination.
        let out = slice::from_raw_parts_mut(dst as *mut T, n_values);
        if decompress_exact(file_decompressor, src, out).is_err() {
            return PCO_ERROR;
        }
        convert_le_bytes_in_place(out);
    } else {
        // Unaligned destination: decode into an aligned scratch, then copy back.
        let mut scratch = vec![T::default(); n_values];
        if decompress_exact(file_decompressor, src, &mut scratch).is_err() {
            return PCO_ERROR;
        }
        convert_le_bytes_in_place(&mut scratch);
        ptr::copy_nonoverlapping(scratch.as_ptr() as *const u8, dst, needed);
    }

    PCO_OK
}

/// See include/pco.h.
#[no_mangle]
pub unsafe extern "C" fn pco_compress(
    number_type: u8,
    src: *const u8,
    n_values: u64,
    compression_level: i32,
    dst: *mut u8,
    dst_capacity: u64,
    out_size: *mut u64,
) -> i32 {
    if src.is_null() || dst.is_null() || out_size.is_null() {
        return PCO_ERROR;
    }
    if compression_level < 0 {
        return PCO_ERROR;
    }

    let n = n_values as usize;
    let level = compression_level as usize;
    let cap = dst_capacity as usize;

    let result = panic::catch_unwind(AssertUnwindSafe(|| match number_type {
        1 => compress_typed::<u32>(src, n, level, false, dst, cap, out_size),
        2 => compress_typed::<u64>(src, n, level, false, dst, cap, out_size),
        3 => compress_typed::<i32>(src, n, level, false, dst, cap, out_size),
        4 => compress_typed::<i64>(src, n, level, false, dst, cap, out_size),
        5 => compress_typed::<f32>(src, n, level, false, dst, cap, out_size),
        6 => compress_typed::<f64>(src, n, level, false, dst, cap, out_size),
        7 => compress_typed::<u16>(src, n, level, false, dst, cap, out_size),
        8 => compress_typed::<i16>(src, n, level, false, dst, cap, out_size),
        9 => compress_typed::<half::f16>(src, n, level, false, dst, cap, out_size),
        10 => compress_typed::<u8>(src, n, level, true, dst, cap, out_size),
        11 => compress_typed::<i8>(src, n, level, true, dst, cap, out_size),
        _ => PCO_ERROR,
    }));

    result.unwrap_or(PCO_ERROR)
}

/// See include/pco.h.
#[no_mangle]
pub unsafe extern "C" fn pco_decompress(
    element_width: u32,
    expected_number_type: u8,
    src: *const u8,
    src_size: u64,
    dst: *mut u8,
    n_values: u64,
    dst_capacity: u64,
) -> i32 {
    if src.is_null() || dst.is_null() {
        return PCO_ERROR;
    }

    let n = n_values as usize;
    let cap = dst_capacity as usize;
    let width = element_width;

    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let source = slice::from_raw_parts(src, src_size as usize);

        // Parse the stream header exactly once: the same `FileDecompressor`
        // (with `rest` positioned right after the header) serves both the
        // number-type peek here and the chunk decode below.
        let Ok((file_decompressor, rest)) = pco::standalone::FileDecompressor::new(source) else {
            return PCO_ERROR;
        };

        // Decode using the stream's own number type, and require its element
        // width to match the width declared by the codec block. This is the
        // only type information available on the untyped method-byte decode
        // path (e.g. HTTP decompress=1), and it makes a malformed block whose
        // declared width disagrees with the embedded stream fail closed.
        let number_type = match file_decompressor.peek_number_type_or_termination(rest) {
            Ok(Some(number_type)) => number_type as u8,
            // A well-formed empty stream ends right at the termination byte —
            // only valid when nothing was expected. Any trailing payload bytes
            // mean corruption rather than emptiness: fail closed even when
            // n == 0.
            Ok(None) if rest.len() == 1 => return if n == 0 { PCO_OK } else { PCO_ERROR },
            // Malformed or truncated header, or trailing garbage.
            _ => return PCO_ERROR,
        };

        // When the caller knows the exact number type the stream must carry (a
        // codec instance created from a concrete column type), a stream whose
        // embedded type merely shares the width (e.g. an `i32` stream for a
        // `u32` column) is corrupt or mismatched: fail closed instead of
        // reinterpreting the values.
        if expected_number_type != 0 && number_type != expected_number_type {
            return PCO_ERROR;
        }

        let fd = &file_decompressor;
        match number_type {
            1 if width == 4 => decompress_typed::<u32>(fd, rest, dst, n, cap),
            2 if width == 8 => decompress_typed::<u64>(fd, rest, dst, n, cap),
            3 if width == 4 => decompress_typed::<i32>(fd, rest, dst, n, cap),
            4 if width == 8 => decompress_typed::<i64>(fd, rest, dst, n, cap),
            5 if width == 4 => decompress_typed::<f32>(fd, rest, dst, n, cap),
            6 if width == 8 => decompress_typed::<f64>(fd, rest, dst, n, cap),
            7 if width == 2 => decompress_typed::<u16>(fd, rest, dst, n, cap),
            8 if width == 2 => decompress_typed::<i16>(fd, rest, dst, n, cap),
            9 if width == 2 => decompress_typed::<half::f16>(fd, rest, dst, n, cap),
            10 if width == 1 => decompress_typed::<u8>(fd, rest, dst, n, cap),
            11 if width == 1 => decompress_typed::<i8>(fd, rest, dst, n, cap),
            _ => PCO_ERROR,
        }
    }));

    result.unwrap_or(PCO_ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Round-trips `values` (as raw little-endian bytes) through the FFI for a
    // given pco number type and element width, asserting byte-exact recovery.
    unsafe fn roundtrip(number_type: u8, width: u32, bytes: &[u8]) {
        let n = (bytes.len() / width as usize) as u64;
        let mut compressed = vec![0u8; bytes.len() + 64];
        let mut out_size = 0u64;
        let rc = pco_compress(
            number_type,
            bytes.as_ptr(),
            n,
            8,
            compressed.as_mut_ptr(),
            compressed.len() as u64,
            &mut out_size,
        );
        assert_eq!(rc, PCO_OK, "compress rc for type {number_type}");

        // Decode both as a typed reader (expecting the exact number type) and
        // as an untyped one (expected type 0, width match only).
        for expected in [number_type, 0] {
            let mut restored = vec![0u8; bytes.len()];
            let rc = pco_decompress(
                width,
                expected,
                compressed.as_ptr(),
                out_size,
                restored.as_mut_ptr(),
                n,
                restored.len() as u64,
            );
            assert_eq!(rc, PCO_OK, "decompress rc for type {number_type} expecting {expected}");
            assert_eq!(&restored, bytes, "roundtrip bytes for type {number_type} expecting {expected}");
        }
    }

    #[test]
    fn roundtrip_all_types() {
        unsafe {
            let u32s: Vec<u8> = (0..5000u32).flat_map(|i| (i.wrapping_mul(2_654_435_761)).to_le_bytes()).collect();
            roundtrip(1, 4, &u32s);
            let u64s: Vec<u8> = (0..5000u64).flat_map(|i| i.wrapping_mul(11_400_714_819_323_198_485).to_le_bytes()).collect();
            roundtrip(2, 8, &u64s);
            let i32s: Vec<u8> = (0..5000i32).flat_map(|i| (i - 2500).to_le_bytes()).collect();
            roundtrip(3, 4, &i32s);
            let i64s: Vec<u8> = (0..5000i64).flat_map(|i| (i - 2500).to_le_bytes()).collect();
            roundtrip(4, 8, &i64s);
            let f32s: Vec<u8> = (0..5000u32).flat_map(|i| (i as f32 * 0.5).to_le_bytes()).collect();
            roundtrip(5, 4, &f32s);
            let f64s: Vec<u8> = (0..5000u32).flat_map(|i| (i as f64 * 0.25).to_le_bytes()).collect();
            roundtrip(6, 8, &f64s);
            let u16s: Vec<u8> = (0..5000u16).flat_map(|i| i.wrapping_mul(40503).to_le_bytes()).collect();
            roundtrip(7, 2, &u16s);
            let i16s: Vec<u8> = (0..5000i16).flat_map(|i| i.wrapping_sub(2500).to_le_bytes()).collect();
            roundtrip(8, 2, &i16s);
            let f16s: Vec<u8> = (0..5000u32).flat_map(|i| half::f16::from_f32(i as f32 * 0.5).to_le_bytes()).collect();
            roundtrip(9, 2, &f16s);
            let u8s: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
            roundtrip(10, 1, &u8s);
            let i8s: Vec<u8> = (0..5000u32).map(|i| ((i % 251) as i32 - 125) as u8).collect();
            roundtrip(11, 1, &i8s);
        }
    }

    #[test]
    fn unaligned_source_compresses_identically() {
        // The aligned fast path borrows the source in place while the
        // unaligned path copies into an aligned buffer; both must produce
        // identical streams.
        unsafe {
            let u32s: Vec<u8> = (0..2000u32).flat_map(|i| i.wrapping_mul(2_654_435_761).to_le_bytes()).collect();
            let mut shifted = vec![0u8; u32s.len() + 1];
            shifted[1..].copy_from_slice(&u32s);

            let mut aligned_out = vec![0u8; u32s.len() + 64];
            let mut shifted_out = vec![0u8; u32s.len() + 64];
            let mut aligned_size = 0u64;
            let mut shifted_size = 0u64;
            assert_eq!(
                pco_compress(1, u32s.as_ptr(), 2000, 8, aligned_out.as_mut_ptr(), aligned_out.len() as u64, &mut aligned_size),
                PCO_OK
            );
            assert_eq!(
                pco_compress(1, shifted.as_ptr().add(1), 2000, 8, shifted_out.as_mut_ptr(), shifted_out.len() as u64, &mut shifted_size),
                PCO_OK
            );
            assert_eq!(aligned_size, shifted_size);
            assert_eq!(&aligned_out[..aligned_size as usize], &shifted_out[..shifted_size as usize]);

            let mut restored = vec![0u8; u32s.len()];
            assert_eq!(
                pco_decompress(4, 1, aligned_out.as_ptr(), aligned_size, restored.as_mut_ptr(), 2000, restored.len() as u64),
                PCO_OK
            );
            assert_eq!(&restored, &u32s);
        }
    }

    #[test]
    fn decompress_external_f16_stream() {
        // A valid standalone `.pco` stream carrying `f16` values (number type
        // byte 9) — as an external pcodec producer or an HTTP `decompress=1`
        // client would send — must decode on the untyped method-byte path. The
        // stream is built with the reference `simple_compress` API (not the FFI
        // compress arm) so this exercises decode independently.
        unsafe {
            let nums: Vec<half::f16> = (0..5000u32).map(|i| half::f16::from_f32(i as f32 * 0.25)).collect();
            let compressed = pco::standalone::simple_compress(&nums, &ChunkConfig::default())
                .expect("compress f16 stream");

            let expected: Vec<u8> = nums.iter().flat_map(|v| v.to_le_bytes()).collect();
            let mut restored = vec![0u8; expected.len()];
            let rc = pco_decompress(
                2,
                0,
                compressed.as_ptr(),
                compressed.len() as u64,
                restored.as_mut_ptr(),
                nums.len() as u64,
                restored.len() as u64,
            );
            assert_eq!(rc, PCO_OK, "decompress rc for external f16 stream");
            assert_eq!(&restored, &expected, "roundtrip bytes for external f16 stream");

            // A width that disagrees with the stream's f16 (2 bytes) fails closed.
            let mut wide = vec![0u8; expected.len() * 2];
            assert_eq!(
                pco_decompress(4, 0, compressed.as_ptr(), compressed.len() as u64, wide.as_mut_ptr(), nums.len() as u64, wide.len() as u64),
                PCO_ERROR
            );
        }
    }

    #[test]
    fn incompressible_data_reports_wont_fit() {
        // Pseudo-random, incompressible u64 data: pco should not beat raw, so
        // compressing with capacity == raw size returns PCO_WONT_FIT.
        unsafe {
            let mut state = 0x9e3779b97f4a7c15u64;
            let raw: Vec<u8> = (0..4096u64)
                .flat_map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state.to_le_bytes()
                })
                .collect();
            let n = (raw.len() / 8) as u64;
            let mut compressed = vec![0u8; raw.len()]; // exactly raw size
            let mut out_size = 0u64;
            let rc = pco_compress(2, raw.as_ptr(), n, 8, compressed.as_mut_ptr(), compressed.len() as u64, &mut out_size);
            assert_eq!(rc, PCO_WONT_FIT);
        }
    }

    #[test]
    fn zero_values_accepts_only_a_well_formed_empty_stream() {
        unsafe {
            // A genuine empty stream (header + termination byte) built with the
            // reference API decodes successfully when no values are expected...
            let empty = pco::standalone::simple_compress::<u32>(&[], &ChunkConfig::default())
                .expect("compress empty stream");
            let mut dst = [0u8; 4];
            assert_eq!(
                pco_decompress(4, 0, empty.as_ptr(), empty.len() as u64, dst.as_mut_ptr(), 0, dst.len() as u64),
                PCO_OK
            );
            // ...but not when values are expected.
            assert_eq!(
                pco_decompress(4, 0, empty.as_ptr(), empty.len() as u64, dst.as_mut_ptr(), 1, dst.len() as u64),
                PCO_ERROR
            );

            // Garbage payload with n_values == 0 must fail closed, not be
            // silently accepted as an empty stream.
            let garbage = [0xabu8; 32];
            assert_eq!(
                pco_decompress(4, 0, garbage.as_ptr(), garbage.len() as u64, dst.as_mut_ptr(), 0, dst.len() as u64),
                PCO_ERROR
            );

            // A truncated header (a prefix of a valid empty stream) must fail
            // closed with n_values == 0.
            for len in 0..empty.len() - 1 {
                assert_eq!(
                    pco_decompress(4, 0, empty.as_ptr(), len as u64, dst.as_mut_ptr(), 0, dst.len() as u64),
                    PCO_ERROR,
                    "truncated to {len} bytes"
                );
            }

            // Trailing bytes after the termination byte must fail closed
            // instead of being silently ignored.
            let mut trailing = empty.clone();
            trailing.extend_from_slice(&[0xab, 0xcd, 0xef]);
            assert_eq!(
                pco_decompress(4, 0, trailing.as_ptr(), trailing.len() as u64, dst.as_mut_ptr(), 0, dst.len() as u64),
                PCO_ERROR
            );
        }
    }

    #[test]
    fn trailing_bytes_after_nonempty_stream_fail_closed() {
        // A valid non-empty stream with extra bytes appended after the
        // termination marker is malformed and must fail closed, not decode the
        // valid prefix and silently ignore the tail.
        unsafe {
            let u32s: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
            let mut compressed = vec![0u8; u32s.len() + 64];
            let mut out_size = 0u64;
            assert_eq!(
                pco_compress(1, u32s.as_ptr(), 1000, 8, compressed.as_mut_ptr(), compressed.len() as u64, &mut out_size),
                PCO_OK
            );
            compressed.truncate(out_size as usize);

            let mut restored = vec![0u8; u32s.len()];
            for garbage in [&[0u8][..], &[0xab, 0xcd, 0xef][..]] {
                let mut trailing = compressed.clone();
                trailing.extend_from_slice(garbage);
                for expected in [1u8, 0u8] {
                    assert_eq!(
                        pco_decompress(4, expected, trailing.as_ptr(), trailing.len() as u64, restored.as_mut_ptr(), 1000, restored.len() as u64),
                        PCO_ERROR,
                        "{} trailing bytes expecting {expected}",
                        garbage.len()
                    );
                }
            }

            // A truncated non-empty stream must fail closed too: the exact-EOF
            // decode path proves completeness, not just a prefix decode.
            assert_eq!(
                pco_decompress(4, 0, compressed.as_ptr(), (compressed.len() - 1) as u64, restored.as_mut_ptr(), 1000, restored.len() as u64),
                PCO_ERROR
            );

            // The untouched stream still decodes.
            assert_eq!(
                pco_decompress(4, 0, compressed.as_ptr(), compressed.len() as u64, restored.as_mut_ptr(), 1000, restored.len() as u64),
                PCO_OK
            );
            assert_eq!(&restored, &u32s);
        }
    }

    #[test]
    fn malformed_stream_and_width_mismatch_fail_closed() {
        unsafe {
            // Valid u32 stream.
            let u32s: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
            let mut compressed = vec![0u8; u32s.len() + 64];
            let mut out_size = 0u64;
            assert_eq!(
                pco_compress(1, u32s.as_ptr(), 1000, 8, compressed.as_mut_ptr(), compressed.len() as u64, &mut out_size),
                PCO_OK
            );

            // Wrong declared width (8 for a 4-byte stream) must fail closed.
            let mut restored = vec![0u8; 8000];
            assert_eq!(
                pco_decompress(8, 0, compressed.as_ptr(), out_size, restored.as_mut_ptr(), 1000, restored.len() as u64),
                PCO_ERROR
            );

            // Garbage stream must fail closed, not panic.
            let garbage = [0xabu8; 32];
            let mut out = vec![0u8; 64];
            assert_eq!(
                pco_decompress(4, 0, garbage.as_ptr(), garbage.len() as u64, out.as_mut_ptr(), 1, out.len() as u64),
                PCO_ERROR
            );
        }
    }

    #[test]
    fn expected_number_type_mismatch_fails_closed() {
        unsafe {
            // Valid i32 stream (number type byte 3).
            let i32s: Vec<u8> = (0..1000i32).flat_map(|i| i.to_le_bytes()).collect();
            let mut compressed = vec![0u8; i32s.len() + 64];
            let mut out_size = 0u64;
            assert_eq!(
                pco_compress(3, i32s.as_ptr(), 1000, 8, compressed.as_mut_ptr(), compressed.len() as u64, &mut out_size),
                PCO_OK
            );

            let mut restored = vec![0u8; i32s.len()];
            // A reader expecting u32 (type byte 1) must reject the same-width
            // i32 stream instead of reinterpreting the values...
            assert_eq!(
                pco_decompress(4, 1, compressed.as_ptr(), out_size, restored.as_mut_ptr(), 1000, restored.len() as u64),
                PCO_ERROR
            );
            // ...while the matching typed reader and the untyped reader accept it.
            for expected in [3u8, 0u8] {
                assert_eq!(
                    pco_decompress(4, expected, compressed.as_ptr(), out_size, restored.as_mut_ptr(), 1000, restored.len() as u64),
                    PCO_OK
                );
                assert_eq!(&restored, &i32s);
            }
        }
    }
}
