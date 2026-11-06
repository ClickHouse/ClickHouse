//! Zigzag varints (Apache Avro specification, "Binary Encoding").
//!
//! Rules, in one place:
//!
//! * `long`: at most 10 bytes; in the 10th byte only the lowest bit may be set
//!   (the other bits would not fit in 64 bits). Anything else is
//!   "Invalid Avro varint".
//! * `int`: at most 5 bytes, and the decoded value must be in the `i32` range
//!   ("Value out of range for Avro int: N").
//! * Non-minimal encodings (`0x80 0x00` for 0) are accepted within those
//!   length caps, as in the C++ fork; the specification does not forbid them.
//! * Truncated input is `Truncated`: the caller decides whether that means
//!   "need more bytes" or "EOF reached".
//!
//! Two paths decode the same language: a fast one used when at least 8 bytes
//! remain (one 8-byte load, terminator found with a trailing-zeros count, the
//! 7-bit groups gathered with shifts and masks; no BMI2, so identical on
//! arm64 and amd64), and a bytewise one for the end of the buffer and for
//! encodings longer than 8 bytes.

use crate::{Error, ErrorCode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VarintError {
    /// The buffer ends before the terminating byte.
    Truncated,
    /// Too many bytes, or bits that do not fit.
    Invalid,
    /// An `int` whose value is outside the `i32` range.
    IntOutOfRange(i64),
}

impl VarintError {
    pub fn into_error(self) -> Error {
        match self {
            VarintError::Truncated => Error::new(ErrorCode::Eof, "EOF reached"),
            VarintError::Invalid => Error::new(ErrorCode::InvalidVarint, "Invalid Avro varint"),
            VarintError::IntOutOfRange(v) => Error::new(
                ErrorCode::IntOutOfRange,
                format!("Value out of range for Avro int: {v}"),
            ),
        }
    }
}

#[inline(always)]
fn unzigzag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Gathers the 7-bit payloads of the low `len` bytes (1..=8) of `x`.
#[inline(always)]
fn gather8(x: u64, len: u32) -> u64 {
    // Keep only the bytes that belong to the varint; `len == 8` keeps all of them.
    let keep = u64::MAX >> (64 - 8 * len);
    let v = x & keep;
    // Pairs of bytes into 14 bits, pairs of pairs into 28 bits, quads into 56 bits.
    let v = (v & 0x007f_007f_007f_007f) | ((v & 0x7f00_7f00_7f00_7f00) >> 1);
    let v = (v & 0x0000_3fff_0000_3fff) | ((v & 0x3fff_0000_3fff_0000) >> 2);
    (v & 0x0000_0000_0fff_ffff) | ((v & 0x0fff_ffff_0000_0000) >> 4)
}

/// Decodes the raw (not yet unzigzagged) value and its length, for at most
/// `max_len` bytes (5 or 10). Returns `Truncated` when the buffer ends first.
#[inline(always)]
fn decode_raw(buf: &[u8], max_len: usize) -> Result<(u64, usize), VarintError> {
    if let Some(&first) = buf.first() {
        // One-byte values are by far the most common.
        if first < 0x80 {
            return Ok((u64::from(first), 1));
        }
    }
    if let Some(window) = buf.first_chunk::<8>() {
        let x = u64::from_le_bytes(*window);
        let stop = !x & 0x8080_8080_8080_8080;
        if stop != 0 {
            let len = stop.trailing_zeros() / 8 + 1;
            if len as usize > max_len {
                return Err(VarintError::Invalid);
            }
            return Ok((gather8(x, len), len as usize));
        }
        // All eight bytes continue: the value needs more than 8 bytes.
    }
    decode_raw_slow(buf, max_len)
}

#[inline(never)]
fn decode_raw_slow(buf: &[u8], max_len: usize) -> Result<(u64, usize), VarintError> {
    let mut value: u64 = 0;
    for i in 0..max_len {
        let Some(&b) = buf.get(i) else {
            return Err(VarintError::Truncated);
        };
        let payload = u64::from(b & 0x7f);
        if i == 9 && payload > 1 {
            // Bits 64 and above would be lost.
            return Err(VarintError::Invalid);
        }
        value |= payload << (7 * i);
        if b & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(VarintError::Invalid)
}

/// Decodes an Avro `long` from the start of `buf`; returns the value and the
/// number of bytes it occupies.
#[inline]
pub fn decode_long(buf: &[u8]) -> Result<(i64, usize), VarintError> {
    let (raw, len) = decode_raw(buf, 10)?;
    Ok((unzigzag(raw), len))
}

/// Decodes an Avro `int`.
#[inline]
pub fn decode_int(buf: &[u8]) -> Result<(i32, usize), VarintError> {
    let (raw, len) = decode_raw(buf, 5)?;
    let v = unzigzag(raw);
    match i32::try_from(v) {
        Ok(v) => Ok((v, len)),
        Err(_) => Err(VarintError::IntOutOfRange(v)),
    }
}

/// Length of the varint at the start of `buf` without decoding its value, with
/// the same validity rules as [`decode_long`] (`int == false`) or
/// [`decode_int`]'s length cap (`int == true`; the value range is not
/// checked here).
#[inline]
pub fn varint_len(buf: &[u8], int: bool) -> Result<usize, VarintError> {
    let max_len = if int { 5 } else { 10 };
    decode_raw(buf, max_len).map(|(_, len)| len)
}

/// The bytewise decoder on its own, for differential testing of the fast path.
pub fn decode_long_bytewise(buf: &[u8]) -> Result<(i64, usize), VarintError> {
    let (raw, len) = decode_raw_slow(buf, 10)?;
    Ok((unzigzag(raw), len))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Independent encoder: spec text, bit by bit.
    fn encode_long(v: i64) -> Vec<u8> {
        let mut n = ((v << 1) ^ (v >> 63)) as u64;
        let mut out = Vec::new();
        while n & !0x7f != 0 {
            out.push(((n & 0x7f) | 0x80) as u8);
            n >>= 7;
        }
        out.push(n as u8);
        out
    }

    /// A deliberately naive reference decoder.
    fn reference(buf: &[u8]) -> Option<(i64, usize)> {
        let mut n: u128 = 0;
        for (i, b) in buf.iter().enumerate() {
            n |= u128::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                if i >= 10 || n > u128::from(u64::MAX) {
                    return None;
                }
                let n = n as u64;
                return Some((((n >> 1) as i64) ^ -((n & 1) as i64), i + 1));
            }
        }
        None
    }

    #[test]
    fn spec_examples() {
        // Avro specification, "Binary Encoding": long examples.
        let table: [(i64, &[u8]); 7] = [
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (-2, &[0x03]),
            (2, &[0x04]),
            (-64, &[0x7f]),
            (64, &[0x80, 0x01]),
        ];
        for (v, bytes) in table {
            assert_eq!(decode_long(bytes), Ok((v, bytes.len())), "{v}");
            assert_eq!(encode_long(v), bytes, "{v}");
        }
        // 8-byte window available too.
        let mut padded = vec![0x80, 0x01];
        padded.extend_from_slice(&[0xff; 8]);
        assert_eq!(decode_long(&padded), Ok((64, 2)));
    }

    #[test]
    fn boundaries() {
        for v in [
            i64::MIN,
            i64::MIN + 1,
            i64::MAX,
            i64::MAX - 1,
            i64::from(i32::MIN),
            i64::from(i32::MAX),
            i64::from(i32::MIN) - 1,
            i64::from(i32::MAX) + 1,
            63,
            -65,
            8191,
            -8192,
            (1 << 31) - 1,
            (1 << 31),
            (1 << 55),
            -(1 << 55),
            (1 << 62),
        ] {
            let e = encode_long(v);
            assert_eq!(decode_long(&e), Ok((v, e.len())), "{v}");
            assert_eq!(decode_long_bytewise(&e), Ok((v, e.len())), "{v} bytewise");
            // With trailing garbage after the value.
            let mut with_tail = e.clone();
            with_tail.extend_from_slice(&[0x80; 12]);
            assert_eq!(decode_long(&with_tail), Ok((v, e.len())), "{v} with tail");
        }
        assert_eq!(encode_long(i64::MIN).len(), 10);
        assert_eq!(encode_long(i64::MAX).len(), 10);
    }

    #[test]
    fn long_length_and_overflow_rules() {
        // Ten bytes, last one 0x01: the largest zigzag value, -2^63.
        let max = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert_eq!(decode_long(&max), Ok((i64::MIN, 10)));
        // Last byte 0x02 would need bit 64: rejected, not silently truncated.
        let over = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        assert_eq!(decode_long(&over), Err(VarintError::Invalid));
        assert_eq!(decode_long_bytewise(&over), Err(VarintError::Invalid));
        // An 11th byte is rejected.
        let eleven = [
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
        ];
        assert_eq!(decode_long(&eleven), Err(VarintError::Invalid));
        // Non-minimal encodings within the cap are accepted (the fork does the same).
        assert_eq!(decode_long(&[0x80, 0x00]), Ok((0, 2)));
        assert_eq!(
            decode_long(&[0x82, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]),
            Ok((1, 10))
        );
        assert_eq!(
            decode_long(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]),
            Ok((0, 10))
        );
    }

    #[test]
    fn int_rules() {
        assert_eq!(
            decode_int(&encode_long(i64::from(i32::MAX))),
            Ok((i32::MAX, 5))
        );
        assert_eq!(
            decode_int(&encode_long(i64::from(i32::MIN))),
            Ok((i32::MIN, 5))
        );
        assert_eq!(
            decode_int(&encode_long(i64::from(i32::MAX) + 1)),
            Err(VarintError::IntOutOfRange(i64::from(i32::MAX) + 1))
        );
        assert_eq!(
            decode_int(&encode_long(i64::from(i32::MIN) - 1)),
            Err(VarintError::IntOutOfRange(i64::from(i32::MIN) - 1))
        );
        assert_eq!(
            decode_int(&encode_long(i64::MAX)),
            Err(VarintError::Invalid)
        );
        // More than 5 bytes is rejected even when the value would fit.
        assert_eq!(
            decode_int(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00]),
            Err(VarintError::Invalid)
        );
        // Five bytes with a zero payload is a non-minimal zero: accepted.
        assert_eq!(decode_int(&[0x80, 0x80, 0x80, 0x80, 0x00]), Ok((0, 5)));
        assert_eq!(
            VarintError::IntOutOfRange(2147483648).into_error().message,
            "Value out of range for Avro int: 2147483648"
        );
        assert_eq!(
            VarintError::Invalid.into_error().message,
            "Invalid Avro varint"
        );
        assert_eq!(VarintError::Truncated.into_error().message, "EOF reached");
    }

    #[test]
    fn every_proper_prefix_is_truncated() {
        for v in [
            0i64,
            1,
            -1,
            63,
            64,
            -65,
            300,
            1 << 20,
            -(1 << 40),
            i64::MAX,
            i64::MIN,
            i64::from(i32::MAX),
            i64::from(i32::MIN),
        ] {
            let e = encode_long(v);
            for cut in 0..e.len() {
                assert_eq!(
                    decode_long(&e[..cut]),
                    Err(VarintError::Truncated),
                    "{v} cut at {cut}"
                );
                assert_eq!(
                    decode_long_bytewise(&e[..cut]),
                    Err(VarintError::Truncated),
                    "{v} cut at {cut} bytewise"
                );
                if e.len() <= 5 {
                    assert_eq!(decode_int(&e[..cut]), Err(VarintError::Truncated));
                }
            }
        }
        // All-continuation input of every length below the cap is truncated, not invalid.
        for n in 0..10 {
            assert_eq!(
                decode_long(&vec![0x80; n]),
                Err(VarintError::Truncated),
                "{n}"
            );
        }
        assert_eq!(decode_long(&[0x80; 10]), Err(VarintError::Invalid));
    }

    /// All 2^32 `i32` values in release builds, a dense sample in debug builds.
    #[test]
    fn every_i32_round_trips() {
        let stride: u64 = if cfg!(debug_assertions) { 997 } else { 1 };
        let mut v: u64 = 0;
        while v < (1u64 << 32) {
            let x = v as u32 as i32;
            let e = encode_long(i64::from(x));
            assert_eq!(decode_int(&e), Ok((x, e.len())), "{x}");
            assert_eq!(decode_long(&e), Ok((i64::from(x), e.len())), "{x} as long");
            v += stride;
        }
        // Boundaries always run.
        for x in [i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX] {
            let e = encode_long(i64::from(x));
            assert_eq!(decode_int(&e), Ok((x, e.len())));
        }
    }

    /// Every 3-byte and every 2-byte string, plus random-looking longer ones, against the naive decoder.
    #[test]
    fn fast_path_agrees_with_reference() {
        let check = |buf: &[u8]| {
            let got = decode_long(buf).ok();
            assert_eq!(got, reference(buf).filter(|(_, l)| *l <= 10), "{buf:02x?}");
            assert_eq!(
                got,
                decode_long_bytewise(buf).ok(),
                "{buf:02x?} vs bytewise"
            );
        };
        for a in 0..=255u8 {
            check(&[a]);
            for b in 0..=255u8 {
                check(&[a, b]);
                for c in [0u8, 1, 0x7f, 0x80, 0x81, 0xff] {
                    check(&[a, b, c]);
                    check(&[a, b, c, 0xff, 0x7f, 0x00, 0x80, 0x01]);
                    check(&[a, b, c, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
                }
            }
        }
        // xorshift for longer buffers.
        let mut s: u64 = 0x9e37_79b9_7f4a_7c15;
        for _ in 0..200_000 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let len = (s % 14) as usize;
            let mut buf = Vec::with_capacity(len);
            let mut t = s;
            for _ in 0..len {
                t ^= t << 13;
                t ^= t >> 7;
                t ^= t << 17;
                // Bias towards continuation bytes so long encodings occur.
                let b = t as u8;
                buf.push(if t >> 60 < 12 { b | 0x80 } else { b });
            }
            check(&buf);
        }
    }

    #[test]
    fn varint_len_matches() {
        assert_eq!(varint_len(&[0x80, 0x01], false), Ok(2));
        assert_eq!(
            varint_len(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00], true),
            Err(VarintError::Invalid)
        );
        assert_eq!(varint_len(&[0x80], false), Err(VarintError::Truncated));
    }
}
