//! Object Container File framing (Apache Avro specification 1.12, "Object
//! Container Files"), on a window of bytes.
//!
//! Every function takes the bytes the caller has buffered so far (`buf`) and
//! `eof`, which says that no more bytes will ever follow `buf`. The result is
//! either a parsed value with the number of bytes it occupies, or
//! [`Parsed::NeedMore`] with the total window size that is needed to make
//! progress (when `eof` is set, running out of bytes is the error `EOF
//! reached` instead). Parsing is restartable from the start of the same
//! element: nothing is consumed on `NeedMore`.
//!
//! This module does not decompress anything. The codec is returned to the
//! caller, which decompresses the block payload with its own, memory-tracked
//! decompressors, and then runs the executor on the plain bytes.
//!
//! The only allocation is the metadata map, whose bytes are actual input. `OcfLimits::max_header_bytes` is an
//! optional cap on those bytes; 0 removes the cap, so the caller's input and memory controls apply.
//!
//! Schema text has no byte-count ceiling. Rust JSON parsing bounds nesting and JSON-value count independently.

use std::collections::HashMap;

use crate::varint::{decode_int, decode_long, VarintError};
use crate::{Error, ErrorCode, Result};


/// The four magic bytes `Obj` and version 1.
pub const MAGIC: [u8; 4] = *b"Obj\x01";
pub const SYNC_LEN: usize = 16;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Null = 0,
    Deflate = 1,
    Snappy = 2,
    Zstandard = 3,
    Bzip2 = 4,
    Xz = 5,
}

impl Codec {
    pub fn from_name(name: &[u8]) -> Option<Codec> {
        Some(match name {
            b"null" => Codec::Null,
            b"deflate" => Codec::Deflate,
            b"snappy" => Codec::Snappy,
            b"zstandard" => Codec::Zstandard,
            b"bzip2" => Codec::Bzip2,
            b"xz" => Codec::Xz,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct OcfLimits {
    /// Upper bound on the size of the header (magic, metadata, sync marker).
    pub max_header_bytes: usize,
    /// Upper bound on a block's payload as declared by its header; `0` means
    /// no bound.
    pub max_block_bytes: u64,
}

impl Default for OcfLimits {
    fn default() -> Self {
        OcfLimits {
            max_header_bytes: 0,
            max_block_bytes: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Parsed<T> {
    /// Parsed; the element occupies `consumed` bytes at the start of the window.
    Done { value: T, consumed: usize },
    /// The window must grow to at least `needed` bytes in total.
    NeedMore { needed: usize },
}

/// Internal early exit: not enough bytes yet, or a real error.
enum Stop {
    More(usize),
    Fail(Error),
}

impl From<Error> for Stop {
    fn from(e: Error) -> Stop {
        Stop::Fail(e)
    }
}

struct Window<'a> {
    buf: &'a [u8],
    pos: usize,
    /// Cap on `needed` before it becomes an error instead of a wait.
    cap: usize,
}

impl Window<'_> {
    fn need(&self, total: usize) -> Stop {
        if total > self.cap {
            return Stop::Fail(Error::new(
                ErrorCode::HeaderTooLarge,
                format!("Avro data file header is larger than {} bytes", self.cap),
            ));
        }
        Stop::More(total)
    }

    fn take(&mut self, n: usize) -> std::result::Result<&[u8], Stop> {
        let end = self.pos.saturating_add(n);
        if end > self.buf.len() {
            return Err(self.need(end));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn long(&mut self) -> std::result::Result<i64, Stop> {
        match decode_long(&self.buf[self.pos..]) {
            Ok((v, n)) => {
                self.pos += n;
                Ok(v)
            }
            Err(VarintError::Truncated) => Err(self.need(self.buf.len() + 1)),
            Err(e) => Err(Stop::Fail(e.into_error())),
        }
    }

    /// An Avro `int` used as a length, as the C++ reader does for strings and bytes.
    fn length(&mut self) -> std::result::Result<usize, Stop> {
        match decode_int(&self.buf[self.pos..]) {
            Ok((v, n)) => {
                if v < 0 {
                    return Err(Stop::Fail(Error::new(
                        ErrorCode::NegativeLength,
                        format!("Cannot have negative length: {v}"),
                    )));
                }
                self.pos += n;
                Ok(v as usize)
            }
            Err(VarintError::Truncated) => Err(self.need(self.buf.len() + 1)),
            Err(e) => Err(Stop::Fail(e.into_error())),
        }
    }
}

fn finish<T>(r: std::result::Result<(T, usize), Stop>, eof: bool) -> Result<Parsed<T>> {
    match r {
        Ok((value, consumed)) => Ok(Parsed::Done { value, consumed }),
        Err(Stop::Fail(e)) => Err(e),
        Err(Stop::More(needed)) => {
            if eof {
                Err(Error::new(ErrorCode::Eof, "EOF reached"))
            } else {
                Ok(Parsed::NeedMore { needed })
            }
        }
    }
}

/// A parsed container file header.
#[derive(Debug)]
pub struct Header {
    /// Metadata in order of first appearance; for a repeated key the value of
    /// the last occurrence.
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    schema_entry: usize,
    pub codec: Codec,
    pub sync: [u8; SYNC_LEN],
}

impl Header {
    /// The writer schema JSON (`avro.schema`).
    pub fn schema_json(&self) -> &[u8] {
        &self.entries[self.schema_entry].1
    }

    pub fn metadata(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.entries
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_slice())
    }
}

/// Parses the file header from the start of `buf`.
pub fn parse_header(buf: &[u8], eof: bool, limits: &OcfLimits) -> Result<Parsed<Header>> {
    finish(header_inner(buf, limits), eof)
}

fn header_inner(buf: &[u8], limits: &OcfLimits) -> std::result::Result<(Header, usize), Stop> {
    let mut w = Window {
        buf,
        pos: 0,
        cap: if limits.max_header_bytes == 0 { usize::MAX } else { limits.max_header_bytes },
    };
    let magic = w.take(4)?;
    if magic != MAGIC {
        return Err(Stop::Fail(Error::new(
            ErrorCode::BadMagic,
            "Invalid data file. Magic does not match: ",
        )));
    }
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    loop {
        let mut count = w.long()?;
        if count == 0 {
            break;
        }
        if count < 0 {
            count = count.checked_neg().ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidCount,
                    "Invalid block count in the metadata map",
                )
            })?;
            // The byte size that follows a negative count is not needed.
            w.long()?;
        }
        // Each entry takes at least two bytes (two lengths). An incomplete
        // input window is not a limit: let parsing request the missing bytes.
        if limits.max_header_bytes != 0 && count as u64 > (limits.max_header_bytes / 2) as u64 {
            return Err(Stop::Fail(Error::new(
                ErrorCode::HeaderTooLarge,
                "Avro data file metadata has too many entries for the header bytes",
            )));
        }
        for _ in 0..count {
            let klen = w.length()?;
            let key = w.take(klen)?.to_vec();
            let vlen = w.length()?;
            let value = w.take(vlen)?.to_vec();
            match index.get(&key) {
                Some(&i) => entries[i].1 = value,
                None => {
                    index.insert(key.clone(), entries.len());
                    entries.push((key, value));
                }
            }
        }
    }
    let Some(&schema_entry) = index.get(b"avro.schema".as_slice()) else {
        return Err(Stop::Fail(Error::new(
            ErrorCode::NoSchema,
            "No schema in metadata",
        )));
    };
    let codec = match index.get(b"avro.codec".as_slice()) {
        None => Codec::Null,
        Some(&i) => {
            let name = &entries[i].1;
            Codec::from_name(name).ok_or_else(|| {
                Error::new(
                    ErrorCode::UnknownCodec,
                    format!(
                        "Unknown codec in data file: {}",
                        String::from_utf8_lossy(name)
                    ),
                )
            })?
        }
    };
    let mut sync = [0u8; SYNC_LEN];
    sync.copy_from_slice(w.take(SYNC_LEN)?);
    Ok((
        Header {
            entries,
            schema_entry,
            codec,
            sync,
        },
        w.pos,
    ))
}

/// What the framing code needs to know about the file to validate a block header.
#[derive(Clone, Copy, Debug)]
pub struct BlockCtx {
    pub codec: Codec,
    /// `Schema::min_encoded_size` of the root, `0` when the schema gives no bound.
    pub min_record_size: u64,
    /// `OcfLimits::max_block_bytes`.
    pub max_block_bytes: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum BlockHeader {
    /// The window is empty and the file ended: a clean end of the file.
    End,
    /// A block: `object_count` records in `byte_count` payload bytes, which
    /// start right after this header.
    Block { object_count: u64, byte_count: u64 },
}

/// Checks a declared object count against what a payload of `payload_len`
/// plain (decompressed) bytes can hold: every record takes at least
/// `min_record_size` bytes. Disabled when the schema gives no lower bound.
pub fn check_object_count(object_count: u64, payload_len: u64, min_record_size: u64) -> Result<()> {
    if min_record_size > 0 {
        if object_count > payload_len / min_record_size {
            return Err(Error::new(
                ErrorCode::ObjectCountExceedsPayload,
                format!(
                    "Invalid data file. Object count {object_count} in block header cannot fit in {payload_len} payload byte(s) for this schema"
                ),
            ));
        }
    }
    Ok(())
}

/// Parses a block header at the start of `buf` (the window starts right after
/// the previous block's sync marker, or after the file header).
///
/// For the `null` codec the object count is checked against the payload size
/// here; for compressed codecs check it with [`check_object_count`] once the
/// payload is decompressed.
pub fn parse_block_header(buf: &[u8], eof: bool, ctx: &BlockCtx) -> Result<Parsed<BlockHeader>> {
    if buf.is_empty() && eof {
        return Ok(Parsed::Done {
            value: BlockHeader::End,
            consumed: 0,
        });
    }
    finish(block_inner(buf, ctx), eof)
}

fn block_inner(buf: &[u8], ctx: &BlockCtx) -> std::result::Result<(BlockHeader, usize), Stop> {
    let mut w = Window {
        buf,
        pos: 0,
        cap: usize::MAX,
    };
    let objects = w.long()?;
    let bytes = w.long()?;
    if objects < 0 {
        return Err(Stop::Fail(Error::new(
            ErrorCode::NegativeObjectCount,
            format!("Invalid data file. Negative object count in block header: {objects}"),
        )));
    }
    if bytes < 0 {
        return Err(Stop::Fail(Error::new(
            ErrorCode::NegativeByteCount,
            format!("Invalid data file. Negative byte count in block header: {bytes}"),
        )));
    }
    let (objects, bytes) = (objects as u64, bytes as u64);
    if ctx.max_block_bytes != 0 && bytes > ctx.max_block_bytes {
        return Err(Stop::Fail(Error::new(
            ErrorCode::BlockTooLarge,
            format!(
                "Avro data file block of {bytes} bytes is larger than the limit of {} bytes",
                ctx.max_block_bytes
            ),
        )));
    }
    if ctx.codec == Codec::Null {
        check_object_count(objects, bytes, ctx.min_record_size)?;
    }
    Ok((
        BlockHeader::Block {
            object_count: objects,
            byte_count: bytes,
        },
        w.pos,
    ))
}

/// Checks the sync marker at the start of `buf` (it follows every block).
pub fn check_sync(buf: &[u8], expected: &[u8; SYNC_LEN], eof: bool) -> Result<Parsed<()>> {
    if buf.len() < SYNC_LEN {
        if eof {
            return Err(Error::new(ErrorCode::Eof, "EOF reached"));
        }
        return Ok(Parsed::NeedMore { needed: SYNC_LEN });
    }
    if buf[..SYNC_LEN] != expected[..] {
        return Err(Error::new(ErrorCode::SyncMismatch, "Sync mismatch"));
    }
    Ok(Parsed::Done {
        value: (),
        consumed: SYNC_LEN,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zz(v: i64) -> Vec<u8> {
        let mut n = ((v << 1) ^ (v >> 63)) as u64;
        let mut out = Vec::new();
        while n >= 0x80 {
            out.push((n as u8 & 0x7f) | 0x80);
            n >>= 7;
        }
        out.push(n as u8);
        out
    }

    fn bytes(b: &[u8]) -> Vec<u8> {
        let mut v = zz(b.len() as i64);
        v.extend_from_slice(b);
        v
    }

    fn header_bytes(entries: &[(&[u8], &[u8])], negative_blocks: bool) -> Vec<u8> {
        let mut h = MAGIC.to_vec();
        if negative_blocks {
            h.extend(zz(-(entries.len() as i64)));
            h.extend(zz(1));
        } else {
            h.extend(zz(entries.len() as i64));
        }
        for (k, v) in entries {
            h.extend(bytes(k));
            h.extend(bytes(v));
        }
        h.extend(zz(0));
        h.extend_from_slice(&[7u8; 16]);
        h
    }

    const OPTS: OcfLimits = OcfLimits {
        max_header_bytes: 1 << 20,
        max_block_bytes: 0,
    };

    fn expect_header(buf: &[u8]) -> (Header, usize) {
        match parse_header(buf, true, &OPTS).unwrap() {
            Parsed::Done { value, consumed } => (value, consumed),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_a_header() {
        let h = header_bytes(
            &[
                (b"avro.schema", b"\"long\""),
                (b"avro.codec", b"deflate"),
                (b"x", b"y"),
            ],
            false,
        );
        let (hdr, used) = expect_header(&h);
        assert_eq!(used, h.len());
        assert_eq!(hdr.schema_json(), b"\"long\"");
        assert_eq!(hdr.codec, Codec::Deflate);
        assert_eq!(hdr.sync, [7u8; 16]);
        assert_eq!(hdr.get(b"x"), Some(&b"y"[..]));
        assert_eq!(hdr.get(b"nope"), None);
        // Negative block count with a size.
        let h = header_bytes(&[(b"avro.schema", b"\"long\"")], true);
        assert_eq!(expect_header(&h).0.codec, Codec::Null);
        // Trailing bytes after the header are not consumed.
        let mut h = header_bytes(&[(b"avro.schema", b"\"long\"")], false);
        let n = h.len();
        h.extend_from_slice(&[1, 2, 3]);
        assert_eq!(expect_header(&h).1, n);
    }

    #[test]
    fn unlimited_header_accepts_partial_metadata_block() {
        let h = header_bytes(
            &[
                (b"avro.schema", b"\"long\""),
                (b"a", b"1"),
                (b"b", b"2"),
                (b"c", b"3"),
                (b"d", b"4"),
                (b"e", b"5"),
            ],
            false,
        );
        let limits = OcfLimits::default();
        for cut in 0..h.len() {
            assert!(matches!(
                parse_header(&h[..cut], false, &limits).unwrap(),
                Parsed::NeedMore { .. }
            ));
            assert_eq!(
                parse_header(&h[..cut], true, &limits).unwrap_err().code,
                ErrorCode::Eof
            );
        }
        assert!(matches!(
            parse_header(&h, true, &limits).unwrap(),
            Parsed::Done { consumed, .. } if consumed == h.len()
        ));
    }

    #[test]
    fn last_duplicate_wins() {
        let h = header_bytes(
            &[
                (b"avro.schema", b"\"int\""),
                (b"k", b"1"),
                (b"avro.schema", b"\"long\""),
                (b"k", b"2"),
            ],
            false,
        );
        let (hdr, _) = expect_header(&h);
        assert_eq!(hdr.schema_json(), b"\"long\"");
        assert_eq!(hdr.get(b"k"), Some(&b"2"[..]));
        assert_eq!(hdr.metadata().len(), 2);
        assert_eq!(hdr.metadata()[0].0, b"avro.schema");
    }

    #[test]
    fn codecs() {
        for (name, codec) in [
            (&b"null"[..], Codec::Null),
            (b"deflate", Codec::Deflate),
            (b"snappy", Codec::Snappy),
            (b"zstandard", Codec::Zstandard),
            (b"bzip2", Codec::Bzip2),
            (b"xz", Codec::Xz),
        ] {
            let h = header_bytes(
                &[(b"avro.schema", b"\"int\""), (b"avro.codec", name)],
                false,
            );
            assert_eq!(expect_header(&h).0.codec, codec);
        }
        for bad in [&b"Deflate"[..], b"zstd", b"", b"null "] {
            let h = header_bytes(&[(b"avro.schema", b"\"int\""), (b"avro.codec", bad)], false);
            let e = parse_header(&h, true, &OPTS).unwrap_err();
            assert_eq!(e.code, ErrorCode::UnknownCodec);
            assert_eq!(
                e.message,
                format!(
                    "Unknown codec in data file: {}",
                    String::from_utf8_lossy(bad)
                )
            );
        }
    }

    #[test]
    fn header_errors() {
        let e = parse_header(b"Obj\x02", true, &OPTS).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (
                ErrorCode::BadMagic,
                "Invalid data file. Magic does not match: "
            )
        );
        let e = parse_header(b"{\"a\":1}", true, &OPTS).unwrap_err();
        assert_eq!(e.code, ErrorCode::BadMagic);
        let e = parse_header(b"Ob", true, &OPTS).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (ErrorCode::Eof, "EOF reached")
        );
        let h = header_bytes(&[(b"x", b"y")], false);
        let e = parse_header(&h, true, &OPTS).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (ErrorCode::NoSchema, "No schema in metadata")
        );
        // Negative length.
        let mut h = MAGIC.to_vec();
        h.extend(zz(1));
        h.extend(zz(-3));
        let e = parse_header(&h, true, &OPTS).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (ErrorCode::NegativeLength, "Cannot have negative length: -3")
        );
        // Absurd lengths are errors before anything is waited for.
        let mut h = MAGIC.to_vec();
        h.extend(zz(1));
        h.extend(zz(i64::from(i32::MAX)));
        let e = parse_header(&h, false, &OPTS).unwrap_err();
        assert_eq!(e.code, ErrorCode::HeaderTooLarge);
        // Absurd entry counts too.
        let mut h = MAGIC.to_vec();
        h.extend(zz(i64::MAX));
        assert_eq!(
            parse_header(&h, false, &OPTS).unwrap_err().code,
            ErrorCode::HeaderTooLarge
        );
        let mut h = MAGIC.to_vec();
        h.extend(zz(i64::MIN));
        assert_eq!(
            parse_header(&h, false, &OPTS).unwrap_err().code,
            ErrorCode::InvalidCount
        );
    }

    #[test]
    fn every_prefix_needs_more_or_hits_eof() {
        let h = header_bytes(
            &[(b"avro.schema", b"\"long\""), (b"avro.codec", b"snappy")],
            false,
        );
        for cut in 0..h.len() {
            match parse_header(&h[..cut], false, &OPTS).unwrap() {
                Parsed::NeedMore { needed } => assert!(
                    needed > cut && needed <= h.len(),
                    "cut {cut}: needed {needed}"
                ),
                other => panic!("cut {cut}: {other:?}"),
            }
            let e = parse_header(&h[..cut], true, &OPTS).unwrap_err();
            assert_eq!(e.code, ErrorCode::Eof, "cut {cut}");
        }
        // A caller that grows the window to `needed` each time converges.
        let mut have = 0;
        loop {
            match parse_header(&h[..have], false, &OPTS).unwrap() {
                Parsed::NeedMore { needed } => {
                    assert!(needed > have);
                    have = needed;
                }
                Parsed::Done { consumed, .. } => {
                    assert_eq!(consumed, h.len());
                    break;
                }
            }
        }
    }

    fn ctx(codec: Codec, min: u64) -> BlockCtx {
        BlockCtx {
            codec,
            min_record_size: min,
            max_block_bytes: 0,
        }
    }

    fn block(objects: i64, bytes: i64) -> Vec<u8> {
        let mut b = zz(objects);
        b.extend(zz(bytes));
        b
    }

    #[test]
    fn block_headers() {
        let b = block(3, 12);
        assert_eq!(
            parse_block_header(&b, false, &ctx(Codec::Null, 4)).unwrap(),
            Parsed::Done {
                value: BlockHeader::Block {
                    object_count: 3,
                    byte_count: 12
                },
                consumed: b.len()
            }
        );
        // Exactly at the bound is fine, one over is not.
        assert!(parse_block_header(&block(4, 12), false, &ctx(Codec::Null, 3)).is_ok());
        let e = parse_block_header(&block(5, 12), false, &ctx(Codec::Null, 3)).unwrap_err();
        assert_eq!(e.code, ErrorCode::ObjectCountExceedsPayload);
        assert_eq!(e.message, "Invalid data file. Object count 5 in block header cannot fit in 12 payload byte(s) for this schema");
        // Compressed blocks cannot be checked against the compressed size.
        assert!(parse_block_header(&block(500, 12), false, &ctx(Codec::Deflate, 3)).is_ok());
        assert!(check_object_count(500, 12, 3).is_err());
        assert!(check_object_count(4, 12, 3).is_ok());
        // Negative counts.
        let e = parse_block_header(&block(-1, 5), false, &ctx(Codec::Null, 0)).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (
                ErrorCode::NegativeObjectCount,
                "Invalid data file. Negative object count in block header: -1"
            )
        );
        let e = parse_block_header(&block(1, -5), false, &ctx(Codec::Null, 0)).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (
                ErrorCode::NegativeByteCount,
                "Invalid data file. Negative byte count in block header: -5"
            )
        );
        // Zero-size records are valid at any declared count; the payload itself remains empty and the
        // decoder's per-row/cancellation guards apply during materialization.
        assert!(parse_block_header(&block(i64::MAX, 0), false, &ctx(Codec::Null, 0)).is_ok());
        // Block size cap.
        let c = BlockCtx {
            codec: Codec::Null,
            min_record_size: 0,
            max_block_bytes: 100,
        };
        assert!(parse_block_header(&block(1, 100), false, &c).is_ok());
        assert_eq!(
            parse_block_header(&block(1, 101), false, &c)
                .unwrap_err()
                .code,
            ErrorCode::BlockTooLarge
        );
        // Clean end.
        assert_eq!(
            parse_block_header(b"", true, &ctx(Codec::Null, 1)).unwrap(),
            Parsed::Done {
                value: BlockHeader::End,
                consumed: 0
            }
        );
        assert_eq!(
            parse_block_header(b"", false, &ctx(Codec::Null, 1)).unwrap(),
            Parsed::NeedMore { needed: 1 }
        );
        // Truncated inside the header.
        let b = block(1 << 20, 1 << 25);
        for cut in 1..b.len() {
            assert!(matches!(
                parse_block_header(&b[..cut], false, &ctx(Codec::Deflate, 1)).unwrap(),
                Parsed::NeedMore { .. }
            ));
            assert_eq!(
                parse_block_header(&b[..cut], true, &ctx(Codec::Deflate, 1))
                    .unwrap_err()
                    .code,
                ErrorCode::Eof
            );
        }
    }

    #[test]
    fn sync_markers() {
        let s = [9u8; 16];
        assert_eq!(
            check_sync(&s, &s, false).unwrap(),
            Parsed::Done {
                value: (),
                consumed: 16
            }
        );
        assert_eq!(
            check_sync(&s[..15], &s, false).unwrap(),
            Parsed::NeedMore { needed: 16 }
        );
        assert_eq!(
            check_sync(&s[..15], &s, true).unwrap_err().message,
            "EOF reached"
        );
        let mut bad = s;
        bad[15] = 0;
        let e = check_sync(&bad, &s, false).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (ErrorCode::SyncMismatch, "Sync mismatch")
        );
    }
}
