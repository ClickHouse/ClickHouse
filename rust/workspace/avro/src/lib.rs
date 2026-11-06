//! Memory-safe Avro decoder for ClickHouse.
//!
//! Scope: everything that parses attacker-controlled Avro bytes.
//!
//! * [`json`], [`schema`], [`canonical`]: schema JSON parser, schema compiler
//!   (Apache Avro 1.12 specification) and Parsing Canonical Form.
//! * [`varint`]: zigzag varints.
//! * [`program`], [`exec`]: a flat op list compiled from a schema plus a
//!   description of the caller's output buffers, and a resumable,
//!   non-recursive executor that decodes a block of rows into those buffers.
//! * [`ocf`]: Object Container File header and block framing on a byte window.
//! * [`outbuf`]: the output buffers of the executor, which may be uninitialized.
//! * `ffi`: the C ABI; the only module that handles raw pointers.
//!
//! Rules that hold for the whole crate:
//!
//! * Nothing here allocates memory proportional to attacker-chosen *counts*.
//!   Allocations are bounded by the schema text, the plan, and fixed-size
//!   stacks; output memory is always owned by the caller.
//! * Nothing here recurses on attacker-controlled depth. Schema parsing,
//!   schema analysis, program compilation and execution use explicit stacks
//!   with explicit limits.
//! * Nothing here sleeps, spawns threads or reads global state.

use std::fmt;

pub mod canonical;
pub mod exec;
pub mod ffi;
pub mod json;
pub mod ocf;
pub mod outbuf;
pub mod program;
pub mod schema;
pub mod varint;

/// Hard cap applied to nesting depths when the caller asks for "unlimited"
/// (a limit of `0`). Explicit stacks make this safe; it only bounds memory.
pub const HARD_MAX_DEPTH: usize = 100_000;

/// Per-array and per-block cap on items whose minimum encoded size is zero
/// (`null`, an empty record, and arrays or maps of those). Such items cost
/// no input bytes, so without a cap a ten-byte header could ask for 2^62 of
/// them in each row. Documented in `include/avro_rust.h`; tested at the boundary
/// and across decoder calls.
pub const MAX_ZERO_SIZE_ITEMS: u64 = 1 << 24;

/// Cap on the number of rows of a block whose rows can occupy zero bytes.
/// Applied by [`ocf::parse_block_header`].
pub const MAX_ZERO_SIZE_ROWS_PER_BLOCK: u64 = 1 << 28;

/// Stable numeric error codes. The numbers are part of the C ABI (the FFI
/// returns `-code`) and never change meaning once assigned.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// A caller bug: null pointer, bad enum value, impossible argument.
    InvalidArgument = 1,
    /// Schema JSON is not valid JSON.
    JsonSyntax = 2,
    /// "JSON nesting depth exceeds the limit".
    JsonDepth = 3,
    /// Schema text or node count above the configured cap.
    SchemaTooLarge = 4,
    /// Schema is valid JSON but not a valid Avro schema.
    SchemaInvalid = 5,
    /// "Avro schema nesting depth exceeds the limit".
    SchemaDepth = 6,
    /// "EOF reached": the input ended inside a value.
    Eof = 10,
    /// "Invalid Avro varint".
    InvalidVarint = 11,
    /// "Value out of range for Avro int: N".
    IntOutOfRange = 12,
    /// "Invalid value for bool".
    InvalidBool = 13,
    /// "Cannot have negative length: N".
    NegativeLength = 14,
    /// "Avro enum index N is out of range, ...".
    EnumIndexOutOfRange = 15,
    /// "Union index out of boundary: got N, the number of branches is M".
    UnionIndexOutOfRange = 16,
    /// A block count that is not a valid count (negative minimum, overflow).
    InvalidCount = 17,
    /// Data nesting deeper than the configured limit.
    DataTooDeep = 18,
    /// More zero-size items than [`MAX_ZERO_SIZE_ITEMS`].
    TooManyItems = 19,
    /// An array or map block count that cannot fit in the remaining bytes.
    CountExceedsPayload = 20,
    /// A decoded value violates a range check requested by the plan.
    ValueOutOfRange = 21,
    /// A decimal value is longer than the target width.
    DecimalTooWide = 22,
    /// "Invalid data file. Magic does not match: ".
    BadMagic = 30,
    /// "No schema in metadata".
    NoSchema = 31,
    /// "Unknown codec in data file: X".
    UnknownCodec = 32,
    /// "Sync mismatch".
    SyncMismatch = 33,
    /// "Invalid data file. Negative object count in block header: N".
    NegativeObjectCount = 34,
    /// "Invalid data file. Negative byte count in block header: N".
    NegativeByteCount = 35,
    /// "Invalid data file. Object count N in block header cannot fit in ...".
    ObjectCountExceedsPayload = 36,
    /// Header or metadata above the configured size cap.
    HeaderTooLarge = 37,
    /// The block is larger than the configured cap.
    BlockTooLarge = 38,
    /// The plan does not match the schema or the sinks.
    PlanInvalid = 40,
    /// The program or plan is larger than the internal cap.
    ProgramTooLarge = 41,
    /// A bug in this crate (including a caught panic).
    Internal = 99,
}

/// An error with a stable code and a human readable message. Messages for the
/// errors that ClickHouse tests grep for match the C++ fork character for
/// character.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Error {
        Error {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;
