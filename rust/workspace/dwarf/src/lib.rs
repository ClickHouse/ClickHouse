//! Pure-Rust parser behind ClickHouse's `DWARF` input format.
//!
//! It replaces the LLVM/`DB::Elf` based parser: the ELF container, `.debug_info` units, DIEs,
//! attribute values, line-table file names and address ranges are all decoded here, and rows are
//! written straight into caller-owned column buffers through the C ABI in `include/dwarf_rust.h`.
//!
//! Only `ffi` contains `unsafe` code. See `COMPATIBILITY.md` for the frozen behavior.
// `gimli::constants` names (`DW_FORM_data1`, ...) are used as match patterns.
#![allow(non_upper_case_globals)]

pub mod attribute;
pub mod control;
pub mod error;
pub mod ffi;
pub mod file;
pub mod line;
pub mod names;
/// Generated from LLVM's `Dwarf.def`; kept byte-exact, never reformatted.
#[rustfmt::skip]
pub mod names_generated;
pub mod outbuf;
pub mod probe;
pub mod range;
pub mod rows;
pub mod unit;
