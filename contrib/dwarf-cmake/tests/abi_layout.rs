//! The Rust `#[repr(C)]` structs and constants against `include/dwarf_rust.h`: numeric constants
//! parsed out of the header, and size/alignment/offsets checked by the system C and C++
//! compilers (skipped, loudly, when no compiler is installed).
#![allow(unsafe_code)]

use _ch_rust_dwarf::error::StatusCode;
use _ch_rust_dwarf::ffi::*;
use _ch_rust_dwarf::outbuf::*;
use core::mem::{align_of, offset_of, size_of};
use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::path::PathBuf;
use std::process::Command;

fn header_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("include/dwarf_rust.h")
}

/// `NAME = value` pairs of every enum in the header (`value` is a number or `(1u << n)`).
fn header_constants() -> HashMap<String, u64> {
    let text = std::fs::read_to_string(header_path()).unwrap();
    let mut out = HashMap::new();
    for line in text.lines() {
        let line = line.trim().trim_end_matches(',');
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let (name, value) = (name.trim(), value.trim());
        if !name.starts_with("CH_DWARF_") {
            continue;
        }
        let v = if let Some(shift) = value
            .strip_prefix("(1u << ")
            .and_then(|s| s.strip_suffix(')'))
        {
            1u64 << shift.parse::<u32>().unwrap()
        } else if let Some(neg) = value.strip_prefix('-') {
            (neg.parse::<i64>().unwrap().wrapping_neg()) as u64
        } else {
            value.parse::<u64>().unwrap()
        };
        out.insert(name.to_string(), v);
    }
    out
}

fn constant(map: &HashMap<String, u64>, name: &str) -> u64 {
    *map.get(name)
        .unwrap_or_else(|| panic!("{name} missing from the header"))
}

#[test]
fn status_codes_match_the_header() {
    let h = header_constants();
    let expected: [(&str, StatusCode); 11] = [
        ("CH_DWARF_OK", StatusCode::Ok),
        ("CH_DWARF_CHUNK_READY", StatusCode::ChunkReady),
        ("CH_DWARF_UNIT_DONE", StatusCode::UnitDone),
        ("CH_DWARF_CANCELLED", StatusCode::Cancelled),
        ("CH_DWARF_ERR_INVALID_ARG", StatusCode::ErrInvalidArg),
        ("CH_DWARF_ERR_ELF", StatusCode::ErrElf),
        (
            "CH_DWARF_ERR_MISSING_REQUIRED_SECTION",
            StatusCode::ErrMissingRequiredSection,
        ),
        ("CH_DWARF_ERR_DWARF", StatusCode::ErrDwarf),
        (
            "CH_DWARF_ERR_MISSING_AUX_SECTION",
            StatusCode::ErrMissingAuxSection,
        ),
        ("CH_DWARF_ERR_CALLBACK", StatusCode::ErrCallback),
        ("CH_DWARF_ERR_INTERNAL", StatusCode::ErrInternal),
    ];
    for (name, code) in expected {
        assert_eq!(constant(&h, name) as i64, i64::from(code as i32), "{name}");
    }
}

#[test]
fn enums_match_the_header() {
    let h = header_constants();
    assert_eq!(
        constant(&h, "CH_DWARF_CANCEL_CONTINUE"),
        CH_DWARF_CANCEL_CONTINUE as u64
    );
    assert_eq!(
        constant(&h, "CH_DWARF_CANCEL_STOP"),
        CH_DWARF_CANCEL_STOP as u64
    );
    assert_eq!(
        constant(&h, "CH_DWARF_CANCEL_FAILED"),
        CH_DWARF_CANCEL_FAILED as u64
    );
    assert_eq!(
        constant(&h, "CH_DWARF_RESERVE_OK"),
        CH_DWARF_RESERVE_OK as u64
    );
    assert_eq!(
        constant(&h, "CH_DWARF_RESERVE_FAILED"),
        CH_DWARF_RESERVE_FAILED as u64
    );
    assert_eq!(constant(&h, "CH_DWARF_NAME_TABLE_TAG"), 0);
    assert_eq!(constant(&h, "CH_DWARF_NAME_TABLE_ATTR"), 1);
    assert_eq!(constant(&h, "CH_DWARF_NAME_TABLE_FORM"), 2);
    assert_eq!(constant(&h, "CH_DWARF_SINK_COUNT"), SINK_COUNT as u64);

    let cols = [
        ("CH_DWARF_COL_OFFSET", COL_OFFSET),
        ("CH_DWARF_COL_SIZE", COL_SIZE),
        ("CH_DWARF_COL_TAG", COL_TAG),
        ("CH_DWARF_COL_UNIT_NAME", COL_UNIT_NAME),
        ("CH_DWARF_COL_UNIT_OFFSET", COL_UNIT_OFFSET),
        ("CH_DWARF_COL_ANCESTOR_TAGS", COL_ANCESTOR_TAGS),
        ("CH_DWARF_COL_ANCESTOR_OFFSETS", COL_ANCESTOR_OFFSETS),
        ("CH_DWARF_COL_NAME", COL_NAME),
        ("CH_DWARF_COL_LINKAGE_NAME", COL_LINKAGE_NAME),
        ("CH_DWARF_COL_DECL_FILE", COL_DECL_FILE),
        ("CH_DWARF_COL_DECL_LINE", COL_DECL_LINE),
        ("CH_DWARF_COL_RANGES", COL_RANGES),
        ("CH_DWARF_COL_ATTR_NAME", COL_ATTR_NAME),
        ("CH_DWARF_COL_ATTR_FORM", COL_ATTR_FORM),
        ("CH_DWARF_COL_ATTR_INT", COL_ATTR_INT),
        ("CH_DWARF_COL_ATTR_STR", COL_ATTR_STR),
    ];
    for (name, bit) in cols {
        assert_eq!(constant(&h, name), u64::from(bit), "{name}");
    }
    assert_eq!(cols.iter().fold(0, |m, (_, b)| m | b), ALL_COLUMNS_MASK);

    let sinks = [
        ("CH_DWARF_SINK_OFFSET", SinkId::Offset),
        ("CH_DWARF_SINK_SIZE", SinkId::Size),
        ("CH_DWARF_SINK_TAG", SinkId::Tag),
        ("CH_DWARF_SINK_ANCESTOR_TAGS", SinkId::AncestorTags),
        ("CH_DWARF_SINK_ANCESTOR_OFFSETS", SinkId::AncestorOffsets),
        (
            "CH_DWARF_SINK_ANCESTOR_ARRAY_OFFSETS",
            SinkId::AncestorArrayOffsets,
        ),
        ("CH_DWARF_SINK_NAME_CHARS", SinkId::NameChars),
        ("CH_DWARF_SINK_NAME_OFFSETS", SinkId::NameOffsets),
        ("CH_DWARF_SINK_LINKAGE_NAME_CHARS", SinkId::LinkageNameChars),
        (
            "CH_DWARF_SINK_LINKAGE_NAME_OFFSETS",
            SinkId::LinkageNameOffsets,
        ),
        ("CH_DWARF_SINK_DECL_FILE_CHARS", SinkId::DeclFileChars),
        ("CH_DWARF_SINK_DECL_FILE_OFFSETS", SinkId::DeclFileOffsets),
        ("CH_DWARF_SINK_DECL_LINE", SinkId::DeclLine),
        ("CH_DWARF_SINK_RANGES_START", SinkId::RangesStart),
        ("CH_DWARF_SINK_RANGES_END", SinkId::RangesEnd),
        ("CH_DWARF_SINK_RANGES_OFFSETS", SinkId::RangesOffsets),
        ("CH_DWARF_SINK_ATTR_NAME", SinkId::AttrName),
        ("CH_DWARF_SINK_ATTR_FORM", SinkId::AttrForm),
        ("CH_DWARF_SINK_ATTR_INT", SinkId::AttrInt),
        ("CH_DWARF_SINK_ATTR_STR_CHARS", SinkId::AttrStrChars),
        ("CH_DWARF_SINK_ATTR_STR_OFFSETS", SinkId::AttrStrOffsets),
        ("CH_DWARF_SINK_ATTR_OFFSETS", SinkId::AttrOffsets),
    ];
    assert_eq!(sinks.len(), SINK_COUNT);
    for (name, id) in sinks {
        assert_eq!(constant(&h, name), id as u64, "{name}");
    }
    let text = std::fs::read_to_string(header_path()).unwrap();
    assert!(text.contains(&format!(
        "#define CH_DWARF_ERROR_BUF_SIZE {CH_DWARF_ERROR_BUF_SIZE}u"
    )));
}

#[test]
fn struct_layout_constants() {
    // Hand-computed from the header on LP64 (8-byte pointers and size_t).
    assert_eq!(
        (size_of::<ChDwarfOptions>(), align_of::<ChDwarfOptions>()),
        (8, 4)
    );
    assert_eq!(
        (
            size_of::<ChDwarfProjection>(),
            align_of::<ChDwarfProjection>()
        ),
        (16, 4)
    );
    assert_eq!(
        (size_of::<ChDwarfSink>(), align_of::<ChDwarfSink>()),
        (32, 8)
    );
    assert_eq!(
        (
            size_of::<ChDwarfStringSink>(),
            align_of::<ChDwarfStringSink>()
        ),
        (64, 8)
    );
    assert_eq!(
        (
            size_of::<ChDwarfBatchSinks>(),
            align_of::<ChDwarfBatchSinks>()
        ),
        (22 * 32, 8)
    );
    assert_eq!(
        (
            size_of::<ChDwarfReserveRequest>(),
            align_of::<ChDwarfReserveRequest>()
        ),
        (16, 8)
    );
    assert_eq!(
        (size_of::<ChDwarfProgress>(), align_of::<ChDwarfProgress>()),
        (40, 8)
    );
    assert_eq!(offset_of!(ChDwarfSink, data), 0);
    assert_eq!(offset_of!(ChDwarfSink, len), 8);
    assert_eq!(offset_of!(ChDwarfSink, cap), 16);
    assert_eq!(offset_of!(ChDwarfSink, elem_size), 24);
    assert_eq!(offset_of!(ChDwarfSink, reserved), 28);
    assert_eq!(offset_of!(ChDwarfStringSink, offsets), 32);
    assert_eq!(offset_of!(ChDwarfProgress, bytes_advanced), 8);
    assert_eq!(offset_of!(ChDwarfProgress, unit_name_ptr), 16);
    assert_eq!(offset_of!(ChDwarfProgress, unit_name_len), 24);
    assert_eq!(offset_of!(ChDwarfProgress, unit_done), 32);
    assert_eq!(offset_of!(ChDwarfReserveRequest, min_cap), 8);
}

fn find_compiler(candidates: &[&str]) -> Option<String> {
    candidates
        .iter()
        .find(|c| {
            Command::new(c)
                .arg("--version")
                .output()
                .is_ok_and(|o| o.status.success())
        })
        .map(|c| (*c).to_string())
}

fn layout_source() -> String {
    let mut s = String::from("#include \"dwarf_rust.h\"\n#include <stddef.h>\n");
    let mut check = |ty: &str, size: usize, align: usize| {
        s += &format!("_Static_assert(sizeof({ty}) == {size}, \"sizeof {ty}\");\n");
        s += &format!("_Static_assert(_Alignof({ty}) == {align}, \"alignof {ty}\");\n");
    };
    check(
        "ChDwarfOptions",
        size_of::<ChDwarfOptions>(),
        align_of::<ChDwarfOptions>(),
    );
    check(
        "ChDwarfProjection",
        size_of::<ChDwarfProjection>(),
        align_of::<ChDwarfProjection>(),
    );
    check(
        "ChDwarfSink",
        size_of::<ChDwarfSink>(),
        align_of::<ChDwarfSink>(),
    );
    check(
        "ChDwarfStringSink",
        size_of::<ChDwarfStringSink>(),
        align_of::<ChDwarfStringSink>(),
    );
    check(
        "ChDwarfBatchSinks",
        size_of::<ChDwarfBatchSinks>(),
        align_of::<ChDwarfBatchSinks>(),
    );
    check(
        "ChDwarfReserveRequest",
        size_of::<ChDwarfReserveRequest>(),
        align_of::<ChDwarfReserveRequest>(),
    );
    check(
        "ChDwarfProgress",
        size_of::<ChDwarfProgress>(),
        align_of::<ChDwarfProgress>(),
    );
    let mut off = |ty: &str, field: &str, at: usize| {
        s += &format!(
            "_Static_assert(offsetof({ty}, {field}) == {at}, \"offsetof {ty}.{field}\");\n"
        );
    };
    off(
        "ChDwarfOptions",
        "reserved",
        offset_of!(ChDwarfOptions, reserved),
    );
    off(
        "ChDwarfProjection",
        "column_mask",
        offset_of!(ChDwarfProjection, column_mask),
    );
    off(
        "ChDwarfProjection",
        "need_ancestor_offsets_vec",
        offset_of!(ChDwarfProjection, need_ancestor_offsets_vec),
    );
    off(
        "ChDwarfProjection",
        "need_attr_offsets_vec",
        offset_of!(ChDwarfProjection, need_attr_offsets_vec),
    );
    off(
        "ChDwarfProjection",
        "reserved",
        offset_of!(ChDwarfProjection, reserved),
    );
    off("ChDwarfSink", "data", offset_of!(ChDwarfSink, data));
    off("ChDwarfSink", "len", offset_of!(ChDwarfSink, len));
    off("ChDwarfSink", "cap", offset_of!(ChDwarfSink, cap));
    off(
        "ChDwarfSink",
        "elem_size",
        offset_of!(ChDwarfSink, elem_size),
    );
    off("ChDwarfSink", "reserved", offset_of!(ChDwarfSink, reserved));
    off(
        "ChDwarfStringSink",
        "chars",
        offset_of!(ChDwarfStringSink, chars),
    );
    off(
        "ChDwarfStringSink",
        "offsets",
        offset_of!(ChDwarfStringSink, offsets),
    );
    off(
        "ChDwarfBatchSinks",
        "sinks",
        offset_of!(ChDwarfBatchSinks, sinks),
    );
    off(
        "ChDwarfReserveRequest",
        "sink_id",
        offset_of!(ChDwarfReserveRequest, sink_id),
    );
    off(
        "ChDwarfReserveRequest",
        "reserved",
        offset_of!(ChDwarfReserveRequest, reserved),
    );
    off(
        "ChDwarfReserveRequest",
        "min_cap",
        offset_of!(ChDwarfReserveRequest, min_cap),
    );
    off("ChDwarfProgress", "rows", offset_of!(ChDwarfProgress, rows));
    off(
        "ChDwarfProgress",
        "bytes_advanced",
        offset_of!(ChDwarfProgress, bytes_advanced),
    );
    off(
        "ChDwarfProgress",
        "unit_name_ptr",
        offset_of!(ChDwarfProgress, unit_name_ptr),
    );
    off(
        "ChDwarfProgress",
        "unit_name_len",
        offset_of!(ChDwarfProgress, unit_name_len),
    );
    off(
        "ChDwarfProgress",
        "unit_done",
        offset_of!(ChDwarfProgress, unit_done),
    );
    off(
        "ChDwarfProgress",
        "reserved",
        offset_of!(ChDwarfProgress, reserved),
    );
    // The exported functions must be assignable from the header prototypes.
    s += "\nvoid check_prototypes(void)\n{\n";
    s += "    int32_t (*open)(const uint8_t *, size_t, const ChDwarfOptions *, ChDwarfFile **, char *) = ch_dwarf_file_open;\n";
    s += "    int32_t (*count)(const ChDwarfFile *, size_t *) = ch_dwarf_file_unit_count;\n";
    s += "    int32_t (*unit_open)(const ChDwarfFile *, size_t, ChDwarfUnit **, uint64_t *, char *) = ch_dwarf_unit_open;\n";
    s += "    int32_t (*parse)(const ChDwarfFile *, ChDwarfUnit *, const ChDwarfProjection *, ChDwarfBatchSinks *, ChDwarfReserveFn, ChDwarfCancelFn, void *, size_t, ChDwarfProgress *, char *) = ch_dwarf_unit_parse_chunk;\n";
    s += "    int32_t (*names)(int32_t, ChDwarfStringSink *, ChDwarfStringReserveFn, void *, char *) = ch_dwarf_write_name_table;\n";
    s += "    void (*unit_free)(ChDwarfUnit *) = ch_dwarf_unit_free;\n";
    s += "    void (*file_free)(ChDwarfFile *) = ch_dwarf_file_free;\n";
    s += "    (void)open; (void)count; (void)unit_open; (void)parse; (void)names; (void)unit_free; (void)file_free;\n}\n";
    s
}

fn compile(compiler: &str, std_flag: &str, lang: &str, source: &str, tag: &str) {
    let dir = std::env::temp_dir().join(format!("dwarf_abi_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ext = if lang == "c" { "c" } else { "cpp" };
    let src = dir.join(format!("layout.{ext}"));
    std::fs::write(&src, source).unwrap();
    let include = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("include");
    let out = Command::new(compiler)
        .args([
            "-x",
            lang,
            std_flag,
            "-fsyntax-only",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-I",
        ])
        .arg(&include)
        .arg(&src)
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        out.status.success(),
        "{compiler} {std_flag} rejected the header or layout assertions:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn header_compiles_as_c99_and_cxx_with_matching_layout() {
    let source = layout_source();
    match find_compiler(&["cc", "clang", "gcc"]) {
        Some(cc) => compile(&cc, "-std=c99", "c", &source, "c99"),
        None => eprintln!("SKIPPED: no C compiler found; C99 layout check not run"),
    }
    match find_compiler(&["c++", "clang++", "g++"]) {
        // `_Static_assert` is spelled `static_assert` in C++.
        Some(cxx) => {
            let cxx_source = format!(
                "#define _Static_assert static_assert\n#define _Alignof alignof\n#include <cstddef>\n{source}"
            );
            compile(&cxx, "-std=c++17", "c++", &cxx_source, "cxx17");
        }
        None => eprintln!("SKIPPED: no C++ compiler found; C++ layout check not run"),
    }
}

/// The exported Rust functions have exactly the types the header declares.
#[test]
fn exported_function_types() {
    type Open = unsafe extern "C" fn(
        *const u8,
        usize,
        *const ChDwarfOptions,
        *mut *mut ChDwarfFile,
        *mut c_char,
    ) -> i32;
    type Count = unsafe extern "C" fn(*const ChDwarfFile, *mut usize) -> i32;
    type UnitOpen = unsafe extern "C" fn(
        *const ChDwarfFile,
        usize,
        *mut *mut ChDwarfUnit,
        *mut u64,
        *mut c_char,
    ) -> i32;
    type Parse = unsafe extern "C" fn(
        *const ChDwarfFile,
        *mut ChDwarfUnit,
        *const ChDwarfProjection,
        *mut ChDwarfBatchSinks,
        ChDwarfReserveFn,
        ChDwarfCancelFn,
        *mut c_void,
        usize,
        *mut ChDwarfProgress,
        *mut c_char,
    ) -> i32;
    type Names = unsafe extern "C" fn(
        i32,
        *mut ChDwarfStringSink,
        ChDwarfStringReserveFn,
        *mut c_void,
        *mut c_char,
    ) -> i32;
    let _: Open = ch_dwarf_file_open;
    let _: Count = ch_dwarf_file_unit_count;
    let _: UnitOpen = ch_dwarf_unit_open;
    let _: Parse = ch_dwarf_unit_parse_chunk;
    let _: Names = ch_dwarf_write_name_table;
    let _: unsafe extern "C" fn(*mut ChDwarfUnit) = ch_dwarf_unit_free;
    let _: unsafe extern "C" fn(*mut ChDwarfFile) = ch_dwarf_file_free;
}

/// Unit handles move between worker threads (`Send`) but are never shared (`!Sync`).
#[test]
fn unit_handle_is_send_but_not_sync() {
    fn is_send<T: Send>() {}
    is_send::<_ch_rust_dwarf::unit::UnitState>();
    is_send::<_ch_rust_dwarf::file::FileInner>();

    // Ambiguity trick: this only has a unique impl when `T` is not `Sync`.
    trait NotSync<A> {
        fn check() {}
    }
    struct Yes;
    impl<T: ?Sized> NotSync<()> for T {}
    impl<T: ?Sized + Sync> NotSync<Yes> for T {}
    // Compiles only if `UnitState: !Sync`; otherwise the call below is ambiguous.
    <_ch_rust_dwarf::unit::UnitState as NotSync<_>>::check();
}
