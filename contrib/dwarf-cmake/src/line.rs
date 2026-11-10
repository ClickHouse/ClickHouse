#![forbid(unsafe_code)]

//! `DW_AT_stmt_list` filename tables (ledger section 9).
//!
//! The header is parsed with `gimli::LineProgramHeader`; only the path strings are resolved here,
//! with LLVM's string-form semantics (`Ctx::resolve_string`). Only the line program *prologue*
//! is read: the line number program itself is never run.

use crate::attribute::{Ctx, FormValue};
use crate::control::Work;
use crate::error::{DwarfError, Result};
use crate::file::Sections;
use crate::probe::describe;
use gimli::constants::*;
use gimli::{
    AttributeValue, DebugLine, DebugLineOffset, DwForm, EndianSlice, LittleEndian, Reader,
};

/// Zero-copy entry in a unit's filename table: either a literal sentinel or a checked `(offset, len)`
/// span inside `.debug_line`, `.debug_str`, or `.debug_line_str`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilenameEntry {
    Empty,
    Invalid,
    Error,
    Line { offset: usize, len: usize },
    Str { offset: usize, len: usize },
    LineStr { offset: usize, len: usize },
}

/// The dictionary the old format built: entry 0 is `""`, then `"<invalid>"` for DWARF <= 4 (file
/// numbers are one-based there), then every file name in table order.
#[derive(Debug, Clone, Default)]
pub struct FilenameTable {
    entries: Vec<FilenameEntry>,
}

impl FilenameTable {
    fn new() -> Self {
        Self {
            entries: vec![FilenameEntry::Empty],
        }
    }

    fn push(&mut self, entry: FilenameEntry) {
        self.entries.push(entry);
    }

    /// Old `filename_table_size`: the number of entries after the leading `""`.
    #[inline]
    pub fn size(&self) -> u64 {
        (self.entries.len() - 1) as u64
    }

    /// Resolves the string at dictionary index `idx` (0 is `""`) against `sections` without copying.
    #[inline]
    pub fn get<'a>(&self, idx: u64, sections: &Sections<'a>) -> &'a [u8] {
        let Ok(i) = usize::try_from(idx) else {
            return b"";
        };
        let Some(&entry) = self.entries.get(i) else {
            return b"";
        };
        match entry {
            FilenameEntry::Empty => b"",
            FilenameEntry::Invalid => b"<invalid>",
            FilenameEntry::Error => b"<error>",
            FilenameEntry::Line { offset, len } => sections
                .line
                .and_then(|s| s.get(offset..offset.saturating_add(len)))
                .unwrap_or(b""),
            FilenameEntry::Str { offset, len } => sections
                .str_
                .get(offset..offset.saturating_add(len))
                .unwrap_or(b""),
            FilenameEntry::LineStr { offset, len } => sections
                .line_str
                .get(offset..offset.saturating_add(len))
                .unwrap_or(b""),
        }
    }
}

/// Parses the line-program prologue at `offset` of `.debug_line` and returns the filename table.
pub fn parse_filename_table(
    ctx: &Ctx<'_>,
    offset: u64,
    work: &mut Work<'_>,
) -> Result<FilenameTable> {
    let line = ctx.sections.line.ok_or_else(|| {
        DwarfError::missing_aux_section("There are DW_AT_stmt_list but no .debug_line section")
    })?;
    let off = usize::try_from(offset)
        .map_err(|_| DwarfError::dwarf("DW_AT_stmt_list offset overflows usize"))?;
    let line_reader = EndianSlice::new(line, LittleEndian);
    let program = DebugLine::from(line_reader)
        .program(DebugLineOffset(off), ctx.params.address_size, None, None)
        .map_err(|e| {
            DwarfError::dwarf(format!(
                "Failed to parse .debug_line unit prologue: {}",
                describe(&e)
            ))
        })?;
    let header = program.header();
    let dir_ticks = header.include_directories().len().max(1);
    work.tick(dir_ticks)?;

    let mut table = FilenameTable::new();
    // DWARF v5 changed file indexes from 1-based to 0-based.
    if header.version() <= 4 {
        table.push(FilenameEntry::Invalid);
    }
    // `gimli` folds `DW_FORM_strp_sup` and `DW_FORM_GNU_strp_alt` into one variant and all `strx`
    // widths into another; the declared path form tells them apart again.
    let path_form = header
        .file_name_entry_format()
        .iter()
        .find(|f| f.content_type == DW_LNCT_path)
        .map(|f| f.form);

    for file in header.file_names() {
        work.tick(1)?;
        let entry = match file.path_name() {
            AttributeValue::String(s) => FilenameEntry::Line {
                offset: s.offset_from(line_reader),
                len: s.len(),
            },
            AttributeValue::DebugStrRef(o) => resolve_entry(ctx, form_ref(DW_FORM_strp, o.0)),
            AttributeValue::DebugLineStrRef(o) => {
                resolve_entry(ctx, form_ref(DW_FORM_line_strp, o.0))
            }
            AttributeValue::DebugStrRefSup(o) => path_form
                .map(|f| resolve_entry(ctx, form_ref(f, o.0)))
                .unwrap_or(FilenameEntry::Error),
            AttributeValue::DebugStrOffsetsIndex(i) => path_form
                .map(|f| resolve_entry(ctx, form_ref(f, i.0)))
                .unwrap_or(FilenameEntry::Error),
            // Any other form is not of the string class: LLVM's `getAsCString` fails.
            _ => FilenameEntry::Error,
        };
        table.push(entry);
    }
    Ok(table)
}

#[inline]
fn resolve_entry(ctx: &Ctx<'_>, v: FormValue<'_>) -> FilenameEntry {
    match ctx.resolve_pool_string(&v) {
        Ok((true, offset, len)) => FilenameEntry::LineStr { offset, len },
        Ok((false, offset, len)) => FilenameEntry::Str { offset, len },
        Err(_) => FilenameEntry::Error,
    }
}

#[inline]
fn form_ref<'a>(form: DwForm, value: usize) -> FormValue<'a> {
    FormValue {
        form,
        uval: value as u64,
        data: &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_indexing() {
        let sections = Sections {
            info: b"",
            abbrev: b"",
            line: Some(b"a.c\0"),
            addr: None,
            addr_llvm: b"",
            ranges: None,
            rnglists: None,
            str_: b"",
            line_str: b"",
            str_offsets: b"",
        };
        let mut t = FilenameTable::new();
        t.push(FilenameEntry::Invalid);
        t.push(FilenameEntry::Line { offset: 0, len: 3 });
        t.push(FilenameEntry::Empty);
        assert_eq!(t.size(), 3);
        assert_eq!(t.get(0, &sections), b"");
        assert_eq!(t.get(1, &sections), b"<invalid>");
        assert_eq!(t.get(2, &sections), b"a.c");
        assert_eq!(t.get(3, &sections), b"");
        assert_eq!(t.get(4, &sections), b"");
        assert_eq!(t.get(u64::MAX, &sections), b"");
    }
}
