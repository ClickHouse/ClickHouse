#![forbid(unsafe_code)]

//! `DW_AT_stmt_list` filename tables (ledger section 9).
//!
//! The header is parsed with `gimli::LineProgramHeader`; only the path strings are resolved here,
//! with LLVM's string-form semantics (`Ctx::resolve_string`). Only the line program *prologue*
//! is read: the line number program itself is never run.

use crate::attribute::{Ctx, FormValue};
use crate::error::{DwarfError, Result};
use crate::probe::describe;
use gimli::constants::*;
use gimli::{AttributeValue, DebugLine, DebugLineOffset, DwForm, EndianSlice, LittleEndian};

/// The dictionary the old format built: entry 0 is `""`, then `"<invalid>"` for DWARF <= 4 (file
/// numbers are one-based there), then every file name in table order.
#[derive(Debug, Clone, Default)]
pub struct FilenameTable {
    data: Vec<u8>,
    ends: Vec<usize>,
}

impl FilenameTable {
    fn new() -> Self {
        let mut t = Self::default();
        t.push(b"");
        t
    }

    fn push(&mut self, s: &[u8]) {
        self.data.extend_from_slice(s);
        self.ends.push(self.data.len());
    }

    /// Old `filename_table_size`: the number of entries after the leading `""`.
    #[inline]
    pub fn size(&self) -> u64 {
        (self.ends.len() - 1) as u64
    }

    /// The string at dictionary index `idx` (0 is `""`).
    #[inline]
    pub fn get(&self, idx: u64) -> &[u8] {
        let Ok(i) = usize::try_from(idx) else {
            return b"";
        };
        let Some(&end) = self.ends.get(i) else {
            return b"";
        };
        let start = if i == 0 { 0 } else { self.ends[i - 1] };
        &self.data[start..end]
    }
}

/// Parses the line-program prologue at `offset` of `.debug_line` and returns the filename table.
pub fn parse_filename_table(ctx: &Ctx<'_>, offset: u64) -> Result<FilenameTable> {
    let line = ctx.sections.line.ok_or_else(|| {
        DwarfError::missing_aux_section("There are DW_AT_stmt_list but no .debug_line section")
    })?;
    let off = usize::try_from(offset)
        .map_err(|_| DwarfError::dwarf("DW_AT_stmt_list offset overflows usize"))?;
    let program = DebugLine::from(EndianSlice::new(line, LittleEndian))
        .program(DebugLineOffset(off), ctx.params.address_size, None, None)
        .map_err(|e| {
            DwarfError::dwarf(format!(
                "Failed to parse .debug_line unit prologue: {}",
                describe(&e)
            ))
        })?;
    let header = program.header();

    let mut table = FilenameTable::new();
    // DWARF v5 changed file indexes from 1-based to 0-based.
    if header.version() <= 4 {
        table.push(b"<invalid>");
    }
    // `gimli` folds `DW_FORM_strp_sup` and `DW_FORM_GNU_strp_alt` into one variant and all `strx`
    // widths into another; the declared path form tells them apart again.
    let path_form = header
        .file_name_entry_format()
        .iter()
        .find(|f| f.content_type == DW_LNCT_path)
        .map(|f| f.form);

    for file in header.file_names() {
        let resolved = match file.path_name() {
            AttributeValue::String(s) => Some(FormValue {
                form: DW_FORM_string,
                uval: u64::MAX,
                data: s.slice(),
            }),
            AttributeValue::DebugStrRef(o) => Some(form_ref(DW_FORM_strp, o.0)),
            AttributeValue::DebugLineStrRef(o) => Some(form_ref(DW_FORM_line_strp, o.0)),
            AttributeValue::DebugStrRefSup(o) => path_form.map(|f| form_ref(f, o.0)),
            AttributeValue::DebugStrOffsetsIndex(i) => path_form.map(|f| form_ref(f, i.0)),
            // Any other form is not of the string class: LLVM's `getAsCString` fails.
            _ => None,
        };
        let name: &[u8] = match resolved.map(|v| ctx.resolve_string(&v)) {
            Some(Ok(s)) => s,
            _ => b"<error>",
        };
        table.push(name);
    }
    Ok(table)
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
        let mut t = FilenameTable::new();
        t.push(b"<invalid>");
        t.push(b"a.c");
        t.push(b"");
        assert_eq!(t.size(), 3);
        assert_eq!(t.get(0), b"");
        assert_eq!(t.get(1), b"<invalid>");
        assert_eq!(t.get(2), b"a.c");
        assert_eq!(t.get(3), b"");
        assert_eq!(t.get(4), b"");
        assert_eq!(t.get(u64::MAX), b"");
    }
}
