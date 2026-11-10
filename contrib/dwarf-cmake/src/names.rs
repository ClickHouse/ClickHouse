#![forbid(unsafe_code)]

use crate::error::{DwarfError, Result};
use crate::names_generated::{ATE_NAMES, ATTR_NAMES, FORM_NAMES, LANG_NAMES, TAG_NAMES};
use crate::outbuf::StringSinkAccess;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameTableKind {
    Tag = 0,
    Attr = 1,
    Form = 2,
}

impl NameTableKind {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::Tag),
            1 => Some(Self::Attr),
            2 => Some(Self::Form),
            _ => None,
        }
    }

    pub fn sparse_table(self) -> &'static [(u16, &'static str)] {
        match self {
            Self::Tag => TAG_NAMES,
            Self::Attr => ATTR_NAMES,
            Self::Form => FORM_NAMES,
        }
    }
}

#[inline]
pub fn lookup_sparse(table: &'static [(u16, &'static str)], id: u16) -> Option<&'static str> {
    table
        .binary_search_by_key(&id, |&(k, _)| k)
        .ok()
        .map(|idx| table[idx].1)
}

#[inline]
pub fn lookup_language_name(code: u32) -> &'static str {
    let Ok(id) = u16::try_from(code) else {
        return "";
    };
    lookup_sparse(LANG_NAMES, id).unwrap_or("")
}

#[inline]
pub fn lookup_encoding_name(code: u32) -> &'static str {
    let Ok(id) = u16::try_from(code) else {
        return "";
    };
    lookup_sparse(ATE_NAMES, id).unwrap_or("")
}

/// Writes all 65,536 dictionary entries (`0` -> `""`, `1..=65535` -> stripped name or `"unknown: N"`)
/// into caller-owned `ColumnString` buffers (`chars` + `offsets`).
pub fn write_full_name_table(kind: NameTableKind, sink: &mut dyn StringSinkAccess) -> Result<()> {
    let table = kind.sparse_table();
    // Compute exact total byte count first so we reserve once.
    let mut total_bytes: usize = 0;
    let mut table_idx = 0usize;
    for id in 1u32..=u16::MAX as u32 {
        let id16 = id as u16;
        let len = if table_idx < table.len() && table[table_idx].0 == id16 {
            let s = table[table_idx].1;
            table_idx += 1;
            s.len()
        } else {
            9 + decimal_len(id)
        };
        total_bytes = total_bytes
            .checked_add(len)
            .ok_or_else(|| DwarfError::internal("name table byte length overflow"))?;
    }

    let total_rows = (u16::MAX as usize) + 1;
    sink.reserve_exact(total_bytes, total_rows)?;

    // Entry 0 is empty string "".
    sink.push_str_unchecked(b"")?;

    let mut buf = *b"unknown: 00000";
    table_idx = 0;
    for id in 1u32..=u16::MAX as u32 {
        let id16 = id as u16;
        if table_idx < table.len() && table[table_idx].0 == id16 {
            let s = table[table_idx].1;
            table_idx += 1;
            sink.push_str_unchecked(s.as_bytes())?;
        } else {
            let digits = decimal_len(id);
            let end = 9 + digits;
            let mut n = id;
            let mut pos = end;
            while pos > 9 {
                pos -= 1;
                buf[pos] = b'0' + (n % 10) as u8;
                n /= 10;
            }
            sink.push_str_unchecked(&buf[..end])?;
        }
    }
    Ok(())
}

#[inline]
const fn decimal_len(n: u32) -> usize {
    if n >= 10_000 {
        5
    } else if n >= 1_000 {
        4
    } else if n >= 100 {
        3
    } else if n >= 10 {
        2
    } else {
        1
    }
}
