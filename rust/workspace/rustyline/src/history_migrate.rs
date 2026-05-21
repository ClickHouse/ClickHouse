//! One-shot migration of replxx-format history files to rustyline's V2
//! format. Idempotent — if the file already looks like V2 (or is empty
//! or doesn't exist), we leave it alone.
//!
//! replxx writes entries as:
//!
//!     ### YYYY-MM-DD HH:MM:SS.SSS
//!     <line 1>
//!     <line 2 — for multi-line entries>
//!     ### YYYY-MM-DD HH:MM:SS.SSS
//!     <next entry>
//!
//! rustyline V2 wants:
//!
//!     #V2
//!     <entry — newlines escaped as backslash-n, backslashes as backslash-backslash>
//!     <next entry>

use std::fs;
use std::io::{self, Write};

const V2_HEADER: &str = "#V2";

fn looks_like_replxx_marker(line: &str) -> bool {
    // `### YYYY-MM-DD HH:MM:SS.SSS` — exactly 27 chars, ASCII-only.
    line.len() == 27
        && line.starts_with("### ")
        && line.as_bytes()[4].is_ascii_digit()
}

fn escape_v2(entry: &str) -> String {
    let mut out = String::with_capacity(entry.len());
    for ch in entry.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

pub fn migrate(path: &str) -> io::Result<()> {
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    let mut lines = content.lines();
    let first = match lines.find(|l| !l.is_empty()) {
        Some(l) => l,
        None => return Ok(()), // empty file
    };

    if first == V2_HEADER {
        return Ok(()); // already migrated
    }
    if !looks_like_replxx_marker(first) {
        // Plain readline-style history (no markers); rustyline can read it
        // as legacy format. Leave untouched.
        return Ok(());
    }

    // Parse: each block of non-marker lines between markers is one entry.
    // (Multi-line replxx entries become single rustyline entries with
    // embedded newlines, preserving the original query as one history
    // record.)
    let mut entries: Vec<String> = Vec::new();
    let mut current = String::new();
    for line in content.lines() {
        if looks_like_replxx_marker(line) {
            if !current.is_empty() {
                // Trim only the trailing newline accumulator we added.
                if current.ends_with('\n') {
                    current.pop();
                }
                entries.push(std::mem::take(&mut current));
            }
        } else {
            current.push_str(line);
            current.push('\n');
        }
    }
    if !current.is_empty() {
        if current.ends_with('\n') {
            current.pop();
        }
        entries.push(current);
    }

    // Adjacent-duplicate dedup (matches rustyline's default behavior).
    entries.dedup();

    // Write atomically: tmp file then rename.
    let tmp_path = format!("{path}.rustyline-migrate.tmp");
    {
        let mut f = fs::File::create(&tmp_path)?;
        writeln!(f, "{V2_HEADER}")?;
        for entry in &entries {
            writeln!(f, "{}", escape_v2(entry))?;
        }
        f.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    Ok(())
}
