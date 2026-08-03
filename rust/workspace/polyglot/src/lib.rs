use std::ffi::{c_char, CString};
use std::panic;
use std::slice;

fn set_output(result: String, out: *mut *mut u8, out_size: *mut u64) {
    if out_size.is_null() || out.is_null() {
        return;
    }

    let cstring = match CString::new(result) {
        Ok(s) => s,
        Err(e) => {
            // If the result contains embedded null bytes, strip them.
            let mut bytes = e.into_vec();
            bytes.retain(|&b| b != 0);
            CString::new(bytes).unwrap_or_default()
        }
    };

    unsafe {
        out_size.write((cstring.as_bytes().len() + 1) as u64);
        out.write(cstring.into_raw() as *mut u8);
    }
}

/// Transpiles SQL from one dialect to ClickHouse SQL.
unsafe fn polyglot_transpile_impl(
    query: *const u8,
    query_size: u64,
    source_dialect: *const u8,
    source_dialect_size: u64,
    out: *mut *mut u8,
    out_size: *mut u64,
) -> i64 {
    let query_slice = slice::from_raw_parts(query, query_size.try_into().unwrap());
    let Ok(query_str) = std::str::from_utf8(query_slice) else {
        set_output(
            "The query must be UTF-8 encoded!".to_string(),
            out,
            out_size,
        );
        return 1;
    };

    let dialect_slice =
        slice::from_raw_parts(source_dialect, source_dialect_size.try_into().unwrap());
    let Ok(dialect_str) = std::str::from_utf8(dialect_slice) else {
        set_output(
            "The dialect name must be UTF-8 encoded!".to_string(),
            out,
            out_size,
        );
        return 1;
    };

    match polyglot_sql::transpile_by_name(query_str, dialect_str, "clickhouse") {
        Ok(statements) if statements.len() == 1 => {
            set_output(statements.into_iter().next().unwrap(), out, out_size);
            0
        }
        Ok(statements) if statements.is_empty() => {
            set_output(
                "Polyglot transpilation returned no statements".to_string(),
                out,
                out_size,
            );
            1
        }
        Ok(_) => {
            set_output(
                "Polyglot transpilation returned multiple statements, but only single-statement queries are supported".to_string(),
                out,
                out_size,
            );
            1
        }
        Err(e) => {
            set_output(format!("Polyglot transpilation failed: {e}"), out, out_size);
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn polyglot_transpile(
    query: *const u8,
    query_size: u64,
    source_dialect: *const u8,
    source_dialect_size: u64,
    out: *mut *mut u8,
    out_size: *mut u64,
) -> i64 {
    panic::catch_unwind(|| {
        polyglot_transpile_impl(query, query_size, source_dialect, source_dialect_size, out, out_size)
    })
    .unwrap_or_else(|_| {
        set_output("polyglot panicked".to_string(), out, out_size);
        1
    })
}

#[no_mangle]
pub unsafe extern "C" fn polyglot_free_pointer(ptr_to_free: *mut u8) {
    if !ptr_to_free.is_null() {
        std::mem::drop(CString::from_raw(ptr_to_free as *mut c_char));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{CStr, CString};

    fn run_transpile(query: &str, dialect: &str) -> (String, i64) {
        let query_cstr = CString::new(query).unwrap();
        let query_ptr = query_cstr.as_ptr() as *const u8;
        let query_size = query_cstr.to_bytes().len() as u64;

        let dialect_cstr = CString::new(dialect).unwrap();
        let dialect_ptr = dialect_cstr.as_ptr() as *const u8;
        let dialect_size = dialect_cstr.to_bytes().len() as u64;

        let mut out: *mut u8 = std::ptr::null_mut();
        let mut out_size = 0_u64;

        unsafe {
            let success = polyglot_transpile(
                query_ptr,
                query_size,
                dialect_ptr,
                dialect_size,
                &mut out,
                &mut out_size,
            );
            let output = CStr::from_ptr(out as *const i8)
                .to_str()
                .unwrap()
                .to_string();
            polyglot_free_pointer(out);
            (output, success)
        }
    }

    #[test]
    fn test_transpile_sqlite_to_clickhouse() {
        let (result, code) = run_transpile("SELECT IFNULL(a, 1) FROM t", "sqlite");
        assert_eq!(code, 0, "Transpilation failed: {result}");
        assert!(
            result.to_uppercase().contains("SELECT"),
            "Expected SELECT in output: {result}"
        );
    }

    #[test]
    fn test_invalid_dialect() {
        let (_result, code) = run_transpile("SELECT 1", "not_a_real_dialect");
        assert_eq!(code, 1);
    }
}
