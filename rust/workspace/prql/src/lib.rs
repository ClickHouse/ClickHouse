use prqlc::sql::Dialect;
use prqlc::{Options, Target};
use std::ffi::{c_char, CString};
use std::panic;
use std::slice;

fn set_output(result: String, out: *mut *mut u8, out_size: *mut u64) {
    assert!(!out_size.is_null());
    let out_size_ptr = unsafe { &mut *out_size };
    *out_size_ptr = (result.len() + 1).try_into().unwrap();

    assert!(!out.is_null());
    let out_ptr = unsafe { &mut *out };
    *out_ptr = CString::new(result).unwrap().into_raw() as *mut u8;
}

/// Converts a PRQL query from a raw C string to SQL, returning an error code if the conversion fails.
pub unsafe extern "C" fn prql_to_sql_impl(
    query: *const u8,
    size: u64,
    out: *mut *mut u8,
    out_size: *mut u64,
) -> i64 {
    let query_vec = slice::from_raw_parts(query, size.try_into().unwrap()).to_vec();
    let Ok(query_str) = String::from_utf8(query_vec) else {
        set_output(
            "The PRQL query must be UTF-8 encoded!".to_string(),
            out,
            out_size,
        );
        return 1;
    };

    let opts = Options {
        format: true,
        target: Target::Sql(Some(Dialect::ClickHouse)),
        signature_comment: false,
        color: false,
    };

    if let Ok(sql_str) = prqlc::compile(&query_str, &opts) {
        // NOTE: Over at PRQL we're considering to un-deprecate & re-enable the
        // `color: false` option. If that happens, we can remove the `strip_str`
        // here, which strips color codes from the output.
        use anstream::adapter::strip_str;
        let sql_str = strip_str(&sql_str).to_string();
        set_output(sql_str, out, out_size);
        0
    } else {
        set_output("PRQL compilation failed!".to_string(), out, out_size);
        1
    }
}

#[no_mangle]
pub unsafe extern "C" fn prql_to_sql(
    query: *const u8,
    size: u64,
    out: *mut *mut u8,
    out_size: *mut u64,
) -> i64 {
    // NOTE: using cxxbridge we can return proper Result<> type.
    panic::catch_unwind(|| prql_to_sql_impl(query, size, out, out_size)).unwrap_or_else(|_| {
        set_output("prqlc panicked".to_string(), out, out_size);
        1
    })
}

#[no_mangle]
pub unsafe extern "C" fn prql_free_pointer(ptr_to_free: *mut u8) {
    std::mem::drop(CString::from_raw(ptr_to_free as *mut c_char));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{CStr, CString};

    /// A test helper to offer a rust interface to the C bindings
    fn run_compile(query: &str) -> (String, i64) {
        let query_cstr = CString::new(query).unwrap();
        let query_ptr = query_cstr.as_ptr() as *const u8;
        let query_size = query_cstr.to_bytes_with_nul().len() as u64 - 1; // Excluding the null terminator

        let mut out: *mut u8 = std::ptr::null_mut();
        let mut out_size = 0_u64;

        unsafe {
            let success = prql_to_sql(query_ptr, query_size, &mut out, &mut out_size);
            let output = CStr::from_ptr(out as *const i8)
                .to_str()
                .unwrap()
                .to_string();
            prql_free_pointer(out);
            (output, success)
        }
    }

    #[test]
    fn test_prql_to_sql() {
        assert!(run_compile("from x").0.contains("SELECT"));
        assert!(run_compile("asdf").1 == 1);
        // In prqlc 0.11.3, this is a panic, so that allows us to test that the
        // panic is caught. When we upgrade prqlc, it won't be a panic any
        // longer.
        assert!(run_compile("x -> y").1 == 1);
    }
}
