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
    let ret = panic::catch_unwind(|| {
        return prql_to_sql_impl(query, size, out, out_size);
    });
    return match ret {
        // NOTE: using cxxbridge we can return proper Result<> type.
        Err(_err) => 1,
        Ok(res) => res,
    }
}

#[no_mangle]
pub unsafe extern "C" fn prql_free_pointer(ptr_to_free: *mut u8) {
    std::mem::drop(CString::from_raw(ptr_to_free as *mut c_char));
}
