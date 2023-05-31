extern crate libc;

use libc::{c_char, c_int};
use prql_compiler::{json, prql_to_pl};
use std::ffi::CStr;
use std::ffi::CString;
use prql_compiler::sql::Options;

#[no_mangle]
pub unsafe extern "C" fn to_sql(query: *const c_char, out: *mut c_char) -> c_int {
    let prql_query: String = CStr::from_ptr(query).to_string_lossy().into_owned();
    let opts = Options::default();
    let (is_err, sql_result) = match prql_compiler::compile(&prql_query, Some(opts)) {
        Ok(sql_str) => (false, sql_str),
        Err(err) => {
            (true, err.to_string())
        }
    };

    if is_err {
        return -1;
    }

    let copylen = sql_result.len();
    let c_str = CString::new(sql_result).unwrap();

    out.copy_from(c_str.as_ptr(), copylen);
    let end_of_string_ptr = out.add(copylen);
    *end_of_string_ptr = 0;

    match is_err {
        true => -1,
        false => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn to_json(query: *const c_char, out: *mut c_char) -> c_int {
    let prql_query: String = CStr::from_ptr(query).to_string_lossy().into_owned();

    let (is_err, sql_result) = match prql_to_pl(&prql_query).and_then(json::from_pl) {
        Ok(sql_str) => (false, sql_str),
        Err(err) => {
            (true, err.to_string())
        }
    };

    let copylen = sql_result.len();
    let c_str = CString::new(sql_result).unwrap();

    out.copy_from(c_str.as_ptr(), copylen);
    let end_of_string_ptr = out.add(copylen);
    *end_of_string_ptr = 0;

    match is_err {
        true => -1,
        false => 0,
    }
}

