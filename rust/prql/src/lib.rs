use prql_compiler::sql::Dialect;
use prql_compiler::{Options, Target};
use std::ffi::{c_char, CString};
use std::slice;

fn set_output(result: String, out: *mut *mut u8, out_size: *mut u64) {
    assert!(!out_size.is_null());
    let out_size_ptr = unsafe { &mut *out_size };
    *out_size_ptr = (result.len() + 1).try_into().unwrap();

    assert!(!out.is_null());
    let out_ptr = unsafe { &mut *out };
    *out_ptr = CString::new(result).unwrap().into_raw() as *mut u8;
}

#[no_mangle]
pub unsafe extern "C" fn prql_to_sql(
    query: *const u8,
    size: u64,
    out: *mut *mut u8,
    out_size: *mut u64,
) -> i64 {
    let query_vec = unsafe { slice::from_raw_parts(query, size.try_into().unwrap()) }.to_vec();
    let maybe_prql_query = String::from_utf8(query_vec);
    if maybe_prql_query.is_err() {
        set_output(
            String::from("The PRQL query must be UTF-8 encoded!"),
            out,
            out_size,
        );
        return 1;
    }
    let prql_query = maybe_prql_query.unwrap();
    let opts = &Options {
        format: true,
        target: Target::Sql(Some(Dialect::ClickHouse)),
        signature_comment: false,
        color: false,
    };
    let (is_err, res) = match prql_compiler::compile(&prql_query, &opts) {
        Ok(sql_str) => (false, sql_str),
        Err(err) => (true, err.to_string()),
    };

    set_output(res, out, out_size);

    match is_err {
        true => 1,
        false => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn prql_free_pointer(ptr_to_free: *mut u8) {
    std::mem::drop(CString::from_raw(ptr_to_free as *mut c_char));
}
