use blake3::{Hasher, OUT_LEN};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim(
    begin: *const c_char,
    _size: u32,
    out_char_data: *mut u8,
) -> *mut c_char {
    if begin.is_null() {
        let err_str = CString::new("input was a null pointer").unwrap();
        return err_str.into_raw();
    }
    let mut hasher = Hasher::new();
    let input_bytes = CStr::from_ptr(begin);
    let input_res = input_bytes.to_bytes();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();
    reader.fill(std::slice::from_raw_parts_mut(out_char_data, OUT_LEN));
    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim_msan_compat(
    mut begin: *const c_char,
    size: u32,
    out_char_data: *mut u8,
) -> *mut c_char {
    if begin.is_null() {
        let err_str = CString::new("input was a null pointer").unwrap();
        return err_str.into_raw();
    }
    let mut hasher = Hasher::new();
    let mut vec = Vec::<u8>::new();
    for _ in 0..size {
        vec.push(*begin as u8);
        begin = begin.add(1);
    }
    let input_res = vec.as_mut_slice();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();
    reader.fill(std::slice::from_raw_parts_mut(out_char_data, OUT_LEN));
    std::ptr::null_mut()
}