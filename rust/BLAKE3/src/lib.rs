extern crate blake3;
extern crate libc;

use std::ffi::{CString};
use std::slice;
use std::os::raw::c_char;
use std::mem;

#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim(
    begin: *const c_char,
    size: u32,
    out_char_data: *mut u8,
) -> *mut c_char {
    if begin.is_null() {
        let err_str = CString::new("input was a null pointer").unwrap();
        return err_str.into_raw();
    }
    let input_res = slice::from_raw_parts(begin as *const u8, size as usize);
    let mut hasher = blake3::Hasher::new();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();

    reader.fill(std::slice::from_raw_parts_mut(out_char_data, blake3::OUT_LEN));
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
    libc::memset(out_char_data as *mut libc::c_void, 0, mem::size_of::<u8>());
    let mut hasher = blake3::Hasher::new();
    let mut vec = Vec::<u8>::new();
    for _ in 0..size {
        vec.push(*begin as u8);
        begin = begin.add(1);
    }
    let input_res = vec.as_mut_slice();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();
    reader.fill(std::slice::from_raw_parts_mut(out_char_data, blake3::OUT_LEN));
    std::ptr::null_mut()
}

// Freeing memory according to docs: https://doc.rust-lang.org/std/ffi/struct.CString.html#method.into_raw
#[no_mangle]
pub unsafe extern "C" fn blake3_free_char_pointer(ptr_to_free: *mut c_char) {
    std::mem::drop(CString::from_raw(ptr_to_free));
}
