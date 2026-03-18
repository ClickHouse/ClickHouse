use daachorse::{DoubleArrayAhoCorasick, DoubleArrayAhoCorasickBuilder, MatchKind};
use std::panic;
use std::slice;

/// Opaque handle to the Aho-Corasick automaton.
pub struct AhoCorasickHandle {
    automaton: DoubleArrayAhoCorasick<u32>,
    /// If true, patterns were lowercased and haystacks should be too
    case_insensitive: bool,
}

/// Creates an Aho-Corasick automaton from the given patterns.
///
/// # Arguments
/// * `patterns` - Pointer to array of pattern data pointers
/// * `pattern_sizes` - Pointer to array of pattern sizes (u64)
/// * `num_patterns` - Number of patterns
/// * `case_insensitive` - If true, use Unicode-aware case-insensitive matching
///
/// # Returns
/// Pointer to the automaton handle, or null on error.
///
/// # Safety
/// Caller must ensure:
/// - All input pointers remain valid throughout the entire function duration
/// - Pattern arrays have exactly `num_patterns` elements
/// - All pattern pointers are valid for their respective sizes
/// - Pattern pointers are not accessed/freed by other threads during this call
#[no_mangle]
pub unsafe extern "C" fn aho_corasick_create(
    patterns: *const *const u8,
    pattern_sizes: *const u64,
    num_patterns: u64,
    case_insensitive: bool,
) -> *mut AhoCorasickHandle {
    let result = panic::catch_unwind(|| {
        if patterns.is_null() || pattern_sizes.is_null() || num_patterns == 0 {
            return std::ptr::null_mut();
        }

        let num = num_patterns as usize;
        let pattern_ptrs = slice::from_raw_parts(patterns, num);
        let sizes = slice::from_raw_parts(pattern_sizes, num);

        // Collect patterns as byte slices
        let mut pattern_vec: Vec<Vec<u8>> = Vec::with_capacity(num);
        for i in 0..num {
            if pattern_ptrs[i].is_null() {
                return std::ptr::null_mut();
            }
            let pattern = slice::from_raw_parts(pattern_ptrs[i], sizes[i] as usize);
            if case_insensitive {
                // Unicode-aware lowercasing: convert bytes to UTF-8 string, lowercase, convert back.
                // For invalid UTF-8 (shouldn't happen with ClickHouse data), fall back to ASCII lowering.
                let lowered = match std::str::from_utf8(pattern) {
                    Ok(s) => s.to_lowercase().into_bytes(),
                    Err(_) => pattern.to_ascii_lowercase(),
                };
                pattern_vec.push(lowered);
            } else {
                pattern_vec.push(pattern.to_vec());
            }
        }

        // Use daachorse with LeftmostFirst match kind
        let builder_result = DoubleArrayAhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostFirst)
            .build(&pattern_vec);

        match builder_result {
            Ok(automaton) => {
                let handle = Box::new(AhoCorasickHandle {
                    automaton,
                    case_insensitive,
                });
                Box::into_raw(handle)
            }
            Err(_) => std::ptr::null_mut(),
        }
    });

    result.unwrap_or(std::ptr::null_mut())
}

/// Searches for any pattern match in a batch of haystacks.
///
/// This function is optimized for ClickHouse's ColumnString format where:
/// - `haystack_data` is a contiguous buffer of all strings (with null terminators)
/// - `haystack_offsets` is an array of cumulative end positions (1-indexed, includes null terminator)
///
/// # Arguments
/// * `handle` - The automaton handle from `aho_corasick_create`
/// * `haystack_data` - Pointer to contiguous haystack data
/// * `haystack_offsets` - Pointer to array of cumulative offsets (ClickHouse format)
/// * `num_rows` - Number of haystacks to search
/// * `results` - Output array of u8 (0 = no match, 1 = match found)
///
/// # Safety
/// Caller must ensure all pointers are valid and arrays have correct sizes.
#[no_mangle]
pub unsafe extern "C" fn aho_corasick_search_batch(
    handle: *const AhoCorasickHandle,
    haystack_data: *const u8,
    haystack_offsets: *const u64,
    num_rows: u64,
    results: *mut u8,
) {
    let _ = panic::catch_unwind(|| {
        if handle.is_null()
            || haystack_data.is_null()
            || haystack_offsets.is_null()
            || results.is_null()
            || num_rows == 0
        {
            return;
        }

        let handle_ref = &*handle;
        let num = num_rows as usize;
        let offsets = slice::from_raw_parts(haystack_offsets, num);
        let results_slice = slice::from_raw_parts_mut(results, num);
        let ac = &handle_ref.automaton;

        let mut prev_offset: u64 = 0;
        // Pre-allocate buffer for case-insensitive lowercasing
        let mut lowercase_buf: Vec<u8> = Vec::with_capacity(256);

        for i in 0..num {
            let end_offset = offsets[i];

            if end_offset < prev_offset {
                results_slice[i] = 0;
                continue;
            }

            let start = prev_offset as usize;
            let end = end_offset as usize;

            if start > end {
                results_slice[i] = 0;
                continue;
            }

            let haystack = slice::from_raw_parts(haystack_data.add(start), end - start);

            // For case-insensitive, we need to lowercase the haystack (Unicode-aware)
            let search_haystack = if handle_ref.case_insensitive {
                lowercase_buf.clear();
                match std::str::from_utf8(haystack) {
                    Ok(s) => lowercase_buf.extend(s.to_lowercase().as_bytes()),
                    Err(_) => lowercase_buf.extend(haystack.iter().map(|b| b.to_ascii_lowercase())),
                }
                &lowercase_buf[..]
            } else {
                haystack
            };

            // leftmost_find_iter with LeftmostFirst is fastest
            results_slice[i] = if ac.leftmost_find_iter(search_haystack).next().is_some() {
                1
            } else {
                0
            };
            prev_offset = end_offset;
        }
    });
}

/// Frees the Aho-Corasick automaton handle.
///
/// # Safety
/// The handle must have been created by `aho_corasick_create` and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn aho_corasick_free(handle: *mut AhoCorasickHandle) {
    if !handle.is_null() {
        if let Err(_) = panic::catch_unwind(|| {
            drop(Box::from_raw(handle));
        }) {
            eprintln!("Warning: panic occurred while freeing Aho-Corasick automaton handle");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_search() {
        let patterns: Vec<&[u8]> = vec![b"hello", b"world"];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                false,
            );
            assert!(!handle.is_null());

            let haystack_data = b"hello\0world\0test\0";
            let offsets: Vec<u64> = vec![6, 12, 17];
            let mut results: Vec<u8> = vec![0; 3];

            aho_corasick_search_batch(
                handle,
                haystack_data.as_ptr(),
                offsets.as_ptr(),
                3,
                results.as_mut_ptr(),
            );

            assert_eq!(results[0], 1);
            assert_eq!(results[1], 1);
            assert_eq!(results[2], 0);

            aho_corasick_free(handle);
        }
    }

    #[test]
    fn test_case_insensitive() {
        let patterns: Vec<&[u8]> = vec![b"HELLO", b"WORLD"];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                true,
            );
            assert!(!handle.is_null());

            let haystack_data = b"hello\0WORLD\0test\0";
            let offsets: Vec<u64> = vec![6, 12, 17];
            let mut results: Vec<u8> = vec![0; 3];

            aho_corasick_search_batch(
                handle,
                haystack_data.as_ptr(),
                offsets.as_ptr(),
                3,
                results.as_mut_ptr(),
            );

            assert_eq!(results[0], 1);
            assert_eq!(results[1], 1);
            assert_eq!(results[2], 0);

            aho_corasick_free(handle);
        }
    }

    #[test]
    fn test_case_insensitive_unicode() {
        // Test Unicode case folding: Ö (U+00D6) should match ö (U+00F6)
        let pattern_straße = "STRASSE".as_bytes(); // ASCII pattern
        let pattern_umlaut = "ÜBER".as_bytes(); // Ü = U+00DC
        let patterns: Vec<&[u8]> = vec![pattern_straße, pattern_umlaut];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                true,
            );
            assert!(!handle.is_null());

            // "über" contains ü (U+00FC) which should match pattern "ÜBER" (Ü = U+00DC)
            // "strasse" should match "STRASSE"
            // "test" should not match
            let haystack1 = "über\0".as_bytes();
            let haystack2 = "strasse\0".as_bytes();
            let haystack3 = "test\0".as_bytes();
            let mut all_data: Vec<u8> = Vec::new();
            all_data.extend_from_slice(haystack1);
            all_data.extend_from_slice(haystack2);
            all_data.extend_from_slice(haystack3);

            let offsets: Vec<u64> = vec![
                haystack1.len() as u64,
                (haystack1.len() + haystack2.len()) as u64,
                (haystack1.len() + haystack2.len() + haystack3.len()) as u64,
            ];
            let mut results: Vec<u8> = vec![0; 3];

            aho_corasick_search_batch(
                handle,
                all_data.as_ptr(),
                offsets.as_ptr(),
                3,
                results.as_mut_ptr(),
            );

            assert_eq!(results[0], 1, "über should match ÜBER case-insensitively");
            assert_eq!(results[1], 1, "strasse should match STRASSE case-insensitively");
            assert_eq!(results[2], 0, "test should not match");

            aho_corasick_free(handle);
        }
    }
}
