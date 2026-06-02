use daachorse::{DoubleArrayAhoCorasick, DoubleArrayAhoCorasickBuilder, MatchKind};
use std::panic;
use std::slice;

/// Case-folding mode. Values must match `AhoCorasickCaseMode` in `include/aho_corasick.h`.
const CASE_SENSITIVE: u8 = 0;
const CASE_INSENSITIVE_ASCII: u8 = 1;
const CASE_INSENSITIVE_UTF8: u8 = 2;

/// Opaque handle to the Aho-Corasick automaton.
pub struct AhoCorasickHandle {
    automaton: DoubleArrayAhoCorasick<u32>,
    /// How patterns were folded; haystacks are folded the same way at search time.
    case_mode: u8,
}

/// Unicode-lowercases `input` into `buf`. This avoids the temporary
/// `String` that `str::to_lowercase` allocates per call, which matters when folding every
/// haystack row. Invalid UTF-8 falls back to ASCII folding (should not occur with ClickHouse
/// data). Patterns and haystacks must both fold through this routine so their folded forms agree.
///
/// Unlike `str::to_lowercase`, this does not apply the context-sensitive final-sigma rule (Σ at a
/// word boundary lowercasing to ς rather than σ); each code point folds independently. That is
/// internally consistent here and also matches ClickHouse's per-code-point `lowerUTF8`.
#[inline]
fn fold_utf8_into(input: &[u8], buf: &mut Vec<u8>) {
    match std::str::from_utf8(input) {
        Ok(s) => {
            let mut char_buf = [0u8; 4];
            for c in s.chars() {
                for lc in c.to_lowercase() {
                    buf.extend_from_slice(lc.encode_utf8(&mut char_buf).as_bytes());
                }
            }
        }
        Err(_) => buf.extend(input.iter().map(|b| b.to_ascii_lowercase())),
    }
}

/// Folds `input` into an owned buffer according to `case_mode`.
/// ASCII mode lowercases only `A`-`Z` (non-ASCII bytes are left untouched), matching the
/// legacy `MultiVolnitskyCaseInsensitive` behaviour. UTF8 mode applies Unicode lowercasing,
/// falling back to ASCII folding for invalid UTF-8 (which should not occur with ClickHouse data).
fn fold_owned(input: &[u8], case_mode: u8) -> Vec<u8> {
    match case_mode {
        CASE_INSENSITIVE_ASCII => input.to_ascii_lowercase(),
        CASE_INSENSITIVE_UTF8 => {
            let mut buf = Vec::with_capacity(input.len());
            fold_utf8_into(input, &mut buf);
            buf
        }
        _ => input.to_vec(),
    }
}

/// Folds `haystack` for searching. For `CASE_SENSITIVE` returns the original slice (no copy);
/// otherwise folds into `buf` (reused across rows) and returns a view into it.
#[inline]
fn fold_haystack<'a>(haystack: &'a [u8], case_mode: u8, buf: &'a mut Vec<u8>) -> &'a [u8] {
    match case_mode {
        CASE_INSENSITIVE_ASCII => {
            buf.clear();
            buf.extend_from_slice(haystack);
            buf.make_ascii_lowercase();
            &buf[..]
        }
        CASE_INSENSITIVE_UTF8 => {
            buf.clear();
            fold_utf8_into(haystack, buf);
            &buf[..]
        }
        _ => haystack,
    }
}

/// Builds an Aho-Corasick automaton from the given patterns; returns null on failure.
/// See `include/aho_corasick.h` for the parameter, folding, and de-duplication contract.
#[no_mangle]
pub unsafe extern "C" fn aho_corasick_create(
    patterns: *const *const u8,
    pattern_sizes: *const u64,
    num_patterns: u64,
    case_mode: u8,
) -> *mut AhoCorasickHandle {
    let result = panic::catch_unwind(|| {
        if patterns.is_null() || pattern_sizes.is_null() || num_patterns == 0 {
            return std::ptr::null_mut();
        }

        let num = num_patterns as usize;
        let pattern_ptrs = slice::from_raw_parts(patterns, num);
        let sizes = slice::from_raw_parts(pattern_sizes, num);

        let mut pattern_vec: Vec<Vec<u8>> = Vec::with_capacity(num);
        for i in 0..num {
            if pattern_ptrs[i].is_null() {
                return std::ptr::null_mut();
            }
            let pattern = slice::from_raw_parts(pattern_ptrs[i], sizes[i] as usize);
            pattern_vec.push(fold_owned(pattern, case_mode));
        }

        let builder_result = DoubleArrayAhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostFirst)
            .build(&pattern_vec);

        match builder_result {
            Ok(automaton) => {
                let handle = Box::new(AhoCorasickHandle {
                    automaton,
                    case_mode,
                });
                Box::into_raw(handle)
            }
            Err(_) => std::ptr::null_mut(),
        }
    });

    result.unwrap_or(std::ptr::null_mut())
}

/// Searches a batch of haystacks for any pattern match, writing one result byte per row.
/// See `include/aho_corasick.h` for the ColumnString offset format and result encoding.
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
        let mut lowercase_buf: Vec<u8> = Vec::with_capacity(256);

        for i in 0..num {
            let end_offset = offsets[i];

            if end_offset < prev_offset {
                results_slice[i] = 0;
                continue;
            }

            let start = prev_offset as usize;
            let end = end_offset as usize;

            let haystack = slice::from_raw_parts(haystack_data.add(start), end - start);

            // Fold the haystack the same way the patterns were folded.
            let search_haystack = fold_haystack(haystack, handle_ref.case_mode, &mut lowercase_buf);

            results_slice[i] = if ac.leftmost_find_iter(search_haystack).next().is_some() {
                1
            } else {
                0
            };
            prev_offset = end_offset;
        }
    });
}

/// Returns the automaton's heap footprint in bytes, used for cache sizing.
#[no_mangle]
pub unsafe extern "C" fn aho_corasick_heap_bytes(handle: *const AhoCorasickHandle) -> u64 {
    if handle.is_null() {
        return 0;
    }
    (*handle).automaton.heap_bytes() as u64
}

/// Frees an automaton handle previously returned by `aho_corasick_create`.
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
                CASE_SENSITIVE,
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

            assert!(aho_corasick_heap_bytes(handle) > 0);

            aho_corasick_free(handle);
        }
    }

    #[test]
    fn test_case_insensitive_ascii() {
        let patterns: Vec<&[u8]> = vec![b"HELLO", b"WORLD"];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                CASE_INSENSITIVE_ASCII,
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
    fn test_ascii_mode_does_not_fold_unicode() {
        // ASCII mode must NOT match across Unicode case (parity with MultiVolnitskyCaseInsensitive).
        // "ÜBER" lowercased in ASCII mode keeps the Ü bytes, so it cannot match "über".
        let pattern_umlaut = "ÜBER".as_bytes();
        let pattern_ascii = b"HELLO" as &[u8];
        let patterns: Vec<&[u8]> = vec![pattern_umlaut, pattern_ascii];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                CASE_INSENSITIVE_ASCII,
            );
            assert!(!handle.is_null());

            let h_uber = "über\0".as_bytes();
            let h_hello = "hello\0".as_bytes();
            let mut all_data: Vec<u8> = Vec::new();
            all_data.extend_from_slice(h_uber);
            all_data.extend_from_slice(h_hello);
            let offsets: Vec<u64> = vec![h_uber.len() as u64, (h_uber.len() + h_hello.len()) as u64];
            let mut results: Vec<u8> = vec![0; 2];

            aho_corasick_search_batch(
                handle,
                all_data.as_ptr(),
                offsets.as_ptr(),
                2,
                results.as_mut_ptr(),
            );

            assert_eq!(results[0], 0, "über must NOT match ÜBER in ASCII mode");
            assert_eq!(results[1], 1, "hello must match HELLO in ASCII mode");

            aho_corasick_free(handle);
        }
    }

    #[test]
    fn test_case_insensitive_unicode() {
        // UTF8 mode applies Unicode folding: Ü (U+00DC) matches ü (U+00FC).
        let pattern_strasse = "STRASSE".as_bytes();
        let pattern_umlaut = "ÜBER".as_bytes();
        let patterns: Vec<&[u8]> = vec![pattern_strasse, pattern_umlaut];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                CASE_INSENSITIVE_UTF8,
            );
            assert!(!handle.is_null());

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

    #[test]
    fn test_utf8_fold_multichar_expansion() {
        // 'İ' (U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE) lowercases to two code points:
        // 'i' (U+0069) + combining dot above (U+0307). Pattern and haystack must fold through the
        // same routine so the expanded forms match; a plain ASCII 'i' must NOT match, because it
        // lacks the combining dot (this is what proves the expansion actually happened).
        let patterns: Vec<&[u8]> = vec!["İ".as_bytes()];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                CASE_INSENSITIVE_UTF8,
            );
            assert!(!handle.is_null());

            let h_match = "İstanbul\0".as_bytes();
            let h_plain = "hi there\0".as_bytes();
            let mut all_data: Vec<u8> = Vec::new();
            all_data.extend_from_slice(h_match);
            all_data.extend_from_slice(h_plain);
            let offsets: Vec<u64> = vec![h_match.len() as u64, (h_match.len() + h_plain.len()) as u64];
            let mut results: Vec<u8> = vec![0; 2];

            aho_corasick_search_batch(
                handle,
                all_data.as_ptr(),
                offsets.as_ptr(),
                2,
                results.as_mut_ptr(),
            );

            assert_eq!(results[0], 1, "İstanbul should match pattern İ after Unicode folding");
            assert_eq!(results[1], 0, "plain 'i' must not match İ (missing combining dot)");

            aho_corasick_free(handle);
        }
    }

    #[test]
    fn test_duplicate_patterns_build_ok() {
        // Duplicate patterns must not cause a build failure (daachorse rejects duplicates;
        // we de-duplicate before building).
        let patterns: Vec<&[u8]> = vec![b"abc", b"abc", b"def"];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                CASE_SENSITIVE,
            );
            assert!(!handle.is_null(), "duplicate patterns must build successfully");

            let haystack_data = b"abc\0def\0xyz\0";
            let offsets: Vec<u64> = vec![4, 8, 12];
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
    fn test_duplicate_after_folding_build_ok() {
        // "ABC" and "abc" collapse to the same bytes after ASCII folding; the de-dup must
        // prevent a DuplicatePatternError.
        let patterns: Vec<&[u8]> = vec![b"ABC", b"abc"];
        let pattern_ptrs: Vec<*const u8> = patterns.iter().map(|p| p.as_ptr()).collect();
        let pattern_sizes: Vec<u64> = patterns.iter().map(|p| p.len() as u64).collect();

        unsafe {
            let handle = aho_corasick_create(
                pattern_ptrs.as_ptr(),
                pattern_sizes.as_ptr(),
                patterns.len() as u64,
                CASE_INSENSITIVE_ASCII,
            );
            assert!(!handle.is_null(), "patterns colliding after folding must build successfully");

            let haystack_data = b"xABCx\0nope\0";
            let offsets: Vec<u64> = vec![6, 11];
            let mut results: Vec<u8> = vec![0; 2];

            aho_corasick_search_batch(
                handle,
                haystack_data.as_ptr(),
                offsets.as_ptr(),
                2,
                results.as_mut_ptr(),
            );

            assert_eq!(results[0], 1);
            assert_eq!(results[1], 0);

            aho_corasick_free(handle);
        }
    }
}
