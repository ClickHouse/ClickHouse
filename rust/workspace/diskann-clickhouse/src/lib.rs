//! DiskANN FFI bridge for ClickHouse.
//!
//! This crate provides a C FFI layer over a vector similarity index for
//! integration with ClickHouse. Phase 2 uses a brute-force implementation
//! to validate the FFI contract (handle management, memory safety, error
//! handling, serialization). The inner algorithm will be replaced by DiskANN
//! graph-based ANN search in a later phase.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_char;
use std::panic;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

// ============= Constants =============

/// Error code: null pointer argument.
const ERR_NULL_PTR: i64 = -2;
/// Error code: invalid handle (not found in the global map).
const ERR_INVALID_HANDLE: i64 = -3;
/// Error code: dimension mismatch between query/insert and index.
const ERR_DIM_MISMATCH: i64 = -4;
/// Error code: internal panic caught by catch_unwind.
const ERR_PANIC: i64 = -5;
/// Error code: serialization / deserialization failure.
const ERR_SERDE: i64 = -6;

// ============= Handle Management =============

static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);
static INDEX_MAP: std::sync::LazyLock<Mutex<HashMap<i64, IndexWrapper>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

// ============= Thread-local Error =============

thread_local! {
    static LAST_ERROR: RefCell<String> = RefCell::new(String::new());
}

fn set_last_error(msg: impl Into<String>) {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = msg.into();
    });
}

// ============= Index Data Structures =============

/// Metric type for distance computation (C-compatible).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiskANNMetric {
    L2 = 0,
    Cosine = 1,
}

/// Internal index storage using brute-force search.
///
/// Each vector is stored as a contiguous `Vec<f32>` of length `dim`.
/// Search computes exact distances against every stored vector.
#[derive(Serialize, Deserialize)]
struct IndexWrapper {
    dim: u32,
    metric: DiskANNMetric,
    /// Stored vectors, each of length `dim`.
    vectors: Vec<Vec<f32>>,
}

impl IndexWrapper {
    fn new(dim: u32, metric: DiskANNMetric) -> Self {
        Self {
            dim,
            metric,
            vectors: Vec::new(),
        }
    }

    /// Insert a single vector. Returns an error string if dimension mismatches.
    fn insert(&mut self, vec: Vec<f32>) -> Result<(), String> {
        if vec.len() != self.dim as usize {
            return Err(format!(
                "dimension mismatch: expected {}, got {}",
                self.dim,
                vec.len()
            ));
        }
        self.vectors.push(vec);
        Ok(())
    }

    /// Brute-force k-NN search. Returns (indices, distances) sorted by distance ascending.
    fn search(&self, query: &[f32], k: usize) -> Result<(Vec<u64>, Vec<f32>), String> {
        if query.len() != self.dim as usize {
            return Err(format!(
                "dimension mismatch: expected {}, got {}",
                self.dim,
                query.len()
            ));
        }

        let mut dists: Vec<(u64, f32)> = self
            .vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, self.compute_distance(query, v)))
            .collect();

        dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        dists.truncate(k);

        let indices: Vec<u64> = dists.iter().map(|(i, _)| *i).collect();
        let distances: Vec<f32> = dists.iter().map(|(_, d)| *d).collect();
        Ok((indices, distances))
    }

    fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            DiskANNMetric::L2 => a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| (x - y) * (x - y))
                .sum(),
            DiskANNMetric::Cosine => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                let denom = norm_a * norm_b;
                if denom < f32::EPSILON {
                    1.0
                } else {
                    1.0 - dot / denom
                }
            }
        }
    }

    fn size(&self) -> usize {
        self.vectors.len()
    }
}

// ============= Helper: wrap FFI body with panic::catch_unwind =============

fn catch_ffi<F: FnOnce() -> i64 + panic::UnwindSafe>(f: F) -> i64 {
    match panic::catch_unwind(f) {
        Ok(v) => v,
        Err(e) => {
            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            set_last_error(format!("panic: {}", msg));
            ERR_PANIC
        }
    }
}

// ============= FFI Functions =============

/// Creates a new DiskANN index. Returns a positive handle on success, or a
/// negative error code on failure.
#[no_mangle]
pub extern "C" fn diskann_create_index(
    dim: u32,
    metric: DiskANNMetric,
    _pruned_degree: u32,
    _max_degree: u32,
    _l_build: u32,
    _alpha: f32,
) -> i64 {
    catch_ffi(|| {
        if dim == 0 {
            set_last_error("dimension must be > 0");
            return -1;
        }
        let wrapper = IndexWrapper::new(dim, metric);
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        INDEX_MAP.lock().unwrap().insert(handle, wrapper);
        handle
    })
}

/// Drops a DiskANN index by handle. No-op if the handle is invalid.
#[no_mangle]
pub extern "C" fn diskann_drop_index(handle: i64) {
    let _ = panic::catch_unwind(|| {
        INDEX_MAP.lock().unwrap().remove(&handle);
    });
}

/// Inserts a batch of vectors into the index.
/// `vectors_ptr` must point to `count * dim` contiguous f32 values.
/// Returns 0 on success, or a negative error code on failure.
#[no_mangle]
pub unsafe extern "C" fn diskann_insert_batch(
    handle: i64,
    vectors_ptr: *const f32,
    count: u64,
    dim: u32,
) -> i64 {
    catch_ffi(move || {
        if vectors_ptr.is_null() {
            set_last_error("vectors_ptr is null");
            return ERR_NULL_PTR;
        }

        let total = count as usize * dim as usize;
        let data = unsafe { std::slice::from_raw_parts(vectors_ptr, total) };

        let mut map = INDEX_MAP.lock().unwrap();
        let index = match map.get_mut(&handle) {
            Some(idx) => idx,
            None => {
                set_last_error(format!("invalid handle: {}", handle));
                return ERR_INVALID_HANDLE;
            }
        };

        if dim != index.dim {
            set_last_error(format!(
                "dimension mismatch: index has {}, got {}",
                index.dim, dim
            ));
            return ERR_DIM_MISMATCH;
        }

        for i in 0..count as usize {
            let start = i * dim as usize;
            let end = start + dim as usize;
            let vec = data[start..end].to_vec();
            if let Err(e) = index.insert(vec) {
                set_last_error(e);
                return -1;
            }
        }
        0
    })
}

/// Searches the index for the `k` nearest neighbors of `query_ptr`.
/// Writes at most `k` result IDs into `results_ptr` and distances into
/// `distances_ptr`. Returns the number of results found (may be < k if
/// the index has fewer vectors), or a negative error code on failure.
#[no_mangle]
pub unsafe extern "C" fn diskann_search(
    handle: i64,
    query_ptr: *const f32,
    dim: u32,
    k: u64,
    results_ptr: *mut u64,
    distances_ptr: *mut f32,
) -> i64 {
    catch_ffi(move || {
        if query_ptr.is_null() || results_ptr.is_null() || distances_ptr.is_null() {
            set_last_error("null pointer argument");
            return ERR_NULL_PTR;
        }

        let query = unsafe { std::slice::from_raw_parts(query_ptr, dim as usize) };

        let map = INDEX_MAP.lock().unwrap();
        let index = match map.get(&handle) {
            Some(idx) => idx,
            None => {
                set_last_error(format!("invalid handle: {}", handle));
                return ERR_INVALID_HANDLE;
            }
        };

        if dim != index.dim {
            set_last_error(format!(
                "dimension mismatch: index has {}, got {}",
                index.dim, dim
            ));
            return ERR_DIM_MISMATCH;
        }

        let (indices, distances) = match index.search(query, k as usize) {
            Ok(r) => r,
            Err(e) => {
                set_last_error(e);
                return -1;
            }
        };

        let n = indices.len();
        let results_out = unsafe { std::slice::from_raw_parts_mut(results_ptr, n) };
        let distances_out = unsafe { std::slice::from_raw_parts_mut(distances_ptr, n) };
        results_out.copy_from_slice(&indices);
        distances_out.copy_from_slice(&distances);
        n as i64
    })
}

/// Serializes the index to a heap-allocated byte buffer. The caller must free
/// the buffer with `diskann_free_buffer`. Returns 0 on success, or a negative
/// error code on failure.
#[no_mangle]
pub unsafe extern "C" fn diskann_serialize(
    handle: i64,
    out_ptr: *mut *mut u8,
    out_size: *mut u64,
) -> i64 {
    catch_ffi(move || {
        if out_ptr.is_null() || out_size.is_null() {
            set_last_error("null output pointer");
            return ERR_NULL_PTR;
        }

        let map = INDEX_MAP.lock().unwrap();
        let index = match map.get(&handle) {
            Some(idx) => idx,
            None => {
                set_last_error(format!("invalid handle: {}", handle));
                return ERR_INVALID_HANDLE;
            }
        };

        let bytes = match bincode::serialize(index) {
            Ok(b) => b,
            Err(e) => {
                set_last_error(format!("serialization failed: {}", e));
                return ERR_SERDE;
            }
        };

        let (ptr, len) = alloc_and_register(bytes);
        unsafe {
            *out_ptr = ptr;
            *out_size = len as u64;
        }
        0
    })
}

/// Deserializes an index from a byte buffer previously produced by
/// `diskann_serialize`. Returns a positive handle on success, or a negative
/// error code on failure.
#[no_mangle]
pub unsafe extern "C" fn diskann_deserialize(data_ptr: *const u8, data_size: u64) -> i64 {
    catch_ffi(move || {
        if data_ptr.is_null() {
            set_last_error("data_ptr is null");
            return ERR_NULL_PTR;
        }

        let data = unsafe { std::slice::from_raw_parts(data_ptr, data_size as usize) };
        let wrapper: IndexWrapper = match bincode::deserialize(data) {
            Ok(w) => w,
            Err(e) => {
                set_last_error(format!("deserialization failed: {}", e));
                return ERR_SERDE;
            }
        };

        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        INDEX_MAP.lock().unwrap().insert(handle, wrapper);
        handle
    })
}

/// Frees a byte buffer previously allocated by `diskann_serialize`.
/// Passing a null pointer is a safe no-op.
#[no_mangle]
pub unsafe extern "C" fn diskann_free_buffer(ptr: *mut u8) {
    // We cannot know the exact length, but the buffer was produced by
    // Box::into_raw on a Box<[u8]>. We rely on the allocator metadata to
    // recover the size. However, Rust's Box<[u8]> requires the length at
    // deallocation. To handle this properly we store the serialized buffer
    // with a known layout: we prepend a u64 length. But that would change
    // the serialize contract.
    //
    // Simpler approach: the caller always pairs serialize + free_buffer.
    // We store allocated buffers in a side map so we can reconstruct the
    // Box<[u8]> with the correct length.
    //
    // Actually, the simplest correct approach: we use Vec::from_raw_parts
    // with the capacity stored alongside. But the C header doesn't carry
    // capacity. So we use a global map to remember (ptr -> len).
    //
    // For safety and simplicity we track all outstanding allocations.
    if ptr.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let mut map = BUFFER_MAP.lock().unwrap();
        if let Some(len) = map.remove(&(ptr as usize)) {
            let _ = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, len)) };
        }
    });
}

/// Global map tracking (ptr_address -> byte_length) for buffers allocated by
/// `diskann_serialize`, so that `diskann_free_buffer` can reconstruct the
/// fat pointer.
static BUFFER_MAP: std::sync::LazyLock<Mutex<HashMap<usize, usize>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Internal helper: allocate a buffer and register it in `BUFFER_MAP`.
fn alloc_and_register(bytes: Vec<u8>) -> (*mut u8, usize) {
    let len = bytes.len();
    let boxed = bytes.into_boxed_slice();
    let raw = Box::into_raw(boxed);
    let ptr = raw as *mut u8;
    BUFFER_MAP.lock().unwrap().insert(ptr as usize, len);
    (ptr, len)
}

/// Returns the number of vectors currently stored in the index, or a negative
/// error code if the handle is invalid.
#[no_mangle]
pub extern "C" fn diskann_index_size(handle: i64) -> i64 {
    catch_ffi(|| {
        let map = INDEX_MAP.lock().unwrap();
        match map.get(&handle) {
            Some(idx) => idx.size() as i64,
            None => {
                set_last_error(format!("invalid handle: {}", handle));
                ERR_INVALID_HANDLE
            }
        }
    })
}

/// Copies the last error message for the calling thread into `buf`.
/// Returns the number of bytes written (excluding NUL), or -1 if there
/// is no error recorded.
#[no_mangle]
pub unsafe extern "C" fn diskann_last_error(buf: *mut c_char, buf_size: u64) -> i64 {
    if buf.is_null() || buf_size == 0 {
        return -1;
    }
    LAST_ERROR.with(|cell| {
        let err = cell.borrow();
        if err.is_empty() {
            return -1;
        }
        let bytes = err.as_bytes();
        let copy_len = bytes.len().min(buf_size as usize - 1);
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, copy_len);
            *buf.add(copy_len) = 0; // NUL terminator
        }
        copy_len as i64
    })
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_drop() {
        let h = diskann_create_index(128, DiskANNMetric::L2, 64, 128, 100, 1.2);
        assert!(h > 0, "expected positive handle, got {}", h);

        // Verify the index exists.
        assert_eq!(diskann_index_size(h), 0);

        // Drop and verify it's gone.
        diskann_drop_index(h);
        assert_eq!(diskann_index_size(h), ERR_INVALID_HANDLE);
    }

    #[test]
    fn test_insert_and_search() {
        let dim = 4u32;
        let h = diskann_create_index(dim, DiskANNMetric::L2, 64, 128, 100, 1.2);
        assert!(h > 0);

        // Insert 3 vectors: [1,0,0,0], [0,1,0,0], [0,0,1,0]
        let vectors: Vec<f32> = vec![
            1.0, 0.0, 0.0, 0.0, //
            0.0, 1.0, 0.0, 0.0, //
            0.0, 0.0, 1.0, 0.0, //
        ];
        let rc = unsafe { diskann_insert_batch(h, vectors.as_ptr(), 3, dim) };
        assert_eq!(rc, 0);
        assert_eq!(diskann_index_size(h), 3);

        // Search for the nearest neighbor of [1,0,0,0] — should be index 0.
        let query: Vec<f32> = vec![1.0, 0.0, 0.0, 0.0];
        let mut results = vec![0u64; 2];
        let mut distances = vec![0.0f32; 2];
        let found = unsafe {
            diskann_search(
                h,
                query.as_ptr(),
                dim,
                2,
                results.as_mut_ptr(),
                distances.as_mut_ptr(),
            )
        };
        assert_eq!(found, 2);
        assert_eq!(results[0], 0); // closest is vector 0
        assert!((distances[0] - 0.0).abs() < 1e-6); // exact match, distance = 0

        diskann_drop_index(h);
    }

    #[test]
    fn test_serialize_roundtrip() {
        let dim = 3u32;
        let h = diskann_create_index(dim, DiskANNMetric::Cosine, 64, 128, 100, 1.2);
        assert!(h > 0);

        let vectors: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let rc = unsafe { diskann_insert_batch(h, vectors.as_ptr(), 2, dim) };
        assert_eq!(rc, 0);

        // Serialize.
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_size: u64 = 0;
        let rc = unsafe { diskann_serialize(h, &mut out_ptr, &mut out_size) };
        assert_eq!(rc, 0);
        assert!(!out_ptr.is_null());
        assert!(out_size > 0);

        // Deserialize into a new index.
        let h2 = unsafe { diskann_deserialize(out_ptr, out_size) };
        assert!(h2 > 0);
        assert_eq!(diskann_index_size(h2), 2);

        // Search the deserialized index.
        let query: Vec<f32> = vec![1.0, 2.0, 3.0];
        let mut results = vec![0u64; 1];
        let mut distances = vec![0.0f32; 1];
        let found = unsafe {
            diskann_search(
                h2,
                query.as_ptr(),
                dim,
                1,
                results.as_mut_ptr(),
                distances.as_mut_ptr(),
            )
        };
        assert_eq!(found, 1);
        assert_eq!(results[0], 0); // vector [1,2,3] is closest to query [1,2,3]

        // Free the serialization buffer.
        unsafe { diskann_free_buffer(out_ptr) };

        diskann_drop_index(h);
        diskann_drop_index(h2);
    }

    #[test]
    fn test_dimension_mismatch() {
        let h = diskann_create_index(4, DiskANNMetric::L2, 64, 128, 100, 1.2);
        assert!(h > 0);

        // Try to insert with wrong dimension (3 instead of 4).
        let vectors: Vec<f32> = vec![1.0, 2.0, 3.0];
        let rc = unsafe { diskann_insert_batch(h, vectors.as_ptr(), 1, 3) };
        assert_eq!(rc, ERR_DIM_MISMATCH);

        // Try to search with wrong dimension.
        let query: Vec<f32> = vec![1.0, 2.0, 3.0];
        let mut results = vec![0u64; 1];
        let mut distances = vec![0.0f32; 1];
        let rc = unsafe {
            diskann_search(
                h,
                query.as_ptr(),
                3,
                1,
                results.as_mut_ptr(),
                distances.as_mut_ptr(),
            )
        };
        assert_eq!(rc, ERR_DIM_MISMATCH);

        diskann_drop_index(h);
    }

    #[test]
    fn test_invalid_handle() {
        let bad_handle = 999999i64;

        assert_eq!(diskann_index_size(bad_handle), ERR_INVALID_HANDLE);

        let vectors: Vec<f32> = vec![1.0, 2.0, 3.0];
        let rc = unsafe { diskann_insert_batch(bad_handle, vectors.as_ptr(), 1, 3) };
        assert_eq!(rc, ERR_INVALID_HANDLE);

        let query: Vec<f32> = vec![1.0, 2.0];
        let mut results = vec![0u64; 1];
        let mut distances = vec![0.0f32; 1];
        let rc = unsafe {
            diskann_search(
                bad_handle,
                query.as_ptr(),
                2,
                1,
                results.as_mut_ptr(),
                distances.as_mut_ptr(),
            )
        };
        assert_eq!(rc, ERR_INVALID_HANDLE);

        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_size: u64 = 0;
        let rc = unsafe { diskann_serialize(bad_handle, &mut out_ptr, &mut out_size) };
        assert_eq!(rc, ERR_INVALID_HANDLE);
    }

    #[test]
    fn test_empty_index() {
        let h = diskann_create_index(2, DiskANNMetric::L2, 64, 128, 100, 1.2);
        assert!(h > 0);
        assert_eq!(diskann_index_size(h), 0);

        // Searching an empty index should return 0 results.
        let query: Vec<f32> = vec![1.0, 2.0];
        let mut results = vec![0u64; 5];
        let mut distances = vec![0.0f32; 5];
        let found = unsafe {
            diskann_search(
                h,
                query.as_ptr(),
                2,
                5,
                results.as_mut_ptr(),
                distances.as_mut_ptr(),
            )
        };
        assert_eq!(found, 0);

        diskann_drop_index(h);
    }

    #[test]
    fn test_last_error() {
        // Clear any pre-existing error by creating a valid index.
        let h = diskann_create_index(4, DiskANNMetric::L2, 64, 128, 100, 1.2);
        assert!(h > 0);

        // Trigger an error: search with invalid handle.
        let bad_handle = 888888i64;
        let query: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
        let mut results = vec![0u64; 1];
        let mut distances = vec![0.0f32; 1];
        let rc = unsafe {
            diskann_search(
                bad_handle,
                query.as_ptr(),
                4,
                1,
                results.as_mut_ptr(),
                distances.as_mut_ptr(),
            )
        };
        assert_eq!(rc, ERR_INVALID_HANDLE);

        // Read the error message.
        let mut buf = vec![0u8; 256];
        let len =
            unsafe { diskann_last_error(buf.as_mut_ptr() as *mut c_char, buf.len() as u64) };
        assert!(len > 0);
        let msg = std::str::from_utf8(&buf[..len as usize]).unwrap();
        assert!(
            msg.contains("invalid handle"),
            "expected 'invalid handle' in error message, got: {}",
            msg
        );

        diskann_drop_index(h);
    }
}
