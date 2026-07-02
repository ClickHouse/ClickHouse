//! C FFI wrapper around the h3o Rust library, providing API-compatible
//! replacements for the H3 C library functions used by ClickHouse.
//!
//! All function signatures match the original C H3 library exactly, so
//! ClickHouse C++ code can link against either backend without changes.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::panic;

use h3o::CellIndex;
use h3o::DirectedEdgeIndex;
use h3o::Resolution;
use h3o::geom::ContainmentMode;

// ---------------------------------------------------------------------------
// C-compatible type definitions matching h3api.h
// ---------------------------------------------------------------------------

type H3Index = u64;
type H3Error = u32;

const E_SUCCESS: H3Error = 0;
const E_FAILED: H3Error = 1;
const E_MEMORY_BOUNDS: H3Error = 14;
const E_OPTION_INVALID: H3Error = 15;
const MAX_CELL_BNDRY_VERTS: usize = 10;

#[repr(C)]
pub struct LatLng {
    pub lat: f64, // radians
    pub lng: f64, // radians
}

#[repr(C)]
pub struct CellBoundary {
    pub num_verts: i32,
    pub verts: [LatLng; MAX_CELL_BNDRY_VERTS],
}

#[repr(C)]
pub struct GeoLoop {
    pub num_verts: i32,
    pub verts: *mut LatLng,
}

#[repr(C)]
pub struct GeoPolygon {
    pub geoloop: GeoLoop,
    pub num_holes: i32,
    pub holes: *mut GeoLoop,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Normalize an H3 index by setting unused digit positions to 7 (0b111).
///
/// The C H3 library was lenient about unused digit bits, but h3o is strict
/// and rejects indexes with non-7 unused digits. This function fixes up
/// such indexes to maintain backward compatibility.
fn normalize_h3_index(h: H3Index) -> H3Index {
    // Resolution is stored in bits 52-55
    let res = ((h >> 52) & 0xF) as u32;
    if res > 15 {
        return h;
    }
    // Each of the 15 cell digits is 3 bits. Digits beyond the resolution
    // must be 7 (0b111). The unused digits occupy the lowest 3*(15-res) bits.
    let unused_bits = 3 * (15 - res);
    if unused_bits == 0 {
        return h;
    }
    let mask = (1_u64 << unused_bits) - 1;
    h | mask
}

/// Try to parse as CellIndex after normalizing unused digits.
fn try_cell(h: H3Index) -> Option<CellIndex> {
    CellIndex::try_from(normalize_h3_index(h)).ok()
}

/// Try to parse as CellIndex with strict validation (no normalization).
fn try_cell_strict(h: H3Index) -> Option<CellIndex> {
    CellIndex::try_from(h).ok()
}

fn try_resolution(res: i32) -> Option<Resolution> {
    if !(0..=15).contains(&res) {
        return None;
    }
    Resolution::try_from(res as u8).ok()
}

/// Try to parse as DirectedEdgeIndex after normalizing unused digits.
fn try_edge(h: H3Index) -> Option<DirectedEdgeIndex> {
    DirectedEdgeIndex::try_from(normalize_h3_index(h)).ok()
}

/// Try to parse as DirectedEdgeIndex with strict validation (no normalization).
fn try_edge_strict(h: H3Index) -> Option<DirectedEdgeIndex> {
    DirectedEdgeIndex::try_from(h).ok()
}

fn h3o_latlng(ll: &LatLng) -> Option<h3o::LatLng> {
    h3o::LatLng::from_radians(ll.lat, ll.lng).ok()
}

fn fill_boundary(boundary: &h3o::Boundary, out: &mut CellBoundary) {
    out.num_verts = boundary.len() as i32;
    for (i, ll) in boundary.iter().enumerate() {
        if i >= MAX_CELL_BNDRY_VERTS {
            break;
        }
        out.verts[i] = LatLng {
            lat: ll.lat_radians(),
            lng: ll.lng_radians(),
        };
    }
}

// ---------------------------------------------------------------------------
// Index conversion: latLngToCell, cellToLatLng, cellToBoundary
//
// `degsToRads` / `radsToDegs` and the trivial bit-extraction inspectors
// (`getResolution`, `getBaseCellNumber`, `isResClassIII`, `res0CellCount`,
// `pentagonCount`) are inlined directly in `h3api.h` so they do not pay FFI
// overhead in tight ClickHouse per-row loops.
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn latLngToCell(g: *const LatLng, res: i32, out: *mut H3Index) -> H3Error {
    let Some(ll) = h3o_latlng(&*g) else {
        return E_FAILED;
    };
    let Some(resolution) = try_resolution(res) else {
        return E_FAILED;
    };
    let cell = ll.to_cell(resolution);
    *out = u64::from(cell);
    E_SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn cellToLatLng(h3: H3Index, g: *mut LatLng) -> H3Error {
    let Some(cell) = try_cell(h3) else {
        return E_FAILED;
    };
    let ll = h3o::LatLng::from(cell);
    (*g).lat = ll.lat_radians();
    (*g).lng = ll.lng_radians();
    E_SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn cellToBoundary(h3: H3Index, gp: *mut CellBoundary) -> H3Error {
    let Some(cell) = try_cell(h3) else {
        return E_FAILED;
    };
    let boundary = cell.boundary();
    fill_boundary(&boundary, &mut *gp);
    E_SUCCESS
}

// ---------------------------------------------------------------------------
// Grid disk / ring
//
// `maxGridDiskSize` is inlined in `h3api.h` (trivial arithmetic), so it has
// no Rust counterpart here.
// ---------------------------------------------------------------------------

/// Inline equivalent of `maxGridDiskSize` for use inside this crate.
#[inline]
fn max_grid_disk_size(k: i32) -> usize
{
    let k = k as i64;
    (3 * k * (k + 1) + 1) as usize
}

/// H3Error gridDisk(H3Index origin, int k, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn gridDisk(origin: H3Index, k: i32, out: *mut H3Index) -> H3Error {
    let Some(cell) = try_cell(origin) else {
        return E_FAILED;
    };
    if k < 0 {
        return E_FAILED;
    }
    let max_size = max_grid_disk_size(k);
    for (i, c) in cell.grid_disk_safe(k as u32).enumerate() {
        if i >= max_size {
            break;
        }
        *out.add(i) = u64::from(c);
    }
    E_SUCCESS
}

/// H3Error gridDiskUnsafe(H3Index origin, int k, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn gridDiskUnsafe(origin: H3Index, k: i32, out: *mut H3Index) -> H3Error {
    let Some(cell) = try_cell(origin) else {
        return E_FAILED;
    };
    if k < 0 {
        return E_FAILED;
    }
    let max_size = max_grid_disk_size(k);
    for (i, maybe_c) in cell.grid_disk_fast(k as u32).enumerate() {
        if i >= max_size {
            break;
        }
        match maybe_c {
            Some(c) => *out.add(i) = u64::from(c),
            None => {
                *out.add(i) = 0;
                return E_FAILED;
            }
        }
    }
    E_SUCCESS
}

/// H3Error gridRingUnsafe(H3Index origin, int k, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn gridRingUnsafe(origin: H3Index, k: i32, out: *mut H3Index) -> H3Error {
    let Some(cell) = try_cell(origin) else {
        return E_FAILED;
    };
    if k < 0 {
        return E_FAILED;
    }
    let max_size = if k == 0 { 1 } else { 6 * k as usize };
    for (i, maybe_c) in cell.grid_ring_fast(k as u32).enumerate() {
        if i >= max_size {
            break;
        }
        match maybe_c {
            Some(c) => *out.add(i) = u64::from(c),
            None => {
                *out.add(i) = 0;
                return E_FAILED;
            }
        }
    }
    E_SUCCESS
}

// ---------------------------------------------------------------------------
// Grid path / distance
// ---------------------------------------------------------------------------

/// H3Error gridPathCellsSize(H3Index start, H3Index end, int64_t *size)
#[no_mangle]
pub unsafe extern "C" fn gridPathCellsSize(
    start: H3Index,
    end: H3Index,
    size: *mut i64,
) -> H3Error {
    let (Some(s), Some(e)) = (try_cell(start), try_cell(end)) else {
        return E_FAILED;
    };
    match s.grid_path_cells_size(e) {
        Ok(sz) => {
            *size = sz as i64;
            E_SUCCESS
        }
        Err(_) => E_FAILED,
    }
}

/// H3Error gridPathCells(H3Index start, H3Index end, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn gridPathCells(
    start: H3Index,
    end: H3Index,
    out: *mut H3Index,
) -> H3Error {
    let (Some(s), Some(e)) = (try_cell(start), try_cell(end)) else {
        return E_FAILED;
    };
    // The C++ caller sizes the `out` buffer from a separate `gridPathCellsSize`
    // call, so bound the fill loop by the same estimate. If the path ever yields
    // more cells than the estimate, fail with `E_MEMORY_BOUNDS` instead of writing
    // past the caller's buffer.
    let max_size = match s.grid_path_cells_size(e) {
        Ok(sz) => sz as usize,
        Err(_) => return E_FAILED,
    };
    match s.grid_path_cells(e) {
        Ok(iter) => {
            for (i, result) in iter.enumerate() {
                if i >= max_size {
                    return E_MEMORY_BOUNDS;
                }
                match result {
                    Ok(c) => *out.add(i) = u64::from(c),
                    Err(_) => return E_FAILED,
                }
            }
            E_SUCCESS
        }
        Err(_) => E_FAILED,
    }
}

/// H3Error gridDistance(H3Index origin, H3Index h3, int64_t *distance)
#[no_mangle]
pub unsafe extern "C" fn gridDistance(
    origin: H3Index,
    h3: H3Index,
    distance: *mut i64,
) -> H3Error {
    let (Some(o), Some(d)) = (try_cell(origin), try_cell(h3)) else {
        return E_FAILED;
    };
    match o.grid_distance(d) {
        Ok(dist) => {
            *distance = dist as i64;
            E_SUCCESS
        }
        Err(_) => E_FAILED,
    }
}

// ---------------------------------------------------------------------------
// Cell inspection
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn isValidCell(h: H3Index) -> i32 {
    if try_cell_strict(h).is_some() { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn isPentagon(h: H3Index) -> i32 {
    try_cell(h).map_or(0, |c| if c.is_pentagon() { 1 } else { 0 })
}

// ---------------------------------------------------------------------------
// Cell hierarchy
// ---------------------------------------------------------------------------

/// H3Error cellToParent(H3Index h, int parentRes, H3Index *parent)
#[no_mangle]
pub unsafe extern "C" fn cellToParent(
    h: H3Index,
    parent_res: i32,
    parent: *mut H3Index,
) -> H3Error {
    let (Some(cell), Some(res)) = (try_cell(h), try_resolution(parent_res)) else {
        return E_FAILED;
    };
    match cell.parent(res) {
        Some(p) => {
            *parent = u64::from(p);
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// H3Error cellToChildrenSize(H3Index h, int childRes, int64_t *out)
#[no_mangle]
pub unsafe extern "C" fn cellToChildrenSize(
    h: H3Index,
    child_res: i32,
    out: *mut i64,
) -> H3Error {
    let (Some(cell), Some(res)) = (try_cell(h), try_resolution(child_res)) else {
        return E_FAILED;
    };
    let count = cell.children_count(res);
    *out = if count > i64::MAX as u64 { i64::MAX } else { count as i64 };
    E_SUCCESS
}

/// H3Error cellToChildren(H3Index h, int childRes, H3Index *children)
#[no_mangle]
pub unsafe extern "C" fn cellToChildren(
    h: H3Index,
    child_res: i32,
    children: *mut H3Index,
) -> H3Error {
    let (Some(cell), Some(res)) = (try_cell(h), try_resolution(child_res)) else {
        return E_FAILED;
    };
    let max_size = cell.children_count(res) as usize;
    for (i, child) in cell.children(res).enumerate() {
        if i >= max_size {
            break;
        }
        *children.add(i) = u64::from(child);
    }
    E_SUCCESS
}

/// H3Error cellToCenterChild(H3Index h, int childRes, H3Index *child)
#[no_mangle]
pub unsafe extern "C" fn cellToCenterChild(
    h: H3Index,
    child_res: i32,
    child: *mut H3Index,
) -> H3Error {
    let (Some(cell), Some(res)) = (try_cell(h), try_resolution(child_res)) else {
        return E_FAILED;
    };
    match cell.center_child(res) {
        Some(c) => {
            *child = u64::from(c);
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

// ---------------------------------------------------------------------------
// String conversion
// ---------------------------------------------------------------------------

/// H3Error stringToH3(const char *str, H3Index *out)
///
/// Matches the C H3 library (`sscanf(str, "%" PRIx64, out)`): reads the longest
/// valid hex prefix (after optional leading whitespace, optional sign, and
/// optional "0x"/"0X"), ignores trailing garbage. Saturates to `u64::MAX` on
/// overflow (more than 16 significant hex digits) and applies unsigned
/// wraparound for a leading `-`, matching glibc `sscanf` behavior.
/// Returns `E_FAILED` only if no hex digit is parsed.
#[no_mangle]
pub unsafe extern "C" fn stringToH3(str_ptr: *const c_char, out: *mut H3Index) -> H3Error {
    if str_ptr.is_null() {
        return E_FAILED;
    }
    let c_str = CStr::from_ptr(str_ptr);
    let bytes = c_str.to_bytes();

    // Skip leading whitespace (matches sscanf).
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    // Optional sign (matches sscanf "%x" which accepts an optional sign and
    // applies unsigned wraparound, e.g. "-1" -> 0xFFFFFFFFFFFFFFFF).
    let mut negate = false;
    if i < bytes.len() && (bytes[i] == b'-' || bytes[i] == b'+') {
        negate = bytes[i] == b'-';
        i += 1;
    }
    // Optional "0x"/"0X" prefix (sscanf %x accepts it). When the prefix is
    // present but no hex digit follows (e.g. "0x", "0xg", "-0x"), glibc
    // `sscanf` still returns a successful conversion with value 0, so we treat
    // the consumed prefix as if a leading `0` digit had been read.
    let mut had_prefix = false;
    if i + 1 < bytes.len() && bytes[i] == b'0' && (bytes[i + 1] == b'x' || bytes[i + 1] == b'X') {
        i += 2;
        had_prefix = true;
    }

    let mut result: u64 = 0;
    let mut count = 0usize;
    let mut overflow = false;
    while i < bytes.len() {
        let digit = match bytes[i] {
            b'0'..=b'9' => bytes[i] - b'0',
            b'a'..=b'f' => bytes[i] - b'a' + 10,
            b'A'..=b'F' => bytes[i] - b'A' + 10,
            _ => break,
        };
        if !overflow {
            // Saturate to u64::MAX once a left-shift would discard a non-zero
            // bit (matches glibc `sscanf` ERANGE behavior).
            if result > (u64::MAX >> 4) {
                result = u64::MAX;
                overflow = true;
            } else {
                result = (result << 4) | u64::from(digit);
            }
        }
        count += 1;
        i += 1;
    }

    if count == 0 && !had_prefix {
        return E_FAILED;
    }
    // After ERANGE saturation glibc `sscanf` returns `UINT64_MAX` regardless of
    // sign, so do not apply unsigned negation to an already-saturated value.
    if negate && !overflow {
        result = result.wrapping_neg();
    }
    *out = result;
    E_SUCCESS
}

/// H3Error h3ToString(H3Index h, char *str, size_t sz)
#[no_mangle]
pub unsafe extern "C" fn h3ToString(h: H3Index, str_ptr: *mut c_char, sz: usize) -> H3Error {
    if str_ptr.is_null() || sz == 0 {
        return E_FAILED;
    }
    // H3 indexes are 16 hex chars + NUL = 17 bytes minimum.
    // Match the C API behavior: return E_MEMORY_BOUNDS if buffer is too small.
    if sz < 17 {
        return E_MEMORY_BOUNDS;
    }
    let hex = format!("{:x}", h);
    let bytes = hex.as_bytes();
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), str_ptr as *mut u8, bytes.len());
    *str_ptr.add(bytes.len()) = 0; // null terminator
    E_SUCCESS
}

// ---------------------------------------------------------------------------
// Neighbors
// ---------------------------------------------------------------------------

/// H3Error areNeighborCells(H3Index origin, H3Index destination, int *out)
#[no_mangle]
pub unsafe extern "C" fn areNeighborCells(
    origin: H3Index,
    destination: H3Index,
    out: *mut i32,
) -> H3Error {
    let (Some(o), Some(d)) = (try_cell(origin), try_cell(destination)) else {
        return E_FAILED;
    };
    match o.is_neighbor_with(d) {
        Ok(true) => {
            *out = 1;
            E_SUCCESS
        }
        Ok(false) => {
            *out = 0;
            E_SUCCESS
        }
        Err(_) => E_FAILED,
    }
}

// ---------------------------------------------------------------------------
// Directed edges
// ---------------------------------------------------------------------------

/// H3Error cellsToDirectedEdge(H3Index origin, H3Index destination, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn cellsToDirectedEdge(
    origin: H3Index,
    destination: H3Index,
    out: *mut H3Index,
) -> H3Error {
    let (Some(o), Some(d)) = (try_cell(origin), try_cell(destination)) else {
        return E_FAILED;
    };
    match o.edge(d) {
        Some(e) => {
            *out = u64::from(e);
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

#[no_mangle]
pub extern "C" fn isValidDirectedEdge(edge: H3Index) -> i32 {
    if try_edge_strict(edge).is_some() { 1 } else { 0 }
}

/// H3Error getDirectedEdgeOrigin(H3Index edge, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn getDirectedEdgeOrigin(edge: H3Index, out: *mut H3Index) -> H3Error {
    match try_edge(edge) {
        Some(e) => {
            *out = u64::from(e.origin());
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// H3Error getDirectedEdgeDestination(H3Index edge, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn getDirectedEdgeDestination(edge: H3Index, out: *mut H3Index) -> H3Error {
    match try_edge(edge) {
        Some(e) => {
            *out = u64::from(e.destination());
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// H3Error directedEdgeToCells(H3Index edge, H3Index *originDestination)
#[no_mangle]
pub unsafe extern "C" fn directedEdgeToCells(
    edge: H3Index,
    origin_destination: *mut H3Index,
) -> H3Error {
    let Some(e) = try_edge(edge) else {
        *origin_destination = 0;
        *origin_destination.add(1) = 0;
        return E_FAILED;
    };
    let (o, d) = e.cells();
    *origin_destination = u64::from(o);
    *origin_destination.add(1) = u64::from(d);
    E_SUCCESS
}

/// H3Error originToDirectedEdges(H3Index origin, H3Index *edges)
#[no_mangle]
pub unsafe extern "C" fn originToDirectedEdges(
    origin: H3Index,
    edges: *mut H3Index,
) -> H3Error {
    for i in 0..6 {
        *edges.add(i) = 0;
    }
    let Some(cell) = try_cell(origin) else {
        return E_FAILED;
    };
    // The H3 C API exposes a fixed six-slot layout where slot `d - 1` holds the
    // edge for direction `d` (1..=6). For pentagon origins the `K` direction (1)
    // is deleted, so slot 0 must stay `H3_NULL`. h3o's `edges()` iterator omits
    // the deleted edge, so placing edges by iterator position would shift the
    // remaining edges into the wrong slots for pentagons. Place each edge by its
    // encoded direction instead. The direction is stored in the reserved bits
    // (56..=58) of the directed-edge index, matching the H3 index layout.
    for e in cell.edges() {
        let dir = ((u64::from(e) >> 56) & 0x7) as usize;
        if (1..=6).contains(&dir) {
            *edges.add(dir - 1) = u64::from(e);
        }
    }
    E_SUCCESS
}

/// H3Error directedEdgeToBoundary(H3Index edge, CellBoundary *gb)
#[no_mangle]
pub unsafe extern "C" fn directedEdgeToBoundary(
    edge: H3Index,
    gb: *mut CellBoundary,
) -> H3Error {
    let Some(e) = try_edge(edge) else {
        (*gb).num_verts = 0;
        return E_FAILED;
    };
    let boundary = e.boundary();
    fill_boundary(&boundary, &mut *gb);
    E_SUCCESS
}

// ---------------------------------------------------------------------------
// Area functions
// ---------------------------------------------------------------------------

/// H3Error cellAreaRads2(H3Index h, double *out)
#[no_mangle]
pub unsafe extern "C" fn cellAreaRads2(h: H3Index, out: *mut f64) -> H3Error {
    match try_cell(h) {
        Some(c) => {
            *out = c.area_rads2();
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// H3Error cellAreaKm2(H3Index h, double *out)
#[no_mangle]
pub unsafe extern "C" fn cellAreaKm2(h: H3Index, out: *mut f64) -> H3Error {
    match try_cell(h) {
        Some(c) => {
            *out = c.area_km2();
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// H3Error cellAreaM2(H3Index h, double *out)
#[no_mangle]
pub unsafe extern "C" fn cellAreaM2(h: H3Index, out: *mut f64) -> H3Error {
    match try_cell(h) {
        Some(c) => {
            *out = c.area_m2();
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// Average hexagon area in km² for each resolution 0–15.
/// Values from the C H3 library lookup table to preserve backward compatibility.
static HEXAGON_AREA_AVG_KM2: [f64; 16] = [
    4.357449416078383e+06,
    6.097884417941332e+05,
    8.680178039899720e+04,
    1.239343465508816e+04,
    1.770347654491307e+03,
    2.529038581819449e+02,
    3.612906216441245e+01,
    5.161293359717191e+00,
    7.373275975944177e-01,
    1.053325134272067e-01,
    1.504750190766435e-02,
    2.149643129451879e-03,
    3.070918756316060e-04,
    4.387026794728296e-05,
    6.267181135324313e-06,
    8.953115907605790e-07,
];

/// Average hexagon area in m² for each resolution 0–15.
static HEXAGON_AREA_AVG_M2: [f64; 16] = [
    4.357449416078390e+12,
    6.097884417941339e+11,
    8.680178039899731e+10,
    1.239343465508818e+10,
    1.770347654491309e+09,
    2.529038581819452e+08,
    3.612906216441250e+07,
    5.161293359717198e+06,
    7.373275975944188e+05,
    1.053325134272069e+05,
    1.504750190766437e+04,
    2.149643129451882e+03,
    3.070918756316063e+02,
    4.387026794728301e+01,
    6.267181135324322e+00,
    8.953115907605802e-01,
];

/// H3Error getHexagonAreaAvgKm2(int res, double *out)
#[no_mangle]
pub unsafe extern "C" fn getHexagonAreaAvgKm2(res: i32, out: *mut f64) -> H3Error {
    if (0..=15).contains(&res) {
        *out = HEXAGON_AREA_AVG_KM2[res as usize];
        E_SUCCESS
    } else {
        E_FAILED
    }
}

/// H3Error getHexagonAreaAvgM2(int res, double *out)
#[no_mangle]
pub unsafe extern "C" fn getHexagonAreaAvgM2(res: i32, out: *mut f64) -> H3Error {
    if (0..=15).contains(&res) {
        *out = HEXAGON_AREA_AVG_M2[res as usize];
        E_SUCCESS
    } else {
        E_FAILED
    }
}

// ---------------------------------------------------------------------------
// Edge length functions
// ---------------------------------------------------------------------------

/// Average hexagon edge length in km for each resolution 0–15.
/// Values from the C H3 library lookup table (contrib/h3/src/h3lib/lib/latLng.c).
static HEXAGON_EDGE_LENGTH_AVG_KM: [f64; 16] = [
    1281.256011,
    483.0568391,
    182.5129565,
    68.97922179,
    26.07175968,
    9.854090990,
    3.724532667,
    1.406475763,
    0.531414010,
    0.200786148,
    0.075863783,
    0.028663897,
    0.010830188,
    0.004092010,
    0.001546100,
    0.000584169,
];

/// Average hexagon edge length in m for each resolution 0–15.
/// Values from the C H3 library lookup table (contrib/h3/src/h3lib/lib/latLng.c).
static HEXAGON_EDGE_LENGTH_AVG_M: [f64; 16] = [
    1281256.011,
    483056.8391,
    182512.9565,
    68979.22179,
    26071.75968,
    9854.090990,
    3724.532667,
    1406.475763,
    531.4140101,
    200.7861476,
    75.86378287,
    28.66389748,
    10.83018784,
    4.092010473,
    1.546099657,
    0.584168630,
];

/// H3Error getHexagonEdgeLengthAvgKm(int res, double *out)
#[no_mangle]
pub unsafe extern "C" fn getHexagonEdgeLengthAvgKm(res: i32, out: *mut f64) -> H3Error {
    if (0..=15).contains(&res) {
        *out = HEXAGON_EDGE_LENGTH_AVG_KM[res as usize];
        E_SUCCESS
    } else {
        E_FAILED
    }
}

/// H3Error getHexagonEdgeLengthAvgM(int res, double *out)
#[no_mangle]
pub unsafe extern "C" fn getHexagonEdgeLengthAvgM(res: i32, out: *mut f64) -> H3Error {
    if (0..=15).contains(&res) {
        *out = HEXAGON_EDGE_LENGTH_AVG_M[res as usize];
        E_SUCCESS
    } else {
        E_FAILED
    }
}

/// H3Error edgeLengthRads(H3Index edge, double *length)
#[no_mangle]
pub unsafe extern "C" fn edgeLengthRads(edge: H3Index, length: *mut f64) -> H3Error {
    match try_edge(edge) {
        Some(e) => {
            *length = e.length_rads();
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// H3Error edgeLengthKm(H3Index edge, double *length)
#[no_mangle]
pub unsafe extern "C" fn edgeLengthKm(edge: H3Index, length: *mut f64) -> H3Error {
    match try_edge(edge) {
        Some(e) => {
            *length = e.length_km();
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// H3Error edgeLengthM(H3Index edge, double *length)
#[no_mangle]
pub unsafe extern "C" fn edgeLengthM(edge: H3Index, length: *mut f64) -> H3Error {
    match try_edge(edge) {
        Some(e) => {
            *length = e.length_m();
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

// ---------------------------------------------------------------------------
// Distance functions (great circle / haversine)
//
// Computed directly from raw radians without routing through `h3o::LatLng`,
// so out-of-range coordinates still produce a mathematically valid haversine
// distance instead of collapsing to a misleading 0.0. Matches the formula
// used by `h3o::LatLng::distance_rads` and the original H3 C library.
// ---------------------------------------------------------------------------

fn haversine_rads(a: &LatLng, b: &LatLng) -> f64 {
    let sin_lat = ((b.lat - a.lat) * 0.5).sin();
    let sin_lng = ((b.lng - a.lng) * 0.5).sin();
    let h = sin_lat * sin_lat + a.lat.cos() * b.lat.cos() * sin_lng * sin_lng;
    2.0 * h.sqrt().atan2((1.0 - h).sqrt())
}

/// double greatCircleDistanceRads(const LatLng *a, const LatLng *b)
#[no_mangle]
pub unsafe extern "C" fn greatCircleDistanceRads(a: *const LatLng, b: *const LatLng) -> f64 {
    haversine_rads(&*a, &*b)
}

/// double greatCircleDistanceKm(const LatLng *a, const LatLng *b)
#[no_mangle]
pub unsafe extern "C" fn greatCircleDistanceKm(a: *const LatLng, b: *const LatLng) -> f64 {
    haversine_rads(&*a, &*b) * h3o::EARTH_RADIUS_KM
}

/// double greatCircleDistanceM(const LatLng *a, const LatLng *b)
#[no_mangle]
pub unsafe extern "C" fn greatCircleDistanceM(a: *const LatLng, b: *const LatLng) -> f64 {
    haversine_rads(&*a, &*b) * h3o::EARTH_RADIUS_KM * 1000.0
}

// ---------------------------------------------------------------------------
// Cell count functions
// ---------------------------------------------------------------------------

/// H3Error getNumCells(int res, int64_t *out)
#[no_mangle]
pub unsafe extern "C" fn getNumCells(res: i32, out: *mut i64) -> H3Error {
    match try_resolution(res) {
        Some(r) => {
            *out = r.cell_count() as i64;
            E_SUCCESS
        }
        None => E_FAILED,
    }
}

/// H3Error getRes0Cells(H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn getRes0Cells(out: *mut H3Index) -> H3Error {
    for (i, cell) in CellIndex::base_cells().enumerate() {
        *out.add(i) = u64::from(cell);
    }
    E_SUCCESS
}

/// H3Error getPentagons(int res, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn getPentagons(res: i32, out: *mut H3Index) -> H3Error {
    let Some(resolution) = try_resolution(res) else {
        return E_FAILED;
    };
    for (i, cell) in resolution.pentagons().enumerate() {
        *out.add(i) = u64::from(cell);
    }
    E_SUCCESS
}

// ---------------------------------------------------------------------------
// Icosahedron faces
// ---------------------------------------------------------------------------

/// H3Error maxFaceCount(H3Index h3, int *out)
#[no_mangle]
pub unsafe extern "C" fn maxFaceCount(h3: H3Index, out: *mut i32) -> H3Error {
    let Some(cell) = try_cell(h3) else {
        return E_FAILED;
    };
    *out = if cell.is_pentagon() { 5 } else { 2 };
    E_SUCCESS
}

/// H3Error getIcosahedronFaces(H3Index h3, int *out)
#[no_mangle]
pub unsafe extern "C" fn getIcosahedronFaces(h3: H3Index, out: *mut i32) -> H3Error {
    let Some(cell) = try_cell(h3) else {
        return E_FAILED;
    };
    let max = if cell.is_pentagon() { 5 } else { 2 };
    // Initialize to -1
    for i in 0..max {
        *out.add(i) = -1;
    }
    for (i, face) in cell.icosahedron_faces().iter().enumerate() {
        if i >= max {
            break;
        }
        *out.add(i) = u8::from(face) as i32;
    }
    E_SUCCESS
}

// ---------------------------------------------------------------------------
// Polygon operations
// ---------------------------------------------------------------------------

/// Map H3 `polygonToCellsExperimental` flags to `h3o` containment modes:
/// 0 = `CONTAINMENT_CENTER`, 1 = `CONTAINMENT_FULL`, 2 = `CONTAINMENT_OVERLAPPING`.
/// Returns `None` for any other value.
///
/// The H3 C library also defines `3 = CONTAINMENT_OVERLAPPING_BBOX`, a
/// deliberately over-inclusive mode that selects every cell whose *bounding
/// box* overlaps the shape. `h3o` has no equivalent: its closest mode
/// (`ContainmentMode::Covers`) is `IntersectsBoundary` plus the
/// fully-covered-by-a-single-cell case, which produces a strict subset of the
/// bounding-box result. Rather than silently return a different (smaller) set
/// for mode 3 — which would break the "result set is preserved" guarantee of
/// this backend swap — we reject it here so callers receive an explicit error.
fn containment_mode_from_flags(flags: u32) -> Option<ContainmentMode> {
    match flags {
        0 => Some(ContainmentMode::ContainsCentroid),
        1 => Some(ContainmentMode::ContainsBoundary),
        2 => Some(ContainmentMode::IntersectsBoundary),
        _ => None,
    }
}

/// Build an `h3o` tiler for the given C polygon, resolution and containment
/// mode. Returns `None` if the resolution is invalid or the geometry is
/// rejected by `h3o`.
unsafe fn build_polygon_tiler(
    geo_polygon: *const GeoPolygon,
    res: i32,
    mode: ContainmentMode,
) -> Option<h3o::geom::Tiler> {
    let resolution = try_resolution(res)?;
    let geo_poly = c_polygon_to_geo(&*geo_polygon);
    let mut tiler = h3o::geom::TilerBuilder::new(resolution)
        .containment_mode(mode)
        .disable_radians_conversion()
        .build();
    if tiler.add(geo_poly).is_err() {
        return None;
    }
    Some(tiler)
}

unsafe fn max_polygon_to_cells_size_impl(
    geo_polygon: *const GeoPolygon,
    res: i32,
    mode: ContainmentMode,
    out: *mut i64,
) -> H3Error {
    let Some(tiler) = build_polygon_tiler(geo_polygon, res, mode) else {
        return E_FAILED;
    };
    let count = tiler.coverage_size_hint();
    *out = if count > i64::MAX as usize { i64::MAX } else { count as i64 };
    E_SUCCESS
}

unsafe fn polygon_to_cells_impl(
    geo_polygon: *const GeoPolygon,
    res: i32,
    mode: ContainmentMode,
    max_count: usize,
    out: *mut H3Index,
) -> H3Error {
    let Some(tiler) = build_polygon_tiler(geo_polygon, res, mode) else {
        return E_FAILED;
    };
    // The caller allocates `max_count` slots (from the matching
    // `maxPolygonToCellsSize*` call). Producing more cells means the two
    // backend calls disagree or the buffer is too small; surface that as
    // `E_MEMORY_BOUNDS` rather than silently truncating or overflowing.
    let mut count = 0usize;
    for cell in tiler.into_coverage() {
        if count >= max_count {
            return E_MEMORY_BOUNDS;
        }
        *out.add(count) = u64::from(cell);
        count += 1;
    }
    E_SUCCESS
}

/// H3Error maxPolygonToCellsSize(const GeoPolygon *geoPolygon, int res,
///                               uint32_t flags, int64_t *out)
///
/// `h3o` validates polygon coordinates inside `TilerBuilder::add` and surfaces
/// invalid geometries as an `Err`, but the transitive `geo` line-sweep code
/// has panicked on malformed inputs before (see
/// https://github.com/HydroniumLabs/h3o/issues/44). We wrap the body in
/// `catch_unwind` so any future panic in geometry code is converted to
/// `E_FAILED` and surfaces as a normal ClickHouse exception rather than
/// crossing the FFI boundary and aborting the server.
///
/// The non-experimental API only supports the default containment mode, so
/// reject `flags != 0` explicitly (matching the H3 C library, which returns an
/// error for any flag here and only honors containment modes through the
/// `*Experimental` variants).
#[no_mangle]
pub unsafe extern "C" fn maxPolygonToCellsSize(
    geo_polygon: *const GeoPolygon,
    res: i32,
    flags: u32,
    out: *mut i64,
) -> H3Error {
    if flags != 0 {
        return E_FAILED;
    }
    panic::catch_unwind(|| {
        max_polygon_to_cells_size_impl(geo_polygon, res, ContainmentMode::ContainsCentroid, out)
    })
    .unwrap_or(E_FAILED)
}

/// H3Error polygonToCells(const GeoPolygon *geoPolygon, int res,
///                        uint32_t flags, H3Index *out)
///
/// See `maxPolygonToCellsSize` — wrapped in `catch_unwind` to convert any
/// panic from transitive geometry code into `E_FAILED` instead of aborting
/// the server across the FFI boundary.
#[no_mangle]
pub unsafe extern "C" fn polygonToCells(
    geo_polygon: *const GeoPolygon,
    res: i32,
    flags: u32,
    out: *mut H3Index,
) -> H3Error {
    if flags != 0 {
        return E_FAILED;
    }
    // The caller sized the buffer with `maxPolygonToCellsSize` /
    // `coverage_size_hint`, so use the same hint as the capacity bound.
    panic::catch_unwind(|| {
        let Some(tiler) = build_polygon_tiler(geo_polygon, res, ContainmentMode::ContainsCentroid)
        else {
            return E_FAILED;
        };
        let max_size = tiler.coverage_size_hint();
        let mut count = 0usize;
        for cell in tiler.into_coverage() {
            if count >= max_size {
                return E_MEMORY_BOUNDS;
            }
            *out.add(count) = u64::from(cell);
            count += 1;
        }
        E_SUCCESS
    })
    .unwrap_or(E_FAILED)
}

/// H3Error maxPolygonToCellsSizeExperimental(const GeoPolygon *geoPolygon,
///                                           int res, uint32_t flags, int64_t *out)
///
/// Experimental variant honoring the containment mode encoded in `flags`
/// (see `containment_mode_from_flags`). Used by the `h3PolygonToCellsWithContainment`
/// ClickHouse function. Wrapped in `catch_unwind` like `maxPolygonToCellsSize`.
#[no_mangle]
pub unsafe extern "C" fn maxPolygonToCellsSizeExperimental(
    geo_polygon: *const GeoPolygon,
    res: i32,
    flags: u32,
    out: *mut i64,
) -> H3Error {
    let Some(mode) = containment_mode_from_flags(flags) else {
        return E_OPTION_INVALID;
    };
    panic::catch_unwind(|| max_polygon_to_cells_size_impl(geo_polygon, res, mode, out))
        .unwrap_or(E_FAILED)
}

/// H3Error polygonToCellsExperimental(const GeoPolygon *geoPolygon, int res,
///                                    uint32_t flags, int64_t size, H3Index *out)
///
/// Experimental variant honoring the containment mode encoded in `flags`. The
/// `size` argument is the number of `H3Index` slots the caller allocated in
/// `out` (obtained from `maxPolygonToCellsSizeExperimental`); producing more
/// cells returns `E_MEMORY_BOUNDS`. Wrapped in `catch_unwind` like `polygonToCells`.
#[no_mangle]
pub unsafe extern "C" fn polygonToCellsExperimental(
    geo_polygon: *const GeoPolygon,
    res: i32,
    flags: u32,
    size: i64,
    out: *mut H3Index,
) -> H3Error {
    let Some(mode) = containment_mode_from_flags(flags) else {
        return E_OPTION_INVALID;
    };
    if size < 0 {
        return E_MEMORY_BOUNDS;
    }
    let max_count = size as usize;
    panic::catch_unwind(|| polygon_to_cells_impl(geo_polygon, res, mode, max_count, out))
        .unwrap_or(E_FAILED)
}

/// const char *describeH3Error(H3Error err)
///
/// Returns a static, null-terminated description of an `H3Error` code,
/// matching the strings used by the H3 C library. Returned pointers are
/// `'static` (string literals) so the caller never frees them.
#[no_mangle]
pub extern "C" fn describeH3Error(err: H3Error) -> *const c_char {
    let msg: &'static [u8] = match err {
        0 => b"Success\0",
        1 => b"The operation failed but a more specific error is not available\0",
        2 => b"Argument was outside of acceptable range\0",
        3 => b"Latitude or longitude arguments were outside of acceptable range\0",
        4 => b"Resolution argument was outside of acceptable range\0",
        5 => b"H3Index cell argument was not valid\0",
        6 => b"H3Index directed edge argument was not valid\0",
        7 => b"H3Index undirected edge argument was not valid\0",
        8 => b"H3Index vertex argument was not valid\0",
        9 => b"Pentagon distortion was encountered\0",
        10 => b"Duplicate input was encountered in the arguments\0",
        11 => b"H3Index cell arguments were not neighbors\0",
        12 => b"H3Index cell arguments had incompatible resolutions\0",
        13 => b"Necessary memory allocation failed\0",
        14 => b"Bounds of provided memory were not large enough\0",
        15 => b"Mode or flags argument was not valid for the operation\0",
        _ => b"Unknown H3 error code\0",
    };
    msg.as_ptr() as *const c_char
}

unsafe fn c_polygon_to_geo(poly: &GeoPolygon) -> geo::Polygon<f64> {
    let exterior = geoloop_to_linestring(&poly.geoloop);
    let mut holes = Vec::new();
    if !poly.holes.is_null() && poly.num_holes > 0 {
        let num_holes = poly.num_holes as usize;
        for i in 0..num_holes {
            let hole = &*poly.holes.add(i);
            holes.push(geoloop_to_linestring(hole));
        }
    }
    geo::Polygon::new(exterior, holes)
}

unsafe fn geoloop_to_linestring(loop_: &GeoLoop) -> geo::LineString<f64> {
    if loop_.verts.is_null() || loop_.num_verts <= 0 {
        return geo::LineString::new(Vec::new());
    }
    let num_verts = loop_.num_verts as usize;
    let mut coords = Vec::with_capacity(num_verts);
    for i in 0..num_verts {
        let v = &*loop_.verts.add(i);
        coords.push(geo::Coord {
            x: v.lng,
            y: v.lat,
        });
    }
    geo::LineString::new(coords)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_cell() {
        assert_eq!(isValidCell(0), 0);
        assert_eq!(isValidCell(0x85283473fffffff), 1);
    }

    #[test]
    fn test_max_grid_disk_size() {
        assert_eq!(max_grid_disk_size(0), 1);
        assert_eq!(max_grid_disk_size(1), 7);
        assert_eq!(max_grid_disk_size(2), 19);
    }

    #[test]
    fn test_string_to_h3_legacy_formats() {
        use std::ffi::CString;
        let mut out: H3Index = 0;
        // Standard hex
        let s = CString::new("85283473fffffff").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, 0x85283473FFFFFFF);
        // 0x prefix
        let s = CString::new("0x85283473fffffffL").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, 0x85283473FFFFFFF);
        // Partial hex prefix (matches sscanf): "foo" -> 0xf (15), stops at 'o'
        out = 0;
        let s = CString::new("foo").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, 15);
        // No hex digit -> E_FAILED
        out = 0;
        let s = CString::new("xyz").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_FAILED);
    }

    #[test]
    fn test_string_to_h3_signed_prefix() {
        use std::ffi::CString;
        let mut out: H3Index = 0;
        // "-1" -> sscanf wraps to 0xFFFFFFFFFFFFFFFF (unsigned negation).
        let s = CString::new("-1").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, u64::MAX);
        // "+1" -> 1
        out = 0;
        let s = CString::new("+1").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, 1);
        // "-0xff" -> -255 wraps to 0xFFFFFFFFFFFFFF01
        out = 0;
        let s = CString::new("-0xff").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, 0xFFFFFFFFFFFFFF01);
        // Sign with no digits -> E_FAILED
        out = 0;
        let s = CString::new("-z").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_FAILED);
    }

    #[test]
    fn test_string_to_h3_prefix_only() {
        use std::ffi::CString;
        // glibc `sscanf("%x")` returns success with value 0 when a "0x"
        // prefix is consumed but no hex digit follows. The Rust H3 backend
        // advertises sscanf-compatible semantics, so reproduce that exactly.
        for input in ["0x", "0X", "0xg", "-0x", "+0x"] {
            let mut out: H3Index = 0xdead_beef;
            let s = CString::new(input).unwrap();
            assert_eq!(
                unsafe { stringToH3(s.as_ptr(), &mut out) },
                E_SUCCESS,
                "input {input:?} should succeed",
            );
            assert_eq!(out, 0, "input {input:?} should produce 0");
        }
    }

    #[test]
    fn test_string_to_h3_overflow_saturates() {
        use std::ffi::CString;
        let mut out: H3Index = 0;
        // Exactly 16 hex digits at u64::MAX boundary.
        let s = CString::new("ffffffffffffffff").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, u64::MAX);
        // Leading zeros do not count toward overflow.
        out = 0;
        let s = CString::new("00ffffffffffffffff").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, u64::MAX);
        // 17 significant hex digits saturate to u64::MAX.
        out = 0;
        let s = CString::new("1ffffffffffffffff").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, u64::MAX);
        // 17 digits with low value still overflows because the top digit shifts out.
        out = 0;
        let s = CString::new("10000000000000000").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr(), &mut out) }, E_SUCCESS);
        assert_eq!(out, u64::MAX);
    }

    #[test]
    fn test_normalize_h3_index() {
        assert_eq!(normalize_h3_index(0x085283473FFFFFFD), 0x085283473FFFFFFF);
        assert_eq!(normalize_h3_index(0x085283473FFFFFFF), 0x085283473FFFFFFF);
        assert_eq!(normalize_h3_index(0x115283473FFFFFFD), 0x115283473FFFFFFF);
    }

    #[test]
    fn test_directed_edge_operations() {
        let edge_valid = 0x115283473FFFFFFF_u64;
        assert_eq!(isValidDirectedEdge(edge_valid), 1);

        let mut dest: H3Index = 0;
        assert_eq!(unsafe { getDirectedEdgeDestination(edge_valid, &mut dest) }, E_SUCCESS);
        assert_eq!(dest, 599686043507097599);

        let mut orig: H3Index = 0;
        assert_eq!(unsafe { getDirectedEdgeOrigin(edge_valid, &mut orig) }, E_SUCCESS);
        assert_eq!(orig, 599686042433355775);
    }

    #[test]
    fn test_cell_to_parent() {
        let h = 0x85283473fffffff_u64;
        let mut parent: H3Index = 0;
        assert_eq!(unsafe { cellToParent(h, 4, &mut parent) }, E_SUCCESS);
        assert_ne!(parent, 0);
    }

    #[test]
    fn test_grid_distance() {
        let a = 0x85283473fffffff_u64;
        let b = 0x85283477fffffff_u64;
        let mut dist: i64 = 0;
        assert_eq!(unsafe { gridDistance(a, b, &mut dist) }, E_SUCCESS);
        assert!(dist >= 0);
    }
}
