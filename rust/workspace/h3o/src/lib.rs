//! C FFI wrapper around the h3o Rust library, providing API-compatible
//! replacements for the H3 C library functions used by ClickHouse.
//!
//! All function signatures match the original C H3 library exactly, so
//! ClickHouse C++ code can link against either backend without changes.

use std::ffi::CStr;
use std::os::raw::c_char;

use h3o::CellIndex;
use h3o::DirectedEdgeIndex;
use h3o::Resolution;

// ---------------------------------------------------------------------------
// C-compatible type definitions matching h3api.h
// ---------------------------------------------------------------------------

type H3Index = u64;
type H3Error = u32;

const E_SUCCESS: H3Error = 0;
const E_FAILED: H3Error = 1;
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
// Coordinate conversion
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn degsToRads(degrees: f64) -> f64 {
    degrees.to_radians()
}

#[no_mangle]
pub extern "C" fn radsToDegs(radians: f64) -> f64 {
    radians.to_degrees()
}

// ---------------------------------------------------------------------------
// Index conversion: latLngToCell, cellToLatLng, cellToBoundary
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
// ---------------------------------------------------------------------------

/// H3Error maxGridDiskSize(int k, int64_t *out)
#[no_mangle]
pub unsafe extern "C" fn maxGridDiskSize(k: i32, out: *mut i64) -> H3Error {
    if k < 0 {
        return E_FAILED;
    }
    let k = k as i64;
    *out = 3 * k * (k + 1) + 1;
    E_SUCCESS
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
    let mut max_size: i64 = 0;
    maxGridDiskSize(k, &mut max_size);
    let max_size = max_size as usize;
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
    let mut max_size: i64 = 0;
    maxGridDiskSize(k, &mut max_size);
    let max_size = max_size as usize;
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
    match s.grid_path_cells(e) {
        Ok(iter) => {
            for (i, result) in iter.enumerate() {
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
pub extern "C" fn getResolution(h: H3Index) -> i32 {
    // Try as cell first (most common case).
    if let Some(c) = try_cell(h) {
        return u8::from(c.resolution()) as i32;
    }
    // For other valid H3 index types (edges, etc.), extract resolution
    // directly from bits 52-55, which is the same for all index modes.
    let mode = (h >> 59) & 0xF;
    if (1..=4).contains(&mode) {
        return ((h >> 52) & 0xF) as i32;
    }
    0
}

#[no_mangle]
pub extern "C" fn getBaseCellNumber(h: H3Index) -> i32 {
    // Try as cell first (most common case).
    if let Some(c) = try_cell(h) {
        return u8::from(c.base_cell()) as i32;
    }
    // For other valid H3 index types (edges, etc.), extract base cell
    // directly from bits 45-51 (7 bits).
    let mode = (h >> 59) & 0xF;
    if (1..=4).contains(&mode) {
        return ((h >> 45) & 0x7F) as i32;
    }
    0
}

#[no_mangle]
pub extern "C" fn isPentagon(h: H3Index) -> i32 {
    try_cell(h).map_or(0, |c| if c.is_pentagon() { 1 } else { 0 })
}

#[no_mangle]
pub extern "C" fn isResClassIII(h: H3Index) -> i32 {
    try_cell(h).map_or(0, |c| {
        if c.resolution().is_class3() { 1 } else { 0 }
    })
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
    for (i, child) in cell.children(res).enumerate() {
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
#[no_mangle]
pub unsafe extern "C" fn stringToH3(str_ptr: *const c_char, out: *mut H3Index) -> H3Error {
    if str_ptr.is_null() {
        return E_FAILED;
    }
    let c_str = CStr::from_ptr(str_ptr);
    let Ok(s) = c_str.to_str() else {
        return E_FAILED;
    };
    // Normalize: strip optional "0x"/"0X" prefix and trailing "l"/"L" suffix
    // to maintain backward compatibility with legacy H3 string formats.
    let s = s.trim();
    let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    let s = s.strip_suffix('l').or_else(|| s.strip_suffix('L')).unwrap_or(s);
    match u64::from_str_radix(s, 16) {
        Ok(v) => {
            *out = v;
            E_SUCCESS
        }
        Err(_) => E_FAILED,
    }
}

/// H3Error h3ToString(H3Index h, char *str, size_t sz)
#[no_mangle]
pub unsafe extern "C" fn h3ToString(h: H3Index, str_ptr: *mut c_char, sz: usize) -> H3Error {
    if str_ptr.is_null() || sz == 0 {
        return E_FAILED;
    }
    let hex = format!("{:x}", h);
    let bytes = hex.as_bytes();
    let copy_len = bytes.len().min(sz - 1);
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), str_ptr as *mut u8, copy_len);
    *str_ptr.add(copy_len) = 0; // null terminator
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
    for (i, e) in cell.edges().enumerate() {
        if i >= 6 {
            break;
        }
        *edges.add(i) = u64::from(e);
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
// ---------------------------------------------------------------------------

/// double greatCircleDistanceRads(const LatLng *a, const LatLng *b)
#[no_mangle]
pub unsafe extern "C" fn greatCircleDistanceRads(a: *const LatLng, b: *const LatLng) -> f64 {
    let (Some(la), Some(lb)) = (h3o_latlng(&*a), h3o_latlng(&*b)) else {
        return 0.0;
    };
    la.distance_rads(lb)
}

/// double greatCircleDistanceKm(const LatLng *a, const LatLng *b)
#[no_mangle]
pub unsafe extern "C" fn greatCircleDistanceKm(a: *const LatLng, b: *const LatLng) -> f64 {
    let (Some(la), Some(lb)) = (h3o_latlng(&*a), h3o_latlng(&*b)) else {
        return 0.0;
    };
    la.distance_km(lb)
}

/// double greatCircleDistanceM(const LatLng *a, const LatLng *b)
#[no_mangle]
pub unsafe extern "C" fn greatCircleDistanceM(a: *const LatLng, b: *const LatLng) -> f64 {
    let (Some(la), Some(lb)) = (h3o_latlng(&*a), h3o_latlng(&*b)) else {
        return 0.0;
    };
    la.distance_m(lb)
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

#[no_mangle]
pub extern "C" fn res0CellCount() -> i32 {
    122
}

/// H3Error getRes0Cells(H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn getRes0Cells(out: *mut H3Index) -> H3Error {
    for (i, cell) in CellIndex::base_cells().enumerate() {
        *out.add(i) = u64::from(cell);
    }
    E_SUCCESS
}

#[no_mangle]
pub extern "C" fn pentagonCount() -> i32 {
    12
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

use h3o::geom::ToCells;

/// H3Error maxPolygonToCellsSize(const GeoPolygon *geoPolygon, int res,
///                               uint32_t flags, int64_t *out)
#[no_mangle]
pub unsafe extern "C" fn maxPolygonToCellsSize(
    geo_polygon: *const GeoPolygon,
    res: i32,
    _flags: u32,
    out: *mut i64,
) -> H3Error {
    let Some(resolution) = try_resolution(res) else {
        return E_FAILED;
    };
    let geo_poly = c_polygon_to_geo(&*geo_polygon);
    let Ok(polygon) = h3o::geom::Polygon::from_radians(geo_poly) else {
        return E_FAILED;
    };
    let config = h3o::geom::PolyfillConfig::new(resolution);
    let count = polygon.max_cells_count(config);
    *out = if count > i64::MAX as usize { i64::MAX } else { count as i64 };
    E_SUCCESS
}

/// H3Error polygonToCells(const GeoPolygon *geoPolygon, int res,
///                        uint32_t flags, H3Index *out)
#[no_mangle]
pub unsafe extern "C" fn polygonToCells(
    geo_polygon: *const GeoPolygon,
    res: i32,
    _flags: u32,
    out: *mut H3Index,
) -> H3Error {
    let Some(resolution) = try_resolution(res) else {
        return E_FAILED;
    };
    let geo_poly = c_polygon_to_geo(&*geo_polygon);
    let Ok(polygon) = h3o::geom::Polygon::from_radians(geo_poly.clone()) else {
        return E_FAILED;
    };
    // Compute the maximum buffer size to defensively bound writes.
    let max_config = h3o::geom::PolyfillConfig::new(resolution);
    let Ok(max_polygon) = h3o::geom::Polygon::from_radians(geo_poly) else {
        return E_FAILED;
    };
    let max_size = max_polygon.max_cells_count(max_config);
    let config = h3o::geom::PolyfillConfig::new(resolution);
    for (i, cell) in polygon.to_cells(config).enumerate() {
        if i >= max_size {
            break;
        }
        *out.add(i) = u64::from(cell);
    }
    E_SUCCESS
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
    fn test_degs_to_rads() {
        let r = degsToRads(180.0);
        assert!((r - std::f64::consts::PI).abs() < 1e-10);
    }

    #[test]
    fn test_rads_to_degs() {
        let d = radsToDegs(std::f64::consts::PI);
        assert!((d - 180.0).abs() < 1e-10);
    }

    #[test]
    fn test_get_resolution() {
        let h = 0x85283473fffffff_u64;
        assert_eq!(getResolution(h), 5);
    }

    #[test]
    fn test_max_grid_disk_size() {
        let mut out: i64 = 0;
        assert_eq!(unsafe { maxGridDiskSize(0, &mut out) }, E_SUCCESS);
        assert_eq!(out, 1);
        assert_eq!(unsafe { maxGridDiskSize(1, &mut out) }, E_SUCCESS);
        assert_eq!(out, 7);
        assert_eq!(unsafe { maxGridDiskSize(2, &mut out) }, E_SUCCESS);
        assert_eq!(out, 19);
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
    fn test_resolution_and_base_cell_for_edges() {
        // getResolution and getBaseCellNumber must work for edge indexes too,
        // not just cell indexes.
        let edge = 0x115283473FFFFFFF_u64;
        // Resolution 5 is encoded in bits 52-55.
        assert_eq!(getResolution(edge), 5);
        // Base cell should be non-zero for this edge.
        assert_ne!(getBaseCellNumber(edge), 0);
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
