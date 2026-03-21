//! C FFI wrapper around the h3o Rust library, providing API-compatible
//! replacements for the H3 C library functions used by ClickHouse.

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
/// Use this for operational functions that should gracefully handle
/// malformed indexes.
fn try_cell(h: H3Index) -> Option<CellIndex> {
    CellIndex::try_from(normalize_h3_index(h)).ok()
}

/// Try to parse as CellIndex with strict validation (no normalization).
/// Use this for `isValidCell` to match the C H3 library's strict behavior.
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

#[no_mangle]
pub extern "C" fn maxGridDiskSize(k: i32) -> i32 {
    if k < 0 {
        return 0;
    }
    // Formula: 3*k*(k+1) + 1
    let k = k as i64;
    let result = 3 * k * (k + 1) + 1;
    result as i32
}

#[no_mangle]
pub unsafe extern "C" fn gridDisk(origin: H3Index, k: i32, out: *mut H3Index) {
    let Some(cell) = try_cell(origin) else {
        return;
    };
    if k < 0 {
        return;
    }
    let max_size = maxGridDiskSize(k) as usize;
    for (i, c) in cell.grid_disk_safe(k as u32).enumerate() {
        if i >= max_size {
            break;
        }
        *out.add(i) = u64::from(c);
    }
}

#[no_mangle]
pub unsafe extern "C" fn gridDiskUnsafe(origin: H3Index, k: i32, out: *mut H3Index) -> i32 {
    let Some(cell) = try_cell(origin) else {
        return -1;
    };
    if k < 0 {
        return -1;
    }
    let max_size = maxGridDiskSize(k) as usize;
    let mut had_none = false;
    for (i, maybe_c) in cell.grid_disk_fast(k as u32).enumerate() {
        if i >= max_size {
            break;
        }
        match maybe_c {
            Some(c) => *out.add(i) = u64::from(c),
            None => {
                *out.add(i) = 0;
                had_none = true;
            }
        }
    }
    if had_none { -1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn gridRingUnsafe(origin: H3Index, k: i32, out: *mut H3Index) -> i32 {
    let Some(cell) = try_cell(origin) else {
        return -1;
    };
    if k < 0 {
        return -1;
    }
    // The ring at distance k has exactly 6*k elements (or 1 for k=0).
    // Bound the writes to prevent buffer overflow if h3o's iterator
    // produces more elements than expected.
    let max_size = if k == 0 { 1 } else { 6 * k as usize };
    let mut had_none = false;
    for (i, maybe_c) in cell.grid_ring_fast(k as u32).enumerate() {
        if i >= max_size {
            break;
        }
        match maybe_c {
            Some(c) => *out.add(i) = u64::from(c),
            None => {
                *out.add(i) = 0;
                had_none = true;
            }
        }
    }
    if had_none { -1 } else { 0 }
}

// ---------------------------------------------------------------------------
// Grid path / distance
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gridPathCellsSize(start: H3Index, end: H3Index) -> i32 {
    let (Some(s), Some(e)) = (try_cell(start), try_cell(end)) else {
        return -1;
    };
    match s.grid_path_cells_size(e) {
        Ok(size) => size,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn gridPathCells(
    start: H3Index,
    end: H3Index,
    out: *mut H3Index,
) -> i32 {
    let (Some(s), Some(e)) = (try_cell(start), try_cell(end)) else {
        return -1;
    };
    match s.grid_path_cells(e) {
        Ok(iter) => {
            for (i, result) in iter.enumerate() {
                match result {
                    Ok(c) => *out.add(i) = u64::from(c),
                    Err(_) => return -1,
                }
            }
            0
        }
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn gridDistance(origin: H3Index, h3: H3Index) -> i32 {
    let (Some(o), Some(d)) = (try_cell(origin), try_cell(h3)) else {
        return -1;
    };
    match o.grid_distance(d) {
        Ok(dist) => dist,
        Err(_) => -1,
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
    try_cell(h).map_or(0, |c| u8::from(c.resolution()) as i32)
}

#[no_mangle]
pub extern "C" fn getBaseCellNumber(h: H3Index) -> i32 {
    try_cell(h).map_or(0, |c| u8::from(c.base_cell()) as i32)
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

#[no_mangle]
pub extern "C" fn cellToParent(h: H3Index, parent_res: i32) -> H3Index {
    let (Some(cell), Some(res)) = (try_cell(h), try_resolution(parent_res)) else {
        return 0;
    };
    cell.parent(res).map_or(0, |p| u64::from(p))
}

#[no_mangle]
pub extern "C" fn cellToChildrenSize(h: H3Index, child_res: i32) -> i64 {
    let (Some(cell), Some(res)) = (try_cell(h), try_resolution(child_res)) else {
        return 0;
    };
    cell.children_count(res) as i64
}

#[no_mangle]
pub unsafe extern "C" fn cellToChildren(h: H3Index, child_res: i32, children: *mut H3Index) {
    let (Some(cell), Some(res)) = (try_cell(h), try_resolution(child_res)) else {
        return;
    };
    for (i, child) in cell.children(res).enumerate() {
        *children.add(i) = u64::from(child);
    }
}

#[no_mangle]
pub extern "C" fn cellToCenterChild(h: H3Index, child_res: i32) -> H3Index {
    let (Some(cell), Some(res)) = (try_cell(h), try_resolution(child_res)) else {
        return 0;
    };
    cell.center_child(res).map_or(0, |c| u64::from(c))
}

// ---------------------------------------------------------------------------
// String conversion
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn stringToH3(str_ptr: *const c_char) -> H3Index {
    if str_ptr.is_null() {
        return 0;
    }
    let c_str = CStr::from_ptr(str_ptr);
    let Ok(s) = c_str.to_str() else {
        return 0;
    };
    // Normalize: strip optional "0x"/"0X" prefix and trailing "l"/"L" suffix
    // to maintain backward compatibility with legacy H3 string formats.
    let s = s.trim();
    let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    let s = s.strip_suffix('l').or_else(|| s.strip_suffix('L')).unwrap_or(s);
    // Parse hex to u64 without validating the index type — the original C H3
    // stringToH3 returns the raw value so callers can use it as cell, edge, etc.
    u64::from_str_radix(s, 16).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn h3ToString(h: H3Index, str_ptr: *mut c_char, sz: usize) {
    if str_ptr.is_null() || sz == 0 {
        return;
    }
    let hex = format!("{:x}", h);
    let bytes = hex.as_bytes();
    let copy_len = bytes.len().min(sz - 1);
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), str_ptr as *mut u8, copy_len);
    *str_ptr.add(copy_len) = 0; // null terminator
}

// ---------------------------------------------------------------------------
// Neighbors
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn areNeighborCells(origin: H3Index, destination: H3Index) -> i32 {
    let (Some(o), Some(d)) = (try_cell(origin), try_cell(destination)) else {
        return 0;
    };
    match o.is_neighbor_with(d) {
        Ok(true) => 1,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Directed edges
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn cellsToDirectedEdge(origin: H3Index, destination: H3Index) -> H3Index {
    let (Some(o), Some(d)) = (try_cell(origin), try_cell(destination)) else {
        return 0;
    };
    o.edge(d).map_or(0, |e| u64::from(e))
}

#[no_mangle]
pub extern "C" fn isValidDirectedEdge(edge: H3Index) -> i32 {
    if try_edge_strict(edge).is_some() { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn getDirectedEdgeOrigin(edge: H3Index) -> H3Index {
    try_edge(edge).map_or(0, |e| u64::from(e.origin()))
}

#[no_mangle]
pub extern "C" fn getDirectedEdgeDestination(edge: H3Index) -> H3Index {
    try_edge(edge).map_or(0, |e| u64::from(e.destination()))
}

#[no_mangle]
pub unsafe extern "C" fn directedEdgeToCells(edge: H3Index, origin_destination: *mut H3Index) {
    let Some(e) = try_edge(edge) else {
        *origin_destination = 0;
        *origin_destination.add(1) = 0;
        return;
    };
    let (o, d) = e.cells();
    *origin_destination = u64::from(o);
    *origin_destination.add(1) = u64::from(d);
}

#[no_mangle]
pub unsafe extern "C" fn originToDirectedEdges(origin: H3Index, edges: *mut H3Index) {
    for i in 0..6 {
        *edges.add(i) = 0;
    }
    let Some(cell) = try_cell(origin) else {
        return;
    };
    for (i, e) in cell.edges().enumerate() {
        if i >= 6 {
            break;
        }
        *edges.add(i) = u64::from(e);
    }
}

#[no_mangle]
pub unsafe extern "C" fn directedEdgeToBoundary(edge: H3Index, gb: *mut CellBoundary) {
    let Some(e) = try_edge(edge) else {
        (*gb).num_verts = 0;
        return;
    };
    let boundary = e.boundary();
    fill_boundary(&boundary, &mut *gb);
}

// ---------------------------------------------------------------------------
// Area functions
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn cellAreaRads2(h: H3Index) -> f64 {
    try_cell(h).map_or(0.0, |c| c.area_rads2())
}

#[no_mangle]
pub extern "C" fn cellAreaKm2(h: H3Index) -> f64 {
    try_cell(h).map_or(0.0, |c| c.area_km2())
}

#[no_mangle]
pub extern "C" fn cellAreaM2(h: H3Index) -> f64 {
    try_cell(h).map_or(0.0, |c| c.area_m2())
}

#[no_mangle]
pub extern "C" fn getHexagonAreaAvgKm2(res: i32) -> f64 {
    try_resolution(res).map_or(0.0, |r| r.area_km2())
}

#[no_mangle]
pub extern "C" fn getHexagonAreaAvgM2(res: i32) -> f64 {
    try_resolution(res).map_or(0.0, |r| r.area_m2())
}

// ---------------------------------------------------------------------------
// Edge length functions
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn getHexagonEdgeLengthAvgKm(res: i32) -> f64 {
    try_resolution(res).map_or(0.0, |r| r.edge_length_km())
}

#[no_mangle]
pub extern "C" fn getHexagonEdgeLengthAvgM(res: i32) -> f64 {
    try_resolution(res).map_or(0.0, |r| r.edge_length_m())
}

#[no_mangle]
pub extern "C" fn exactEdgeLengthRads(edge: H3Index) -> f64 {
    try_edge(edge).map_or(0.0, |e| e.length_rads())
}

#[no_mangle]
pub extern "C" fn exactEdgeLengthKm(edge: H3Index) -> f64 {
    try_edge(edge).map_or(0.0, |e| e.length_km())
}

#[no_mangle]
pub extern "C" fn exactEdgeLengthM(edge: H3Index) -> f64 {
    try_edge(edge).map_or(0.0, |e| e.length_m())
}

// ---------------------------------------------------------------------------
// Distance functions (great circle / haversine)
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn distanceRads(a: *const LatLng, b: *const LatLng) -> f64 {
    let (Some(la), Some(lb)) = (h3o_latlng(&*a), h3o_latlng(&*b)) else {
        return 0.0;
    };
    la.distance_rads(lb)
}

#[no_mangle]
pub unsafe extern "C" fn distanceKm(a: *const LatLng, b: *const LatLng) -> f64 {
    let (Some(la), Some(lb)) = (h3o_latlng(&*a), h3o_latlng(&*b)) else {
        return 0.0;
    };
    la.distance_km(lb)
}

#[no_mangle]
pub unsafe extern "C" fn distanceM(a: *const LatLng, b: *const LatLng) -> f64 {
    let (Some(la), Some(lb)) = (h3o_latlng(&*a), h3o_latlng(&*b)) else {
        return 0.0;
    };
    la.distance_m(lb)
}

// ---------------------------------------------------------------------------
// Cell count functions
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn getNumCells(res: i32) -> i64 {
    try_resolution(res).map_or(0, |r| r.cell_count() as i64)
}

#[no_mangle]
pub extern "C" fn res0CellCount() -> i32 {
    122
}

#[no_mangle]
pub unsafe extern "C" fn getRes0Cells(out: *mut H3Index) {
    for (i, cell) in CellIndex::base_cells().enumerate() {
        *out.add(i) = u64::from(cell);
    }
}

#[no_mangle]
pub extern "C" fn pentagonCount() -> i32 {
    12
}

#[no_mangle]
pub unsafe extern "C" fn getPentagons(res: i32, out: *mut H3Index) {
    let Some(resolution) = try_resolution(res) else {
        return;
    };
    for (i, cell) in resolution.pentagons().enumerate() {
        *out.add(i) = u64::from(cell);
    }
}

// ---------------------------------------------------------------------------
// Icosahedron faces
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn maxFaceCount(h3: H3Index) -> i32 {
    let Some(cell) = try_cell(h3) else {
        return 0;
    };
    if cell.is_pentagon() { 5 } else { 2 }
}

#[no_mangle]
pub unsafe extern "C" fn getIcosahedronFaces(h3: H3Index, out: *mut i32) {
    let Some(cell) = try_cell(h3) else {
        return;
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
}

// ---------------------------------------------------------------------------
// Polygon operations
// ---------------------------------------------------------------------------

use h3o::geom::ToCells;

#[no_mangle]
pub unsafe extern "C" fn maxPolygonToCellsSize(geo_polygon: *const GeoPolygon, res: i32) -> i32 {
    let Some(resolution) = try_resolution(res) else {
        return 0;
    };
    let geo_poly = c_polygon_to_geo(&*geo_polygon);
    let Ok(polygon) = h3o::geom::Polygon::from_radians(geo_poly) else {
        return 0;
    };
    let config = h3o::geom::PolyfillConfig::new(resolution);
    polygon.max_cells_count(config) as i32
}

#[no_mangle]
pub unsafe extern "C" fn polygonToCells(
    geo_polygon: *const GeoPolygon,
    res: i32,
    out: *mut H3Index,
) {
    let Some(resolution) = try_resolution(res) else {
        return;
    };
    let geo_poly = c_polygon_to_geo(&*geo_polygon);
    let Ok(polygon) = h3o::geom::Polygon::from_radians(geo_poly) else {
        return;
    };
    let config = h3o::geom::PolyfillConfig::new(resolution);
    for (i, cell) in polygon.to_cells(config).enumerate() {
        *out.add(i) = u64::from(cell);
    }
}

unsafe fn c_polygon_to_geo(poly: &GeoPolygon) -> geo::Polygon<f64> {
    let exterior = geoloop_to_linestring(&poly.geoloop);
    let mut holes = Vec::new();
    if !poly.holes.is_null() && poly.num_holes > 0 {
        for i in 0..poly.num_holes as usize {
            let hole = &*poly.holes.add(i);
            holes.push(geoloop_to_linestring(hole));
        }
    }
    geo::Polygon::new(exterior, holes)
}

unsafe fn geoloop_to_linestring(loop_: &GeoLoop) -> geo::LineString<f64> {
    let mut coords = Vec::with_capacity(loop_.num_verts as usize);
    if !loop_.verts.is_null() {
        for i in 0..loop_.num_verts as usize {
            let v = &*loop_.verts.add(i);
            // h3o::geom::Polygon::from_radians expects radians in geo coords.
            // geo uses (x=lng, y=lat) convention.
            coords.push(geo::Coord {
                x: v.lng,
                y: v.lat,
            });
        }
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
        assert_eq!(maxGridDiskSize(0), 1);
        assert_eq!(maxGridDiskSize(1), 7);
        assert_eq!(maxGridDiskSize(2), 19);
    }

    #[test]
    fn test_grid_ring_sizes() {
        // Use a higher-resolution cell to avoid slow iteration at res 0
        let cell = CellIndex::try_from(0x85283473FFFFFFF_u64).unwrap(); // res 5

        // k=0: 1 element
        let count_0: usize = cell.grid_ring_fast(0).count();
        assert_eq!(count_0, 1, "grid_ring_fast(0) should return 1 element");

        // k=1: 6 elements
        let count_1: usize = cell.grid_ring_fast(1).count();
        assert_eq!(count_1, 6, "grid_ring_fast(1) should return 6 elements");

        // k=2: 12 elements
        let count_2: usize = cell.grid_ring_fast(2).count();
        assert_eq!(count_2, 12, "grid_ring_fast(2) should return 12 elements");
    }

    #[test]
    fn test_grid_ring_unsafe_ffi() {
        // Test the FFI function directly with the CI test values
        let cell = 579205133326352383_u64;
        for k in [1_i32, 2, 3] {
            let max_size = if k == 0 { 1 } else { 6 * k as usize };
            let mut out = vec![0_u64; max_size];
            let err = unsafe { gridRingUnsafe(cell, k, out.as_mut_ptr()) };
            // This might return -1 (failure) for pentagon-adjacent cells,
            // but it must not crash
            eprintln!("gridRingUnsafe(cell, {}) = err:{}, out={:?}", k, err, out);
        }
    }

    #[test]
    fn test_string_to_h3_legacy_formats() {
        use std::ffi::CString;
        // Standard hex
        let s = CString::new("85283473fffffff").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr()) }, 0x85283473FFFFFFF);
        // 0x prefix
        let s = CString::new("0x85283473fffffffL").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr()) }, 0x85283473FFFFFFF);
        // 0X prefix, l suffix
        let s = CString::new("0X85283473fffffffl").unwrap();
        assert_eq!(unsafe { stringToH3(s.as_ptr()) }, 0x85283473FFFFFFF);
    }

    #[test]
    fn test_normalize_h3_index() {
        // Cell with non-7 unused digits at resolution 5
        assert_eq!(normalize_h3_index(0x085283473FFFFFFD), 0x085283473FFFFFFF);
        // Already valid cell — unchanged
        assert_eq!(normalize_h3_index(0x085283473FFFFFFF), 0x085283473FFFFFFF);
        // Edge with non-7 unused digits at resolution 5
        assert_eq!(normalize_h3_index(0x115283473FFFFFFD), 0x115283473FFFFFFF);
    }

    #[test]
    fn test_malformed_cell_handling() {
        let cell_malformed = 0x085283473FFFFFFD_u64;
        // Strict validation rejects malformed cells (matches C H3 behavior)
        assert_eq!(isValidCell(cell_malformed), 0);
        // But operational functions normalize and accept them
        assert!(try_cell(cell_malformed).is_some());
    }

    #[test]
    fn test_directed_edge_values() {
        let edge_valid = 0x115283473FFFFFFF_u64; // 1248204388774707199
        let edge_malformed = 0x115283473FFFFFFD_u64; // 1248204388774707197

        // Strict validation: valid=1, malformed=0
        assert_eq!(isValidDirectedEdge(edge_valid), 1);
        assert_eq!(isValidDirectedEdge(edge_malformed), 0);

        // Operational functions normalize and accept both
        assert!(try_edge(edge_valid).is_some());
        assert!(try_edge(edge_malformed).is_some());

        // Destination of valid edge
        assert_eq!(getDirectedEdgeDestination(edge_valid), 599686043507097599);

        // Destination of malformed edge (normalizes to valid first)
        assert_eq!(getDirectedEdgeDestination(edge_malformed), 599686043507097599);

        // Origin
        assert_eq!(getDirectedEdgeOrigin(edge_valid), 599686042433355775);
        assert_eq!(getDirectedEdgeOrigin(edge_malformed), 599686042433355775);

        // Boundary
        let mut gb: CellBoundary = unsafe { std::mem::zeroed() };
        unsafe { directedEdgeToBoundary(edge_valid, &mut gb) };
        assert_eq!(gb.num_verts, 2);

        // cellsToDirectedEdge
        let cell_a = 0x85283473FFFFFFF_u64;
        let cell_b = 0x85283477FFFFFFF_u64;
        assert_eq!(cellsToDirectedEdge(cell_a, cell_b), 1248204388774707199);
    }

    #[test]
    fn test_lat_lng_to_cell() {
        let mut out: H3Index = 0;
        let ll = LatLng {
            lat: 0.9729587430904422, // ~55.75 degrees in radians
            lng: 0.6565511658498814, // ~37.62 degrees in radians
        };
        let err = unsafe { latLngToCell(&ll, 9, &mut out) };
        assert_eq!(err, E_SUCCESS);
        assert_ne!(out, 0);
        assert_eq!(isValidCell(out), 1);
        assert_eq!(getResolution(out), 9);
    }

    #[test]
    fn test_cell_to_lat_lng_roundtrip() {
        let h = 0x85283473fffffff_u64;
        let mut ll = LatLng { lat: 0.0, lng: 0.0 };
        let err = unsafe { cellToLatLng(h, &mut ll) };
        assert_eq!(err, E_SUCCESS);
        assert!(ll.lat != 0.0);
        assert!(ll.lng != 0.0);
    }
}
