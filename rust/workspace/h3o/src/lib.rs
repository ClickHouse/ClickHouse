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

fn try_cell(h: H3Index) -> Option<CellIndex> {
    CellIndex::try_from(h).ok()
}

fn try_resolution(res: i32) -> Option<Resolution> {
    if !(0..=15).contains(&res) {
        return None;
    }
    Resolution::try_from(res as u8).ok()
}

fn try_edge(h: H3Index) -> Option<DirectedEdgeIndex> {
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
    for (i, c) in cell.grid_disk_safe(k as u32).enumerate() {
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
    let mut had_none = false;
    for (i, maybe_c) in cell.grid_disk_fast(k as u32).enumerate() {
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
    let mut had_none = false;
    for (i, maybe_c) in cell.grid_ring_fast(k as u32).enumerate() {
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
    if try_cell(h).is_some() { 1 } else { 0 }
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
    u64::from_str_radix(s, 16)
        .ok()
        .and_then(|v| CellIndex::try_from(v).ok())
        .map_or(0, |c| u64::from(c))
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
    if try_edge(edge).is_some() { 1 } else { 0 }
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
