/*
 * h3api.h - C API header for the Rust h3o-backed H3 implementation.
 * This header is API-compatible with the original H3 C library header,
 * so ClickHouse C++ code can link against either backend without changes.
 */

#ifndef H3API_H
#define H3API_H

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t H3Index;
typedef uint32_t H3Error;

typedef enum {
    E_SUCCESS = 0,
    E_FAILED = 1,
    E_DOMAIN = 2,
    E_LATLNG_DOMAIN = 3,
    E_RES_DOMAIN = 4,
    E_CELL_INVALID = 5,
    E_DIR_EDGE_INVALID = 6,
    E_UNDIR_EDGE_INVALID = 7,
    E_VERTEX_INVALID = 8,
    E_PENTAGON = 9,
    E_DUPLICATE_INPUT = 10,
    E_NOT_NEIGHBORS = 11,
    E_RES_MISMATCH = 12,
    E_MEMORY = 13,
    E_MEMORY_BOUNDS = 14
} H3ErrorCodes;

#define H3_VERSION_MAJOR 3
#define H3_VERSION_MINOR 7
#define H3_VERSION_PATCH 1

#define H3_NULL 0
#define MAX_CELL_BNDRY_VERTS 10

typedef struct {
    double lat;
    double lng;
} LatLng;

typedef struct {
    int numVerts;
    LatLng verts[MAX_CELL_BNDRY_VERTS];
} CellBoundary;

typedef struct {
    int numVerts;
    LatLng *verts;
} GeoLoop;

typedef struct {
    GeoLoop geoloop;
    int numHoles;
    GeoLoop *holes;
} GeoPolygon;

/* Coordinate conversion */
double degsToRads(double degrees);
double radsToDegs(double radians);

/* Index conversion */
H3Error latLngToCell(const LatLng *g, int res, H3Index *out);
H3Error cellToLatLng(H3Index h3, LatLng *g);
H3Error cellToBoundary(H3Index h3, CellBoundary *gp);

/* Grid disk / ring */
H3Error maxGridDiskSize(int k, int64_t *out);
H3Error gridDisk(H3Index origin, int k, H3Index *out);
H3Error gridDiskUnsafe(H3Index origin, int k, H3Index *out);
H3Error gridRingUnsafe(H3Index origin, int k, H3Index *out);

/* Grid path / distance */
H3Error gridPathCellsSize(H3Index start, H3Index end, int64_t *size);
H3Error gridPathCells(H3Index start, H3Index end, H3Index *out);
H3Error gridDistance(H3Index origin, H3Index h3, int64_t *distance);

/* Cell inspection */
int isValidCell(H3Index h);
int getResolution(H3Index h);
int getBaseCellNumber(H3Index h);
int isPentagon(H3Index h);
int isResClassIII(H3Index h);

/* Cell hierarchy */
H3Error cellToParent(H3Index h, int parentRes, H3Index *parent);
H3Error cellToChildrenSize(H3Index h, int childRes, int64_t *out);
H3Error cellToChildren(H3Index h, int childRes, H3Index *children);
H3Error cellToCenterChild(H3Index h, int childRes, H3Index *child);

/* String conversion */
H3Error stringToH3(const char *str, H3Index *out);
H3Error h3ToString(H3Index h, char *str, size_t sz);

/* Neighbors */
H3Error areNeighborCells(H3Index origin, H3Index destination, int *out);

/* Directed edges */
H3Error cellsToDirectedEdge(H3Index origin, H3Index destination, H3Index *out);
int isValidDirectedEdge(H3Index edge);
H3Error getDirectedEdgeOrigin(H3Index edge, H3Index *out);
H3Error getDirectedEdgeDestination(H3Index edge, H3Index *out);
H3Error directedEdgeToCells(H3Index edge, H3Index *originDestination);
H3Error originToDirectedEdges(H3Index origin, H3Index *edges);
H3Error directedEdgeToBoundary(H3Index edge, CellBoundary *gb);

/* Area */
H3Error cellAreaRads2(H3Index h, double *out);
H3Error cellAreaKm2(H3Index h, double *out);
H3Error cellAreaM2(H3Index h, double *out);
H3Error getHexagonAreaAvgKm2(int res, double *out);
H3Error getHexagonAreaAvgM2(int res, double *out);

/* Edge length */
H3Error getHexagonEdgeLengthAvgKm(int res, double *out);
H3Error getHexagonEdgeLengthAvgM(int res, double *out);
H3Error edgeLengthRads(H3Index edge, double *length);
H3Error edgeLengthKm(H3Index edge, double *length);
H3Error edgeLengthM(H3Index edge, double *length);

/* Distance */
double greatCircleDistanceRads(const LatLng *a, const LatLng *b);
double greatCircleDistanceKm(const LatLng *a, const LatLng *b);
double greatCircleDistanceM(const LatLng *a, const LatLng *b);

/* Cell count */
H3Error getNumCells(int res, int64_t *out);
int res0CellCount(void);
H3Error getRes0Cells(H3Index *out);
int pentagonCount(void);
H3Error getPentagons(int res, H3Index *out);

/* Icosahedron faces */
H3Error maxFaceCount(H3Index h3, int *out);
H3Error getIcosahedronFaces(H3Index h3, int *out);

/* Polygon */
H3Error maxPolygonToCellsSize(const GeoPolygon *geoPolygon, int res,
                              uint32_t flags, int64_t *out);
H3Error polygonToCells(const GeoPolygon *geoPolygon, int res,
                       uint32_t flags, H3Index *out);

#ifdef __cplusplus
}
#endif

#endif /* H3API_H */
