/*
 * h3api.h - C API header for the Rust h3o-backed H3 implementation.
 * This header is API-compatible with the original H3 C library header.
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
int maxGridDiskSize(int k);
void gridDisk(H3Index origin, int k, H3Index *out);
int gridDiskUnsafe(H3Index origin, int k, H3Index *out);
int gridRingUnsafe(H3Index origin, int k, H3Index *out);

/* Grid path / distance */
int gridPathCellsSize(H3Index start, H3Index end);
int gridPathCells(H3Index start, H3Index end, H3Index *out);
int gridDistance(H3Index origin, H3Index h3);

/* Cell inspection */
int isValidCell(H3Index h);
int getResolution(H3Index h);
int getBaseCellNumber(H3Index h);
int isPentagon(H3Index h);
int isResClassIII(H3Index h);

/* Cell hierarchy */
H3Index cellToParent(H3Index h, int parentRes);
int64_t cellToChildrenSize(H3Index h, int childRes);
void cellToChildren(H3Index h, int childRes, H3Index *children);
H3Index cellToCenterChild(H3Index h, int childRes);

/* String conversion */
H3Index stringToH3(const char *str);
void h3ToString(H3Index h, char *str, size_t sz);

/* Neighbors */
int areNeighborCells(H3Index origin, H3Index destination);

/* Directed edges */
H3Index cellsToDirectedEdge(H3Index origin, H3Index destination);
int isValidDirectedEdge(H3Index edge);
H3Index getDirectedEdgeOrigin(H3Index edge);
H3Index getDirectedEdgeDestination(H3Index edge);
void directedEdgeToCells(H3Index edge, H3Index *originDestination);
void originToDirectedEdges(H3Index origin, H3Index *edges);
void directedEdgeToBoundary(H3Index edge, CellBoundary *gb);

/* Area */
double cellAreaRads2(H3Index h);
double cellAreaKm2(H3Index h);
double cellAreaM2(H3Index h);
double getHexagonAreaAvgKm2(int res);
double getHexagonAreaAvgM2(int res);

/* Edge length */
double getHexagonEdgeLengthAvgKm(int res);
double getHexagonEdgeLengthAvgM(int res);
double exactEdgeLengthRads(H3Index edge);
double exactEdgeLengthKm(H3Index edge);
double exactEdgeLengthM(H3Index edge);

/* Distance */
double distanceRads(const LatLng *a, const LatLng *b);
double distanceKm(const LatLng *a, const LatLng *b);
double distanceM(const LatLng *a, const LatLng *b);

/* Cell count */
int64_t getNumCells(int res);
int res0CellCount(void);
void getRes0Cells(H3Index *out);
int pentagonCount(void);
void getPentagons(int res, H3Index *out);

/* Icosahedron faces */
int maxFaceCount(H3Index h3);
void getIcosahedronFaces(H3Index h3, int *out);

/* Polygon */
int maxPolygonToCellsSize(const GeoPolygon *geoPolygon, int res);
void polygonToCells(const GeoPolygon *geoPolygon, int res, H3Index *out);

#ifdef __cplusplus
}
#endif

#endif /* H3API_H */
