-- Regression test for the h3o backend: for a pentagon origin, the K direction
-- (slot 0) is deleted and must be reported as H3_NULL (0), with the remaining
-- five edges occupying slots 1..5, matching the H3 C API's fixed six-slot layout.

-- A res-0 pentagon (base cell 4).
WITH stringToH3('8009fffffffffff') AS pent
SELECT h3IsPentagon(pent), length(h3GetUnidirectionalEdgesFromHexagon(pent)), h3GetUnidirectionalEdgesFromHexagon(pent)[1];

-- All six slots; slot 0 is H3_NULL, slots 1..5 are non-zero edges.
WITH stringToH3('8009fffffffffff') AS pent
SELECT h3GetUnidirectionalEdgesFromHexagon(pent) AS edges, arrayCount(x -> x = 0, edges), arrayCount(x -> x != 0, edges);

-- A hexagon origin still fills all six slots in direction order (unchanged behaviour).
SELECT h3IsPentagon(599686042433355775), h3GetUnidirectionalEdgesFromHexagon(599686042433355775);
