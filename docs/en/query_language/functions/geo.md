# Functions for working with geographical coordinates

## greatCircleDistance

Calculate the distance between two points on the Earth's surface using [the great-circle formula](https://en.wikipedia.org/wiki/Great-circle_distance).

```
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Input parameters**

- `lon1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]`.
- `lat1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]`.
- `lon2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]`.
- `lat2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]`.

Positive values correspond to North latitude and East longitude, and negative values correspond to South latitude and West longitude.

**Returned value**

The distance between two points on the Earth's surface, in meters.

Generates an exception when the input parameter values fall outside of the range.

**Example**

```sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

```text
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## pointInEllipses

Checks whether the point belongs to at least one of the ellipses.

```
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Input parameters**

- `x` — Latitude of the point.
- `y` — Longitude of the point.
- `xᵢ, yᵢ` — Coordinates of the center of the `i`-th ellipsis.
- `aᵢ, bᵢ` — Axes of the `i`-th ellipsis in meters.

The input parameters must be `2+4⋅n`, where `n` is the number of ellipses.

**Returned values**

`1` if the point is inside at least one of the ellipses; `0`if it is not.

**Examples:**

```sql
SELECT pointInEllipses(55.755831, 37.617673, 55.755831, 37.617673, 1.0, 2.0)
```

```text
┌─pointInEllipses(55.755831, 37.617673, 55.755831, 37.617673, 1., 2.)─┐
│                                                                   1 │
└─────────────────────────────────────────────────────────────────────┘
```

