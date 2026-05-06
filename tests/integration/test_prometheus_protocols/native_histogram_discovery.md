# PromQL native histogram discovery

This note records the discovery boundary for native histogram support in the
`TimeSeries` PromQL path. It is a design input, not an implementation plan that
changes scalar/vector compliance behavior.

## Current ClickHouse TimeSeries shape

The current `TimeSeries` engine stores float samples as a separate data table
plus tag metadata:

- `src/Storages/TimeSeries/TimeSeriesColumnNames.h` names the data columns
  `id`, `timestamp`, and `value`.
- `TimeSeriesInnerTablesCreator.cpp` creates the data target with exactly those
  three columns.
- `TimeSeriesColumnsValidator::validateColumnForValue` accepts only `Float32` or
  `Float64` value columns.
- `PrometheusRemoteWriteProtocol.cpp` ingests only `TimeSeries.samples()` by
  writing `sample.timestamp()` and `sample.value()`; it does not consume
  `TimeSeries.histograms()`.
- `PrometheusHTTPProtocolAPI.cpp` serializes query and range results by reading
  `Float64` values and writing Prometheus `value` / `values` arrays.
- The generated protobuf already contains Prometheus native histogram fields:
  `contrib/prometheus-protobufs/prompb/types.proto` has
  `TimeSeries.histograms` and `Histogram` with count, sum, schema, zero bucket,
  positive/negative spans, bucket counts, reset hint, and timestamp.

Implication: native histograms do not fit the current `value Float64` sample
path. Treating them as special float rows would lose schema, bucket layout,
reset hints, and mixed float/histogram sample ordering.

## Prometheus semantic surfaces to preserve

Prometheus models series as separate float and histogram point streams at the
same label set. A sample may be a float or a `FloatHistogram`; range vectors can
contain both sample kinds. HTTP responses expose histogram points separately from
float points through `histograms` fields.

Native histogram support needs decisions for these surfaces:

- **Ingestion representation.** Remote-write histograms carry count, sum,
  schema, zero bucket, positive and negative bucket spans/counts, reset hint,
  and timestamp. Both integer-count and float-count histograms can appear.
- **Instant/range selectors.** Selectors must be able to return histogram
  samples without converting them to floats. Mixed float/histogram series must
  preserve timestamp ordering and annotations or warnings for invalid mixes.
- **Arithmetic and comparison.** Prometheus restricts operations involving
  histograms. Invalid scalar/vector combinations are dropped or annotated rather
  than blindly applying float operations.
- **Aggregation.** Histogram aggregation requires compatible schemas/bucket
  layouts or conversion logic, and reset hints matter for counter-like
  operations.
- **Histogram functions.** `histogram_sum`, `histogram_count`, and
  `histogram_avg` project histograms to floats. `histogram_fraction` and
  `histogram_quantile` operate on native histograms directly, while also having
  separate classic-bucket handling for `_bucket` series.

## Representation options

### Option A: first-class histogram sample table

Add a separate TimeSeries data target for histogram samples keyed by the same
series id and timestamp, storing the protobuf-shaped payload as typed columns.
For example: count, sum, schema, zero threshold/count, positive/negative spans,
positive/negative counts, reset hint, and timestamp.

Pros:

- Preserves remote-write native histogram semantics without encoding them into
  label-derived classic bucket rows.
- Keeps float sample scans unchanged for scalar/vector PromQL.
- Lets histogram-specific functions operate on typed arrays/columns.

Cons:

- Requires storage, ingestion, query planning, and HTTP serialization changes.
- Requires clear merge/ordering rules for mixed float and histogram samples.

### Option B: generic typed payload column

Store histograms in a separate table with a compact serialized or object-like
payload plus id and timestamp.

Pros:

- Smaller first storage surface and less schema churn.
- Easier to preserve unknown future protobuf fields.

Cons:

- Harder to query efficiently from SQL helpers.
- Pushes correctness into payload codecs and custom functions.

### Option C: derive classic bucket rows

Expand a native histogram into synthetic `_bucket`, `_sum`, and `_count` series.

Pros:

- Reuses existing classic histogram PromQL patterns.

Cons:

- Loses native histogram schema and reset-hint semantics.
- Bloats the series/tag space.
- Confuses classic bucket compatibility with native histogram support.
- Should not be the first upstream strategy unless maintainers explicitly choose
  classic compatibility over native storage.

## Classic bucket relationship

Classic bucket metrics and native histograms should be tracked as separate work:

- Classic `_bucket` support is a PromQL compatibility feature over ordinary
  float series and can be implemented independently.
- Native histogram support should ingest and preserve `TimeSeries.histograms`
  as histogram samples, not as pre-expanded `_bucket` float samples.
- Compliance reporting may keep histogram categories deferred until a native
  representation and first prototype are agreed.

## HTTP API compatibility

ClickHouse's Prometheus HTTP API currently writes only float scalar, vector, and
matrix values. Native histogram support needs response serialization compatible
with Prometheus HTTP JSON, including histogram points in instant and range
vectors. A design should specify:

- whether query results can contain both `values` and `histograms` for one
  series;
- how timestamps are ordered when both float and histogram samples exist;
- how annotations or warnings are surfaced for incompatible histogram mixes;
- whether the SQL table function exposes histogram result columns or only the
  HTTP API serializes them.

## Recommended first prototype boundary

Use Option A unless maintainers prefer a payload-first storage boundary. The
first prototype should be intentionally narrow:

1. Ingest one remote-write native histogram sample into a separate histogram data
   target sharing the existing tags/id path.
2. Expose instant and range selectors for histogram samples through the
   Prometheus HTTP API without supporting arithmetic or aggregation.
3. Add one reference-backed integration test that writes a simple native
   histogram and compares selector output with Prometheus.
4. Add `histogram_count(v)` and `histogram_sum(v)` as the first projection
   functions only after selector serialization is correct.

Do not include `histogram_quantile` in the first prototype. It depends on bucket
iteration, interpolation, NaN handling, and classic-vs-native conflict rules;
that belongs after representation and selector semantics are stable.
