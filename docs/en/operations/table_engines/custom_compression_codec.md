
# Column Compression Codecs

Besides default data compression, defined in [server settings](../server_settings/settings.md#compression), per-column specification is also available.

Supported compression algorithms:

- `NONE` - no compression for data applied
- `LZ4`
- `LZ4HC(level)` - (level) - LZ4_HC compression algorithm with defined level.
Possible `level` range: \[3, 12\]. Default value: 9. Greater values stands for better compression and higher CPU usage. Recommended value range: [4,9].
- `ZSTD(level)` - ZSTD compression algorithm with defined `level`. Possible `level` value range: \[1, 22\]. Default value: 1.
Greater values stands for better compression and higher CPU usage.
- `Delta(delta_bytes)` - compression approach when raw values are replace with difference of two neighbour values. Up to `delta_bytes` are used for storing delta value.
Possible `delta_bytes` values: 1, 2, 4, 8. Default value for delta bytes is `sizeof(type)`, if it is equals to 1, 2, 4, 8 and equals to 1 otherwise.

Syntax example:
```
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD), /* используется уровень сжатия по-умолчанию */
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY dt
```

Codecs can be combined in a pipeline. Default table codec is not included into pipeline (if it should be applied to a column, you have to specify it explicitly in pipeline). Example below shows an optimization approach for storing timeseries metrics.
Usually, values for particular metric, stored in `path` does not differ significantly from point to point. Using delta-encoding allows to reduce disk space usage significantly.
```
CREATE TABLE timeseries_example
(
    dt Date,
    ts DateTime,
    path String,
    value Float32 CODEC(Delta(2), ZSTD)
)
ENGINE = MergeTree
PARTITION BY dt
ORDER BY (path, ts)
```
