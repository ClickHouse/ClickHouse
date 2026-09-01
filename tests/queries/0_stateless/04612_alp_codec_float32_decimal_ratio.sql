SET enable_alp_codec = 1;

-- Compression ratio before https://github.com/ClickHouse/ClickHouse/pull/111627: 1
-- After: 3

DROP TABLE IF EXISTS alp_decimals;
CREATE TABLE alp_decimals (f Float32 CODEC(ALP(STD))) Engine = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO alp_decimals SELECT toFloat32(round((number + 100000) / 100, 2)) FROM numbers(1024);

SELECT arraySort(groupArray(bin(f))) = (SELECT arraySort(groupArray(bin(toFloat32(round((number + 100000) / 100, 2))))) FROM numbers(1024)) FROM alp_decimals;

-- Compression ratio
SELECT round(data_uncompressed_bytes / data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 'alp_decimals' AND name = 'f';

DROP TABLE alp_decimals;
