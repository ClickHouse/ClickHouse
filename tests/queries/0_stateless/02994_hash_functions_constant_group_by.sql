SELECT halfMD5(1, materialize(toLowCardinality(2))) GROUP BY (1, halfMD5(materialize(1))) settings allow_experimental_analyzer=1;
SELECT xxHash64(1, toLowCardinality(materialize(-1))) = 1 GROUP BY xxHash64(CAST(-154477, 'UInt64')) = toUInt64(1162348840373071858), 1 settings allow_experimental_analyzer=1;
