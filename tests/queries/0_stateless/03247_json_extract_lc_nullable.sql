select JSONExtract('{"a" : 128}', 'a', 'LowCardinality(Nullable(Int128))');

