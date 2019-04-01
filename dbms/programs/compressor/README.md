## ClickHouse compressor

Simple program for data compression and decompression.

### Examples

Compress data with LZ4:
```
$ ./clickhouse-compressor < input_file > output_file
```

Decompress data from LZ4 format:
```
$ ./clickhouse-compressor --decompress < input_file > output_file
```

Compress data with ZSTD at level 5:

```
$ ./clickhouse-compressor --codec 'ZSTD(5)' < input_file > output_file
```

Compress data with ZSTD level 10, LZ4HC level 7 and LZ4.

```
$ ./clickhouse-compressor --codec ZSTD --codec LZ4HC --level 7 --codec LZ4 < input_file > output_file
```
