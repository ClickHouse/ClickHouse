
# clickhouse-compressor 

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

Compress data with Delta of four bytes and ZSTD level 10.

```
$ ./clickhouse-compressor --codec 'Delta(4)' --codec 'ZSTD(10)' < input_file > output_file
```
