---
slug: /ja/operations/utilities/clickhouse-compressor
title: clickhouse-compressor 
---

データの圧縮および解凍のためのシンプルなプログラム。

### 例

LZ4でデータを圧縮する:
```
$ ./clickhouse-compressor < input_file > output_file
```

LZ4形式からデータを解凍する:
```
$ ./clickhouse-compressor --decompress < input_file > output_file
```

レベル5でデータをZSTDで圧縮する:

```
$ ./clickhouse-compressor --codec 'ZSTD(5)' < input_file > output_file
```

4バイトのDeltaとZSTDレベル10でデータを圧縮する:

```
$ ./clickhouse-compressor --codec 'Delta(4)' --codec 'ZSTD(10)' < input_file > output_file
```
