Allow to build ClickHouse in Docker for different platforms with different
compilers and build settings. Correctly configured Docker daemon is single dependency.

Usage:

Build deb package with `clang-11` in `debug` mode:
```
$ mkdir deb/test_output
$ ./packager --output-dir deb/test_output/ --package-type deb --compiler=clang-11 --build-type=debug
$ ls -l deb/test_output
-rw-r--r-- 1 root root      3730 clickhouse-client_18.14.2+debug_all.deb
-rw-r--r-- 1 root root  84221888 clickhouse-common-static_18.14.2+debug_amd64.deb
-rw-r--r-- 1 root root 255967314 clickhouse-common-static-dbg_18.14.2+debug_amd64.deb
-rw-r--r-- 1 root root     14940 clickhouse-server_18.14.2+debug_all.deb
-rw-r--r-- 1 root root 340206010 clickhouse-server-base_18.14.2+debug_amd64.deb
-rw-r--r-- 1 root root      7900 clickhouse-server-common_18.14.2+debug_all.deb
-rw-r--r-- 1 root root   2880432 clickhouse-test_18.14.2+debug_all.deb

```

Build ClickHouse binary with `clang-11` and `address` sanitizer in `relwithdebuginfo`
mode:
```
$ mkdir $HOME/some_clickhouse
$ ./packager --output-dir=$HOME/some_clickhouse --package-type binary --compiler=clang-11 --sanitizer=address
$ ls -l $HOME/some_clickhouse
-rwxr-xr-x 1 root root 787061952  clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-benchmark -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-clang -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-client -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-compressor -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-copier -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-extract-from-config -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-format -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-lld -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-local -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-obfuscator -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-odbc-bridge -> clickhouse
lrwxrwxrwx 1 root root        10  clickhouse-server -> clickhouse
```
