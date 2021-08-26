---
toc_priority: 70
toc_title: Third-Party Libraries Used
---

# Third-Party Libraries Used {#third-party-libraries-used}

The list of third-party libraries can be obtained by the following query:

``` sql
SELECT library_name, license_type, license_path FROM system.licenses ORDER BY library_name COLLATE 'en'
```

[Example](https://gh-api.clickhouse.tech/play?user=play#U0VMRUNUIGxpYnJhcnlfbmFtZSwgbGljZW5zZV90eXBlLCBsaWNlbnNlX3BhdGggRlJPTSBzeXN0ZW0ubGljZW5zZXMgT1JERVIgQlkgbGlicmFyeV9uYW1lIENPTExBVEUgJ2VuJw==)

| library_name | license_type | license_path |
|:-|:-|:-|
| abseil-cpp | Apache | /contrib/abseil-cpp/LICENSE |
| AMQP-CPP | Apache | /contrib/AMQP-CPP/LICENSE |
| arrow | Apache | /contrib/arrow/LICENSE.txt |
| avro | Apache | /contrib/avro/LICENSE.txt |
| aws | Apache | /contrib/aws/LICENSE.txt |
| aws-c-common | Apache | /contrib/aws-c-common/LICENSE |
| aws-c-event-stream | Apache | /contrib/aws-c-event-stream/LICENSE |
| aws-checksums | Apache | /contrib/aws-checksums/LICENSE |
| base64 | BSD 2-clause | /contrib/base64/LICENSE |
| boost | Boost | /contrib/boost/LICENSE_1_0.txt |
| boringssl | BSD | /contrib/boringssl/LICENSE |
| brotli | MIT | /contrib/brotli/LICENSE |
| capnproto | MIT | /contrib/capnproto/LICENSE |
| cassandra | Apache | /contrib/cassandra/LICENSE.txt |
| cctz | Apache | /contrib/cctz/LICENSE.txt |
| cityhash102 | MIT | /contrib/cityhash102/COPYING |
| cppkafka | BSD 2-clause | /contrib/cppkafka/LICENSE |
| croaring | Apache | /contrib/croaring/LICENSE |
| curl | Apache | /contrib/curl/docs/LICENSE-MIXING.md |
| cyrus-sasl | BSD 2-clause | /contrib/cyrus-sasl/COPYING |
| double-conversion | BSD 3-clause | /contrib/double-conversion/LICENSE |
| dragonbox | Apache | /contrib/dragonbox/LICENSE-Apache2-LLVM |
| fast_float | Apache | /contrib/fast_float/LICENSE |
| fastops | MIT | /contrib/fastops/LICENSE |
| flatbuffers | Apache | /contrib/flatbuffers/LICENSE.txt |
| fmtlib | Unknown | /contrib/fmtlib/LICENSE.rst |
| gcem | Apache | /contrib/gcem/LICENSE |
| googletest | BSD 3-clause | /contrib/googletest/LICENSE |
| grpc | Apache | /contrib/grpc/LICENSE |
| h3 | Apache | /contrib/h3/LICENSE |
| hyperscan | Boost | /contrib/hyperscan/LICENSE |
| icu | Public Domain | /contrib/icu/icu4c/LICENSE |
| icudata | Public Domain | /contrib/icudata/LICENSE |
| jemalloc | BSD 2-clause | /contrib/jemalloc/COPYING |
| krb5 | MIT | /contrib/krb5/src/lib/gssapi/LICENSE |
| libc-headers | LGPL | /contrib/libc-headers/LICENSE |
| libcpuid | BSD 2-clause | /contrib/libcpuid/COPYING |
| libcxx | Apache | /contrib/libcxx/LICENSE.TXT |
| libcxxabi | Apache | /contrib/libcxxabi/LICENSE.TXT |
| libdivide | zLib | /contrib/libdivide/LICENSE.txt |
| libfarmhash | MIT | /contrib/libfarmhash/COPYING |
| libgsasl | LGPL | /contrib/libgsasl/LICENSE |
| libhdfs3 | Apache | /contrib/libhdfs3/LICENSE.txt |
| libmetrohash | Apache | /contrib/libmetrohash/LICENSE |
| libpq | Unknown | /contrib/libpq/COPYRIGHT |
| libpqxx | BSD 3-clause | /contrib/libpqxx/COPYING |
| librdkafka | MIT | /contrib/librdkafka/LICENSE.murmur2 |
| libunwind | Apache | /contrib/libunwind/LICENSE.TXT |
| libuv | BSD | /contrib/libuv/LICENSE |
| llvm | Apache | /contrib/llvm/llvm/LICENSE.TXT |
| lz4 | BSD | /contrib/lz4/LICENSE |
| mariadb-connector-c | LGPL | /contrib/mariadb-connector-c/COPYING.LIB |
| miniselect | Boost | /contrib/miniselect/LICENSE_1_0.txt |
| msgpack-c | Boost | /contrib/msgpack-c/LICENSE_1_0.txt |
| murmurhash | Public Domain | /contrib/murmurhash/LICENSE |
| NuRaft | Apache | /contrib/NuRaft/LICENSE |
| openldap | Unknown | /contrib/openldap/LICENSE |
| orc | Apache | /contrib/orc/LICENSE |
| poco | Boost | /contrib/poco/LICENSE |
| protobuf | BSD 3-clause | /contrib/protobuf/LICENSE |
| rapidjson | MIT | /contrib/rapidjson/bin/jsonschema/LICENSE |
| re2 | BSD 3-clause | /contrib/re2/LICENSE |
| replxx | BSD 3-clause | /contrib/replxx/LICENSE.md |
| rocksdb | BSD 3-clause | /contrib/rocksdb/LICENSE.leveldb |
| sentry-native | MIT | /contrib/sentry-native/LICENSE |
| simdjson | Apache | /contrib/simdjson/LICENSE |
| snappy | Public Domain | /contrib/snappy/COPYING |
| sparsehash-c11 | BSD 3-clause | /contrib/sparsehash-c11/LICENSE |
| stats | Apache | /contrib/stats/LICENSE |
| thrift | Apache | /contrib/thrift/LICENSE |
| unixodbc | LGPL | /contrib/unixodbc/COPYING |
| xz | Public Domain | /contrib/xz/COPYING |
| zlib-ng | zLib | /contrib/zlib-ng/LICENSE.md |
| zstd | BSD | /contrib/zstd/LICENSE |

## Guidelines for adding new third-party libraries and maintaining custom changes in them {#adding-third-party-libraries}

1. All external third-party code should reside in the dedicated directories under `contrib` directory of ClickHouse repo. Prefer Git submodules, when available.
2. Fork/mirror the official repo in [Clickhouse-extras](https://github.com/ClickHouse-Extras). Prefer official GitHub repos, when available.
3. Branch from the branch you want to integrate, e.g., `master` -> `clickhouse/master`, or `release/vX.Y.Z` -> `clickhouse/release/vX.Y.Z`.
4. All forks in [Clickhouse-extras](https://github.com/ClickHouse-Extras) can be automatically synchronized with upstreams. `clickhouse/...` branches will remain unaffected, since virtually nobody is going to use that naming pattern in their upstream repos.
5. Add submodules under `contrib` of ClickHouse repo that refer the above forks/mirrors. Set the submodules to track the corresponding `clickhouse/...` branches.
6. Every time the custom changes have to be made in the library code, a dedicated branch should be created, like `clickhouse/my-fix`. Then this branch should be merged into the branch, that is tracked by the submodule, e.g., `clickhouse/master` or `clickhouse/release/vX.Y.Z`.
7. No code should be pushed in any branch of the forks in [Clickhouse-extras](https://github.com/ClickHouse-Extras), whose names do not follow `clickhouse/...` pattern.
8. Always write the custom changes with the official repo in mind. Once the PR is merged from (a feature/fix branch in) your personal fork into the fork in [Clickhouse-extras](https://github.com/ClickHouse-Extras), and the submodule is bumped in ClickHouse repo, consider opening another PR from (a feature/fix branch in) the fork in [Clickhouse-extras](https://github.com/ClickHouse-Extras) to the official repo of the library. This will make sure, that 1) the contribution has more than a single use case and importance, 2) others will also benefit from it, 3) the change will not remain a maintenance burden solely on ClickHouse developers.
9. When a submodule needs to start using a newer code from the original branch (e.g., `master`), and since the custom changes might be merged in the branch it is tracking (e.g., `clickhouse/master`) and so it may diverge from its original counterpart (i.e., `master`), a careful merge should be carried out first, i.e., `master` -> `clickhouse/master`, and only then the submodule can be bumped in ClickHouse.
