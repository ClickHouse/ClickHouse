#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# gRPC queries with gzip and deflate compression of both the request and the result.
# gRPC sends a message uncompressed if compression does not shrink it, so the query carries a long comment.
python3 - "$CURDIR/../../../utils/grpc-client/pb2" <<'EOF'
import sys
sys.path.insert(0, sys.argv[1])
import grpc
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

padding = " -- " + "x" * 1000
ok = 0
for algorithm, compression in (("gzip", grpc.Compression.Gzip), ("deflate", grpc.Compression.Deflate)):
    with grpc.insecure_channel("localhost:9100", compression=compression) as channel:
        stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
        for i in range(100):
            result = stub.ExecuteQuery(clickhouse_grpc_pb2.QueryInfo(
                query=f"SELECT {i}{padding}", transport_compression_type=algorithm, transport_compression_level=3))
            ok += result.output == f"{i}\n".encode() and not result.HasField("exception")
print(ok)
EOF
