#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs s3

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
INSERT INTO TABLE FUNCTION s3('http://localhost:11111/test/content-type.csv.gz', 'test', 'testtest', 'CSV', 'number UInt64') SELECT number FROM numbers(1000000) SETTINGS s3_min_upload_part_size = 10000, s3_truncate_on_insert = 1;
"

# strace -e trace=network,write -s1000 -f ~/work/ClickHouse/mc cat clickminio/test/content-type.csv.gz >/dev/null
# I've also added Connection: close.

echo -ne "HEAD /test/content-type.csv.gz HTTP/1.1\r
Host: localhost:11111\r
Connection: close\r
User-Agent: MinIO (linux; amd64) minio-go/v7.0.21 mc/RELEASE.2022-02-07T09-25-34Z\r
Authorization: AWS4-HMAC-SHA256 Credential=clickhouse/20220209/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=d879656845e6e0eafc60c292fe917a6c1b3b629a3df3c326ad6175fe2dd32c89\r
X-Amz-Content-Sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\r
X-Amz-Date: 20220209T025035Z\r
\r
" | nc localhost 11111 | grep -F Content
