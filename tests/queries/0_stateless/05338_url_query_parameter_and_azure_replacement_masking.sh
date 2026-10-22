#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Azure, whose arguments it then hides whole

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `url` keeps its host and path visible but hides its password and the credentials in its query parameters.
# A partially masked Azure connection string stays a valid string literal: the output parses again.
while read -r query; do
    formatted=$($CLICKHOUSE_FORMAT --oneline --query "$query")
    echo "$formatted"
    $CLICKHOUSE_FORMAT --oneline --query "$formatted" > /dev/null 2>&1 || echo "Does not parse: $formatted"
done <<'QUERIES'
SELECT * FROM url('https://example.com/data.csv?access_token=plain_token&format=csv', 'CSV')
SELECT * FROM url('https://user:plain_password@example.com/data.csv?sig=plain_sas_signature&sv=2022-11-02', 'CSV')
SELECT * FROM url('https://bucket.s3.amazonaws.com/file.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=plain_credential&X-Amz-Signature=plain_signature', 'CSV')
SELECT * FROM urlCluster('test_cluster', 'https://example.com/data.csv?api_key=plain_key', 'CSV')
SELECT * FROM url(url_creds, url = 'https://example.com/data.csv?token=plain_token')
CREATE TABLE test_url (key UInt64) ENGINE = URL('https://example.com/data.csv?password=plain_password', 'CSV')
SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=https;AccountName=it''s\\name;AccountKey=plain_account_key;EndpointSuffix=core.windows.net', 'container', 'blob.csv')
QUERIES
