#!/usr/bin/env bash

curl -vsS 'http://localhost:8123/?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0' -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse|^[0-9]'
curl -sS 'http://localhost:8123/?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&enable_http_compression=1' -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 10' | gzip -d

# 'send_progress_in_http_headers' is false by default
curl -vsS 'http://localhost:8123/?max_block_size=1&http_headers_progress_interval_ms=0' -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep -q 'X-ClickHouse-Progress' && echo 'Fail' || true
