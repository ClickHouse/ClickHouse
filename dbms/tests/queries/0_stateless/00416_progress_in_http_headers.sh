#!/usr/bin/env bash

curl -vsS 'http://localhost:8123/?max_block_size=1&http_headers_progress_interval_ms=0' -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse|^[0-9]'
curl -sS 'http://localhost:8123/?max_block_size=1&http_headers_progress_interval_ms=0&enable_http_compression=1' -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 10' | gzip -d
