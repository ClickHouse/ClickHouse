#!/usr/bin/env bash

echo -ne '\x50\x74\x32\xf2\x59\xe9\x8a\xdb\x37\xc6\x4a\xa7\xfb\x22\xc4\x39''\x82\x13\x00\x00\x00\x09\x00\x00\x00''\x90SELECT 1\n' | curl -sS 'http://localhost:8123/?decompress=1' --data-binary @-
echo -ne 'xxxxxxxxxxxxxxxx''\x82\x13\x00\x00\x00\x09\x00\x00\x00''\x90SELECT 1\n' | curl -sS 'http://localhost:8123/?decompress=1&http_native_compression_disable_checksumming_on_decompress=1' --data-binary @-
