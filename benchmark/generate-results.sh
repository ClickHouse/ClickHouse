#!/bin/bash

ls -1 */results/*.txt | while read file; do SYSTEM=$(echo "$file" | grep -oP '^[\w-]+'); SETUP=$(echo "$file" | sed -r -e 's/^.*\/([a-zA-Z0-9_.-]+)\.txt$/\1/'); echo "[{\"system\": \"${SYSTEM} (${SETUP})\", \"version\": \"\", \"data_size\": 100000000, \"time\": \"2022-07-01 00:00:00\", \"comments\": \"\", \"result\": [
$(grep -P '^\[.+\]' $file)
]}]" > ../website/benchmark/dbms/results/${SYSTEM}.${SETUP}.json; done
