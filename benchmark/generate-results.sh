#!/bin/bash

ls -1 */results/*.txt | while read file
do
  SYSTEM=$(echo "$file" | grep -oP '^[\w-]+');
  SETUP=$(echo "$file" | sed -r -e 's/^.*\/([a-zA-Z0-9_.-]+)\.txt$/\1/');

  echo "{\"system\": \"${SYSTEM}\", \"machine\": \"${SETUP}\", \"time\": \"$(git log -1 --pretty="format:%cs" $file)\", \"result\": [
$(grep -P '^\[.+\]' $file)
]},"
done
