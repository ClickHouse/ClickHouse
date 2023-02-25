#!/usr/bin/env bash

# Tags: use-vectorscan, no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p $USER_FILES_PATH/test_02504

yaml=$USER_FILES_PATH/test_02504/test.yaml

cat > "$yaml" <<EOL
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Andriod'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
EOL

$CLICKHOUSE_CLIENT -n --query="
drop dictionary if exists regexp_dict1;
create dictionary regexp_dict1
(
    regexp String,
    name String,
    version Nullable(String) default 'default',
    lucky Int64
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '$yaml'))
LIFETIME(0)
LAYOUT(regexp_tree);

select dictGet('regexp_dict1', ('name', 'version'), 'Linux/123.45.67 tlinux');
select dictGet('regexp_dict1', ('name', 'version'), '31/tclwebkit1024');
select dictGet('regexp_dict1', ('name', 'version'), '999/tclwebkit1024');
select dictGet('regexp_dict1', ('name', 'version'), '28/tclwebkit1024');
"

cat > "$yaml" <<EOL
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Andriod'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
    - regexp: '28/tclwebkit'
      version:
      lucky: 'abcde'
EOL

$CLICKHOUSE_CLIENT -n --query="
system reload dictionary regexp_dict1; -- { serverError 489 }
"

cat > "$yaml" <<EOL
- regexp: 
  name: 'TencentOS'
  version: '\1'
EOL

$CLICKHOUSE_CLIENT -n --query="
system reload dictionary regexp_dict1; -- { serverError 318 }
"
cat > "$yaml" <<EOL
- regexp: 'abc'
  col_bool: 'true'
  col_uuid: '61f0c404-5cb3-11e7-907b-a6006ad3dba0'
  col_date: '2023-01-01'
  col_datetime: '2023-01-01 01:01:01'
  col_array: '[1,2,3,-1,-2,-3]'
EOL

$CLICKHOUSE_CLIENT -n --query="
create dictionary regexp_dict2
(
    regexp String,
    col_bool Boolean,
    col_uuid UUID,
    col_date Date,
    col_datetime DateTime,
    col_array Array(Int64)
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '$yaml'))
LIFETIME(0)
LAYOUT(regexp_tree);

select dictGet('regexp_dict2', ('col_bool','col_uuid', 'col_date', 'col_datetime', 'col_array'), 'abc');
"

$CLICKHOUSE_CLIENT -n --query="
drop dictionary regexp_dict1;
drop dictionary regexp_dict2;
"

rm -rf "$USER_FILES_PATH/test_02504"
