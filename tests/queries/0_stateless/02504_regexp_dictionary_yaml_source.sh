#!/usr/bin/env bash
# Tags: use-vectorscan, no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p $USER_FILES_PATH/test_02504

yaml=$USER_FILES_PATH/test_02504/test.yaml

cat > "$yaml" <<EOL
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
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

$CLICKHOUSE_CLIENT --query="
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
LAYOUT(regexp_tree)
SETTINGS(regexp_dict_allow_hyperscan = true);

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
  name: 'Android'
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

$CLICKHOUSE_CLIENT --query="
system reload dictionary regexp_dict1; -- { serverError 489 }
"

cat > "$yaml" <<EOL
- regexp:
  name: 'TencentOS'
  version: '\1'
EOL

$CLICKHOUSE_CLIENT --query="
system reload dictionary regexp_dict1; -- { serverError 318 }
"

cat > "$yaml" <<EOL
- name: BlackBerry WebKit
  regexp: (PlayBook).{1,200}RIM Tablet OS (\d+)\.(\d+)\.(\d+)
  version: '\2.\3'
- name: BlackBerry WebKit
  regexp: (Black[bB]erry|BB10).{1,200}Version/(\d+)\.(\d+)\.(\d+)
  version: '\2.\3'
EOL

$CLICKHOUSE_CLIENT --query="
system reload dictionary regexp_dict1;
select dictGet('regexp_dict1', ('name', 'version'), 'Mozilla/5.0 (BB10; Touch) AppleWebKit/537.3+ (KHTML, like Gecko) Version/10.0.9.388 Mobile Safari/537.3+');
select dictGet('regexp_dict1', ('name', 'version'), 'Mozilla/5.0 (PlayBook; U; RIM Tablet OS 1.0.0; en-US) AppleWebKit/534.8+ (KHTML, like Gecko) Version/0.0.1 Safari/534.8+');
"

cat > "$yaml" <<EOL
- regexp: 'abc'
  col_bool: 'true'
  col_uuid: '61f0c404-5cb3-11e7-907b-a6006ad3dba0'
  col_date: '2023-01-01'
  col_datetime: '2023-01-01 01:01:01'
  col_array: '[1,2,3,-1,-2,-3]'
EOL

$CLICKHOUSE_CLIENT --query="
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

cat > "$yaml" <<EOL
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
EOL

# dictGetAll
$CLICKHOUSE_CLIENT --query="
drop dictionary if exists regexp_dict3;
create dictionary regexp_dict3
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '$yaml'))
LIFETIME(0)
LAYOUT(regexp_tree);

select dictGetAll('regexp_dict3', ('tag', 'topological_index', 'captured', 'parent'), 'clickhouse.com');
select dictGetAll('regexp_dict3', ('tag', 'topological_index', 'captured', 'parent'), 'clickhouse.com', 2);

select dictGetAll('regexp_dict3', ('tag', 'topological_index', 'captured', 'parent'), 'clickhouse.com/docs/en');
select dictGetAll('regexp_dict3', ('tag', 'topological_index', 'captured', 'parent'), 'clickhouse.com/docs/en', 2);

select dictGetAll('regexp_dict3', ('tag', 'topological_index', 'captured', 'parent'), 'github.com/clickhouse/tree/master/docs');
select dictGetAll('regexp_dict3', ('tag', 'topological_index', 'captured', 'parent'), 'github.com/clickhouse/tree/master/docs', 2);
"

# Test that things work the same for "simple" regexps that go through Hyperscan and "complex" regexps that go through RE2.
# An easy way to force the use of RE2 is to disable Hyperscan.
# This tree is constructed purposely so that text might (falsely) match leaf nodes without matching their corresponding parent nodes
cat > "$yaml" <<EOL
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  paths:
    - regexp: 'docs'
      tag: 'ClickHouse Documentation'

- regexp: 'github\.com'
  tag: 'GitHub'
  paths:
    - regexp: 'docs'
      tag: 'GitHub Documentation'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
EOL

$CLICKHOUSE_CLIENT --query="
drop dictionary if exists regexp_dict3;
create dictionary regexp_dict3
(
    regexp String,
    tag String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '$yaml'))
LIFETIME(0)
LAYOUT(regexp_tree)
SETTINGS(regexp_dict_allow_hyperscan = true);

select dictGet('regexp_dict3', 'tag', 'clickhouse.com');
select dictGetAll('regexp_dict3', 'tag', 'clickhouse.com');
select dictGet('regexp_dict3', 'tag', 'clickhouse.com/docs');
select dictGetAll('regexp_dict3', 'tag', 'clickhouse.com/docs');
select dictGet('regexp_dict3', 'tag', 'docs.github.com');
select dictGetAll('regexp_dict3', 'tag', 'docs.github.com');
select dictGet('regexp_dict3', 'tag', '/docs');
select dictGetAll('regexp_dict3', 'tag', '/docs');

drop dictionary if exists regexp_dict3;
create dictionary regexp_dict3
(
    regexp String,
    tag String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '$yaml'))
LIFETIME(0)
LAYOUT(regexp_tree)
SETTINGS(regexp_dict_allow_hyperscan = false);

select dictGet('regexp_dict3', 'tag', 'clickhouse.com');
select dictGetAll('regexp_dict3', 'tag', 'clickhouse.com');
select dictGet('regexp_dict3', 'tag', 'clickhouse.com/docs');
select dictGetAll('regexp_dict3', 'tag', 'clickhouse.com/docs');
select dictGet('regexp_dict3', 'tag', 'docs.github.com');
select dictGetAll('regexp_dict3', 'tag', 'docs.github.com');
select dictGet('regexp_dict3', 'tag', '/docs');
select dictGetAll('regexp_dict3', 'tag', '/docs');
"

# Test case-insensitive and dot-all match modes
cat > "$yaml" <<EOL
- regexp: 'foo'
  pattern: 'foo'
- regexp: '(?i)foo'
  pattern: '(?i)foo'
- regexp: '(?-i)foo'
  pattern: '(?-i)foo'
- regexp: 'hello.*world'
  pattern: 'hello.*world'
- regexp: '(?i)hello.*world'
  pattern: '(?i)hello.*world'
- regexp: '(?-i)hello.*world'
  pattern: '(?-i)hello.*world'
EOL

$CLICKHOUSE_CLIENT --query="
drop dictionary if exists regexp_dict4;
create dictionary regexp_dict4
(
    regexp String,
    pattern String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '$yaml'))
LIFETIME(0)
LAYOUT(regexp_tree);

select dictGetAll('regexp_dict4', 'pattern', 'foo');
select dictGetAll('regexp_dict4', 'pattern', 'FOO');
select dictGetAll('regexp_dict4', 'pattern', 'hello world');
select dictGetAll('regexp_dict4', 'pattern', 'hello\nworld');
select dictGetAll('regexp_dict4', 'pattern', 'HELLO WORLD');
select dictGetAll('regexp_dict4', 'pattern', 'HELLO\nWORLD');

drop dictionary if exists regexp_dict4;
create dictionary regexp_dict4
(
    regexp String,
    pattern String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '$yaml'))
LIFETIME(0)
LAYOUT(regexp_tree)
SETTINGS(regexp_dict_flag_case_insensitive = true, regexp_dict_flag_dotall = true);

select dictGetAll('regexp_dict4', 'pattern', 'foo');
select dictGetAll('regexp_dict4', 'pattern', 'FOO');
select dictGetAll('regexp_dict4', 'pattern', 'hello world');
select dictGetAll('regexp_dict4', 'pattern', 'hello\nworld');
select dictGetAll('regexp_dict4', 'pattern', 'HELLO WORLD');
select dictGetAll('regexp_dict4', 'pattern', 'HELLO\nWORLD');
"

$CLICKHOUSE_CLIENT --query="
drop dictionary regexp_dict1;
drop dictionary regexp_dict2;
drop dictionary regexp_dict3;
drop dictionary regexp_dict4;
"

rm -rf "$USER_FILES_PATH/test_02504"
