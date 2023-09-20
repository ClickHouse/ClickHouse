#!/usr/bin/env bash

# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p $user_files_path/test_02504

cp $CURDIR/data_ua_parser/os.yaml ${user_files_path}/test_02504/
cp $CURDIR/data_ua_parser/browser.yaml ${user_files_path}/test_02504/
cp $CURDIR/data_ua_parser/device.yaml ${user_files_path}/test_02504/

$CLICKHOUSE_CLIENT -n --query="
drop dictionary if exists regexp_os;
drop dictionary if exists regexp_browser;
drop dictionary if exists regexp_device;
drop table if exists user_agents;
create dictionary regexp_os
(
    regex String,
    os_replacement String default 'Other',
    os_v1_replacement String default '0',
    os_v2_replacement String default '0',
    os_v3_replacement String default '0',
    os_v4_replacement String default '0'
)
PRIMARY KEY(regex)
SOURCE(YAMLRegExpTree(PATH '${user_files_path}/test_02504/os.yaml'))
LIFETIME(0)
LAYOUT(regexp_tree);

create dictionary regexp_browser
(
    regex String,
    family_replacement String default 'Other',
    v1_replacement String default '0',
    v2_replacement String default '0'
)
PRIMARY KEY(regex)
SOURCE(YAMLRegExpTree(PATH '${user_files_path}/test_02504/browser.yaml'))
LIFETIME(0)
LAYOUT(regexp_tree);

create dictionary regexp_device
(
    regex String,
    device_replacement String default 'Other',
    brand_replacement String,
    model_replacement String
)
PRIMARY KEY(regex)
SOURCE(YAMLRegExpTree(PATH '${user_files_path}/test_02504/device.yaml'))
LIFETIME(0)
LAYOUT(regexp_tree);

create table user_agents
(
    ua String
)
Engine = Log();
"

$CLICKHOUSE_CLIENT -n --query="
insert into user_agents select ua from input('ua String') FORMAT LineAsString" < $CURDIR/data_ua_parser/useragents.txt

$CLICKHOUSE_CLIENT -n --query="
select ua, device,
concat(tupleElement(browser, 1), ' ', tupleElement(browser, 2), '.', tupleElement(browser, 3)) as browser ,
concat(tupleElement(os, 1), ' ', tupleElement(os, 2), '.', tupleElement(os, 3), '.', tupleElement(os, 4)) as os
from (
     select ua, dictGet('regexp_os', ('os_replacement', 'os_v1_replacement', 'os_v2_replacement', 'os_v3_replacement'), ua) os,
     dictGet('regexp_browser', ('family_replacement', 'v1_replacement', 'v2_replacement'), ua) as browser,
     dictGet('regexp_device', 'device_replacement', ua) device from user_agents) order by ua;
"

$CLICKHOUSE_CLIENT -n --query="
drop dictionary if exists regexp_os;
drop dictionary if exists regexp_browser;
drop dictionary if exists regexp_device;
drop table if exists user_agents;
"

rm -rf "$user_files_path/test_02504"
