#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p ${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}

cp $CURDIR/data_ua_parser/os.yaml ${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/
cp $CURDIR/data_ua_parser/browser.yaml ${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/
cp $CURDIR/data_ua_parser/device.yaml ${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/

$CLICKHOUSE_CLIENT --query="
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
SOURCE(YAMLRegExpTree(PATH '${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/os.yaml'))
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
SOURCE(YAMLRegExpTree(PATH '${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/browser.yaml'))
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
SOURCE(YAMLRegExpTree(PATH '${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/device.yaml'))
LIFETIME(0)
LAYOUT(regexp_tree);

create table user_agents
(
    ua String
)
Engine = Log();
"

$CLICKHOUSE_CLIENT --query="
insert into user_agents select ua from input('ua String') FORMAT LineAsString" < $CURDIR/data_ua_parser/useragents.txt

$CLICKHOUSE_CLIENT --query="
select ua, device,
concat(tupleElement(browser, 1), ' ', tupleElement(browser, 2), '.', tupleElement(browser, 3)) as browser ,
concat(tupleElement(os, 1), ' ', tupleElement(os, 2), '.', tupleElement(os, 3), '.', tupleElement(os, 4)) as os
from (
     select ua, dictGet('regexp_os', ('os_replacement', 'os_v1_replacement', 'os_v2_replacement', 'os_v3_replacement'), ua) os,
     dictGet('regexp_browser', ('family_replacement', 'v1_replacement', 'v2_replacement'), ua) as browser,
     dictGet('regexp_device', 'device_replacement', ua) device from user_agents) order by ua;
"

$CLICKHOUSE_CLIENT --query="
drop dictionary if exists regexp_os;
drop dictionary if exists regexp_browser;
drop dictionary if exists regexp_device;
drop table if exists user_agents;
"

rm -rf ${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}
