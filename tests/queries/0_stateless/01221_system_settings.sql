select * from system.settings where name = 'send_timeout';
select * from system.merge_tree_settings order by length(description) limit 1;

with [
    'SettingSeconds',
    'SettingBool',
    'SettingInt64',
    'SettingString',
    'SettingChar',
    'SettingLogsLevel',
    'SettingURI',
    'SettingFloat',
    'SettingUInt64',
    'SettingMaxThreads',
    'SettingMilliseconds',
    'SettingJoinStrictness',
    'SettingJoinAlgorithm',
    'SettingOverflowMode',
    'SettingTotalsMode',
    'SettingLoadBalancing',
    'SettingOverflowModeGroupBy',
    'SettingDateTimeInputFormat',
    'SettingDistributedProductMode'
] as types select hasAll(arrayDistinct(groupArray(type)), types) from system.settings;

with [
    'SettingSeconds',
    'SettingBool',
    'SettingInt64',
    'SettingString',
    'SettingFloat',
    'SettingUInt64',
    'SettingMaxThreads'
] as types select hasAll(arrayDistinct(groupArray(type)), types) from system.merge_tree_settings;
