SET custom_a = 'value_a';
SET custom_b = 'value_b';
SET custom_c = null;
SET custom_d = 5;

SELECT getSettingOrDefault('custom_a', 'default_a');
SELECT getSettingOrDefault('custom_b', 'default_b');
SELECT getSettingOrDefault('custom_c', 'default_c');
SELECT getSettingOrDefault('custom_d', 'default_d');

SELECT getSetting('custom_e');  -- { serverError UNKNOWN_SETTING }

SELECT getSettingOrDefault('custom_e', 'default_e');
SELECT getSettingOrDefault('custom_e', 500);
SELECT getSettingOrDefault('custom_e', null);
SELECT isNull(getSettingOrDefault('custom_e', null));

SELECT getSettingOrDefault('custom_e'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT getSettingOrDefault(115, 'name should be string');  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count(*) FROM numbers(10) WHERE number = getSettingOrDefault('custom_e', 5);

SET custom_e_backup = 'backup';
SELECT getSettingOrDefault('custom_e', getSetting('custom_e_backup'));
