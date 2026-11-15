-- `allow_experimental_delta_kernel_rs` was renamed to `allow_delta_kernel_rs` and kept as an alias,
-- so both names must address the same setting.

SELECT name, alias_for FROM system.settings
WHERE name IN ('allow_delta_kernel_rs', 'allow_experimental_delta_kernel_rs')
ORDER BY name;

-- Setting the alias changes the setting it points to.
SET allow_experimental_delta_kernel_rs = 0;
SELECT getSetting('allow_delta_kernel_rs'), getSetting('allow_experimental_delta_kernel_rs');
SELECT value FROM system.settings WHERE name = 'allow_delta_kernel_rs';

-- And the other way round.
SET allow_delta_kernel_rs = 1;
SELECT getSetting('allow_delta_kernel_rs'), getSetting('allow_experimental_delta_kernel_rs');
SELECT value FROM system.settings WHERE name = 'allow_delta_kernel_rs';
