SELECT metric
FROM system.metrics
WHERE metric IN ('SettingsObjects', 'SettingsImplementations', 'SettingsDenseData', 'SettingsSnapshotStates', 'SettingsSnapshotChunks', 'SettingsStructuralMemoryBytes')
ORDER BY metric;

SELECT countIf(value < 0)
FROM system.metrics
WHERE metric IN ('SettingsObjects', 'SettingsImplementations', 'SettingsDenseData', 'SettingsSnapshotStates', 'SettingsSnapshotChunks', 'SettingsStructuralMemoryBytes');
