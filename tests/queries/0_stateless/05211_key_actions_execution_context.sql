DROP TABLE IF EXISTS key_actions_current_settings;
SET short_circuit_function_evaluation = 'disable';
CREATE TABLE key_actions_current_settings (n UInt64)
ENGINE = MergeTree ORDER BY if(n = 0, 0, intDiv(1, n));

ALTER TABLE key_actions_current_settings COMMENT COLUMN n 'Rebuild key metadata';

-- Key metadata settings must not replace the storage execution context.
INSERT INTO key_actions_current_settings SETTINGS short_circuit_function_evaluation = 'force_enable' VALUES (0), (1);
SELECT n FROM key_actions_current_settings ORDER BY n;
DROP TABLE key_actions_current_settings;
