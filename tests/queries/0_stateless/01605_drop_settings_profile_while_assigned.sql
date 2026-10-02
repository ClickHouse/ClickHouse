CREATE USER OR REPLACE 'test_01605';
CREATE SETTINGS PROFILE OR REPLACE 'test_01605';
ALTER USER 'test_01605' SETTINGS PROFILE 'test_01605';
SELECT * FROM system.settings_profile_elements WHERE user_name='test_01605' OR profile_name='test_01605';
DROP SETTINGS PROFILE 'test_01605';
SELECT 'PROFILE DROPPED';
SELECT * FROM system.settings_profile_elements WHERE user_name='test_01605' OR profile_name='test_01605';
DROP USER 'test_01605';
