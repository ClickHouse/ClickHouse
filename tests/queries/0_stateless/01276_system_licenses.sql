SELECT count() > 10 FROM system.licenses;
SELECT library_name, license_type, license_path FROM system.licenses WHERE library_name = 'abseil-cpp';
