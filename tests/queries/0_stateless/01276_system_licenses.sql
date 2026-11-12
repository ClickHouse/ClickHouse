SELECT count() > 10 FROM system.licenses;
SELECT library_name, license_type, license_path FROM system.licenses WHERE library_name = 'abseil-cpp';
SELECT library_name, license_type, license_path FROM system.licenses WHERE library_name IN ('libxml2', 'pdqsort') ORDER BY library_name;
